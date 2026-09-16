package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sivchari/kumo/internal/service"
	"github.com/sivchari/kumo/internal/service/eventbridge"
	"github.com/sivchari/kumo/internal/service/lambda"
	"github.com/sivchari/kumo/internal/service/s3"
	"github.com/sivchari/kumo/internal/service/sns"
	"github.com/sivchari/kumo/internal/service/sqs"
)

const drainTestBaseURL = "http://localhost:4566"

// newIsolatedServer builds a Server with fresh, test-local instances of the
// messaging services, so shutdown tests never touch the process-global
// service singletons (and never double-close them across tests).
func newIsolatedServer(t *testing.T) *Server {
	t.Helper()

	logger := discardLogger()
	registry := service.NewRegistry()
	router := NewRouter(logger)

	srv := &Server{
		config:          DefaultConfig(),
		router:          router,
		registry:        registry,
		jsonDispatcher:  NewJSONProtocolDispatcher(),
		queryDispatcher: NewQueryProtocolDispatcher(),
		cborDispatcher:  NewCBORProtocolDispatcher(),
		logger:          logger,
	}

	srv.RegisterService(sqs.New(sqs.NewMemoryStorage(drainTestBaseURL), drainTestBaseURL))
	srv.RegisterService(sns.New(sns.NewMemoryStorage(drainTestBaseURL)))
	srv.RegisterService(lambda.New(lambda.NewMemoryStorage(drainTestBaseURL), drainTestBaseURL))
	srv.RegisterService(s3.New(s3.NewMemoryStorage(), drainTestBaseURL))

	internal := &inProcessDoer{handler: router}

	wireSNStoSQS(registry)
	wireS3toSQS(registry)
	wireS3toLambda(registry, internal)
	wireS3toSNS(registry)
	wireS3EventBridgeInProcess(registry, internal)

	router.HandleFunc("POST", "/", srv.unifiedDispatcher)

	return srv
}

func isolatedS3(t *testing.T, srv *Server) *s3.Service {
	t.Helper()

	svc, ok := srv.Registry().Get("s3")
	if !ok {
		t.Fatal("s3 service not registered")
	}

	s3svc, ok := svc.(*s3.Service)
	if !ok {
		t.Fatalf("s3 service has unexpected type %T", svc)
	}

	return s3svc
}

// drainGate is a blocking stand-in for every S3 notification target (SQS,
// SNS, Lambda publishers and the EventBridge HTTP client). Deliveries park
// on release and report cancellation, so a test can observe the full drain
// lifecycle.
type drainGate struct {
	release      <-chan struct{}
	started      chan string
	canceled     chan string
	doerStarted  chan struct{}
	doerCanceled chan struct{}
}

func newDrainGate(release <-chan struct{}) *drainGate {
	return &drainGate{
		release:      release,
		started:      make(chan string, 16),
		canceled:     make(chan string, 16),
		doerStarted:  make(chan struct{}, 16),
		doerCanceled: make(chan struct{}, 16),
	}
}

func (g *drainGate) install(s3svc *s3.Service) {
	s3svc.SetSQSPublisher(g)
	s3svc.SetSNSPublisher(g)
	s3svc.SetLambdaInvoker(g)
	s3svc.SetEventBridgeClient(g)
}

func (g *drainGate) wait(ctx context.Context, name string) error {
	g.started <- name

	select {
	case <-g.release:
		return nil
	case <-ctx.Done():
		g.canceled <- name

		return fmt.Errorf("%s notification delivery canceled: %w", name, ctx.Err())
	}
}

func (g *drainGate) PublishToSQS(ctx context.Context, _, _ string) error {
	return g.wait(ctx, "sqs")
}

func (g *drainGate) Publish(ctx context.Context, _, _, _ string) error {
	return g.wait(ctx, "sns")
}

func (g *drainGate) InvokeAsync(ctx context.Context, _ string, _ []byte) error {
	return g.wait(ctx, "lambda")
}

func (g *drainGate) Do(req *http.Request) (*http.Response, error) {
	g.doerStarted <- struct{}{}

	select {
	case <-g.release:
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(bytes.NewReader(nil)),
		}, nil
	case <-req.Context().Done():
		g.doerCanceled <- struct{}{}

		return nil, fmt.Errorf("eventbridge notification delivery canceled: %w", req.Context().Err())
	}
}

// configureDrainBucket creates a bucket with one suffix-filtered
// configuration per publisher target and EventBridge enabled.
func configureDrainBucket(t *testing.T, s3svc *s3.Service, bucket string) {
	t.Helper()

	store := s3svc.Storage()

	if err := store.CreateBucket(context.Background(), bucket); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	body := `<NotificationConfiguration>` +
		`<QueueConfiguration><Id>q</Id><Queue>arn:aws:sqs:us-east-1:000000000000:drain</Queue>` +
		`<Event>s3:ObjectCreated:*</Event>` +
		`<Filter><S3Key><FilterRule><Name>suffix</Name><Value>.sqs</Value></FilterRule></S3Key></Filter>` +
		`</QueueConfiguration>` +
		`<TopicConfiguration><Id>n</Id><Topic>arn:aws:sns:us-east-1:000000000000:drain</Topic>` +
		`<Event>s3:ObjectCreated:*</Event>` +
		`<Filter><S3Key><FilterRule><Name>suffix</Name><Value>.sns</Value></FilterRule></S3Key></Filter>` +
		`</TopicConfiguration>` +
		`<CloudFunctionConfiguration><Id>l</Id><CloudFunction>arn:aws:lambda:us-east-1:00000000000:function:drain</CloudFunction>` +
		`<Event>s3:ObjectCreated:*</Event>` +
		`<Filter><S3Key><FilterRule><Name>suffix</Name><Value>.lambda</Value></FilterRule></S3Key></Filter>` +
		`</CloudFunctionConfiguration>` +
		`</NotificationConfiguration>`

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPut, "/"+bucket+"?notification", strings.NewReader(body))
	req.SetPathValue("bucket", bucket)

	rec := httptest.NewRecorder()
	s3svc.PutBucketNotificationConfiguration(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("PutBucketNotificationConfiguration status: got %d, want %d (body=%s)", rec.Code, http.StatusOK, rec.Body.String())
	}

	store.SetEventBridgeNotification(context.Background(), bucket, true)
}

func putObjectThroughHandler(t *testing.T, s3svc *s3.Service, bucket, key string) {
	t.Helper()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPut, "/"+bucket+"/"+key, strings.NewReader("data"))
	req.SetPathValue("bucket", bucket)
	req.SetPathValue("key", key)

	rec := httptest.NewRecorder()
	s3svc.PutObject(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("PutObject(%s) status: got %d, want %d", key, rec.Code, http.StatusOK)
	}
}

func waitGateStarted(t *testing.T, g *drainGate, want int) {
	t.Helper()

	started := make(map[string]bool)
	deadline := time.After(2 * time.Second)

	for len(started) < want {
		select {
		case name := <-g.started:
			started[name] = true
		case <-g.doerStarted:
			started["eventbridge"] = true
		case <-deadline:
			t.Fatalf("only %d/%d targets started: %v", len(started), want, started)
		}
	}
}

// TestServerShutdown_DrainsAcceptedNotifications is the end-to-end version
// of the drain contract: after the HTTP server stops, a blocked delivery
// that recovers within the deadline keeps its targets available and
// Shutdown waits for every accepted notification before closing services.
func TestServerShutdown_DrainsAcceptedNotifications(t *testing.T) {
	srv := newIsolatedServer(t)
	s3svc := isolatedS3(t, srv)

	const bucket = "server-drain-recovers"

	release := make(chan struct{})
	gate := newDrainGate(release)
	gate.install(s3svc)

	configureDrainBucket(t, s3svc, bucket)

	// An http.Server that has never served: Shutdown returns nil at once,
	// exercising only the drain + close ordering under test.
	srv.server = &http.Server{Handler: srv.Handler(), ReadHeaderTimeout: time.Second}

	for _, key := range []string{"hit.sqs", "hit.sns", "hit.lambda"} {
		putObjectThroughHandler(t, s3svc, bucket, key)
	}

	waitGateStarted(t, gate, 4)

	go func() {
		time.Sleep(100 * time.Millisecond)
		close(release)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()

	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() = %v, want nil after blocked targets recovered", err)
	}

	if elapsed := time.Since(start); elapsed < 80*time.Millisecond {
		t.Fatalf("Shutdown returned after %v, want it to wait for the drain", elapsed)
	}
}

// TestServerShutdown_DrainDeadlineReturnsDeadlineExceeded verifies the
// deadline path through the real Server: the recognizable context error is
// returned, in-flight deliveries are canceled, and services are still
// closed afterwards.
func TestServerShutdown_DrainDeadlineReturnsDeadlineExceeded(t *testing.T) {
	srv := newIsolatedServer(t)
	s3svc := isolatedS3(t, srv)

	const bucket = "server-drain-deadline"

	gate := newDrainGate(make(chan struct{})) // never released
	gate.install(s3svc)

	configureDrainBucket(t, s3svc, bucket)

	srv.server = &http.Server{Handler: srv.Handler(), ReadHeaderTimeout: time.Second}

	for _, key := range []string{"hit.sqs", "hit.sns", "hit.lambda"} {
		putObjectThroughHandler(t, s3svc, bucket, key)
	}

	waitGateStarted(t, gate, 4)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := srv.Shutdown(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown() = %v, want context.DeadlineExceeded", err)
	}

	canceled := make(map[string]bool)

	for range 3 {
		select {
		case name := <-gate.canceled:
			canceled[name] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("only %v targets canceled, want all publishers", canceled)
		}
	}

	for _, want := range []string{"sqs", "sns", "lambda"} {
		if !canceled[want] {
			t.Fatalf("canceled targets = %v, missing %q", canceled, want)
		}
	}

	select {
	case <-gate.doerCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("EventBridge delivery was not canceled")
	}
}

// TestServerShutdown_EventBridgeStaysAvailableAfterListenerClosed verifies
// the explicit requirement that EventBridge stays a usable target during the
// drain even though no network listener exists: the S3 Object Created event
// is delivered in-process into EventBridge, matches a rule, and is recorded
// with its payload intact.
func TestServerShutdown_EventBridgeStaysAvailableAfterListenerClosed(t *testing.T) {
	srv := newIsolatedServer(t)

	eventsStore := eventbridge.NewMemoryStorage(eventbridge.WithBaseURL(drainTestBaseURL))
	eventsSvc := eventbridge.New(eventsStore)
	srv.RegisterService(eventsSvc)

	ctx := context.Background()

	if _, err := eventsStore.PutRule(ctx, &eventbridge.PutRuleRequest{
		Name:         "s3-object-created",
		EventPattern: `{"source":["aws.s3"],"detail-type":["Object Created"]}`,
	}); err != nil {
		t.Fatalf("PutRule: %v", err)
	}

	// A Step Functions-style ARN is neither an SQS/Lambda ARN nor an API
	// destination, so the rule records delivery without spawning external
	// HTTP calls.
	if _, err := eventsStore.PutTargets(ctx, "", "s3-object-created", []eventbridge.TargetInput{{
		ID:  "states-target",
		Arn: "arn:aws:states:us-east-1:000000000000:stateMachine:drain",
	}}); err != nil {
		t.Fatalf("PutTargets: %v", err)
	}

	s3svc := isolatedS3(t, srv)

	const bucket = "server-drain-eventbridge"

	if err := s3svc.Storage().CreateBucket(ctx, bucket); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	s3svc.Storage().SetEventBridgeNotification(ctx, bucket, true)

	srv.server = &http.Server{Handler: srv.Handler(), ReadHeaderTimeout: time.Second}

	putObjectThroughHandler(t, s3svc, bucket, "hello.txt")

	drainCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(drainCtx); err != nil {
		t.Fatalf("Shutdown() = %v, want nil", err)
	}

	getReq := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/kumo/eventbridge/delivered-events", http.NoBody)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, getReq)

	if rec.Code != http.StatusOK {
		t.Fatalf("GetDeliveredEvents status: got %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()

	for _, want := range []string{`"Source":"aws.s3"`, `"DetailType":"Object Created"`, bucket, "hello.txt"} {
		if !strings.Contains(body, want) {
			t.Fatalf("delivered events body missing %q:\n%s", want, body)
		}
	}
}

// TestS3ToLambdaInvoker_InProcessRouting verifies the in-process doer wired
// into the Lambda adapter still surfaces handler-level status codes: a
// missing function is a 404 from the Lambda route and therefore an error,
// with no listener involved.
func TestS3ToLambdaInvoker_InProcessRouting(t *testing.T) {
	srv := newIsolatedServer(t)

	inv := &s3ToLambdaInvoker{
		baseURL: drainTestBaseURL,
		doer:    &inProcessDoer{handler: srv.Handler()},
	}

	err := inv.InvokeAsync(context.Background(), "arn:aws:lambda:us-east-1:00000000000:function:missing", []byte(`{}`))
	if err == nil {
		t.Fatal("InvokeAsync() = nil, want an error for a missing function")
	}

	if !strings.Contains(err.Error(), "404") {
		t.Fatalf("InvokeAsync() error = %q, want it to contain 404", err.Error())
	}
}
