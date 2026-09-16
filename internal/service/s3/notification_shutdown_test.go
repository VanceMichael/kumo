package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

const drainTestQueueARN = "arn:aws:sqs:us-east-1:000000000000:drain-queue"

// gateNotifier is a controllable blocking stand-in for the SQS, SNS and
// Lambda notification targets. Every delivery parks on release and reports
// which lifecycle phase it reached, so tests can prove the drain waits for
// blocked targets, cancels them on deadline, and observes their completion.
type gateNotifier struct {
	started   chan string
	canceled  chan string
	completed chan string
	release   <-chan struct{}
	fail      map[string]bool
}

func newGateNotifier(release <-chan struct{}) *gateNotifier {
	return &gateNotifier{
		started:   make(chan string, 16),
		canceled:  make(chan string, 16),
		completed: make(chan string, 16),
		release:   release,
		fail:      make(map[string]bool),
	}
}

func (g *gateNotifier) wait(ctx context.Context, name string) error {
	g.started <- name

	select {
	case <-g.release:
	case <-ctx.Done():
		g.canceled <- name

		return fmt.Errorf("%s notification delivery canceled: %w", name, ctx.Err())
	}

	if g.fail[name] {
		return errors.New(name + " forced delivery failure")
	}

	g.completed <- name

	return nil
}

func (g *gateNotifier) PublishToSQS(ctx context.Context, _, _ string) error {
	return g.wait(ctx, "sqs")
}

func (g *gateNotifier) Publish(ctx context.Context, _, _, _ string) error {
	return g.wait(ctx, "sns")
}

func (g *gateNotifier) InvokeAsync(ctx context.Context, _ string, _ []byte) error {
	return g.wait(ctx, "lambda")
}

// gateDoer is the controllable blocking stand-in for the EventBridge HTTP
// target.
type gateDoer struct {
	started   chan struct{}
	canceled  chan struct{}
	completed chan struct{}
	release   <-chan struct{}
	fail      bool
}

func newGateDoer(release <-chan struct{}) *gateDoer {
	return &gateDoer{
		started:   make(chan struct{}, 16),
		canceled:  make(chan struct{}, 16),
		completed: make(chan struct{}, 16),
		release:   release,
	}
}

func (d *gateDoer) Do(req *http.Request) (*http.Response, error) {
	d.started <- struct{}{}

	select {
	case <-d.release:
	case <-req.Context().Done():
		d.canceled <- struct{}{}

		return nil, fmt.Errorf("eventbridge notification delivery canceled: %w", req.Context().Err())
	}

	if d.fail {
		return nil, errors.New("eventbridge forced delivery failure")
	}

	d.completed <- struct{}{}

	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(nil)),
	}, nil
}

// drainFixture bundles a fully wired S3 service with its blocking target
// fakes for the shutdown tests.
type drainFixture struct {
	store    *MemoryStorage
	svc      *Service
	notifier *gateNotifier
	doer     *gateDoer
	release  chan struct{}
}

// newDrainFixture builds an S3 service with one configuration of every
// target type matching s3:ObjectCreated:* plus EventBridge enabled, all
// routed through controllable gate fakes.
func newDrainFixture(t *testing.T, bucket string) *drainFixture {
	t.Helper()

	store := NewMemoryStorage()
	svc := New(store, "")

	if err := store.CreateBucket(context.Background(), bucket); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	// Distinct suffix filters keep the three configurations unambiguous
	// under the overlap validation (filterless same-event configs are
	// rejected), so three differently-suffixed writes fan out to all
	// three publisher targets.
	body := `<NotificationConfiguration>` +
		`<QueueConfiguration><Id>drain-q</Id><Queue>` + drainTestQueueARN + `</Queue>` +
		`<Event>s3:ObjectCreated:*</Event>` +
		`<Filter><S3Key><FilterRule><Name>suffix</Name><Value>.sqs</Value></FilterRule></S3Key></Filter>` +
		`</QueueConfiguration>` +
		`<TopicConfiguration><Id>drain-t</Id><Topic>` + testTopicArn + `</Topic>` +
		`<Event>s3:ObjectCreated:*</Event>` +
		`<Filter><S3Key><FilterRule><Name>suffix</Name><Value>.sns</Value></FilterRule></S3Key></Filter>` +
		`</TopicConfiguration>` +
		`<CloudFunctionConfiguration><Id>drain-l</Id><CloudFunction>` + testLambdaArn + `</CloudFunction>` +
		`<Event>s3:ObjectCreated:*</Event>` +
		`<Filter><S3Key><FilterRule><Name>suffix</Name><Value>.lambda</Value></FilterRule></S3Key></Filter>` +
		`</CloudFunctionConfiguration>` +
		`</NotificationConfiguration>`

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPut, "/"+bucket+"?notification", strings.NewReader(body))
	req.SetPathValue("bucket", bucket)

	w := httptest.NewRecorder()
	svc.PutBucketNotificationConfiguration(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("PutBucketNotificationConfiguration status: got %d, want %d (body=%s)", w.Code, http.StatusOK, w.Body.String())
	}

	store.SetEventBridgeNotification(context.Background(), bucket, true)

	release := make(chan struct{})
	notifier := newGateNotifier(release)
	doer := newGateDoer(release)

	svc.SetSQSPublisher(notifier)
	svc.SetSNSPublisher(notifier)
	svc.SetLambdaInvoker(notifier)
	svc.SetEventBridgeClient(doer)

	return &drainFixture{
		store:    store,
		svc:      svc,
		notifier: notifier,
		doer:     doer,
		release:  release,
	}
}

// waitTargetsStarted waits until the expected number of target deliveries
// (the three named publisher targets plus EventBridge) have parked.
func waitTargetsStarted(t *testing.T, n *gateNotifier, d *gateDoer, want int) {
	t.Helper()

	deadline := time.After(2 * time.Second)
	started := make(map[string]bool)

	for len(started) < want {
		select {
		case name := <-n.started:
			started[name] = true
		case <-d.started:
			started["eventbridge"] = true
		case <-deadline:
			t.Fatalf("only %d/%d targets started delivery: %v", len(started), want, started)
		}
	}
}

// putAllTargetTriggerObjects performs one successful PutObject per
// publisher-target suffix so the SQS, SNS, Lambda targets each accept one
// delivery (EventBridge accepts one per write).
func putAllTargetTriggerObjects(t *testing.T, svc *Service, bucket string) {
	t.Helper()

	for _, key := range []string{"hit.sqs", "hit.sns", "hit.lambda"} {
		w := httptest.NewRecorder()
		svc.PutObject(w, putObjectRequest(t, bucket, key, "data"))

		if w.Code != http.StatusOK {
			t.Fatalf("PutObject(%s) status: got %d, want %d", key, w.Code, http.StatusOK)
		}
	}
}

// collectNames drains count signals from ch into a set.
func collectNames(t *testing.T, ch <-chan string, count int) map[string]bool {
	t.Helper()

	got := make(map[string]bool)

	for range count {
		select {
		case name := <-ch:
			got[name] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("only %v of %d signals received: %v", got, count, got)
		}
	}

	return got
}

// TestShutdownNotifications_BlockedTargetsRecoverBeforeDeadline verifies the
// core drain promise: deliveries parked when shutdown starts keep their
// targets available, and when the block clears within the deadline the
// drain waits for every delivery to finish and then returns nil.
func TestShutdownNotifications_BlockedTargetsRecoverBeforeDeadline(t *testing.T) {
	t.Parallel()

	const bucket = "drain-recovers"

	f := newDrainFixture(t, bucket)

	putAllTargetTriggerObjects(t, f.svc, bucket)

	waitTargetsStarted(t, f.notifier, f.doer, 4)

	go func() {
		time.Sleep(100 * time.Millisecond)
		close(f.release)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()

	if err := f.svc.ShutdownNotifications(ctx); err != nil {
		t.Fatalf("ShutdownNotifications() = %v, want nil after targets recovered", err)
	}

	if elapsed := time.Since(start); elapsed < 80*time.Millisecond {
		t.Fatalf("drain returned after %v, want it to wait for the blocked targets to recover", elapsed)
	}

	completed := collectNames(t, f.notifier.completed, 3)

	for _, want := range []string{"sqs", "sns", "lambda"} {
		if !completed[want] {
			t.Fatalf("completed targets = %v, missing %q", completed, want)
		}
	}

	select {
	case <-f.doer.completed:
	case <-time.After(2 * time.Second):
		t.Fatal("EventBridge target did not complete")
	}
}

// TestShutdownNotifications_DeadlineCancelsAndSettlesOnce verifies that an
// expired shutdown deadline cancels the in-flight deliveries, returns the
// recognizable context.DeadlineExceeded, waits for the goroutines to exit
// (no leak), and that a repeated shutdown reports the same settled result
// instead of settling the batch a second time.
func TestShutdownNotifications_DeadlineCancelsAndSettlesOnce(t *testing.T) {
	t.Parallel()

	const bucket = "drain-deadline"

	f := newDrainFixture(t, bucket)

	putAllTargetTriggerObjects(t, f.svc, bucket)

	waitTargetsStarted(t, f.notifier, f.doer, 4)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	err := f.svc.ShutdownNotifications(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ShutdownNotifications() = %v, want context.DeadlineExceeded", err)
	}

	// Every blocked delivery must observe cancellation, proving its
	// goroutine unwound instead of outliving the server.
	canceled := collectNames(t, f.notifier.canceled, 3)

	for _, want := range []string{"sqs", "sns", "lambda"} {
		if !canceled[want] {
			t.Fatalf("canceled targets = %v, missing %q", canceled, want)
		}
	}

	select {
	case <-f.doer.canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("EventBridge target was not canceled")
	}

	// Repeated shutdown must not re-settle: it returns the same stored
	// result immediately, regardless of the new caller's context.
	start := time.Now()

	err = f.svc.ShutdownNotifications(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("repeated ShutdownNotifications() = %v, want same context.DeadlineExceeded", err)
	}

	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("repeated ShutdownNotifications took %v, want the already-settled result", elapsed)
	}
}

// TestShutdownNotifications_ConcurrentCallsSettleOnce verifies that
// concurrent shutdown callers all observe a single successful settlement.
func TestShutdownNotifications_ConcurrentCallsSettleOnce(t *testing.T) {
	t.Parallel()

	const bucket = "drain-concurrent"

	f := newDrainFixture(t, bucket)
	close(f.release) // targets never block

	putAllTargetTriggerObjects(t, f.svc, bucket)

	const callers = 6

	var wg sync.WaitGroup

	errs := make(chan error, callers)

	for range callers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			errs <- f.svc.ShutdownNotifications(ctx)
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent ShutdownNotifications() = %v, want nil", err)
		}
	}

	if err := f.svc.ShutdownNotifications(context.Background()); err != nil {
		t.Fatalf("repeated ShutdownNotifications() = %v, want nil", err)
	}
}

// TestShutdownNotifications_TargetFailureDoesNotCancelOthers verifies that a
// single target failing neither cancels the other targets nor turns the
// drain (or the already-returned object write) into a failure.
func TestShutdownNotifications_TargetFailureDoesNotCancelOthers(t *testing.T) {
	t.Parallel()

	const bucket = "drain-target-failure"

	f := newDrainFixture(t, bucket)
	f.notifier.fail["sqs"] = true

	putAllTargetTriggerObjects(t, f.svc, bucket)

	waitTargetsStarted(t, f.notifier, f.doer, 4)
	close(f.release)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := f.svc.ShutdownNotifications(ctx); err != nil {
		t.Fatalf("ShutdownNotifications() = %v, want nil despite SQS failure", err)
	}

	completed := collectNames(t, f.notifier.completed, 2)
	if !completed["sns"] || !completed["lambda"] {
		t.Fatalf("completed targets = %v, want both sns and lambda unaffected by the SQS failure", completed)
	}

	select {
	case <-f.doer.completed:
	case <-time.After(2 * time.Second):
		t.Fatal("EventBridge target did not complete after a sibling target failed")
	}
}

// TestShutdownNotifications_FailedObjectWriteProducesNoWork verifies the
// existing "no event on write failure" contract also means there is nothing
// to drain: the drain returns immediately and no target is ever contacted.
func TestShutdownNotifications_FailedObjectWriteProducesNoWork(t *testing.T) {
	t.Parallel()

	f := newDrainFixture(t, "drain-configured-bucket")
	close(f.release)

	w := httptest.NewRecorder()
	f.svc.PutObject(w, putObjectRequest(t, "drain-no-such-bucket", "hello.txt", "hello world"))

	if w.Code == http.StatusOK {
		t.Fatal("PutObject status = 200, want a failure for a missing bucket")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := f.svc.ShutdownNotifications(ctx); err != nil {
		t.Fatalf("ShutdownNotifications() = %v, want nil with no accepted work", err)
	}

	select {
	case <-f.notifier.started:
		t.Fatal("unexpected delivery start after a failed object write")
	case <-f.doer.started:
		t.Fatal("unexpected EventBridge delivery start after a failed object write")
	case <-time.After(150 * time.Millisecond):
	}
}

// objectWriteFunc performs one of the four object-write operations against
// the given bucket, producing the key "hit.sqs" so the SQS suffix filter and
// EventBridge each accept a notification.
type objectWriteFunc func(t *testing.T, bucket string, store *MemoryStorage, svc *Service)

func performPutObject(t *testing.T, bucket string, _ *MemoryStorage, svc *Service) {
	t.Helper()

	w := httptest.NewRecorder()
	svc.PutObject(w, putObjectRequest(t, bucket, "hit.sqs", "data"))

	if w.Code != http.StatusOK {
		t.Fatalf("PutObject status: got %d, want %d", w.Code, http.StatusOK)
	}
}

func performCopyObject(t *testing.T, bucket string, store *MemoryStorage, svc *Service) {
	t.Helper()

	if _, err := store.PutObject(context.Background(), bucket, "src.txt", strings.NewReader("data"), nil); err != nil {
		t.Fatalf("seed PutObject: %v", err)
	}

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPut, "/"+bucket+"/hit.sqs", http.NoBody)
	req.SetPathValue("bucket", bucket)
	req.SetPathValue("key", "hit.sqs")
	req.Header.Set("X-Amz-Copy-Source", "/"+bucket+"/src.txt")

	w := httptest.NewRecorder()
	svc.CopyObject(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("CopyObject status: got %d, want %d (body=%s)", w.Code, http.StatusOK, w.Body.String())
	}
}

func performPostObject(t *testing.T, bucket string, _ *MemoryStorage, svc *Service) {
	t.Helper()

	var body bytes.Buffer
	mw := multipart.NewWriter(&body)

	if err := mw.WriteField("key", "hit.sqs"); err != nil {
		t.Fatalf("WriteField: %v", err)
	}

	fw, err := mw.CreateFormFile("file", "hit.sqs")
	if err != nil {
		t.Fatalf("CreateFormFile: %v", err)
	}

	if _, err := io.WriteString(fw, "data"); err != nil {
		t.Fatalf("write file field: %v", err)
	}

	if err := mw.Close(); err != nil {
		t.Fatalf("multipart close: %v", err)
	}

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/"+bucket, &body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.SetPathValue("bucket", bucket)

	w := httptest.NewRecorder()
	svc.PostObject(w, req)

	if w.Code != http.StatusNoContent && w.Code != http.StatusOK && w.Code != http.StatusCreated {
		t.Fatalf("PostObject status: got %d, want a success status", w.Code)
	}
}

func performCompleteMultipartUpload(t *testing.T, bucket string, store *MemoryStorage, svc *Service) {
	t.Helper()

	ctx := context.Background()

	upload, err := store.CreateMultipartUpload(ctx, bucket, "hit.sqs", nil)
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}

	part, err := store.UploadPart(ctx, bucket, "hit.sqs", upload.UploadID, 1, strings.NewReader("all the bytes"))
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}

	completeBody := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + part.ETag + `</ETag></Part></CompleteMultipartUpload>`

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/"+bucket+"/hit.sqs?uploadId="+upload.UploadID, strings.NewReader(completeBody))
	req.SetPathValue("bucket", bucket)
	req.SetPathValue("key", "hit.sqs")

	w := httptest.NewRecorder()
	svc.CompleteMultipartUpload(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("CompleteMultipartUpload status: got %d, want %d (body=%s)", w.Code, http.StatusOK, w.Body.String())
	}
}

// TestShutdownNotifications_TracksEveryObjectWriteOperation verifies that
// the four object-write operations the requirement names (PutObject,
// CopyObject, POST Object, CompleteMultipartUpload) all register their
// accepted notifications with the drain.
func TestShutdownNotifications_TracksEveryObjectWriteOperation(t *testing.T) {
	t.Parallel()

	const bucket = "drain-all-operations"

	operations := []struct {
		name    string
		perform objectWriteFunc
	}{
		{name: "PutObject", perform: performPutObject},
		{name: "CopyObject", perform: performCopyObject},
		{name: "PostObject", perform: performPostObject},
		{name: "CompleteMultipartUpload", perform: performCompleteMultipartUpload},
	}

	for _, tt := range operations {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			opBucket := bucket + "-" + strings.ToLower(strings.ReplaceAll(tt.name, "Object", ""))
			f := newDrainFixture(t, opBucket)

			tt.perform(t, opBucket, f.store, f.svc)

			// One SQS delivery plus one EventBridge delivery must park.
			waitTargetsStarted(t, f.notifier, f.doer, 2)

			go func() {
				time.Sleep(50 * time.Millisecond)
				close(f.release)
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if err := f.svc.ShutdownNotifications(ctx); err != nil {
				t.Fatalf("ShutdownNotifications() = %v, want nil", err)
			}
		})
	}
}
