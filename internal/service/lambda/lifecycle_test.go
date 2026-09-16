package lambda

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---- test scaffolding -----------------------------------------------------

// newLifecycleTestService returns a Service whose functions are created
// through the CreateFunction handler so live generations always exist.
func newLifecycleTestService(t *testing.T) *Service {
	t.Helper()

	svc := New(NewMemoryStorage(defaultBaseURL), defaultBaseURL)

	t.Cleanup(func() {
		if err := svc.Close(); err != nil {
			t.Fatalf("close service: %v", err)
		}
	})

	return svc
}

func createFunctionHTTP(t *testing.T, svc *Service, name, endpoint string) {
	t.Helper()

	body, err := json.Marshal(&CreateFunctionRequest{
		FunctionName:   name,
		Role:           "arn:aws:iam::000000000000:role/test",
		InvokeEndpoint: endpoint,
	})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/2015-03-31/functions", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	svc.CreateFunction(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("CreateFunction %s: status %d, body %s", name, rec.Code, rec.Body.String())
	}
}

func deleteFunctionHTTP(ctx context.Context, svc *Service, name string) *httptest.ResponseRecorder {
	req := httptest.NewRequestWithContext(ctx, http.MethodDelete,
		"/2015-03-31/functions/"+name, http.NoBody)
	rec := httptest.NewRecorder()

	svc.DeleteFunction(rec, req)

	return rec
}

func invokeHTTP(svc *Service, fn, invocationType string, payload []byte) *httptest.ResponseRecorder {
	path := "/2015-03-31/functions/" + fn + "/invocations"
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(payload))

	if invocationType != "" {
		req.Header.Set("X-Amz-Invocation-Type", invocationType)
	}

	rec := httptest.NewRecorder()
	svc.Invoke(rec, req)

	return rec
}

func getFunctionHTTP(svc *Service, name string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/2015-03-31/functions/"+name, http.NoBody)
	rec := httptest.NewRecorder()

	svc.GetFunction(rec, req)

	return rec
}

func runtimeNextRequest(ctx context.Context, fn string) *http.Request {
	return httptest.NewRequestWithContext(ctx, http.MethodGet,
		"/_runtime/"+fn+"/2018-06-01/runtime/invocation/next", http.NoBody)
}

func runtimeResponseRequest(fn, id string, body []byte) *http.Request {
	return httptest.NewRequest(http.MethodPost,
		"/_runtime/"+fn+"/2018-06-01/runtime/invocation/"+id+"/response",
		bytes.NewReader(body))
}

// barrierServer is an InvokeEndpoint stand-in that records every request,
// signals request once per call, and either sleeps latency or blocks until
// releaseAll is called.
type barrierServer struct {
	*httptest.Server
	request  chan struct{}
	release  chan struct{}
	count    atomic.Int32
	mu       sync.Mutex
	payloads []string
}

func newBarrierServer(t *testing.T) *barrierServer {
	t.Helper()

	return newControlledServer(t, 0, true)
}

func newLatencyServer(t *testing.T, latency time.Duration) *barrierServer {
	t.Helper()

	return newControlledServer(t, latency, false)
}

func newControlledServer(t *testing.T, latency time.Duration, block bool) *barrierServer {
	b := &barrierServer{
		request: make(chan struct{}, 128),
		release: make(chan struct{}),
	}

	b.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)

		b.mu.Lock()
		b.payloads = append(b.payloads, string(body))
		b.mu.Unlock()

		b.count.Add(1)
		b.request <- struct{}{}

		if latency > 0 {
			time.Sleep(latency)
		}

		if block {
			<-b.release
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))

	t.Cleanup(b.Server.Close)

	return b
}

func (b *barrierServer) releaseAll() {
	close(b.release)
}

func (b *barrierServer) requests() int32 { return b.count.Load() }

func (b *barrierServer) payloadsSnapshot() []string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return append([]string(nil), b.payloads...)
}

// waitRequests blocks until n requests have hit the endpoint.
func (b *barrierServer) waitRequests(t *testing.T, n int) {
	t.Helper()

	for range n {
		select {
		case <-b.request:
		case <-time.After(5 * time.Second):
			t.Fatalf("endpoint: expected %d requests, got %d", n, b.count.Load())
		}
	}
}

// deleteAsync runs DeleteFunction in a goroutine and returns the recorder
// channel.
func deleteAsync(t *testing.T, svc *Service, name string, timeout time.Duration) <-chan *httptest.ResponseRecorder {
	t.Helper()

	out := make(chan *httptest.ResponseRecorder, 1)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		out <- deleteFunctionHTTP(ctx, svc, name)
	}()

	return out
}

// ---- tests ---------------------------------------------------------------

// TestDeleteFunction_DrainsAsyncEventsBefore204 verifies the core quiesce
// guarantee: every Event invocation that got a 202 before the delete
// boundary is delivered before DeleteFunction returns 204, and after the
// 204 no old request reaches the endpoint and the function is gone.
func TestDeleteFunction_DrainsAsyncEventsBefore204(t *testing.T) {
	svc := newLifecycleTestService(t)

	srv := newLatencyServer(t, 20*time.Millisecond)
	createFunctionHTTP(t, svc, "fn", srv.URL)

	for i := 1; i <= 3; i++ {
		rec := invokeHTTP(svc, "fn", "Event", []byte(fmt.Sprintf(`{"seq":%d}`, i)))
		if rec.Code != http.StatusAccepted {
			t.Fatalf("event invoke %d: status %d", i, rec.Code)
		}
	}

	srv.waitRequests(t, 3)

	rec := deleteFunctionHTTP(context.Background(), svc, "fn")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, want 204, body %s", rec.Code, rec.Body.String())
	}

	// No further requests must ever reach the old endpoint.
	time.Sleep(150 * time.Millisecond)

	if got := srv.requests(); got != 3 {
		t.Fatalf("endpoint got %d requests after delete, want exactly 3", got)
	}

	got := srv.payloadsSnapshot()
	want := []string{`{"seq":1}`, `{"seq":2}`, `{"seq":3}`}

	if len(got) != len(want) {
		t.Fatalf("deliveries = %v, want %v", got, want)
	}

	for i := range want {
		if got[i] != want[i] {
			t.Errorf("delivery %d = %s, want %s", i, got[i], want[i])
		}
	}

	// Generation retired and function gone.
	if g := svc.lifecycles.get("fn"); g != nil {
		t.Errorf("generation still registered after 204")
	}

	if rec := getFunctionHTTP(svc, "fn"); rec.Code != http.StatusNotFound {
		t.Errorf("GetFunction after delete = %d, want 404", rec.Code)
	}

	if rec := invokeHTTP(svc, "fn", "Event", []byte(`{}`)); rec.Code != http.StatusNotFound {
		t.Errorf("Invoke after delete = %d, want 404", rec.Code)
	}
}

// TestDeleteFunction_WaitsForInFlightSyncInvoke verifies that a 204 is not
// returned while a synchronous endpoint delivery is still in flight, and
// that invocations admitted after the boundary are rejected and never
// delivered.
func TestDeleteFunction_WaitsForInFlightSyncInvoke(t *testing.T) {
	svc := newLifecycleTestService(t)

	srv := newBarrierServer(t)
	createFunctionHTTP(t, svc, "fn", srv.URL)

	syncDone := make(chan *httptest.ResponseRecorder, 1)

	go func() {
		syncDone <- invokeHTTP(svc, "fn", "RequestResponse", []byte(`{"sync":1}`))
	}()

	srv.waitRequests(t, 1)

	del := deleteAsync(t, svc, "fn", 5*time.Second)

	// Delete must not complete while the sync call is in flight.
	select {
	case rec := <-del:
		t.Fatalf("delete returned %d while sync invoke in flight", rec.Code)
	case <-time.After(150 * time.Millisecond):
	}

	// Once the boundary is visible, both async and sync invokes must be
	// rejected; none of them may reach the endpoint.
	acceptedAfterBoundary := 0

	if !waitFor(t, 2*time.Second, func() bool {
		return invokeHTTP(svc, "fn", "Event", []byte(`{"late":1}`)).Code == http.StatusConflict
	}) {
		t.Fatal("post-boundary Event invoke was never rejected")
	}

	for range 5 {
		if rec := invokeHTTP(svc, "fn", "Event", []byte(`{"late":1}`)); rec.Code == http.StatusAccepted {
			acceptedAfterBoundary++
		}

		if rec := invokeHTTP(svc, "fn", "", []byte(`{"late":1}`)); rec.Code != http.StatusConflict {
			t.Errorf("post-boundary sync invoke = %d, want 409", rec.Code)
		}
	}

	if acceptedAfterBoundary != 0 {
		t.Errorf("%d post-boundary events accepted, want 0", acceptedAfterBoundary)
	}

	// Release the in-flight sync invoke; delete now converges.
	srv.releaseAll()

	var delRec *httptest.ResponseRecorder

	select {
	case delRec = <-del:
	case <-time.After(5 * time.Second):
		t.Fatal("delete did not complete after in-flight invoke finished")
	}

	if delRec.Code != http.StatusNoContent {
		t.Fatalf("delete = %d, want 204, body %s", delRec.Code, delRec.Body.String())
	}

	select {
	case rec := <-syncDone:
		if rec.Code != http.StatusOK {
			t.Errorf("sync invoke = %d, want 200", rec.Code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sync invoke never completed")
	}

	// Exactly the single pre-boundary delivery reached the endpoint.
	time.Sleep(100 * time.Millisecond)

	if got := srv.requests(); got != 1 {
		t.Fatalf("endpoint got %d requests, want exactly the 1 admitted sync invoke", got)
	}
}

// TestDeleteFunction_TimeoutRetainsFunction verifies that a delete that
// cannot converge within the request deadline fails, rolls the boundary
// back, and leaves the function queryable and usable; a later delete then
// succeeds.
func TestDeleteFunction_TimeoutRetainsFunction(t *testing.T) {
	svc := newLifecycleTestService(t)

	srv := newBarrierServer(t)
	createFunctionHTTP(t, svc, "fn", srv.URL)

	// A sync invoke wedged for the whole delete deadline.
	syncDone := make(chan *httptest.ResponseRecorder, 1)

	go func() {
		syncDone <- invokeHTTP(svc, "fn", "RequestResponse", []byte(`{}`))
	}()

	srv.waitRequests(t, 1)

	// An event queued behind the wedged sync invoke (gate serialization).
	if rec := invokeHTTP(svc, "fn", "Event", []byte(`{"queued":1}`)); rec.Code != http.StatusAccepted {
		t.Fatalf("queued event = %d, want 202", rec.Code)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	rec := deleteFunctionHTTP(ctx, svc, "fn")
	if rec.Code != http.StatusConflict {
		t.Fatalf("timed-out delete = %d, want 409, body %s", rec.Code, rec.Body.String())
	}

	// Function remains queryable.
	if rec := getFunctionHTTP(svc, "fn"); rec.Code != http.StatusOK {
		t.Errorf("GetFunction after failed delete = %d, want 200", rec.Code)
	}

	// Release the wedged endpoint: the sync invoke finishes and the queued
	// event drains — the rolled-back generation's queue and gate are still
	// live.
	srv.releaseAll()

	select {
	case syncRec := <-syncDone:
		if syncRec.Code != http.StatusOK {
			t.Errorf("wedged sync invoke = %d, want 200", syncRec.Code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("wedged sync invoke never completed after release")
	}

	srv.waitRequests(t, 1) // the queued event lands

	// New work is accepted and executed on the retained generation.
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer fast.Close()

	// The retained function still points at the (now unblocked) original
	// endpoint; a sync invoke must succeed.
	if rec := invokeHTTP(svc, "fn", "RequestResponse", []byte(`{}`)); rec.Code != http.StatusOK {
		t.Errorf("retained function sync invoke = %d, want 200, body %s", rec.Code, rec.Body.String())
	}

	// Retry the delete with enough time: it now converges and commits.
	if rec := deleteFunctionHTTP(context.Background(), svc, "fn"); rec.Code != http.StatusNoContent {
		t.Fatalf("retry delete = %d, want 204, body %s", rec.Code, rec.Body.String())
	}
}

// TestDeleteFunction_WakesRuntimeNextPoll verifies that an idle Runtime
// API long poll is woken with the same stable error when the function is
// deleted, and stays rejected once the generation is gone.
func TestDeleteFunction_WakesRuntimeNextPoll(t *testing.T) {
	svc := newLifecycleTestService(t)
	createFunctionHTTP(t, svc, "fn", "")

	polled := make(chan *httptest.ResponseRecorder, 1)

	go func() {
		rec := httptest.NewRecorder()
		svc.RuntimeNext(rec, runtimeNextRequest(context.Background(), "fn"))
		polled <- rec
	}()

	// Wait until the long poll is parked.
	time.Sleep(100 * time.Millisecond)

	if rec := deleteFunctionHTTP(context.Background(), svc, "fn"); rec.Code != http.StatusNoContent {
		t.Fatalf("delete = %d, want 204", rec.Code)
	}

	select {
	case rec := <-polled:
		if rec.Code != http.StatusGone {
			t.Fatalf("RuntimeNext after delete = %d, want 410, body %s", rec.Code, rec.Body.String())
		}

		var body map[string]string
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("gone response body: %v", err)
		}

		if body["errorType"] != "Runtime.FunctionDeleted" {
			t.Errorf("errorType = %q, want Runtime.FunctionDeleted", body["errorType"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RuntimeNext long poll was not woken by delete")
	}

	// Stable error on subsequent polls too — no hanging, no recreated state.
	rec := httptest.NewRecorder()
	svc.RuntimeNext(rec, runtimeNextRequest(context.Background(), "fn"))

	if rec.Code != http.StatusGone {
		t.Errorf("RuntimeNext after retirement = %d, want 410", rec.Code)
	}
}

// TestDeleteFunction_InFlightRuntimeResponseCompletes verifies that an
// invocation already handed to a runtime handler can still respond while
// the delete is draining, and that the same request id posted after a
// same-name recreate is rejected instead of reaching the new generation.
func TestDeleteFunction_InFlightRuntimeResponseCompletes(t *testing.T) {
	svc := newLifecycleTestService(t)
	createFunctionHTTP(t, svc, "fn", "")

	oldGen := svc.lifecycles.get("fn")

	// Scripted poller: take one invocation, report its id, then respond on
	// demand.
	nextRec := make(chan *httptest.ResponseRecorder, 1)

	go func() {
		rec := httptest.NewRecorder()
		svc.RuntimeNext(rec, runtimeNextRequest(context.Background(), "fn"))
		nextRec <- rec
	}()

	// Wait for the poll to register.
	if !waitFor(t, 2*time.Second, func() bool {
		return svc.broker.registered(oldGen)
	}) {
		t.Fatal("handler never registered")
	}

	syncDone := make(chan *httptest.ResponseRecorder, 1)

	go func() {
		syncDone <- invokeHTTP(svc, "fn", "RequestResponse", []byte(`{"old":true}`))
	}()

	var id string

	select {
	case rec := <-nextRec:
		if rec.Code != http.StatusOK {
			t.Fatalf("next = %d, want 200, body %s", rec.Code, rec.Body.String())
		}

		id = rec.Header().Get("Lambda-Runtime-Aws-Request-Id")
		if id == "" {
			t.Fatal("missing request id header")
		}

		if rec.Body.String() != `{"old":true}` {
			t.Errorf("handoff payload = %s", rec.Body.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("invocation never reached the poller")
	}

	del := deleteAsync(t, svc, "fn", 5*time.Second)

	// Delete must wait for the handler's response.
	select {
	case rec := <-del:
		t.Fatalf("delete returned %d while runtime invocation unresponded", rec.Code)
	case <-time.After(150 * time.Millisecond):
	}

	// The handler responds: accepted, and it unblocks both the invoke and
	// the delete.
	respRec := httptest.NewRecorder()
	svc.RuntimeResponse(respRec, runtimeResponseRequest("fn", id, []byte(`{"result":"ok"}`)))

	if respRec.Code != http.StatusAccepted {
		t.Fatalf("in-flight response = %d, want 202, body %s", respRec.Code, respRec.Body.String())
	}

	select {
	case rec := <-syncDone:
		if rec.Code != http.StatusOK || rec.Body.String() != `{"result":"ok"}` {
			t.Fatalf("sync invoke = %d body %s, want 200 with result", rec.Code, rec.Body.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sync invoke did not complete after response")
	}

	select {
	case rec := <-del:
		if rec.Code != http.StatusNoContent {
			t.Fatalf("delete = %d, want 204", rec.Code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("delete did not complete after the in-flight response")
	}

	// Recreate the function under the same name: brand-new generation.
	createFunctionHTTP(t, svc, "fn", "")

	newGen := svc.lifecycles.get("fn")
	if newGen == nil || newGen == oldGen {
		t.Fatal("same-name CreateFunction did not produce a distinct generation")
	}

	if newGen.gate == oldGen.gate {
		t.Error("new generation inherited the old gate")
	}

	if svc.broker.registered(newGen) {
		t.Error("new generation inherited the old runtime poller registration")
	}

	// The late response for the old request id must not land in the new
	// generation's pending state.
	late := httptest.NewRecorder()
	svc.RuntimeResponse(late, runtimeResponseRequest("fn", id, []byte(`{"evil":true}`)))

	if late.Code != http.StatusForbidden {
		t.Fatalf("late response into new generation = %d, want 403, body %s", late.Code, late.Body.String())
	}

	newGen.rtMu.Lock()
	pendingCount := len(newGen.pending)
	newGen.rtMu.Unlock()

	if pendingCount != 0 {
		t.Errorf("new generation has %d pending entries inherited from the old generation", pendingCount)
	}
}

// TestCreateFunction_SameNameIsFreshGeneration verifies that a function
// recreated after delete uses only its new endpoint and none of the old
// generation's runtime state, and that the old endpoint receives nothing.
func TestCreateFunction_SameNameIsFreshGeneration(t *testing.T) {
	svc := newLifecycleTestService(t)

	var deleted atomic.Bool

	oldEndpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)

		if deleted.Load() {
			t.Errorf("old endpoint received a request after delete committed: %s", body)
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))

	newEndpointCalls := make(chan []byte, 8)
	newEndpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		newEndpointCalls <- body
		w.WriteHeader(http.StatusOK)
	}))

	defer oldEndpoint.Close()
	defer newEndpoint.Close()

	createFunctionHTTP(t, svc, "fn", oldEndpoint.URL)
	oldGen := svc.lifecycles.get("fn")

	if rec := invokeHTTP(svc, "fn", "Event", []byte(`{"gen":"old"}`)); rec.Code != http.StatusAccepted {
		t.Fatalf("old event = %d", rec.Code)
	}

	if rec := deleteFunctionHTTP(context.Background(), svc, "fn"); rec.Code != http.StatusNoContent {
		t.Fatalf("delete = %d", rec.Code)
	}

	deleted.Store(true)

	createFunctionHTTP(t, svc, "fn", newEndpoint.URL)
	newGen := svc.lifecycles.get("fn")

	if newGen == oldGen {
		t.Fatal("new generation is the same object as the old one")
	}

	if newGen.gate == oldGen.gate {
		t.Error("new generation inherited the old gate")
	}

	if newGen.queue != nil {
		t.Error("new generation inherited the old async queue")
	}

	if rec := invokeHTTP(svc, "fn", "Event", []byte(`{"gen":"new"}`)); rec.Code != http.StatusAccepted {
		t.Fatalf("new event = %d, want 202", rec.Code)
	}

	select {
	case body := <-newEndpointCalls:
		if string(body) != `{"gen":"new"}` {
			t.Errorf("new endpoint got %s, want new payload", body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("new endpoint never received the event")
	}
}

// TestDeleteFunction_OtherFunctionsUnaffected verifies that one function
// stuck draining neither blocks invokes/deletes on other functions nor
// creates for new names.
func TestDeleteFunction_OtherFunctionsUnaffected(t *testing.T) {
	svc := newLifecycleTestService(t)

	srvA := newBarrierServer(t)
	srvB := newLatencyServer(t, 10*time.Millisecond)

	createFunctionHTTP(t, svc, "fn-a", srvA.URL)
	createFunctionHTTP(t, svc, "fn-b", srvB.URL)

	// Wedge fn-a with an in-flight sync invoke.
	go func() { invokeHTTP(svc, "fn-a", "RequestResponse", []byte(`{}`)) }()
	srvA.waitRequests(t, 1)

	delA := deleteAsync(t, svc, "fn-a", 200*time.Millisecond)

	// fn-b stays fully operational: events delivered, delete commits.
	if rec := invokeHTTP(svc, "fn-b", "Event", []byte(`{"b":1}`)); rec.Code != http.StatusAccepted {
		t.Fatalf("fn-b event while fn-a draining = %d, want 202", rec.Code)
	}

	srvB.waitRequests(t, 1)

	createFunctionHTTP(t, svc, "fn-c", srvB.URL)

	if rec := deleteFunctionHTTP(context.Background(), svc, "fn-b"); rec.Code != http.StatusNoContent {
		t.Fatalf("delete fn-b while fn-a draining = %d, want 204, body %s", rec.Code, rec.Body.String())
	}

	// fn-a's delete times out and retains the function.
	select {
	case rec := <-delA:
		if rec.Code != http.StatusConflict {
			t.Fatalf("delete fn-a = %d, want 409", rec.Code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("delete fn-a never returned")
	}

	if rec := getFunctionHTTP(svc, "fn-a"); rec.Code != http.StatusOK {
		t.Errorf("fn-a GetFunction = %d, want 200", rec.Code)
	}

	if rec := getFunctionHTTP(svc, "fn-b"); rec.Code != http.StatusNotFound {
		t.Errorf("fn-b GetFunction = %d, want 404", rec.Code)
	}

	if rec := getFunctionHTTP(svc, "fn-c"); rec.Code != http.StatusOK {
		t.Errorf("fn-c GetFunction = %d, want 200", rec.Code)
	}

	// Unblock fn-a; it remains usable and can be deleted on retry.
	srvA.releaseAll()

	if rec := deleteFunctionHTTP(context.Background(), svc, "fn-a"); rec.Code != http.StatusNoContent {
		t.Fatalf("retry delete fn-a = %d, want 204", rec.Code)
	}
}

// TestDeleteFunction_ConcurrentInvokeOrdering is the deterministic ordering
// stress test: with one in-flight invoke and a batch of queued events,
// DeleteFunction commits only after every pre-boundary 202 is delivered;
// every post-boundary invoke is rejected and never hits the endpoint.
func TestDeleteFunction_ConcurrentInvokeOrdering(t *testing.T) {
	svc := newLifecycleTestService(t)

	srv := newBarrierServer(t)
	createFunctionHTTP(t, svc, "fn", srv.URL)

	// One in-flight sync invoke (holds the per-generation gate).
	go func() { invokeHTTP(svc, "fn", "RequestResponse", []byte(`{"sync":1}`)) }()
	srv.waitRequests(t, 1)

	// Ten events admitted before the boundary; they queue behind the gate.
	for range 10 {
		if rec := invokeHTTP(svc, "fn", "Event", []byte(`{"pre":1}`)); rec.Code != http.StatusAccepted {
			t.Fatalf("pre-boundary event = %d, want 202", rec.Code)
		}
	}

	del := deleteAsync(t, svc, "fn", 10*time.Second)

	// Probe until the boundary is observable. Probes that slip in before
	// the boundary (202) are themselves admitted work and must be drained,
	// so count them.
	var probeAccepted atomic.Int32

	if !waitFor(t, 2*time.Second, func() bool {
		rec := invokeHTTP(svc, "fn", "Event", []byte(`{"probe":1}`))
		if rec.Code == http.StatusAccepted {
			probeAccepted.Add(1)

			return false
		}

		return rec.Code == http.StatusConflict
	}) {
		t.Fatal("delete boundary never became visible")
	}

	// A burst of post-boundary invokes: none accepted.
	var wg sync.WaitGroup

	var rejected atomic.Int32

	for range 20 {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if invokeHTTP(svc, "fn", "Event", []byte(`{"post":1}`)).Code == http.StatusConflict {
				rejected.Add(1)
			}
		}()
	}

	wg.Wait()

	if got := rejected.Load(); got != 20 {
		t.Errorf("post-boundary rejected = %d, want 20", got)
	}

	// Release everything: 1 sync + 10 queued events + any probes admitted
	// before the boundary must finish, then 204.
	srv.releaseAll()

	select {
	case rec := <-del:
		if rec.Code != http.StatusNoContent {
			t.Fatalf("delete = %d, want 204, body %s", rec.Code, rec.Body.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("delete never converged")
	}

	time.Sleep(100 * time.Millisecond)

	wantDeliveries := int32(11) + probeAccepted.Load()
	if got := srv.requests(); got != wantDeliveries {
		t.Fatalf("endpoint got %d requests, want exactly %d (1 sync + 10 events + %d pre-boundary probes)",
			got, wantDeliveries, probeAccepted.Load())
	}
}

// TestServiceClose_DrainsGenerationsWithoutHang verifies the service-wide
// shutdown semantics remain compatible: queued deliveries are aborted and
// Close returns even while a function has retrying events.
func TestServiceClose_DrainsGenerationsWithoutHang(t *testing.T) {
	svc := New(NewMemoryStorage(defaultBaseURL), defaultBaseURL)

	addr := reserveAddr(t)
	_, g, err := svc.lifecycles.create("fn", func() (*Function, error) {
		return svc.storage.CreateFunction(context.Background(), &CreateFunctionRequest{
			FunctionName:   "fn",
			Role:           "arn:aws:iam::000000000000:role/test",
			InvokeEndpoint: "http://" + addr + "/invoke",
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	svc.async.initialBackoff = 5 * time.Millisecond
	svc.async.maxBackoff = 20 * time.Millisecond

	for range 3 {
		svc.async.enqueue(g, &endpointDeliverer{
			client:   svc.async.client,
			endpoint: "http://" + addr + "/invoke",
			gate:     g.gate,
		}, []byte(`{}`))
	}

	closed := make(chan struct{})

	go func() {
		_ = svc.Close()
		close(closed)
	}()

	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Service.Close hung while generations had retrying deliveries")
	}
}
