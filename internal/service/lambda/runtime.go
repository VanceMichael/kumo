package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// runtimeInvokeTimeout bounds how long an invocation waits to be picked up by
// a polling handler and to receive its response.
const runtimeInvokeTimeout = 30 * time.Second

// errRuntimeNoPoller is returned when no handler polls next and picks up an
// invocation within the wait timeout — nobody ever started the work, which
// AWS treats as a system-level delivery failure (retried with backoff).
var errRuntimeNoPoller = errors.New("no runtime handler available to poll the invocation")

// errRuntimeResponseTimeout is returned when a handler polled next and took
// the invocation but never posted a response or error within the wait
// timeout — the handler picked up the work but its function timed out,
// which AWS treats as a function error (limited retries).
var errRuntimeResponseTimeout = errors.New("runtime handler did not respond before the timeout")

// errRuntimeFunctionGone is the stable error returned to a Runtime API
// long poll whose generation is being deleted, has been retired, or whose
// service is shutting down. It is stable (same status and body on every
// retry) so an old handler process terminates instead of attaching itself
// to a later, same-name generation.
var errRuntimeFunctionGone = errors.New("runtime function generation is gone")

// errRuntimeDeleting is returned to an invocation handoff that loses the
// race with a delete boundary. The work was never picked up, so the async
// deliverer treats it as a system error and retries; if the drain cannot
// finish in time the delete is aborted and the delivery goes through.
var errRuntimeDeleting = errors.New("runtime function generation is being deleted")

// runtimeBroker bridges kumo invocations to handlers that speak the AWS
// Lambda Runtime API (lambda.Start). It is stateless itself: every piece
// of per-function state lives on the functionGeneration, so a handler
// polls/responds against exactly one generation and a same-name
// CreateFunction inherits none of the old runtime state.
type runtimeBroker struct {
	// shutdown is closed when the whole service closes.
	shutdown <-chan struct{}
}

func newRuntimeBroker(shutdown <-chan struct{}) *runtimeBroker {
	return &runtimeBroker{shutdown: shutdown}
}

type runtimeInvocation struct {
	id      string
	payload []byte
}

type runtimeResult struct {
	payload []byte
	errored bool
}

// registered reports whether a handler has polled next on THIS generation,
// i.e. the generation is backed by a Runtime API handler. A successor
// generation always starts unregistered even if its name had a handler
// before the delete.
func (b *runtimeBroker) registered(g *functionGeneration) bool {
	g.rtMu.Lock()
	defer g.rtMu.Unlock()

	return g.registered
}

// invoke hands an invocation to a polling handler and waits for its
// response, up to timeout for each of the two phases (waiting to be picked
// up by next, then waiting for a response/error). Callers that want async
// (fire-and-forget-but-not-really) semantics should queue a runtimeDeliverer
// on the asyncDispatcher instead of calling invoke directly — see
// invokeViaRuntime.
func (b *runtimeBroker) invoke(ctx context.Context, g *functionGeneration, payload []byte, timeout time.Duration) (runtimeResult, error) {
	inv := &runtimeInvocation{id: uuid.New().String(), payload: payload}

	resCh := make(chan runtimeResult, 1)

	g.rtMu.Lock()
	g.pending[inv.id] = resCh
	g.rtMu.Unlock()

	defer func() {
		g.rtMu.Lock()
		delete(g.pending, inv.id)
		g.rtMu.Unlock()
	}()

	// Phase 1: hand the invocation to a handler blocked in next. The
	// delete and shutdown signals abort the handoff so queued attempts do
	// not start work on a generation that is going away.
	select {
	case g.invocations <- inv:
	case <-ctx.Done():
		return runtimeResult{}, fmt.Errorf("invocation canceled: %w", ctx.Err())
	case <-g.deletionSignal():
		return runtimeResult{}, errRuntimeDeleting
	case <-b.shutdown:
		return runtimeResult{}, errRuntimeFunctionGone
	case <-time.After(timeout):
		return runtimeResult{}, errRuntimeNoPoller
	}

	// Phase 2: wait for the handler's result. The delete signal is
	// deliberately not selected here: this invocation was already handed
	// out before the boundary, so it is admitted work the delete must wait
	// for. Interrupting it could only strand the handler and make its
	// response "late".
	select {
	case res := <-resCh:
		return res, nil
	case <-ctx.Done():
		return runtimeResult{}, fmt.Errorf("invocation canceled: %w", ctx.Err())
	case <-time.After(timeout):
		return runtimeResult{}, errRuntimeResponseTimeout
	}
}

// runtimeDeliverer delivers an event by handing it to a Runtime API handler
// through the runtimeBroker's synchronous path — the async queue itself
// provides the asynchrony, so the deliverer only ever waits for one poll/
// response round trip per attempt. Counterpart of endpointDeliverer
// (async.go) for functions backed by an InvokeEndpoint.
type runtimeDeliverer struct {
	broker *runtimeBroker
	g      *functionGeneration

	// waitTimeout bounds how long one delivery attempt waits for a handler
	// to pick up and respond to the invocation. Zero means
	// runtimeInvokeTimeout; tests inject a short value so retry scenarios
	// don't need to wait out the real 30s default.
	waitTimeout time.Duration
}

// deliver hands the event to a polling handler and waits for its response.
// errRuntimeResponseTimeout means a handler took the invocation but never
// responded — the function itself timed out, so this is a function error
// with limited retries, matching AWS async semantics. Any other failure
// (nobody polled, delete boundary, context canceled, ...) means the work
// was never picked up, so it is a system error retried with backoff until
// the event's deadline. A handler-reported error is likewise a function
// error.
func (r *runtimeDeliverer) deliver(ctx context.Context, _ string, payload []byte) deliveryResult {
	timeout := r.waitTimeout
	if timeout == 0 {
		timeout = runtimeInvokeTimeout
	}

	res, err := r.broker.invoke(ctx, r.g, payload, timeout)
	if err != nil {
		if errors.Is(err, errRuntimeResponseTimeout) {
			return asyncFunctionError
		}

		return asyncSystemError
	}

	if res.errored {
		return asyncFunctionError
	}

	return asyncDelivered
}

// next blocks until an invocation is queued for the generation or until the
// generation goes away (delete/retire/shutdown) or ctx is done.
func (b *runtimeBroker) next(ctx context.Context, g *functionGeneration) (*runtimeInvocation, error) {
	g.rtMu.Lock()
	g.registered = true
	g.rtMu.Unlock()

	select {
	case inv := <-g.invocations:
		return inv, nil
	case <-g.deletionSignal():
		return nil, errRuntimeFunctionGone
	case <-b.shutdown:
		return nil, errRuntimeFunctionGone
	case <-ctx.Done():
		return nil, fmt.Errorf("next canceled: %w", ctx.Err())
	}
}

// respond delivers a handler's result to the waiting invoker. It returns
// false when this generation does not own the request id — for example a
// response posted after the generation was retired and a same-name
// successor created. The caller then rejects the response instead of ever
// letting it reach the successor's pending table.
func (b *runtimeBroker) respond(g *functionGeneration, id string, payload []byte, errored bool) bool {
	g.rtMu.Lock()
	ch := g.pending[id]
	g.rtMu.Unlock()

	if ch == nil {
		return false
	}

	ch <- runtimeResult{payload: payload, errored: errored}

	return true
}

// ---- Runtime API HTTP handlers ----
//
// These implement the subset of the AWS Lambda Runtime API that lambda.Start
// uses, under /_runtime/{functionName}/2018-06-01/runtime/... . A handler is
// pointed at kumo with AWS_LAMBDA_RUNTIME_API=<host>/_runtime/{functionName}.

// RuntimeNext handles GET .../runtime/invocation/next (long-poll).
func (s *Service) RuntimeNext(w http.ResponseWriter, r *http.Request) {
	fn := runtimeFunctionName(r.URL.Path)
	if fn == "" {
		writeFunctionError(w, ErrInvalidParameterValue, "FunctionName is required", http.StatusBadRequest)

		return
	}

	g := s.lifecycles.get(fn)
	if g == nil {
		writeRuntimeGone(w, fn)

		return
	}

	inv, err := s.broker.next(r.Context(), g)
	if err != nil {
		if errors.Is(err, errRuntimeFunctionGone) {
			// The function was deleted (or the service is shutting down):
			// wake the long poll with a stable error instead of leaving it
			// hanging.
			writeRuntimeGone(w, fn)

			return
		}

		// Client (handler) disconnected; nothing to write.
		return
	}

	// The aws-lambda-go runtime requires a parseable deadline header.
	deadline := time.Now().Add(runtimeInvokeTimeout).UnixMilli()

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Lambda-Runtime-Aws-Request-Id", inv.id)
	w.Header().Set("Lambda-Runtime-Deadline-Ms", strconv.FormatInt(deadline, 10))
	w.Header().Set("Lambda-Runtime-Invoked-Function-Arn", "arn:aws:lambda:local:000000000000:function:"+fn)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(inv.payload)
}

// RuntimeResponse handles POST .../runtime/invocation/{requestId}/response.
func (s *Service) RuntimeResponse(w http.ResponseWriter, r *http.Request) {
	s.runtimeResult(w, r, false)
}

// RuntimeError handles POST .../runtime/invocation/{requestId}/error.
func (s *Service) RuntimeError(w http.ResponseWriter, r *http.Request) {
	s.runtimeResult(w, r, true)
}

func (s *Service) runtimeResult(w http.ResponseWriter, r *http.Request, errored bool) {
	fn := runtimeFunctionName(r.URL.Path)
	id := runtimeRequestID(r.URL.Path)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeFunctionError(w, ErrInvalidParameterValue, "failed to read body", http.StatusBadRequest)

		return
	}

	// Resolve the generation that OWNS the request id. A response from an
	// old handler must never land in a same-name successor generation, and
	// a response to an id nobody is waiting on is rejected with the same
	// stable error RIE gives for expired request ids.
	g := s.lifecycles.get(fn)

	if g == nil || !s.broker.respond(g, id, body, errored) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)

		_ = json.NewEncoder(w).Encode(map[string]string{
			"errorMessage": fmt.Sprintf("RequestId %s does not exist or belongs to a retired function generation", id),
			"errorType":    "Runtime.InvalidRequestId",
		})

		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"status":"OK"}`))
}

// RuntimeInitError handles POST .../runtime/init/error (best-effort).
func (s *Service) RuntimeInitError(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"status":"OK"}`))
}

// writeRuntimeGone writes the stable error returned to Runtime API calls
// whose function generation is being deleted or no longer exists.
func writeRuntimeGone(w http.ResponseWriter, fn string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusGone)

	_ = json.NewEncoder(w).Encode(map[string]string{
		"errorMessage": fmt.Sprintf("Function %s is being deleted or no longer exists; terminate this runtime", fn),
		"errorType":    "Runtime.FunctionDeleted",
	})
}

// runtimeFunctionName extracts {functionName} from a /_runtime/{fn}/... path.
func runtimeFunctionName(path string) string {
	return segmentAfter(path, "_runtime")
}

// runtimeRequestID extracts {requestId} from a .../invocation/{id}/... path.
func runtimeRequestID(path string) string {
	return segmentAfter(path, "invocation")
}

func segmentAfter(path, marker string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, p := range parts {
		if p == marker && i+1 < len(parts) {
			return parts[i+1]
		}
	}

	return ""
}
