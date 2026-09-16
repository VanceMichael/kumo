package lambda

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

const (
	// asyncQueueCapacity bounds each function's event queue. Enqueue never
	// blocks the Invoke handler; events beyond this are dropped and logged.
	asyncQueueCapacity = 1024

	// asyncInitialBackoff is the first retry delay after a delivery failure.
	asyncInitialBackoff = 100 * time.Millisecond

	// asyncMaxBackoff caps the exponential backoff between retries.
	asyncMaxBackoff = 5 * time.Second

	// asyncMaxEventAge is how long an event is retried after a system error
	// (endpoint unreachable) before it is dropped, mirroring Lambda's
	// default maximum event age for asynchronous invocation.
	asyncMaxEventAge = 6 * time.Hour

	// asyncMaxFunctionErrorRetries is the number of retries after the
	// endpoint responds with an error status, mirroring Lambda's default
	// of two retry attempts on function errors.
	asyncMaxFunctionErrorRetries = 2
)

// asyncDeliverer delivers one queued event to its execution target. It
// reports whether the attempt succeeded, failed as a function error (limited
// retries), or failed as a system error (retried until the event's
// deadline), mirroring AWS async invocation semantics. Implementations:
// endpointDeliverer (InvokeEndpoint) and runtimeDeliverer (Runtime API).
type asyncDeliverer interface {
	deliver(ctx context.Context, functionName string, payload []byte) deliveryResult
}

// asyncEvent is one queued asynchronous (InvocationType: Event) invocation.
type asyncEvent struct {
	deliverer asyncDeliverer
	payload   []byte
	deadline  time.Time
}

// asyncDispatcher owns the process-wide machinery behind Event invocations:
// the HTTP client, retry/backoff configuration, the service shutdown signal
// and the WaitGroup tracking every drain goroutine. Per-function state
// (queues, gates, runtime handoff) lives on functionGeneration so it is
// drained and retired together with the function resource.
type asyncDispatcher struct {
	client *http.Client
	done   chan struct{}
	wg     sync.WaitGroup

	initialBackoff time.Duration
	maxBackoff     time.Duration
	maxEventAge    time.Duration
}

func newAsyncDispatcher() *asyncDispatcher {
	return &asyncDispatcher{
		client:         http.DefaultClient,
		done:           make(chan struct{}),
		initialBackoff: asyncInitialBackoff,
		maxBackoff:     asyncMaxBackoff,
		maxEventAge:    asyncMaxEventAge,
	}
}

// invokeGate is a binary semaphore (a capacity-1 channel) that serializes
// InvokeEndpoint HTTP calls for one generation across the synchronous and
// asynchronous invoke paths. Acquisition is context-aware: a caller gives
// up waiting when its context is done instead of blocking forever on a
// peer that never releases the gate. This matters most for the async drain
// goroutine, whose delivery context is canceled when the dispatcher closes
// — without that, a stuck synchronous invoke holding the gate would
// prevent asyncDispatcher.close from ever returning.
type invokeGate chan struct{}

func newInvokeGate() invokeGate {
	return make(invokeGate, 1)
}

// acquire blocks until the gate is free or ctx is done, reporting which.
func (g invokeGate) acquire(ctx context.Context) bool {
	select {
	case g <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

// release frees the gate for the next acquirer.
func (g invokeGate) release() {
	<-g
}

// enqueue places an event on the generation's FIFO queue and accounts for
// it as in-flight work before the caller can return 202, so a concurrent
// DeleteFunction is guaranteed to wait for it. It returns false when the
// generation's delete boundary has already been set, in which case nothing
// was queued and the caller must reject the invocation. Enqueue itself
// never blocks: a full queue is reported as a dropped event whose work
// count is released immediately.
func (d *asyncDispatcher) enqueue(g *functionGeneration, deliverer asyncDeliverer, payload []byte) bool {
	g.admMu.Lock()
	if g.deleting {
		g.admMu.Unlock()

		return false
	}

	g.inflight++

	if g.queue == nil {
		g.queue = make(chan *asyncEvent, asyncQueueCapacity)
	}

	if !g.drainStarted {
		g.drainStarted = true

		d.wg.Add(1)

		go d.drain(g)
	}

	q := g.queue
	g.admMu.Unlock()

	payloadCopy := make([]byte, len(payload))
	copy(payloadCopy, payload)

	ev := &asyncEvent{
		deliverer: deliverer,
		payload:   payloadCopy,
		deadline:  time.Now().Add(d.maxEventAge),
	}

	select {
	case q <- ev:
	default:
		g.workDone()
		slog.Error("async invoke queue full, event dropped", "function", g.name)
	}

	return true
}

// close stops all drain goroutines and waits for them to exit. In-flight
// requests are aborted through each drain goroutine's context.
func (d *asyncDispatcher) close() {
	close(d.done)
	d.wg.Wait()
}

// drain delivers queued events one at a time. Head-of-line blocking is
// deliberate: it preserves per-function delivery order. The goroutine exits
// on service shutdown or when its generation has been drained and retired.
func (d *asyncDispatcher) drain(g *functionGeneration) {
	defer d.wg.Done()

	// Lifecycle context for this goroutine's deliveries, canceled when the
	// dispatcher closes so in-flight requests are aborted.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		select {
		case <-d.done:
			cancel()
		case <-ctx.Done():
		}
	}()

	for {
		select {
		case <-d.done:
			return
		case <-g.stopDrain:
			return
		case ev := <-g.queue:
			d.deliver(ctx, g, ev)
		}
	}
}

// deliveryResult classifies one delivery attempt.
type deliveryResult int

const (
	// asyncDelivered: the deliverer accepted the event.
	asyncDelivered deliveryResult = iota
	// asyncFunctionError: the target responded with an error.
	asyncFunctionError
	// asyncSystemError: the target could not be reached.
	asyncSystemError
	// asyncPermanentFailure: the event can never be delivered (e.g. bad endpoint URL).
	asyncPermanentFailure
)

// deliver hands the event to its deliverer, retrying failures until the
// event is delivered, exhausts its function-error retries, or expires. The
// event's work count — acquired in enqueue — is released when it reaches a
// terminal state, which is exactly what DeleteFunction waits for.
func (d *asyncDispatcher) deliver(ctx context.Context, g *functionGeneration, ev *asyncEvent) {
	defer g.workDone()

	backoff := d.initialBackoff
	functionErrorRetries := 0

	for {
		switch ev.deliverer.deliver(ctx, g.name, ev.payload) {
		case asyncDelivered, asyncPermanentFailure:
			return
		case asyncFunctionError:
			if functionErrorRetries >= asyncMaxFunctionErrorRetries {
				slog.Error("async invoke dropped after function error retries",
					"function", g.name, "retries", functionErrorRetries)

				return
			}

			functionErrorRetries++
		case asyncSystemError:
			if time.Now().After(ev.deadline) {
				slog.Error("async invoke dropped, event expired", "function", g.name)

				return
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		backoff = min(backoff*2, d.maxBackoff)
	}
}

// endpointDeliverer delivers an event by POSTing it to a function's
// InvokeEndpoint. Counterpart of runtimeDeliverer (runtime.go) for functions
// backed by a Runtime API handler.
type endpointDeliverer struct {
	client   *http.Client
	endpoint string

	// gate serializes this attempt's HTTP call against every other
	// InvokeEndpoint call (sync or async) for the same generation.
	gate invokeGate
}

// deliver performs a single delivery attempt.
func (e *endpointDeliverer) deliver(ctx context.Context, functionName string, payload []byte) deliveryResult {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.endpoint, bytes.NewReader(payload))
	if err != nil {
		slog.Error("async invoke failed to create request", "function", functionName, "error", err)

		return asyncPermanentFailure
	}

	req.Header.Set("Content-Type", "application/json")

	if !e.gate.acquire(ctx) {
		// The dispatcher is shutting down (or the event's own context was
		// canceled): give up waiting on a peer holding the gate rather than
		// blocking asyncDispatcher.close forever.
		return asyncSystemError
	}

	resp, err := e.client.Do(req)
	e.gate.release()

	if err != nil {
		slog.Warn("async invoke attempt failed, will retry", "function", functionName, "error", err)

		return asyncSystemError
	}

	_ = resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		slog.Warn("async invoke returned error status", "function", functionName, "status", resp.StatusCode)

		return asyncFunctionError
	}

	return asyncDelivered
}
