package lambda

import (
	"context"
	"sync"
)

// functionGeneration is one CreateFunction incarnation of a function name.
// It binds every piece of per-function state that must share the function's
// lifecycle: the resource in storage, the async event queue, the
// InvokeEndpoint serialization gate, and the Runtime API poll/pending
// state. DeleteFunction drains and retires a generation as a single unit;
// a same-name CreateFunction afterwards always starts a fresh generation
// that inherits none of the old queue, gate, payloads or pollers.
type functionGeneration struct {
	id   uint64
	name string

	// shutdown is closed when the whole service closes; long polls and
	// deliveries select on it in addition to their own contexts.
	shutdown <-chan struct{}

	// admMu is the linearization point between DeleteFunction and Invoke
	// admission. Delete flips deleting under it before waiting on inflight;
	// every admitted invocation increments inflight under it. A 202 (or
	// 200) is written only after that increment, so for every invocation
	// either the admission is visible before the boundary (delete waits
	// for it) or the boundary is visible first (admission rejects it) —
	// never both. zeroWaiters are signaled once inflight reaches zero; the
	// counter-based design (rather than a WaitGroup) stays correct when a
	// timed-out delete is aborted and the generation is admitted to again.
	admMu       sync.Mutex
	deleting    bool
	inflight    int
	zeroWaiters []chan struct{}

	// stopDrain is closed once, after work has reached zero, to release
	// the generation's drain goroutine.
	stopDrain chan struct{}

	// gate serializes InvokeEndpoint HTTP calls (sync and async) for this
	// generation. A recreated function gets a brand-new gate.
	gate invokeGate

	// queue is the FIFO channel consumed by the async dispatcher drain
	// goroutine; it and drainStarted are created lazily on the first Event
	// invocation, under admMu.
	queue        chan *asyncEvent
	drainStarted bool

	// Runtime API state for this generation: an unbuffered handoff
	// channel, response channels of in-flight invocations keyed by request
	// id, and registered which becomes true the first time a handler polls
	// next on THIS generation — an older generation's poller never counts.
	rtMu        sync.Mutex
	registered  bool
	invocations chan *runtimeInvocation
	pending     map[string]chan runtimeResult

	// deleteCtx is canceled while a DeleteFunction call is draining this
	// generation: it wakes idle RuntimeNext long polls and blocks new
	// runtime handoffs. abortDeletion replaces it with a fresh context so
	// the generation is fully usable again after a failed (timed-out)
	// delete, without ever "uncanceling" a channel.
	sigMu        sync.Mutex
	deleteCtx    context.Context
	deleteCancel context.CancelFunc
}

func newFunctionGeneration(id uint64, name string, shutdown <-chan struct{}) *functionGeneration {
	deleteCtx, cancel := context.WithCancel(context.Background())

	return &functionGeneration{
		id:           id,
		name:         name,
		shutdown:     shutdown,
		stopDrain:    make(chan struct{}),
		gate:         newInvokeGate(),
		invocations:  make(chan *runtimeInvocation),
		pending:      make(map[string]chan runtimeResult),
		deleteCtx:    deleteCtx,
		deleteCancel: cancel,
	}
}

// admit registers one unit of in-flight work against the generation. It
// returns false once a DeleteFunction drain has started, in which case the
// caller must not touch the generation's queue, gate or runtime state.
func (g *functionGeneration) admit() bool {
	g.admMu.Lock()
	defer g.admMu.Unlock()

	if g.deleting {
		return false
	}

	g.inflight++

	return true
}

// workDone releases one unit of admitted work. If this brings the count to
// zero, every pending drain wait is signaled exactly once.
func (g *functionGeneration) workDone() {
	g.admMu.Lock()
	defer g.admMu.Unlock()

	g.inflight--

	if g.inflight < 0 {
		panic("lambda: functionGeneration inflight counter went negative")
	}

	if g.inflight == 0 && len(g.zeroWaiters) > 0 {
		for _, ch := range g.zeroWaiters {
			close(ch)
		}

		g.zeroWaiters = nil
	}
}

// beginDeletion establishes the delete boundary: subsequent admissions
// fail, queued deliveries keep draining, and idle RuntimeNext polls are
// woken through the delete signal.
func (g *functionGeneration) beginDeletion() {
	g.admMu.Lock()
	g.deleting = true
	g.admMu.Unlock()

	g.sigMu.Lock()
	g.deleteCancel()
	g.sigMu.Unlock()
}

// abortDeletion rolls back the boundary when the drain cannot finish in
// the request deadline. The function stays queryable and usable; in-flight
// deliveries were never interrupted.
func (g *functionGeneration) abortDeletion() {
	ctx, cancel := context.WithCancel(context.Background())

	g.sigMu.Lock()
	g.deleteCtx = ctx
	g.deleteCancel = cancel
	g.sigMu.Unlock()

	g.admMu.Lock()
	g.deleting = false
	g.admMu.Unlock()
}

// deletionSignal returns the channel closed while a delete is draining
// this generation.
func (g *functionGeneration) deletionSignal() <-chan struct{} {
	g.sigMu.Lock()
	defer g.sigMu.Unlock()

	return g.deleteCtx.Done()
}

// waitWork blocks until every admitted invocation has finished or until
// ctx expires. It never interrupts the work itself: on timeout the caller
// aborts the boundary and the function keeps running. A timeout does not
// leak a waiter that would conflict with later admissions — the registered
// channel simply fires (harmlessly) the next time the count reaches zero.
func (g *functionGeneration) waitWork(ctx context.Context) error {
	zero := make(chan struct{})

	g.admMu.Lock()
	if g.inflight == 0 {
		g.admMu.Unlock()

		return nil
	}

	g.zeroWaiters = append(g.zeroWaiters, zero)
	g.admMu.Unlock()

	select {
	case <-zero:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// stopDrainLoop releases the generation's drain goroutine after waitWork
// has returned successfully.
func (g *functionGeneration) stopDrainLoop() {
	close(g.stopDrain)
}

// lifecycleRegistry owns the live function generations keyed by function
// name and the per-name mutexes that serialize Create/Delete operations.
// All map access is short; no wait ever happens while mu is held.
type lifecycleRegistry struct {
	mu       sync.Mutex
	gens     map[string]*functionGeneration
	opLocks  map[string]*sync.Mutex
	nextID   uint64
	shutdown <-chan struct{}
}

func newLifecycleRegistry(shutdown <-chan struct{}) *lifecycleRegistry {
	return &lifecycleRegistry{
		gens:     make(map[string]*functionGeneration),
		opLocks:  make(map[string]*sync.Mutex),
		shutdown: shutdown,
	}
}

// opLock returns the mutex serializing CreateFunction and DeleteFunction
// for one name. Distinct names get distinct mutexes, so a draining delete
// on one function never blocks operations on another.
func (r *lifecycleRegistry) opLock(name string) *sync.Mutex {
	r.mu.Lock()
	defer r.mu.Unlock()

	m := r.opLocks[name]
	if m == nil {
		m = &sync.Mutex{}
		r.opLocks[name] = m
	}

	return m
}

// create inserts the resource through commit and, on success, the fresh
// generation in the same critical section. The lock order is
// registry.mu -> storage.mu everywhere.
func (r *lifecycleRegistry) create(name string, commit func() (*Function, error)) (*Function, *functionGeneration, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	fn, err := commit()
	if err != nil {
		return nil, nil, err
	}

	r.nextID++
	g := newFunctionGeneration(r.nextID, name, r.shutdown)
	r.gens[name] = g

	return fn, g, nil
}

// bootstrap creates generations for functions already present in storage
// (e.g. restored from a persistence snapshot at startup).
func (r *lifecycleRegistry) bootstrap(names []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, name := range names {
		if _, ok := r.gens[name]; ok {
			continue
		}

		r.nextID++
		r.gens[name] = newFunctionGeneration(r.nextID, name, r.shutdown)
	}
}

// get returns the current generation for name, or nil if no live function
// generation exists.
func (r *lifecycleRegistry) get(name string) *functionGeneration {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.gens[name]
}

// retire removes the generation and commits the storage deletion in one
// critical section, after the caller has fully drained it. The opLock
// entry is removed as well; the mutex itself stays valid for any caller
// still holding it.
func (r *lifecycleRegistry) retire(name string, g *functionGeneration, commit func() error) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if current := r.gens[name]; current != g {
		return &FunctionError{
			Type:    ErrServiceException,
			Message: "function generation changed while deleting: " + name,
		}
	}

	if err := commit(); err != nil {
		return err
	}

	delete(r.gens, name)
	delete(r.opLocks, name)

	return nil
}
