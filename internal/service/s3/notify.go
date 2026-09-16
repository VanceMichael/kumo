package s3

import (
	"context"
	"fmt"
	"sync"
)

// notificationTracker tracks the event-notification deliveries that S3
// accepts together with a successful object write (PutObject, CopyObject,
// POST Object and CompleteMultipartUpload).
//
// During normal operation deliveries remain detached and asynchronous,
// preserving the fire-and-forget behavior clients expect. Shutdown drains
// the outstanding set so a SIGTERM landing immediately after a write
// returned success does not strand the SQS, SNS, Lambda and EventBridge
// notifications that write promised.
type notificationTracker struct {
	// wg counts deliveries in flight. goNotify increments it synchronously
	// in the calling HTTP handler, so every delivery a successful response
	// promised is registered before the handler returns and
	// http.Server.Shutdown's in-flight handler wait completes.
	wg sync.WaitGroup

	// root is the parent context of every tracked delivery. It is
	// uncanceled during normal operation (deliveries behave as they did
	// with context.Background) and is canceled when a drain deadline
	// expires, aborting calls that are still in flight.
	root   context.Context
	cancel context.CancelFunc

	// once/settled/result make draining a single-flight: concurrent and
	// repeated Shutdown calls observe one settlement of the same batch of
	// work. result is written before settled is closed.
	once    sync.Once
	settled chan struct{}
	result  error
}

func newNotificationTracker() *notificationTracker {
	ctx, cancel := context.WithCancel(context.Background())

	return &notificationTracker{
		root:    ctx,
		cancel:  cancel,
		settled: make(chan struct{}),
	}
}

// goNotify runs fn in a goroutine counted as in-flight notification work.
// The WaitGroup counter is incremented synchronously before the goroutine
// is scheduled, so no accepted delivery can escape a subsequent drain.
func (t *notificationTracker) goNotify(fn func(ctx context.Context)) {
	t.wg.Add(1)

	go func() {
		defer t.wg.Done()

		fn(t.root)
	}()
}

// Shutdown waits for every accepted notification delivery to reach a
// terminal state (success or failure). It is safe for concurrent and
// repeated use: the first call settles the outstanding batch once and
// every caller observes the same stored result.
//
// If a temporarily blocked target recovers and its delivery finishes
// before ctx expires, Shutdown waits for it and returns nil. If ctx
// expires first, in-flight deliveries are canceled through their context
// and Shutdown waits for their goroutines to exit before returning an
// error that wraps context.DeadlineExceeded (recognizable via errors.Is),
// so no notification goroutine outlives the server. A caller whose own
// ctx expires while a settlement started by another caller is still
// running receives a wrap of its own ctx.Err() without triggering a
// second settlement.
func (t *notificationTracker) Shutdown(ctx context.Context) error {
	t.once.Do(func() {
		go t.settle(ctx)
	})

	select {
	case <-t.settled:
		return t.result
	case <-ctx.Done():
		return fmt.Errorf("s3 notification drain: %w", ctx.Err())
	}
}

// settle runs once under t.once. It waits for the tracked deliveries and,
// on deadline, cancels them before waiting again so cancellation cannot
// leak goroutines.
func (t *notificationTracker) settle(ctx context.Context) {
	completed := make(chan struct{})

	go func() {
		t.wg.Wait()
		close(completed)
	}()

	select {
	case <-completed:
		t.result = nil
	case <-ctx.Done():
		// Cancel every delivery still in flight, then wait for the
		// goroutines to actually observe it and exit. The %w wrap keeps
		// errors.Is(err, context.DeadlineExceeded) true for callers that
		// need to recognize the timeout.
		t.cancel()
		t.wg.Wait()
		t.result = fmt.Errorf("s3 notification drain: %w", ctx.Err())
	}

	// Releases the root context on the all-delivered path as well; cancel
	// is a no-op for deliveries already gone and is safe to call twice.
	t.cancel()
	close(t.settled)
}
