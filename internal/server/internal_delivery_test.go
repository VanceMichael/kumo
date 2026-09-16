package server

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestInProcessDoer_DispatchesWithoutListener verifies the in-process
// transport reaches the registered handler and preserves the status code and
// body without any network listener — the property S3 notification delivery
// to Lambda/EventBridge depends on during shutdown.
func TestInProcessDoer_DispatchesWithoutListener(t *testing.T) {
	t.Parallel()

	router := NewRouter(discardLogger())
	router.Handle("POST", "/internal-target", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("ReadAll: %v", err)
		}

		w.Header().Set("X-Kumo-Test", string(body))
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("accepted"))
	})

	doer := &inProcessDoer{handler: router}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		"http://localhost/internal-target", strings.NewReader("payload"))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	resp, err := doer.Do(req)
	if err != nil {
		t.Fatalf("Do() error = %v, want nil", err)
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}

	if got := resp.Header.Get("X-Kumo-Test"); got != "payload" {
		t.Fatalf("handler saw body %q, want payload", got)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if string(body) != "accepted" {
		t.Fatalf("response body = %q, want accepted", body)
	}
}

// TestInProcessDoer_ContextCancelUnwindsHandler verifies a canceled request
// context unblocks the in-flight handler and Do returns only after the
// handler goroutine has exited — the no-leak guarantee the shutdown deadline
// relies on.
func TestInProcessDoer_ContextCancelUnwindsHandler(t *testing.T) {
	t.Parallel()

	handlerEntered := make(chan struct{})
	handlerExited := make(chan struct{})

	router := NewRouter(discardLogger())
	router.Handle("POST", "/blocked", func(_ http.ResponseWriter, r *http.Request) {
		close(handlerEntered)
		<-r.Context().Done()
		close(handlerExited)
	})

	doer := &inProcessDoer{handler: router}

	ctx, cancel := context.WithCancel(context.Background())

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost/blocked", http.NoBody)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	done := make(chan struct{})

	go func() {
		defer close(done)

		resp, err := doer.Do(req)
		if err == nil {
			_ = resp.Body.Close()
		}
	}()

	<-handlerEntered

	cancel()

	select {
	case <-handlerExited:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not unwind after request context cancel")
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Do did not return after the handler unwound")
	}
}
