package server

import (
	"net/http"
	"net/http/httptest"

	"github.com/sivchari/kumo/internal/service"
	"github.com/sivchari/kumo/internal/service/s3"
)

// httpDoer is the minimal HTTP client surface the internal notification
// adapters depend on. *http.Client satisfies it.
type httpDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// inProcessDoer delivers internal HTTP requests straight into the server's
// own router without touching a TCP listener.
//
// S3 event-notification deliveries to Lambda (async Invoke) and EventBridge
// (PutEvents) use it so those targets stay reachable during graceful
// shutdown: http.Server.Shutdown closes the network listener before the
// notification drain begins, and a self-HTTP call would then fail with a
// connection refusal. In-process dispatch keeps the exact handler-level
// semantics (routing, status codes, payloads) while requiring no listener,
// so target services remain available until the drain completes and their
// own Close runs afterwards.
type inProcessDoer struct {
	handler http.Handler
}

// Do dispatches req through the embedded handler synchronously and converts
// the recorded result into an *http.Response. The call runs inline, so the
// request context — including the shutdown deadline — is honored end to
// end and a canceled or expired context aborts the handler like a real
// transport abort would.
func (d *inProcessDoer) Do(req *http.Request) (*http.Response, error) {
	// http.Handler.ServeHTTP requires RequestURI to be empty; requests
	// built with http.NewRequest already satisfy that, and clearing it
	// keeps the adapter safe for any caller.
	req.RequestURI = ""

	if req.Body == nil {
		req.Body = http.NoBody
	}

	rec := httptest.NewRecorder()
	d.handler.ServeHTTP(rec, req)

	return rec.Result(), nil
}

// wireS3EventBridgeInProcess installs the in-process HTTP client on the S3
// service so Object Created events reach EventBridge without a network
// listener. It must run after the S3 service has been registered; without
// an S3 service there is nothing to wire.
func wireS3EventBridgeInProcess(registry *service.Registry, internal httpDoer) {
	s3Svc, ok := registry.Get("s3")
	if !ok {
		return
	}

	s3Typed, ok := s3Svc.(*s3.Service)
	if !ok {
		return
	}

	s3Typed.SetEventBridgeClient(internal)
}
