// Package kumo provides a public API for running an in-process AWS service emulator.
//
// Usage:
//
//	srv := kumo.NewServer()
//	defer srv.Close()
//
//	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
//	    o.BaseEndpoint = aws.String(srv.URL)
//	})
package kumo

import (
	"net"
	"net/http/httptest"
	"strconv"
	"sync"

	// Register all services via init(). See internal/registry for the
	// single canonical list shared with the CLI and the README generator.
	_ "github.com/sivchari/kumo/internal/registry"
	"github.com/sivchari/kumo/internal/server"
)

// Server is an in-process AWS service emulator.
// It wraps httptest.Server to provide a familiar API for Go testing.
type Server struct {
	// URL is the base URL of the server in the form "http://host:port".
	URL string

	httpServer *httptest.Server
	internal   *server.Server
	closeOnce  sync.Once
}

// NewServer creates and starts a new in-process AWS emulator server.
// The server listens on a random available port on localhost.
// Use srv.URL as the BaseEndpoint for AWS SDK clients.
//
// Every server owns an independent set of services, storage and
// background tasks: multiple NewServer calls in one test binary never
// share queues, buckets or events, even for identically named resources.
// Each instance is fully ephemeral — KUMO_DATA_DIR is intentionally
// ignored here so instances cannot share on-disk state; the CLI keeps
// KUMO_DATA_DIR persistence.
func NewServer() *Server {
	// Bind the listener before constructing services so every service is
	// configured with the actual random address. Resource URLs returned to
	// clients (QueueUrl, function locations, ...) and all loopback event
	// delivery then target this exact listener rather than localhost:4566.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}

	host, portStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		panic(err)
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		panic(err)
	}

	cfg := server.DefaultConfig()
	cfg.LogLevel = 100 // Suppress all logs in test mode.
	cfg.Host = host
	cfg.Port = port
	cfg.DataDir = "" // Ephemeral: instances must not share snapshots.

	internalSrv := server.New(cfg)

	ts := httptest.NewUnstartedServer(internalSrv.Handler())
	// Discard the placeholder listener httptest allocated and serve on
	// the listener whose address the services were built against.
	_ = ts.Listener.Close()
	ts.Listener = ln
	ts.Start()

	return &Server{
		URL:        ts.URL,
		httpServer: ts,
		internal:   internalSrv,
	}
}

// Close shuts down the server and releases its background work.
// It stops accepting requests and waits for in-flight handlers to finish
// before stopping the instance's own dispatchers/delivery goroutines.
// It is safe to call Close repeatedly or concurrently; only the first
// call performs the shutdown.
func (s *Server) Close() {
	s.closeOnce.Do(func() {
		// Stop accepting and let in-flight handlers finish first, so no
		// handler mutates state or enqueues deliveries during shutdown.
		s.httpServer.Close()

		// Then stop this instance's own dispatchers/delivery goroutines
		// and flush any persistence state.
		_ = s.internal.Close()
	})
}
