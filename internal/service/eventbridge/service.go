// Package eventbridge provides AWS EventBridge service emulation.
package eventbridge

import (
	"fmt"
	"io"

	"github.com/sivchari/kumo/internal/service"
)

// Compile-time check that Service implements io.Closer.
var _ io.Closer = (*Service)(nil)

// Service implements the EventBridge service.
type Service struct {
	storage Storage
}

// New creates a new EventBridge service.
func New(storage Storage) *Service {
	return &Service{storage: storage}
}

// Name returns the service name.
func (s *Service) Name() string {
	return "events"
}

// TargetPrefix returns the X-Amz-Target prefix.
func (s *Service) TargetPrefix() string {
	return "AWSEvents"
}

// JSONProtocol marks this service as using JSON protocol.
func (s *Service) JSONProtocol() {}

// RegisterRoutes registers the service routes.
func (s *Service) RegisterRoutes(r service.Router) {
	// kumo-specific endpoint for testing.
	r.HandleFunc("GET", "/kumo/eventbridge/delivered-events", s.GetDeliveredEvents)
}

func init() {
	service.Register(func(d service.Deps) service.Service {
		opts := []Option{WithBaseURL(d.BaseURL)}

		if dir := d.DataDir; dir != "" {
			opts = append(opts, WithDataDir(dir))
		}

		return New(NewMemoryStorage(opts...))
	})
}

// Close saves the storage state if persistence is enabled.
func (s *Service) Close() error {
	if c, ok := s.storage.(io.Closer); ok {
		if err := c.Close(); err != nil {
			return fmt.Errorf("failed to close storage: %w", err)
		}
	}

	return nil
}

// Meta returns the service's documentation metadata.
func (s *Service) Meta() service.Meta {
	return service.Meta{
		Display:     "EventBridge",
		Category:    "Messaging & Integration",
		Description: "Event bus",
	}
}
