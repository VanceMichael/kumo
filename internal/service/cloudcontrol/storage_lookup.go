package cloudcontrol

import (
	"fmt"

	"github.com/sivchari/kumo/internal/service"
)

// serviceResolver returns a sibling service of the same server instance
// by name. A nil resolver represents a registry without access to other
// services (e.g. focused unit tests).
type serviceResolver func(name string) (service.Service, bool)

// lookupStorage finds the Service named serviceName through the owning
// server instance's resolver and casts it to a type exposing
// `Storage() T`, letting resource handlers share the same in-memory store
// as the underlying service of their own server instance — never another
// concurrently running instance's store.
func lookupStorage[T any](resolve serviceResolver, serviceName string) (T, error) {
	var zero T

	if resolve == nil {
		return zero, fmt.Errorf("%s service is not available in this registry", serviceName)
	}

	svc, ok := resolve(serviceName)
	if !ok {
		return zero, fmt.Errorf("%s service is not registered", serviceName)
	}

	provider, ok := svc.(interface{ Storage() T })
	if !ok {
		return zero, fmt.Errorf("%s service does not expose Storage() returning the expected type", serviceName)
	}

	return provider.Storage(), nil
}
