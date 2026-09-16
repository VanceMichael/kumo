package service

import (
	"os"
	"sync"

	"github.com/sivchari/kumo/internal/streams"
)

// Deps carries the per-server inputs every Factory receives. A server
// builds one Deps and uses it for every service it creates, so all
// services of one server instance share that instance's identity
// (BaseURL), persistence scope (DataDir) and cross-service state
// (Streams) and never the state of another concurrently running server.
type Deps struct {
	// BaseURL is the URL clients use to reach the owning server listener,
	// e.g. http://127.0.0.1:51342 for an in-process test server or
	// http://localhost:4566 for the CLI. Services embed it in resource
	// URLs they return (QueueUrl, function locations, ...) and use it for
	// loopback event delivery (S3 -> EventBridge, ...).
	BaseURL string

	// DataDir is the snapshot directory. Empty means the instance is
	// fully ephemeral: nothing is loaded or persisted.
	DataDir string

	// Streams is the DynamoDB stream store shared between the dynamodb
	// (producer) and dynamodbstreams (consumer) services of the same
	// server instance.
	Streams *streams.Store

	// Resolve returns another service of the same server instance by
	// name. It is safe to call at request time (after the whole instance
	// has been built); calling it during construction may miss a service
	// that has not been constructed yet.
	Resolve func(name string) (Service, bool)
}

// DefaultDeps returns Deps derived from the process environment. It keeps
// the historical single-process conventions: KUMO_DATA_DIR enables
// persistence and the loopback URL is localhost:4566 (or KUMO_HOST /
// KUMO_PORT when set). It is used for the process-wide singleton set and
// for standalone usage; multi-instance callers (the in-process test
// server) build their own Deps instead.
func DefaultDeps() Deps {
	host := os.Getenv("KUMO_HOST")
	port := os.Getenv("KUMO_PORT")

	switch host {
	case "", "0.0.0.0", "::", "[::]":
		host = "localhost"
	}

	if port == "" {
		port = "4566"
	}

	return Deps{
		BaseURL: "http://" + host + ":" + port,
		DataDir: os.Getenv("KUMO_DATA_DIR"),
		Streams: streams.NewStore(),
	}
}

// Factory constructs a fresh service for one server instance. Each
// service package registers a Factory from its init() instead of a
// ready-made singleton, so every server.New gets independent services
// and storage.
type Factory func(Deps) Service

var (
	factoryMu sync.Mutex
	factories []Factory
)

// Register adds a service Factory to the process-wide factory catalog.
// It is called from each service package's init().
func Register(f Factory) {
	factoryMu.Lock()
	defer factoryMu.Unlock()

	factories = append(factories, f)
}

// Factories returns a copy of every registered Factory.
func Factories() []Factory {
	factoryMu.Lock()
	defer factoryMu.Unlock()

	out := make([]Factory, len(factories))
	copy(out, factories)

	return out
}

// BuildAll constructs a fresh instance of every registered service with
// the given deps.
func BuildAll(deps Deps) []Service {
	svcs := make([]Service, 0, len(factories))

	for _, f := range Factories() {
		svcs = append(svcs, f(deps))
	}

	return svcs
}

// defaultInstances is the lazily built process-wide singleton set. It
// preserves the historical behavior of Services() (used by the CLI /
// README generators, which only introspect metadata) where every call
// returns the same instances.
var (
	defaultOnce      sync.Once
	defaultInstances []Service
)

// Services returns the process-wide singleton service set, building it
// once from DefaultDeps on first use. Runtime code that needs isolated
// instances must use Factories/BuildAll instead.
func Services() []Service {
	defaultOnce.Do(func() {
		defaultInstances = BuildAll(DefaultDeps())
	})

	out := make([]Service, len(defaultInstances))
	copy(out, defaultInstances)

	return out
}

// Registry manages service registration and discovery.
type Registry struct {
	mu       sync.RWMutex
	services map[string]Service
}

// NewRegistry creates a new service registry.
func NewRegistry() *Registry {
	return &Registry{
		services: make(map[string]Service),
	}
}

// Register adds a service to the registry.
func (r *Registry) Register(svc Service) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.services[svc.Name()] = svc
}

// Get returns a service by name.
func (r *Registry) Get(name string) (Service, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	svc, ok := r.services[name]

	return svc, ok
}

// All returns all registered services.
func (r *Registry) All() []Service {
	r.mu.RLock()
	defer r.mu.RUnlock()

	services := make([]Service, 0, len(r.services))

	for _, svc := range r.services {
		services = append(services, svc)
	}

	return services
}

// Names returns the names of all registered services.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.services))

	for name := range r.services {
		names = append(names, name)
	}

	return names
}
