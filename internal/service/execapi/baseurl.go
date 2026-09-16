package execapi

// DefaultBaseURL is the fallback kumo server URL for execute-api
// self-calls when a service has not been configured with the URL of its
// owning listener. Normally every service receives the owning server's
// actual address through service.Deps.BaseURL at construction time.
const DefaultBaseURL = "http://localhost:4566"
