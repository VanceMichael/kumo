package lambda

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/sivchari/kumo/internal/service"
)

const pathSegmentFunctions = "functions"

// CreateFunction handles the CreateFunction API.
func (s *Service) CreateFunction(w http.ResponseWriter, r *http.Request) {
	var req CreateFunctionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeFunctionError(w, ErrInvalidParameterValue, "Invalid request body", http.StatusBadRequest)

		return
	}

	if req.FunctionName == "" {
		writeFunctionError(w, ErrInvalidParameterValue, "FunctionName is required", http.StatusBadRequest)

		return
	}

	if req.Role == "" {
		writeFunctionError(w, ErrInvalidParameterValue, "Role is required", http.StatusBadRequest)

		return
	}

	// Serialize against a concurrent DeleteFunction (and CreateFunction)
	// for the same name so resource insertion and the fresh generation are
	// committed as one step.
	op := s.lifecycles.opLock(req.FunctionName)
	op.Lock()
	defer op.Unlock()

	fn, _, err := s.lifecycles.create(req.FunctionName, func() (*Function, error) {
		return s.storage.CreateFunction(r.Context(), &req)
	})
	if err != nil {
		var lambdaErr *FunctionError
		if errors.As(err, &lambdaErr) {
			status := http.StatusBadRequest
			if lambdaErr.Type == ErrResourceConflict {
				status = http.StatusConflict
			}

			writeFunctionError(w, lambdaErr.Type, lambdaErr.Message, status)

			return
		}

		writeFunctionError(w, ErrServiceException, "Internal server error", http.StatusInternalServerError)

		return
	}

	resp := functionToCreateResponse(fn)
	writeJSONResponse(w, http.StatusCreated, resp)
}

// GetFunction handles the GetFunction API.
func (s *Service) GetFunction(w http.ResponseWriter, r *http.Request) {
	functionName := extractFunctionName(r.URL.Path)
	if functionName == "" {
		writeFunctionError(w, ErrInvalidParameterValue, "FunctionName is required", http.StatusBadRequest)

		return
	}

	fn, err := s.storage.GetFunction(r.Context(), functionName)
	if err != nil {
		var lambdaErr *FunctionError
		if errors.As(err, &lambdaErr) {
			status := http.StatusBadRequest
			if lambdaErr.Type == ErrResourceNotFound {
				status = http.StatusNotFound
			}

			writeFunctionError(w, lambdaErr.Type, lambdaErr.Message, status)

			return
		}

		writeFunctionError(w, ErrServiceException, "Internal server error", http.StatusInternalServerError)

		return
	}

	// Snapshot the tags through storage so serializing the response cannot
	// race with concurrent TagResource/UntagResource mutations.
	tags, err := s.storage.ListTags(r.Context(), fn.FunctionArn)
	if err != nil {
		writeFunctionError(w, ErrServiceException, "Internal server error", http.StatusInternalServerError)

		return
	}

	resp := &GetFunctionResponse{
		Configuration: functionToConfiguration(fn),
		Code: &FunctionCodeLocation{
			RepositoryType: "S3",
			Location:       s.baseURL + "/lambda-code/" + functionName,
		},
		Tags: tags,
	}

	writeJSONResponse(w, http.StatusOK, resp)
}

// GetFunctionConfiguration handles GET /functions/{name}/configuration.
// Returns only the configuration portion of GetFunction.
func (s *Service) GetFunctionConfiguration(w http.ResponseWriter, r *http.Request) {
	functionName := extractFunctionName(r.URL.Path)
	if functionName == "" {
		writeFunctionError(w, ErrInvalidParameterValue, "FunctionName is required", http.StatusBadRequest)

		return
	}

	fn, err := s.storage.GetFunction(r.Context(), functionName)
	if err != nil {
		handleGetFunctionError(w, err)

		return
	}

	writeJSONResponse(w, http.StatusOK, functionToConfiguration(fn))
}

// DeleteFunction handles the DeleteFunction API.
//
// Deletion is two-phased against the function's generation: first a
// boundary is set that rejects new Invoke admissions and wakes idle
// Runtime API long polls, then the call waits for every invocation admitted
// before the boundary (synchronous and asynchronous, endpoint and runtime)
// to finish. The storage resource is removed and the generation retired
// only once that work has fully exited. If the drain cannot finish within
// the request deadline, the boundary is rolled back and the request fails:
// the function stays queryable and usable so the caller can restore the
// execution target and retry. A same-name CreateFunction afterwards starts
// a brand-new generation that inherits no queue, gate, payload or poller.
func (s *Service) DeleteFunction(w http.ResponseWriter, r *http.Request) {
	functionName := extractFunctionName(r.URL.Path)
	if functionName == "" {
		writeFunctionError(w, ErrInvalidParameterValue, "FunctionName is required", http.StatusBadRequest)

		return
	}

	// Serialize against CreateFunction and a concurrent DeleteFunction for
	// the same name. This is a per-name lock: deleting one function never
	// blocks operations on another.
	op := s.lifecycles.opLock(functionName)
	op.Lock()
	defer op.Unlock()

	if _, err := s.storage.GetFunction(r.Context(), functionName); err != nil {
		handleGetFunctionError(w, err)

		return
	}

	g := s.lifecycles.get(functionName)
	if g == nil {
		writeFunctionError(w, ErrServiceException, "Internal server error", http.StatusInternalServerError)

		return
	}

	// Phase 1: establish the delete boundary. Invokes admitted after this
	// point are rejected; queued and in-flight work keeps running.
	g.beginDeletion()

	// Wait for all pre-boundary work to end. The work itself is never
	// interrupted here; a timeout means convergence was impossible within
	// the request deadline.
	if err := g.waitWork(r.Context()); err != nil {
		// Roll the boundary back: the function remains queryable and fully
		// usable, including its queued deliveries and pollers.
		g.abortDeletion()

		writeFunctionError(w, ErrResourceConflict,
			fmt.Sprintf("Function %s could not be drained before the request deadline (%v); the function was retained and is still usable", functionName, err),
			http.StatusConflict)

		return
	}

	// Phase 2: the old generation has fully exited. Commit resource
	// removal plus generation retirement first; only then release the
	// (idle) drain goroutine. If the commit somehow fails the boundary is
	// rolled back and the generation — drain included — is fully usable.
	if err := s.lifecycles.retire(functionName, g, func() error {
		return s.storage.DeleteFunction(r.Context(), functionName)
	}); err != nil {
		g.abortDeletion()

		handleFunctionError(w, err)

		return
	}

	g.stopDrainLoop()

	w.WriteHeader(http.StatusNoContent)
}

// ListFunctions handles the ListFunctions API.
func (s *Service) ListFunctions(w http.ResponseWriter, r *http.Request) {
	marker := r.URL.Query().Get("Marker")
	maxItemsStr := r.URL.Query().Get("MaxItems")

	maxItems := 50

	if maxItemsStr != "" {
		if parsed, err := strconv.Atoi(maxItemsStr); err == nil {
			maxItems = parsed
		}
	}

	functions, nextMarker, err := s.storage.ListFunctions(r.Context(), marker, maxItems)
	if err != nil {
		writeFunctionError(w, ErrServiceException, "Internal server error", http.StatusInternalServerError)

		return
	}

	configs := make([]*FunctionConfiguration, 0, len(functions))
	for _, fn := range functions {
		configs = append(configs, functionToConfiguration(fn))
	}

	resp := &ListFunctionsResponse{
		Functions:  configs,
		NextMarker: nextMarker,
	}

	writeJSONResponse(w, http.StatusOK, resp)
}

// UpdateFunctionCode handles the UpdateFunctionCode API.
func (s *Service) UpdateFunctionCode(w http.ResponseWriter, r *http.Request) {
	functionName := extractFunctionNameFromCodePath(r.URL.Path)
	if functionName == "" {
		writeFunctionError(w, ErrInvalidParameterValue, "FunctionName is required", http.StatusBadRequest)

		return
	}

	var req UpdateFunctionCodeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeFunctionError(w, ErrInvalidParameterValue, "Invalid request body", http.StatusBadRequest)

		return
	}

	fn, err := s.storage.UpdateFunctionCode(r.Context(), functionName, &req)
	if err != nil {
		var lambdaErr *FunctionError
		if errors.As(err, &lambdaErr) {
			status := http.StatusBadRequest
			if lambdaErr.Type == ErrResourceNotFound {
				status = http.StatusNotFound
			}

			writeFunctionError(w, lambdaErr.Type, lambdaErr.Message, status)

			return
		}

		writeFunctionError(w, ErrServiceException, "Internal server error", http.StatusInternalServerError)

		return
	}

	resp := functionToCreateResponse(fn)
	writeJSONResponse(w, http.StatusOK, resp)
}

// UpdateFunctionConfiguration handles the UpdateFunctionConfiguration API.
func (s *Service) UpdateFunctionConfiguration(w http.ResponseWriter, r *http.Request) {
	functionName := extractFunctionNameFromConfigPath(r.URL.Path)
	if functionName == "" {
		writeFunctionError(w, ErrInvalidParameterValue, "FunctionName is required", http.StatusBadRequest)

		return
	}

	var req UpdateFunctionConfigurationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeFunctionError(w, ErrInvalidParameterValue, "Invalid request body", http.StatusBadRequest)

		return
	}

	fn, err := s.storage.UpdateFunctionConfiguration(r.Context(), functionName, &req)
	if err != nil {
		var lambdaErr *FunctionError
		if errors.As(err, &lambdaErr) {
			status := http.StatusBadRequest
			if lambdaErr.Type == ErrResourceNotFound {
				status = http.StatusNotFound
			}

			writeFunctionError(w, lambdaErr.Type, lambdaErr.Message, status)

			return
		}

		writeFunctionError(w, ErrServiceException, "Internal server error", http.StatusInternalServerError)

		return
	}

	resp := functionToCreateResponse(fn)
	writeJSONResponse(w, http.StatusOK, resp)
}

// Invoke handles the Invoke API.
func (s *Service) Invoke(w http.ResponseWriter, r *http.Request) {
	functionName := extractFunctionNameFromInvokePath(r.URL.Path)
	if functionName == "" {
		writeFunctionError(w, ErrInvalidParameterValue, "FunctionName is required", http.StatusBadRequest)

		return
	}

	fn, err := s.storage.GetFunction(r.Context(), functionName)
	if err != nil {
		handleGetFunctionError(w, err)

		return
	}

	payload, err := io.ReadAll(r.Body)
	if err != nil {
		writeFunctionError(w, ErrInvalidParameterValue, "Failed to read request body", http.StatusBadRequest)

		return
	}

	invocationType := r.Header.Get("X-Amz-Invocation-Type")
	if invocationType == "" {
		invocationType = "RequestResponse"
	}

	// DryRun validates access without executing the function.
	if invocationType == "DryRun" {
		writeInvokeHeaders(w)
		w.WriteHeader(http.StatusNoContent)

		return
	}

	async := invocationType == "Event"

	g := s.lifecycles.get(functionName)
	if g == nil {
		writeFunctionError(w, ErrResourceNotFound, "Function not found: "+functionName, http.StatusNotFound)

		return
	}

	// Synchronous invocations are admitted here: the work count is
	// acquired (and thus visible to a concurrent DeleteFunction) before
	// anything is dispatched, and released only when this handler returns.
	// Asynchronous admissions happen atomically inside enqueue.
	if !async {
		if !g.admit() {
			writeFunctionError(w, ErrResourceConflict,
				"Function "+functionName+" is being deleted", http.StatusConflict)

			return
		}

		defer g.workDone()
	}

	// Resolution order: a handler polling the Runtime API on THIS
	// generation wins, then a configured InvokeEndpoint, otherwise there
	// is nothing to execute.
	switch {
	case s.broker.registered(g):
		s.invokeViaRuntime(w, r, g, payload, async)
	case fn.InvokeEndpoint != "":
		s.invokeViaEndpoint(w, r, g, fn.InvokeEndpoint, payload, async)
	default:
		s.invokeNoBackend(w, functionName, g, async)
	}
}

// writeFunctionDeleting rejects an invocation that reached a generation
// whose delete boundary has already been set.
func writeFunctionDeleting(w http.ResponseWriter, fn string) {
	writeFunctionError(w, ErrResourceConflict,
		"Function "+fn+" is being deleted", http.StatusConflict)
}

// invokeViaRuntime dispatches to a handler connected through the Runtime API.
func (s *Service) invokeViaRuntime(w http.ResponseWriter, r *http.Request, g *functionGeneration, payload []byte, async bool) {
	if async {
		if !s.async.enqueue(g, &runtimeDeliverer{broker: s.broker, g: g}, payload) {
			writeFunctionDeleting(w, g.name)

			return
		}

		writeInvokeAccepted(w)

		return
	}

	res, err := s.broker.invoke(r.Context(), g, payload, runtimeInvokeTimeout)
	if err != nil {
		writeFunctionError(w, ErrServiceException, "runtime invocation failed: "+err.Error(), http.StatusBadGateway)

		return
	}

	writeInvokeHeaders(w)

	if res.errored {
		w.Header().Set("X-Amz-Function-Error", "Unhandled")
	}

	w.WriteHeader(http.StatusOK)
	writeInvokePayload(w, res.payload)
}

// invokeViaEndpoint forwards to a function's configured InvokeEndpoint.
// Async invocations are queued on the dispatcher so the 202 means "accepted
// for delivery": failed deliveries are retried instead of dropped. Both the
// async delivery attempt and the synchronous call below share the
// generation's gate so a single-concurrency endpoint, such as the Lambda
// RIE, never sees two overlapping requests (issue #859).
func (s *Service) invokeViaEndpoint(w http.ResponseWriter, r *http.Request, g *functionGeneration, endpoint string, payload []byte, async bool) {
	if async {
		if !s.async.enqueue(g, &endpointDeliverer{client: s.async.client, endpoint: endpoint, gate: g.gate}, payload) {
			writeFunctionDeleting(w, g.name)

			return
		}

		writeInvokeAccepted(w)

		return
	}

	s.invokeSync(r.Context(), w, endpoint, payload, g.gate)
}

// invokeNoBackend handles a function with neither a Runtime API handler nor an
// InvokeEndpoint. Async invocations are accepted (and dropped); a
// RequestResponse invocation has nothing to execute and fails — kumo does not
// fabricate an echo response.
func (s *Service) invokeNoBackend(w http.ResponseWriter, fn string, g *functionGeneration, async bool) {
	if async {
		// Nothing is queued or executed, but the boundary still applies:
		// an Event after it must not be reported as accepted.
		if !g.admit() {
			writeFunctionDeleting(w, fn)

			return
		}

		g.workDone()

		writeInvokeAccepted(w)

		return
	}

	writeFunctionError(w, ErrServiceException,
		"function "+fn+" has no runtime handler; run it with AWS_LAMBDA_RUNTIME_API=<kumo>/_runtime/"+fn+" or set InvokeEndpoint",
		http.StatusBadGateway)
}

// writeInvokeAccepted writes the 202 response for an async invocation.
func writeInvokeAccepted(w http.ResponseWriter) {
	writeInvokeHeaders(w)
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte("{}"))
}

// writeInvokePayload writes a sync invocation payload, defaulting to "null".
func writeInvokePayload(w http.ResponseWriter, payload []byte) {
	if len(payload) == 0 {
		_, _ = w.Write([]byte("null"))

		return
	}

	_, _ = w.Write(payload)
}

// handleGetFunctionError writes error response for GetFunction errors.
func handleGetFunctionError(w http.ResponseWriter, err error) {
	var lambdaErr *FunctionError
	if errors.As(err, &lambdaErr) {
		status := http.StatusBadRequest
		if lambdaErr.Type == ErrResourceNotFound {
			status = http.StatusNotFound
		}

		writeFunctionError(w, lambdaErr.Type, lambdaErr.Message, status)

		return
	}

	writeFunctionError(w, ErrServiceException, "Internal server error", http.StatusInternalServerError)
}

// writeInvokeHeaders writes common invoke response headers.
func writeInvokeHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Amz-Executed-Version", "$LATEST")
	w.Header().Set("X-Amz-Request-Id", uuid.New().String())
}

// invokeSync invokes the function synchronously and writes the response.
// gate is held only around the HTTP call so it never overlaps with an async
// delivery attempt for the same function (issue #859). Acquiring is bounded
// by ctx, so a client that disconnects while waiting for a peer to finish
// gives up instead of leaking this goroutine.
func (s *Service) invokeSync(ctx context.Context, w http.ResponseWriter, endpoint string, payload []byte, gate invokeGate) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		writeFunctionError(w, ErrServiceException,
			fmt.Sprintf("Failed to create request: %v", err), http.StatusInternalServerError)

		return
	}

	req.Header.Set("Content-Type", "application/json")

	if !gate.acquire(ctx) {
		writeFunctionError(w, ErrServiceException,
			fmt.Sprintf("Failed to invoke endpoint: %v", ctx.Err()), http.StatusInternalServerError)

		return
	}

	resp, err := http.DefaultClient.Do(req)

	gate.release()

	if err != nil {
		writeFunctionError(w, ErrServiceException,
			fmt.Sprintf("Failed to invoke endpoint: %v", err), http.StatusInternalServerError)

		return
	}

	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		writeFunctionError(w, ErrServiceException,
			"Failed to read response from endpoint", http.StatusInternalServerError)

		return
	}

	writeInvokeHeaders(w)
	w.WriteHeader(http.StatusOK)

	if len(respBody) == 0 {
		_, _ = w.Write([]byte("null"))
	} else {
		_, _ = w.Write(respBody)
	}
}

// extractFunctionName extracts function name from URL paths like:
//
//   - /lambda/2015-03-31/functions/{name}              (SDK BaseEndpoint = .../lambda)
//   - /2015-03-31/functions/{name}                     (terraform-provider-aws, single endpoint)
//   - /lambda/2015-03-31/functions/{name}/code         (sub-resources accepted as well)
//
// Routes are registered for both prefixes, so the helper finds the
// "functions" segment regardless of where it appears in the path. The
// trailing /code, /configuration, /invocations sub-resources are tolerated
// — the dedicated FromXPath helpers below assert which sub-resource was
// matched.
func extractFunctionName(path string) string {
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	for i, p := range parts {
		if p == pathSegmentFunctions && i+1 < len(parts) {
			return parts[i+1]
		}
	}

	return ""
}

// extractFunctionNameFromCodePath returns the function name when the path
// ends in /functions/{name}/code; "" if the shape does not match.
func extractFunctionNameFromCodePath(path string) string {
	return extractFunctionNameFromSubresource(path, "code")
}

// extractFunctionNameFromConfigPath returns the function name when the path
// ends in /functions/{name}/configuration; "" if the shape does not match.
func extractFunctionNameFromConfigPath(path string) string {
	return extractFunctionNameFromSubresource(path, "configuration")
}

// extractFunctionNameFromInvokePath returns the function name when the path
// ends in /functions/{name}/invocations; "" if the shape does not match.
func extractFunctionNameFromInvokePath(path string) string {
	return extractFunctionNameFromSubresource(path, "invocations")
}

// extractFunctionNameFromSubresource returns the function name when the
// path matches /functions/{name}/<sub>, accepting any leading prefix.
func extractFunctionNameFromSubresource(path, sub string) string {
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	for i, p := range parts {
		if p == pathSegmentFunctions && i+2 < len(parts) && parts[i+2] == sub {
			return parts[i+1]
		}
	}

	return ""
}

// functionToCreateResponse converts a Function to CreateFunctionResponse.
func functionToCreateResponse(fn *Function) *CreateFunctionResponse {
	return &CreateFunctionResponse{
		FunctionName:     fn.FunctionName,
		FunctionArn:      fn.FunctionArn,
		Runtime:          fn.Runtime,
		Role:             fn.Role,
		Handler:          fn.Handler,
		CodeSize:         fn.CodeSize,
		Description:      fn.Description,
		Timeout:          fn.Timeout,
		MemorySize:       fn.MemorySize,
		LastModified:     fn.LastModified.Format("2006-01-02T15:04:05.000+0000"),
		CodeSha256:       fn.CodeSha256,
		Version:          fn.Version,
		State:            fn.State,
		StateReason:      fn.StateReason,
		StateReasonCode:  fn.StateReasonCode,
		LastUpdateStatus: fn.LastUpdateStatus,
		PackageType:      fn.PackageType,
		Architectures:    fn.Architectures,
		Environment:      fn.Environment,
	}
}

// functionToConfiguration converts a Function to FunctionConfiguration.
func functionToConfiguration(fn *Function) *FunctionConfiguration {
	return &FunctionConfiguration{
		FunctionName:     fn.FunctionName,
		FunctionArn:      fn.FunctionArn,
		Runtime:          fn.Runtime,
		Role:             fn.Role,
		Handler:          fn.Handler,
		CodeSize:         fn.CodeSize,
		Description:      fn.Description,
		Timeout:          fn.Timeout,
		MemorySize:       fn.MemorySize,
		LastModified:     fn.LastModified.Format("2006-01-02T15:04:05.000+0000"),
		CodeSha256:       fn.CodeSha256,
		Version:          fn.Version,
		State:            fn.State,
		StateReason:      fn.StateReason,
		StateReasonCode:  fn.StateReasonCode,
		LastUpdateStatus: fn.LastUpdateStatus,
		PackageType:      fn.PackageType,
		Architectures:    fn.Architectures,
		Environment:      fn.Environment,
	}
}

// writeJSONResponse writes a JSON response.
func writeJSONResponse(w http.ResponseWriter, status int, v any) {
	service.WriteJSONResponseWithStatus(w, service.ContentTypeJSON, status, v)
}

// writeFunctionError writes a Lambda error response.
func writeFunctionError(w http.ResponseWriter, errType, message string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Amzn-Requestid", uuid.New().String())
	w.Header().Set("X-Amzn-Errortype", errType)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"Type":    errType,
		"Message": message,
	})
}

// CreateEventSourceMapping handles the CreateEventSourceMapping API.
func (s *Service) CreateEventSourceMapping(w http.ResponseWriter, r *http.Request) {
	var req CreateEventSourceMappingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeFunctionError(w, ErrInvalidParameterValue, "Invalid request body", http.StatusBadRequest)

		return
	}

	if req.FunctionName == "" {
		writeFunctionError(w, ErrInvalidParameterValue, "FunctionName is required", http.StatusBadRequest)

		return
	}

	mapping, err := s.storage.CreateEventSourceMapping(r.Context(), &req)
	if err != nil {
		handleFunctionError(w, err)

		return
	}

	writeJSONResponse(w, http.StatusCreated, mapping)
}

// GetEventSourceMapping handles the GetEventSourceMapping API.
func (s *Service) GetEventSourceMapping(w http.ResponseWriter, r *http.Request) {
	mappingUUID := extractEventSourceMappingUUID(r.URL.Path)
	if mappingUUID == "" {
		writeFunctionError(w, ErrInvalidParameterValue, "UUID is required", http.StatusBadRequest)

		return
	}

	mapping, err := s.storage.GetEventSourceMapping(r.Context(), mappingUUID)
	if err != nil {
		handleFunctionError(w, err)

		return
	}

	writeJSONResponse(w, http.StatusOK, mapping)
}

// DeleteEventSourceMapping handles the DeleteEventSourceMapping API.
func (s *Service) DeleteEventSourceMapping(w http.ResponseWriter, r *http.Request) {
	mappingUUID := extractEventSourceMappingUUID(r.URL.Path)
	if mappingUUID == "" {
		writeFunctionError(w, ErrInvalidParameterValue, "UUID is required", http.StatusBadRequest)

		return
	}

	mapping, err := s.storage.GetEventSourceMapping(r.Context(), mappingUUID)
	if err != nil {
		handleFunctionError(w, err)

		return
	}

	if err := s.storage.DeleteEventSourceMapping(r.Context(), mappingUUID); err != nil {
		handleFunctionError(w, err)

		return
	}

	mapping.State = "Deleting"
	writeJSONResponse(w, http.StatusOK, mapping)
}

// ListEventSourceMappings handles the ListEventSourceMappings API.
func (s *Service) ListEventSourceMappings(w http.ResponseWriter, r *http.Request) {
	functionName := r.URL.Query().Get("FunctionName")
	eventSourceArn := r.URL.Query().Get("EventSourceArn")
	marker := r.URL.Query().Get("Marker")

	maxItems := 100

	if maxItemsStr := r.URL.Query().Get("MaxItems"); maxItemsStr != "" {
		if parsed, err := strconv.Atoi(maxItemsStr); err == nil {
			maxItems = parsed
		}
	}

	mappings, nextMarker, err := s.storage.ListEventSourceMappings(r.Context(), functionName, eventSourceArn, marker, maxItems)
	if err != nil {
		writeFunctionError(w, ErrServiceException, "Internal server error", http.StatusInternalServerError)

		return
	}

	resp := &ListEventSourceMappingsResponse{
		EventSourceMappings: mappings,
		NextMarker:          nextMarker,
	}

	writeJSONResponse(w, http.StatusOK, resp)
}

// UpdateEventSourceMapping handles the UpdateEventSourceMapping API.
func (s *Service) UpdateEventSourceMapping(w http.ResponseWriter, r *http.Request) {
	mappingUUID := extractEventSourceMappingUUID(r.URL.Path)
	if mappingUUID == "" {
		writeFunctionError(w, ErrInvalidParameterValue, "UUID is required", http.StatusBadRequest)

		return
	}

	var req UpdateEventSourceMappingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeFunctionError(w, ErrInvalidParameterValue, "Invalid request body", http.StatusBadRequest)

		return
	}

	mapping, err := s.storage.UpdateEventSourceMapping(r.Context(), mappingUUID, &req)
	if err != nil {
		handleFunctionError(w, err)

		return
	}

	writeJSONResponse(w, http.StatusOK, mapping)
}

// handleFunctionError handles FunctionError and writes appropriate response.
func handleFunctionError(w http.ResponseWriter, err error) {
	var lambdaErr *FunctionError
	if errors.As(err, &lambdaErr) {
		status := http.StatusBadRequest
		if lambdaErr.Type == ErrResourceNotFound {
			status = http.StatusNotFound
		}

		writeFunctionError(w, lambdaErr.Type, lambdaErr.Message, status)

		return
	}

	writeFunctionError(w, ErrServiceException, "Internal server error", http.StatusInternalServerError)
}

// extractEventSourceMappingUUID extracts UUID from paths like:
//
//   - /lambda/2015-03-31/event-source-mappings/{UUID}  (SDK BaseEndpoint = .../lambda)
//   - /2015-03-31/event-source-mappings/{UUID}          (terraform-provider-aws, single endpoint)
//
// Routes are registered under both prefixes, so the helper finds the
// "event-source-mappings" segment regardless of where it appears in the path.
func extractEventSourceMappingUUID(path string) string {
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	for i, p := range parts {
		if p == "event-source-mappings" && i+1 < len(parts) {
			return parts[i+1]
		}
	}

	return ""
}

// ListVersionsByFunction returns a single $LATEST entry for any existing
// function. terraform-provider-aws calls this on every refresh of
// aws_lambda_function and apply errors immediately after CreateFunction
// without it.
func (s *Service) ListVersionsByFunction(w http.ResponseWriter, r *http.Request) {
	name := extractFunctionNameForListChild(r.URL.Path, "versions")
	if name == "" {
		writeFunctionError(w, "InvalidParameterValueException", "FunctionName is required", http.StatusBadRequest)

		return
	}

	fn, err := s.storage.ListVersionsByFunction(r.Context(), name)
	if err != nil {
		writeFunctionError(w, "ResourceNotFoundException", err.Error(), http.StatusNotFound)

		return
	}

	writeJSONResponse(w, http.StatusOK, listVersionsByFunctionResponse{
		Versions: []functionConfigurationVersion{
			{
				FunctionName: fn.FunctionName,
				FunctionArn:  fn.FunctionArn,
				Runtime:      fn.Runtime,
				Role:         fn.Role,
				Handler:      fn.Handler,
				Version:      "$LATEST",
				LastModified: fn.LastModified.UTC().Format("2006-01-02T15:04:05.000+0000"),
			},
		},
	})
}

// ListAliases returns an empty Aliases list. terraform-provider-aws calls
// this on every refresh of aws_lambda_function. Aliases are not modeled.
func (s *Service) ListAliases(w http.ResponseWriter, r *http.Request) {
	name := extractFunctionNameForListChild(r.URL.Path, "aliases")
	if name == "" {
		writeFunctionError(w, ErrInvalidParameterValue, "FunctionName is required", http.StatusBadRequest)

		return
	}

	if err := s.storage.ListAliases(r.Context(), name); err != nil {
		handleFunctionError(w, err)

		return
	}

	writeJSONResponse(w, http.StatusOK, listAliasesResponse{Aliases: []aliasConfiguration{}})
}

// GetFunctionCodeSigningConfig reports no code-signing config for any
// function. terraform-provider-aws reads this on every refresh.
func (s *Service) GetFunctionCodeSigningConfig(w http.ResponseWriter, r *http.Request) {
	name := extractFunctionNameForListChild(r.URL.Path, "code-signing-config")
	if name == "" {
		writeFunctionError(w, ErrInvalidParameterValue, "FunctionName is required", http.StatusBadRequest)

		return
	}

	functionName, err := s.storage.GetFunctionCodeSigningConfig(r.Context(), name)
	if err != nil {
		handleFunctionError(w, err)

		return
	}

	writeJSONResponse(w, http.StatusOK, getFunctionCodeSigningConfigResponse{FunctionName: functionName})
}

// ListFunctionEventInvokeConfigs returns an empty list.
func (s *Service) ListFunctionEventInvokeConfigs(w http.ResponseWriter, r *http.Request) {
	name := extractFunctionNameForListChild(r.URL.Path, "event-invoke-config")
	if name == "" {
		writeFunctionError(w, ErrInvalidParameterValue, "FunctionName is required", http.StatusBadRequest)

		return
	}

	if err := s.storage.ListFunctionEventInvokeConfigs(r.Context(), name); err != nil {
		handleFunctionError(w, err)

		return
	}

	writeJSONResponse(w, http.StatusOK, listFunctionEventInvokeConfigsResponse{
		FunctionEventInvokeConfigs: []map[string]any{},
	})
}

// GetPolicy returns the resource policy for a function. AWS returns 404
// when a function has no attached policy; terraform-provider-aws expects
// that and treats it as "no policy".
func (s *Service) GetPolicy(w http.ResponseWriter, r *http.Request) {
	name := extractFunctionNameForListChild(r.URL.Path, "policy")
	if name == "" {
		writeFunctionError(w, ErrInvalidParameterValue, "FunctionName is required", http.StatusBadRequest)

		return
	}

	policy, err := s.storage.GetPolicy(r.Context(), name)
	if err != nil {
		handleFunctionError(w, err)

		return
	}

	if policy == nil || len(policy.Statements) == 0 {
		writeFunctionError(w, "ResourceNotFoundException", "The resource you requested does not exist.", http.StatusNotFound)

		return
	}

	policyJSON, err := json.Marshal(policy)
	if err != nil {
		writeFunctionError(w, ErrServiceException, "Internal server error", http.StatusInternalServerError)

		return
	}

	writeJSONResponse(w, http.StatusOK, getPolicyResponse{
		Policy:     string(policyJSON),
		RevisionID: "default",
	})
}

// AddPermission adds a permission to a Lambda function's resource policy.
func (s *Service) AddPermission(w http.ResponseWriter, r *http.Request) {
	name := extractFunctionNameForListChild(r.URL.Path, "policy")
	if name == "" {
		writeFunctionError(w, ErrInvalidParameterValue, "FunctionName is required", http.StatusBadRequest)

		return
	}

	var req addPermissionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeFunctionError(w, ErrInvalidParameterValue, "Invalid request body", http.StatusBadRequest)

		return
	}

	// Validate required fields (first empty wins).
	for _, f := range []struct{ name, value string }{
		{"StatementId", req.StatementID},
		{"Action", req.Action},
		{"Principal", req.Principal},
	} {
		if f.value == "" {
			writeFunctionError(w, ErrInvalidParameterValue, f.name+" is required", http.StatusBadRequest)

			return
		}
	}

	fn, err := s.storage.GetFunction(r.Context(), name)
	if err != nil {
		handleGetFunctionError(w, err)

		return
	}

	stmt := buildPermissionStatement(&req, fn.FunctionArn)

	if err := s.storage.AddPermission(r.Context(), name, stmt); err != nil {
		handleFunctionError(w, err)

		return
	}

	stmtJSON, err := json.Marshal(stmt)
	if err != nil {
		writeFunctionError(w, ErrServiceException, "Internal server error", http.StatusInternalServerError)

		return
	}

	writeJSONResponse(w, http.StatusCreated, addPermissionResponse{
		Statement: string(stmtJSON),
	})
}

// buildPermissionStatement builds the resource-policy statement for AddPermission,
// attaching SourceArn/SourceAccount conditions when present.
func buildPermissionStatement(req *addPermissionRequest, resourceArn string) *PolicyStatement {
	stmt := &PolicyStatement{
		Sid:       req.StatementID,
		Effect:    "Allow",
		Principal: map[string]string{"Service": req.Principal},
		Action:    req.Action,
		Resource:  resourceArn,
	}

	if req.SourceArn != "" {
		stmt.Condition = map[string]any{
			"ArnLike": map[string]string{
				"AWS:SourceArn": req.SourceArn,
			},
		}
	}

	if req.SourceAccount != "" {
		if stmt.Condition == nil {
			stmt.Condition = make(map[string]any)
		}

		stmt.Condition["StringEquals"] = map[string]string{
			"AWS:SourceAccount": req.SourceAccount,
		}
	}

	return stmt
}

// RemovePermission removes a permission from a Lambda function's resource policy.
func (s *Service) RemovePermission(w http.ResponseWriter, r *http.Request) {
	name, statementID := extractFunctionNameAndStatementID(r.URL.Path)
	if name == "" || statementID == "" {
		writeFunctionError(w, ErrInvalidParameterValue, "FunctionName and StatementId are required", http.StatusBadRequest)

		return
	}

	if err := s.storage.RemovePermission(r.Context(), name, statementID); err != nil {
		handleFunctionError(w, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// ListTags returns the tags for a Lambda function identified by its ARN.
// The AWS API path is GET /2017-03-31/tags/{ARN}.
func (s *Service) ListTags(w http.ResponseWriter, r *http.Request) {
	arn := extractARNFromTagsPath(r.URL.Path)
	if arn == "" {
		writeFunctionError(w, ErrInvalidParameterValue, "Resource ARN is required", http.StatusBadRequest)

		return
	}

	tags, err := s.storage.ListTags(r.Context(), arn)
	if err != nil {
		handleGetFunctionError(w, err)

		return
	}

	writeJSONResponse(w, http.StatusOK, listTagsResponse{Tags: tags})
}

// TagResource adds or overwrites tags on a Lambda function identified by its ARN.
// The AWS API path is POST /2017-03-31/tags/{ARN}.
func (s *Service) TagResource(w http.ResponseWriter, r *http.Request) {
	arn := extractARNFromTagsPath(r.URL.Path)
	if arn == "" {
		writeFunctionError(w, ErrInvalidParameterValue, "Resource ARN is required", http.StatusBadRequest)

		return
	}

	var req tagResourceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeFunctionError(w, ErrInvalidParameterValue, "Invalid request body", http.StatusBadRequest)

		return
	}

	if err := s.storage.TagResource(r.Context(), arn, req.Tags); err != nil {
		handleFunctionError(w, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// UntagResource removes tags from a Lambda function identified by its ARN.
// The AWS API path is DELETE /2017-03-31/tags/{ARN}?tagKeys=key1&tagKeys=key2.
func (s *Service) UntagResource(w http.ResponseWriter, r *http.Request) {
	arn := extractARNFromTagsPath(r.URL.Path)
	if arn == "" {
		writeFunctionError(w, ErrInvalidParameterValue, "Resource ARN is required", http.StatusBadRequest)

		return
	}

	tagKeys := r.URL.Query()["tagKeys"]
	if len(tagKeys) == 0 {
		writeFunctionError(w, ErrInvalidParameterValue, "tagKeys is required", http.StatusBadRequest)

		return
	}

	if err := s.storage.UntagResource(r.Context(), arn, tagKeys); err != nil {
		handleFunctionError(w, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// extractFunctionNameForListChild returns the function name from a path
// like /.../functions/{name}/<child>. Empty if the shape does not match.
func extractFunctionNameForListChild(path, child string) string {
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	for i, p := range parts {
		if p == pathSegmentFunctions && i+2 < len(parts) && parts[i+2] == child {
			return parts[i+1]
		}
	}

	return ""
}

// extractFunctionNameAndStatementID extracts function name and statement ID
// from paths like /.../functions/{name}/policy/{statementId}.
func extractFunctionNameAndStatementID(path string) (string, string) {
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	for i, p := range parts {
		if p == pathSegmentFunctions && i+3 < len(parts) && parts[i+2] == "policy" {
			return parts[i+1], parts[i+3]
		}
	}

	return "", ""
}

// extractARNFromTagsPath extracts the ARN from a path like
// /lambda/2017-03-31/tags/{arn} or /2017-03-31/tags/{arn}.
// The ARN is URL-encoded in the path and contains colons.
func extractARNFromTagsPath(path string) string {
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	for i, p := range parts {
		if p == "tags" && i+1 < len(parts) {
			// The ARN may span multiple segments if it contains slashes,
			// but Lambda ARNs use colons. Join remaining parts.
			raw := strings.Join(parts[i+1:], "/")

			decoded, err := url.PathUnescape(raw)
			if err != nil {
				return raw
			}

			return decoded
		}
	}

	return ""
}

// policyID returns a deterministic policy ID for a function.
func policyID(functionName string) string {
	return fmt.Sprintf("%s-policy", functionName)
}
