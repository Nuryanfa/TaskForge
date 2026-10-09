package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/Nuryanfa/TaskForge/internal/job"
)

const (
	ErrorUnknownJobKind   = "UNKNOWN_JOB_KIND"
	ErrorInvalidPayload   = "INVALID_JOB_PAYLOAD"
	ErrorExecutionTimeout = "EXECUTION_TIMEOUT"
	ErrorShutdownTimeout  = "SHUTDOWN_TIMEOUT"
)

type Handler interface {
	Execute(context.Context, job.Execution, json.RawMessage) (json.RawMessage, *job.Failure)
}

type HandlerFunc func(context.Context, job.Execution, json.RawMessage) (json.RawMessage, *job.Failure)

func (f HandlerFunc) Execute(ctx context.Context, execution job.Execution, payload json.RawMessage) (json.RawMessage, *job.Failure) {
	return f(ctx, execution, payload)
}

type Registry struct {
	handlers map[string]Handler
}

func NewRegistry(maxResultBytes int64) *Registry {
	return &Registry{handlers: map[string]Handler{
		"demo.echo": echoHandler{maxResultBytes: maxResultBytes},
	}}
}

func newRegistry(handlers map[string]Handler) *Registry {
	copyOfHandlers := make(map[string]Handler, len(handlers))
	for kind, handler := range handlers {
		copyOfHandlers[kind] = handler
	}
	return &Registry{handlers: copyOfHandlers}
}

func (r *Registry) Execute(ctx context.Context, execution job.Execution, kind string, payload json.RawMessage) (json.RawMessage, *job.Failure) {
	handler, ok := r.handlers[kind]
	if !ok {
		return nil, &job.Failure{Code: ErrorUnknownJobKind, Retryable: false}
	}
	return handler.Execute(ctx, execution, payload)
}

type echoHandler struct {
	maxResultBytes int64
}

func (h echoHandler) Execute(ctx context.Context, _ job.Execution, payload json.RawMessage) (json.RawMessage, *job.Failure) {
	if err := ctx.Err(); err != nil {
		return nil, &job.Failure{Code: ErrorExecutionTimeout, Retryable: true}
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var request struct {
		Message string `json:"message"`
	}
	if err := decoder.Decode(&request); err != nil || request.Message == "" {
		return nil, &job.Failure{Code: ErrorInvalidPayload, Retryable: false}
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, &job.Failure{Code: ErrorInvalidPayload, Retryable: false}
	}
	result, err := json.Marshal(map[string]string{"message": request.Message})
	if err != nil {
		return nil, &job.Failure{Code: "RESULT_ENCODING_FAILED", Retryable: false}
	}
	if int64(len(result)) > h.maxResultBytes {
		return nil, &job.Failure{Code: "RESULT_TOO_LARGE", Retryable: false}
	}
	return result, nil
}
