package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
)

const (
	ErrorUnknownJobKind   = "UNKNOWN_JOB_KIND"
	ErrorInvalidPayload   = "INVALID_JOB_PAYLOAD"
	ErrorExecutionTimeout = "EXECUTION_TIMEOUT"
	ErrorShutdownTimeout  = "SHUTDOWN_TIMEOUT"
)

type Handler interface {
	Execute(context.Context, json.RawMessage) (json.RawMessage, error)
}

type HandlerFunc func(context.Context, json.RawMessage) (json.RawMessage, error)

func (f HandlerFunc) Execute(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	return f(ctx, payload)
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

func (r *Registry) Execute(ctx context.Context, kind string, payload json.RawMessage) (json.RawMessage, string) {
	handler, ok := r.handlers[kind]
	if !ok {
		return nil, ErrorUnknownJobKind
	}
	result, err := handler.Execute(ctx, payload)
	if err != nil {
		return nil, ErrorInvalidPayload
	}
	return result, ""
}

type echoHandler struct {
	maxResultBytes int64
}

func (h echoHandler) Execute(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var request struct {
		Message string `json:"message"`
	}
	if err := decoder.Decode(&request); err != nil || request.Message == "" {
		return nil, errors.New("message is required")
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("payload must contain one object")
	}
	result, err := json.Marshal(map[string]string{"message": request.Message})
	if err != nil {
		return nil, errors.New("encode echo result")
	}
	if int64(len(result)) > h.maxResultBytes {
		return nil, errors.New("echo result exceeds configured limit")
	}
	return result, nil
}
