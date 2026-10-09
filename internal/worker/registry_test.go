package worker

import (
	"context"
	"encoding/json"
	"testing"
)

func TestRegistry(t *testing.T) {
	registry := NewRegistry(1024)
	result, code := registry.Execute(context.Background(), "demo.echo", json.RawMessage(`{"message":"hello"}`))
	if code != "" || string(result) != `{"message":"hello"}` {
		t.Fatalf("unexpected echo outcome: result=%s code=%s", result, code)
	}
	if _, code := registry.Execute(context.Background(), "unknown.kind", json.RawMessage(`{}`)); code != ErrorUnknownJobKind {
		t.Fatalf("unexpected unknown kind code: %s", code)
	}
	if _, code := registry.Execute(context.Background(), "demo.echo", json.RawMessage(`{"message":""}`)); code != ErrorInvalidPayload {
		t.Fatalf("unexpected invalid payload code: %s", code)
	}
}
