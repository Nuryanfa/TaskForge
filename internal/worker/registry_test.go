package worker

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Nuryanfa/TaskForge/internal/job"
)

func TestRegistry(t *testing.T) {
	registry := NewRegistry(1024)
	result, failure := registry.Execute(context.Background(), job.Execution{}, "demo.echo", json.RawMessage(`{"message":"hello"}`))
	if failure != nil || string(result) != `{"message":"hello"}` {
		t.Fatalf("unexpected echo outcome: result=%s failure=%v", result, failure)
	}
	if _, failure := registry.Execute(context.Background(), job.Execution{}, "unknown.kind", json.RawMessage(`{}`)); failure == nil || failure.Code != ErrorUnknownJobKind || failure.Retryable {
		t.Fatalf("unexpected unknown kind failure: %+v", failure)
	}
	if _, failure := registry.Execute(context.Background(), job.Execution{}, "demo.echo", json.RawMessage(`{"message":""}`)); failure == nil || failure.Code != ErrorInvalidPayload || failure.Retryable {
		t.Fatalf("unexpected invalid payload failure: %+v", failure)
	}
}

func TestHandlerReceivesStableExecutionMetadata(t *testing.T) {
	want := job.Execution{JobID: "job", Attempt: 2, WorkerID: "worker", FencingToken: 7, ExecutionKey: "execution-key"}
	var got job.Execution
	registry := newRegistry(map[string]Handler{"test": HandlerFunc(func(_ context.Context, execution job.Execution, _ json.RawMessage) (json.RawMessage, *job.Failure) {
		got = execution
		return json.RawMessage(`{}`), nil
	})})
	if _, failure := registry.Execute(context.Background(), want, "test", json.RawMessage(`{}`)); failure != nil {
		t.Fatal(failure)
	}
	if got != want {
		t.Fatalf("metadata=%+v want=%+v", got, want)
	}
}
