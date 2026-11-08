package worker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/job"
)

func runDemo(t *testing.T, ctx context.Context, kind string, attempt int, payload string) (json.RawMessage, *job.Failure) {
	t.Helper()
	return NewRegistry(1024).Execute(ctx, job.Execution{Attempt: attempt}, kind, json.RawMessage(payload))
}

func expectFailure(t *testing.T, failure *job.Failure, code string, retryable bool) {
	t.Helper()
	if failure == nil || failure.Code != code || failure.Retryable != retryable {
		t.Fatalf("failure=%+v want code=%s retryable=%v", failure, code, retryable)
	}
}

func TestDemoFail(t *testing.T) {
	ctx := context.Background()
	_, failure := runDemo(t, ctx, "demo.fail", 1, `{}`)
	expectFailure(t, failure, ErrorDemoFailure, false)
	_, failure = runDemo(t, ctx, "demo.fail", 1, `{"code":"OUT_OF_STOCK"}`)
	expectFailure(t, failure, "OUT_OF_STOCK", false)
	_, failure = runDemo(t, ctx, "demo.fail", 1, `{"code":"bad"}`)
	expectFailure(t, failure, ErrorInvalidPayload, false)
	_, failure = runDemo(t, ctx, "demo.fail", 1, `{"other":1}`)
	expectFailure(t, failure, ErrorInvalidPayload, false)
}

func TestDemoFlaky(t *testing.T) {
	ctx := context.Background()
	for attempt := 1; attempt <= 2; attempt++ {
		_, failure := runDemo(t, ctx, "demo.flaky", attempt, `{"fail_attempts":2}`)
		expectFailure(t, failure, ErrorDemoTransient, true)
	}
	if result, failure := runDemo(t, ctx, "demo.flaky", 3, `{"fail_attempts":2}`); failure != nil || string(result) != `{"ok":true}` {
		t.Fatalf("attempt 3 result=%s failure=%+v", result, failure)
	}
	_, failure := runDemo(t, ctx, "demo.flaky", 1, `{"fail_attempts":11}`)
	expectFailure(t, failure, ErrorInvalidPayload, false)
	_, failure = runDemo(t, ctx, "demo.flaky", 1, `{}`)
	expectFailure(t, failure, ErrorInvalidPayload, false)
}

func TestDemoSleep(t *testing.T) {
	started := time.Now()
	if result, failure := runDemo(t, context.Background(), "demo.sleep", 1, `{"ms":10}`); failure != nil || string(result) != `{"ok":true}` {
		t.Fatalf("result=%s failure=%+v", result, failure)
	}
	if time.Since(started) < 10*time.Millisecond {
		t.Fatal("demo.sleep returned before its duration")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, failure := runDemo(t, canceled, "demo.sleep", 1, `{"ms":5000}`)
	expectFailure(t, failure, ErrorExecutionTimeout, true)
	_, failure = runDemo(t, context.Background(), "demo.sleep", 1, `{"ms":60001}`)
	expectFailure(t, failure, ErrorInvalidPayload, false)
	_, failure = runDemo(t, context.Background(), "demo.sleep", 1, `{}`)
	expectFailure(t, failure, ErrorInvalidPayload, false)
}
