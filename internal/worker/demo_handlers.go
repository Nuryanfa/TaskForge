package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/job"
)

const (
	ErrorDemoFailure   = "DEMO_FAILURE"
	ErrorDemoTransient = "DEMO_TRANSIENT"
	maxDemoFailures    = 10
	maxDemoSleepMS     = 60000
)

var demoCodePattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,63}$`)

var demoOK = json.RawMessage(`{"ok":true}`)

// decodeStrict decodes exactly one JSON object with no unknown fields.
func decodeStrict(payload json.RawMessage, target any) bool {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return false
	}
	var extra any
	return errors.Is(decoder.Decode(&extra), io.EOF)
}

func invalidPayload() *job.Failure {
	return &job.Failure{Code: ErrorInvalidPayload, Retryable: false}
}

// demoFail always fails permanently. It lets a workflow demonstrate saga
// compensation without any external dependency.
type demoFail struct{}

func (demoFail) Execute(_ context.Context, _ job.Execution, payload json.RawMessage) (json.RawMessage, *job.Failure) {
	var request struct {
		Code *string `json:"code"`
	}
	if !decodeStrict(payload, &request) {
		return nil, invalidPayload()
	}
	code := ErrorDemoFailure
	if request.Code != nil {
		if !demoCodePattern.MatchString(*request.Code) {
			return nil, invalidPayload()
		}
		code = *request.Code
	}
	return nil, &job.Failure{Code: code, Retryable: false}
}

// demoFlaky fails retryably for the first fail_attempts attempts.
type demoFlaky struct{}

func (demoFlaky) Execute(_ context.Context, execution job.Execution, payload json.RawMessage) (json.RawMessage, *job.Failure) {
	var request struct {
		FailAttempts *int `json:"fail_attempts"`
	}
	if !decodeStrict(payload, &request) || request.FailAttempts == nil ||
		*request.FailAttempts < 0 || *request.FailAttempts > maxDemoFailures {
		return nil, invalidPayload()
	}
	if execution.Attempt <= *request.FailAttempts {
		return nil, &job.Failure{Code: ErrorDemoTransient, Retryable: true}
	}
	return demoOK, nil
}

// demoSleep waits for ms milliseconds, honoring cancellation. It keeps a step
// in flight for abort, cancel, and crash scenarios.
type demoSleep struct{}

func (demoSleep) Execute(ctx context.Context, _ job.Execution, payload json.RawMessage) (json.RawMessage, *job.Failure) {
	var request struct {
		MS *int `json:"ms"`
	}
	if !decodeStrict(payload, &request) || request.MS == nil || *request.MS < 0 || *request.MS > maxDemoSleepMS {
		return nil, invalidPayload()
	}
	timer := time.NewTimer(time.Duration(*request.MS) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, &job.Failure{Code: ErrorExecutionTimeout, Retryable: true}
	case <-timer.C:
		return demoOK, nil
	}
}
