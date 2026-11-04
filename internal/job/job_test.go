package job

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func TestNewSubmissionValidation(t *testing.T) {
	validKey := "request-1"
	tests := []struct {
		name, queue, kind, payload string
		priority                   int
		key                        *string
		wantErr                    bool
	}{
		{name: "valid default queue", kind: "demo.echo", payload: `{}`},
		{name: "valid explicit", queue: "reports.high", kind: "report.generate", payload: `{"x":1}`, priority: 100, key: &validKey},
		{name: "invalid queue", queue: "Bad Queue", kind: "demo.echo", payload: `{}`, wantErr: true},
		{name: "invalid kind", kind: "Demo Echo", payload: `{}`, wantErr: true},
		{name: "non object", kind: "demo.echo", payload: `[]`, wantErr: true},
		{name: "trailing value", kind: "demo.echo", payload: `{} {}`, wantErr: true},
		{name: "priority too high", kind: "demo.echo", payload: `{}`, priority: 101, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			submission, err := NewSubmission(tt.queue, tt.kind, json.RawMessage(tt.payload), tt.priority, tt.key, 1024)
			if (err != nil) != tt.wantErr {
				t.Fatalf("NewSubmission() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil && submission.Queue == "" {
				t.Fatal("expected normalized queue")
			}
		})
	}
}

func TestSubmissionFingerprintCanonicalizesObjectKeysAndWhitespace(t *testing.T) {
	first, err := NewSubmission("default", "demo.echo", json.RawMessage(`{"b":2,"a":1}`), 0, nil, 1024)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewSubmission("default", "demo.echo", json.RawMessage("{\n \"a\": 1, \"b\": 2 }"), 0, nil, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Fingerprint[:], second.Fingerprint[:]) {
		t.Fatal("equivalent JSON objects produced different fingerprints")
	}
}

func TestNewSubmissionRejectsInvalidIdempotencyKeysAndPayloadBounds(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		payload json.RawMessage
		maximum int64
	}{
		{name: "empty key", key: "", payload: json.RawMessage(`{}`), maximum: 1024},
		{name: "space in key", key: "unsafe key", payload: json.RawMessage(`{}`), maximum: 1024},
		{name: "control in key", key: "unsafe\nkey", payload: json.RawMessage(`{}`), maximum: 1024},
		{name: "oversized payload", key: "valid", payload: json.RawMessage(`{"message":"long"}`), maximum: 4},
		{name: "invalid utf8", key: "valid", payload: json.RawMessage{'{', '"', 'x', '"', ':', '"', 0xff, '"', '}'}, maximum: 1024},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewSubmission("default", "demo.echo", tt.payload, 0, &tt.key, tt.maximum); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestNewDelayedSubmissionValidationAndFingerprint(t *testing.T) {
	now := time.Date(2026, time.November, 5, 12, 0, 0, 0, time.UTC)
	runAtUTC := now.Add(10 * time.Minute)
	Jakarta, _ := time.LoadLocation("Asia/Jakarta")
	runAtJakarta := runAtUTC.In(Jakarta)

	subUTC, err := NewDelayedSubmission("default", "demo.echo", json.RawMessage(`{"message":"hi"}`), 0, nil, &runAtUTC, nil, now, 1024)
	if err != nil {
		t.Fatal(err)
	}
	subJakarta, err := NewDelayedSubmission("default", "demo.echo", json.RawMessage(`{"message":"hi"}`), 0, nil, &runAtJakarta, nil, now, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(subUTC.Fingerprint[:], subJakarta.Fingerprint[:]) {
		t.Fatal("equivalent timestamps in different timezones produced different fingerprints")
	}

	delay := 60
	subDelay, err := NewDelayedSubmission("default", "demo.echo", json.RawMessage(`{"message":"hi"}`), 0, nil, nil, &delay, now, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(subUTC.Fingerprint[:], subDelay.Fingerprint[:]) {
		t.Fatal("run_at and delay_seconds should produce distinct fingerprints")
	}

	if _, err := NewDelayedSubmission("default", "demo.echo", json.RawMessage(`{}`), 0, nil, &runAtUTC, &delay, now, 1024); err == nil {
		t.Fatal("expected mutually exclusive run_at and delay_seconds error")
	}
	negativeDelay := -1
	if _, err := NewDelayedSubmission("default", "demo.echo", json.RawMessage(`{}`), 0, nil, nil, &negativeDelay, now, 1024); err == nil {
		t.Fatal("expected negative delay_seconds error")
	}
	excessiveDelay := MaxDelaySeconds + 1
	if _, err := NewDelayedSubmission("default", "demo.echo", json.RawMessage(`{}`), 0, nil, nil, &excessiveDelay, now, 1024); err == nil {
		t.Fatal("expected excessive delay_seconds error")
	}
	tooFarFuture := now.Add(MaxScheduleHorizon + time.Minute)
	if _, err := NewDelayedSubmission("default", "demo.echo", json.RawMessage(`{}`), 0, nil, &tooFarFuture, nil, now, 1024); err == nil {
		t.Fatal("expected excessive run_at error")
	}
}
