package job

import (
	"bytes"
	"encoding/json"
	"testing"
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
