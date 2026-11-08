package outbox

import (
	"encoding/json"
	"testing"
	"time"
)

func TestSubjectMsgIDAndEncode(t *testing.T) {
	event := Event{
		ID: 42, JobID: "0b6c3c1e-2f7a-4f43-9b38-6d1d0f4f4a10", Type: "job.succeeded",
		Payload:   json.RawMessage(`{"job_id":"0b6c3c1e-2f7a-4f43-9b38-6d1d0f4f4a10","queue":"billing.eu","status":"succeeded"}`),
		CreatedAt: time.Date(2026, 11, 7, 10, 0, 0, 123456000, time.FixedZone("WIB", 7*3600)),
	}
	if got := event.Subject(); got != "taskforge.events.job.succeeded" {
		t.Fatalf("subject=%q", got)
	}
	if got := event.MsgID(); got != "taskforge-outbox-42" {
		t.Fatalf("msg id=%q", got)
	}
	body, err := event.Encode()
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Version    int             `json:"version"`
		EventID    int64           `json:"event_id"`
		Type       string          `json:"type"`
		OccurredAt string          `json:"occurred_at"`
		Job        json.RawMessage `json:"job"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Version != 1 || decoded.EventID != 42 || decoded.Type != "job.succeeded" ||
		decoded.OccurredAt != "2026-11-07T03:00:00.123456Z" || string(decoded.Job) != string(event.Payload) {
		t.Fatalf("decoded=%+v", decoded)
	}
	if got := event.Queue(); got != "billing.eu" {
		t.Fatalf("queue=%q", got)
	}
}

func TestEncodeRejectsInvalidEvents(t *testing.T) {
	valid := Event{ID: 1, Type: "job.queued", Payload: json.RawMessage(`{}`), CreatedAt: time.Now()}
	for name, mutate := range map[string]func(*Event){
		"unknown type":   func(e *Event) { e.Type = "job.exploded" },
		"missing id":     func(e *Event) { e.ID = 0 },
		"invalid json":   func(e *Event) { e.Payload = json.RawMessage(`{`) },
		"non-object job": func(e *Event) { e.Payload = json.RawMessage(`[]`) },
	} {
		event := valid
		mutate(&event)
		if _, err := event.Encode(); err == nil {
			t.Fatalf("%s: expected error", name)
		}
	}
}

func TestDueOnOccurrenceComparesDatabaseTimestampsOnly(t *testing.T) {
	cases := map[string]struct {
		body string
		due  bool
	}{
		"immediate job (same transaction NOW())": {`{"occurred_at":"2026-11-07T03:00:00.123456Z","job":{"available_at":"2026-11-07T10:00:00.123456+07:00"}}`, true},
		"delayed job":                            {`{"occurred_at":"2026-11-07T03:00:00Z","job":{"available_at":"2026-11-07T03:00:30Z"}}`, false},
		"backlogged old event":                   {`{"occurred_at":"2020-01-01T00:00:00Z","job":{"available_at":"2019-12-31T23:59:59Z"}}`, true},
	}
	for name, tc := range cases {
		due, err := DueOnOccurrence([]byte(tc.body))
		if err != nil || due != tc.due {
			t.Fatalf("%s: due=%v err=%v", name, due, err)
		}
	}
	for _, body := range []string{`{"job":{"available_at":"2026-11-07T03:00:00Z"}}`, `{"occurred_at":"2026-11-07T03:00:00Z","job":{}}`, `nope`} {
		if _, err := DueOnOccurrence([]byte(body)); err == nil {
			t.Fatalf("expected error for %s", body)
		}
	}
}
