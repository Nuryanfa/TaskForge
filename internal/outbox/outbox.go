// Package outbox defines the job lifecycle event contract that the relay
// publishes from the PostgreSQL outbox. It has no broker dependency.
package outbox

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

const (
	SubjectPrefix = "taskforge.events."
	// QueuedSubject carries job.queued events; workers use it as a wake-up hint.
	QueuedSubject  = SubjectPrefix + "job.queued"
	StreamSubjects = SubjectPrefix + ">"
	Version        = 1

	HeaderEventType = "TaskForge-Event-Type"
	HeaderQueue     = "TaskForge-Queue"
)

var eventTypes = map[string]bool{
	"job.queued": true, "job.started": true, "job.retry_scheduled": true,
	"job.succeeded": true, "job.failed": true, "job.canceled": true,
}

// Event is one outbox row. Payload is the privacy-safe job snapshot written
// by the database trigger.
type Event struct {
	ID        int64
	JobID     string
	Type      string
	Payload   json.RawMessage
	CreatedAt time.Time
}

func (e Event) Subject() string { return SubjectPrefix + e.Type }

// MsgID is the JetStream deduplication ID; it is stable across republishes.
func (e Event) MsgID() string { return "taskforge-outbox-" + strconv.FormatInt(e.ID, 10) }

// Queue returns the job queue from the snapshot, or "" if it is absent.
func (e Event) Queue() string {
	var snapshot struct {
		Queue string `json:"queue"`
	}
	_ = json.Unmarshal(e.Payload, &snapshot)
	return snapshot.Queue
}

type envelope struct {
	Version    int             `json:"version"`
	EventID    int64           `json:"event_id"`
	Type       string          `json:"type"`
	OccurredAt string          `json:"occurred_at"`
	Job        json.RawMessage `json:"job"`
}

// Encode renders the versioned message body published to NATS.
func (e Event) Encode() ([]byte, error) {
	if e.ID <= 0 {
		return nil, errors.New("outbox event id must be positive")
	}
	if !eventTypes[e.Type] {
		return nil, fmt.Errorf("unknown outbox event type %q", e.Type)
	}
	trimmed := bytes.TrimSpace(e.Payload)
	if !json.Valid(trimmed) || len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, errors.New("outbox event payload must be a JSON object")
	}
	return json.Marshal(envelope{Version: Version, EventID: e.ID, Type: e.Type,
		OccurredAt: e.CreatedAt.UTC().Format(time.RFC3339Nano), Job: trimmed})
}

// DueOnOccurrence reports whether the job in an encoded event was already
// claimable when the event occurred. Both timestamps come from the same
// PostgreSQL transaction clock, so the answer never depends on the reader's
// local clock. Jobs delayed past the event are left to polling.
func DueOnOccurrence(body []byte) (bool, error) {
	var message struct {
		OccurredAt *time.Time `json:"occurred_at"`
		Job        struct {
			AvailableAt *time.Time `json:"available_at"`
		} `json:"job"`
	}
	if err := json.Unmarshal(body, &message); err != nil {
		return false, err
	}
	if message.OccurredAt == nil || message.Job.AvailableAt == nil {
		return false, errors.New("event needs occurred_at and job.available_at")
	}
	return !message.Job.AvailableAt.After(*message.OccurredAt), nil
}
