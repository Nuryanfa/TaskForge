package job

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"time"
	"unicode/utf8"
)

const (
	DefaultQueue = "default"
	MinPriority  = -100
	MaxPriority  = 100
)

var identifierPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

type Job struct {
	ID             string
	Queue          string
	Kind           string
	Payload        json.RawMessage
	PayloadHash    []byte
	Status         Status
	Priority       int16
	IdempotencyKey *string
	AvailableAt    time.Time
	AttemptCount   int
	Result         json.RawMessage
	LastErrorCode  *string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	StartedAt      *time.Time
	CompletedAt    *time.Time
	LeaseOwner     *string
	LeaseExpiresAt *time.Time
	FencingToken   int64
}

// Execution identifies one ownership epoch. Its values must accompany every
// mutation so an execution that lost its lease cannot alter newer work.
type Execution struct {
	JobID        string
	Attempt      int
	WorkerID     string
	FencingToken int64
}

type Claimed struct {
	Job       Job
	Execution Execution
}

type Submission struct {
	Queue          string
	Kind           string
	Payload        json.RawMessage
	Priority       int16
	IdempotencyKey *string
	Fingerprint    [sha256.Size]byte
}

func NewSubmission(queue, kind string, payload json.RawMessage, priority int, idempotencyKey *string, maxPayloadBytes int64) (Submission, error) {
	if queue == "" {
		queue = DefaultQueue
	}
	if !identifierPattern.MatchString(queue) {
		return Submission{}, errors.New("queue must be 1-64 lowercase letters, digits, dots, underscores, or hyphens")
	}
	if !identifierPattern.MatchString(kind) {
		return Submission{}, errors.New("kind must be a 1-64 character lowercase namespace-style identifier")
	}
	if priority < MinPriority || priority > MaxPriority {
		return Submission{}, fmt.Errorf("priority must be between %d and %d", MinPriority, MaxPriority)
	}
	if idempotencyKey != nil {
		if err := validateIdempotencyKey(*idempotencyKey); err != nil {
			return Submission{}, err
		}
	}
	canonicalPayload, err := canonicalObject(payload, maxPayloadBytes)
	if err != nil {
		return Submission{}, err
	}
	submission := Submission{Queue: queue, Kind: kind, Payload: canonicalPayload, Priority: int16(priority), IdempotencyKey: idempotencyKey}
	fingerprintInput, err := json.Marshal(struct {
		Queue    string          `json:"queue"`
		Kind     string          `json:"kind"`
		Payload  json.RawMessage `json:"payload"`
		Priority int16           `json:"priority"`
	}{submission.Queue, submission.Kind, submission.Payload, submission.Priority})
	if err != nil {
		return Submission{}, fmt.Errorf("encode submission fingerprint: %w", err)
	}
	submission.Fingerprint = sha256.Sum256(fingerprintInput)
	return submission, nil
}

func validateIdempotencyKey(value string) error {
	if len(value) < 1 || len(value) > 128 {
		return errors.New("idempotency_key must be 1-128 printable safe characters")
	}
	for _, r := range value {
		if r < 0x21 || r > 0x7e {
			return errors.New("idempotency_key must be 1-128 printable safe characters")
		}
	}
	return nil
}

func canonicalObject(payload json.RawMessage, maximum int64) (json.RawMessage, error) {
	if len(payload) == 0 {
		return nil, errors.New("payload is required")
	}
	if int64(len(payload)) > maximum || !utf8.Valid(payload) {
		return nil, errors.New("payload exceeds the configured limit or is not valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var object map[string]any
	if err := decoder.Decode(&object); err != nil || object == nil {
		return nil, errors.New("payload must be a JSON object")
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("payload must contain exactly one JSON object")
	}
	canonical, err := json.Marshal(object)
	if err != nil {
		return nil, fmt.Errorf("canonicalize payload: %w", err)
	}
	if int64(len(canonical)) > maximum {
		return nil, errors.New("payload exceeds the configured limit")
	}
	return canonical, nil
}
