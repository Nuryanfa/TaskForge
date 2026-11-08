package postgres

import "errors"

var (
	ErrNotFound            = errors.New("not found")
	ErrStateConflict       = errors.New("state conflict")
	ErrIdempotencyConflict = errors.New("idempotency conflict")
	ErrUnavailable         = errors.New("dependency unavailable")
	ErrOwnershipLost       = errors.New("execution ownership lost")
	ErrInvalidResult       = errors.New("result is invalid or exceeds configured limit")
	ErrOutboxBusy          = errors.New("another outbox relay is active")
	ErrWorkflowOwnedJob    = errors.New("job belongs to a workflow")
)
