package postgres

import (
	"errors"
	"fmt"
)

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

// RecoveryError reports expired jobs whose recovery was rolled back while the
// rest of the batch committed. Job IDs are safe to log; Err may carry
// database detail and is for errors.Is/As only.
type RecoveryError struct {
	JobIDs []string
	Err    error
}

func (e *RecoveryError) Error() string {
	return fmt.Sprintf("lease recovery failed for %d job(s): %v", len(e.JobIDs), e.Err)
}

func (e *RecoveryError) Unwrap() error { return e.Err }
