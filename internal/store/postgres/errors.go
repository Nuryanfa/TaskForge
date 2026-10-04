package postgres

import "errors"

var (
	ErrNotFound            = errors.New("not found")
	ErrStateConflict       = errors.New("state conflict")
	ErrIdempotencyConflict = errors.New("idempotency conflict")
	ErrUnavailable         = errors.New("dependency unavailable")
)
