package job

import "fmt"

type Status string

const (
	StatusQueued         Status = "queued"
	StatusRunning        Status = "running"
	StatusRetryScheduled Status = "retry_scheduled"
	StatusSucceeded      Status = "succeeded"
	StatusFailed         Status = "failed"
	StatusDeadLettered   Status = "dead_lettered"
	StatusCanceled       Status = "canceled"
)

var transitions = map[Status]map[Status]struct{}{
	StatusQueued: {
		StatusRunning:  {},
		StatusCanceled: {},
	},
	StatusRunning: {
		StatusSucceeded:      {},
		StatusRetryScheduled: {},
		StatusFailed:         {},
		StatusDeadLettered:   {},
		StatusCanceled:       {},
	},
	StatusRetryScheduled: {
		StatusQueued:   {},
		StatusCanceled: {},
	},
}

func (s Status) Terminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusDeadLettered, StatusCanceled:
		return true
	default:
		return false
	}
}

func ValidateTransition(from, to Status) error {
	allowed, known := transitions[from]
	if !known {
		if from.Terminal() {
			return fmt.Errorf("terminal job cannot transition from %q to %q", from, to)
		}
		return fmt.Errorf("unknown source status %q", from)
	}
	if _, ok := allowed[to]; !ok {
		return fmt.Errorf("invalid job transition from %q to %q", from, to)
	}
	return nil
}
