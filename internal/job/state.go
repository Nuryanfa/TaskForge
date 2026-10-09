package job

import "fmt"

type Status string

const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
	StatusCanceled  Status = "canceled"
)

func (s Status) Terminal() bool {
	return s == StatusSucceeded || s == StatusFailed || s == StatusCanceled
}

func ValidateTransition(from, to Status) error {
	if from.Terminal() {
		return fmt.Errorf("terminal job cannot transition from %q to %q", from, to)
	}
	if (from == StatusQueued && (to == StatusRunning || to == StatusCanceled)) ||
		(from == StatusRunning && (to == StatusSucceeded || to == StatusFailed)) {
		return nil
	}
	if from != StatusQueued && from != StatusRunning {
		return fmt.Errorf("unknown source status %q", from)
	}
	return fmt.Errorf("invalid job transition from %q to %q", from, to)
}
