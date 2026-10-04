package job

import "testing"

func TestValidateTransitionExhaustive(t *testing.T) {
	statuses := []Status{StatusQueued, StatusRunning, StatusSucceeded, StatusFailed, StatusCanceled, Status("unknown")}
	allowed := map[[2]Status]bool{
		{StatusQueued, StatusRunning}:    true,
		{StatusQueued, StatusCanceled}:   true,
		{StatusRunning, StatusSucceeded}: true,
		{StatusRunning, StatusFailed}:    true,
	}
	for _, from := range statuses {
		for _, to := range statuses {
			err := ValidateTransition(from, to)
			if allowed[[2]Status{from, to}] != (err == nil) {
				t.Fatalf("transition %q -> %q returned %v", from, to, err)
			}
		}
	}
}

func TestTerminal(t *testing.T) {
	for _, status := range []Status{StatusSucceeded, StatusFailed, StatusCanceled} {
		if !status.Terminal() {
			t.Fatalf("expected %q to be terminal", status)
		}
	}
	for _, status := range []Status{StatusQueued, StatusRunning, Status("unknown")} {
		if status.Terminal() {
			t.Fatalf("did not expect %q to be terminal", status)
		}
	}
}
