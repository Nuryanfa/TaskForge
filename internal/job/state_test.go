package job

import "testing"

func TestValidateTransition(t *testing.T) {
	tests := []struct {
		name    string
		from    Status
		to      Status
		wantErr bool
	}{
		{name: "claim queued job", from: StatusQueued, to: StatusRunning},
		{name: "finish running job", from: StatusRunning, to: StatusSucceeded},
		{name: "schedule retry", from: StatusRunning, to: StatusRetryScheduled},
		{name: "release retry", from: StatusRetryScheduled, to: StatusQueued},
		{name: "reject skipped execution", from: StatusQueued, to: StatusSucceeded, wantErr: true},
		{name: "reject terminal transition", from: StatusSucceeded, to: StatusQueued, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTransition(tt.from, tt.to)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateTransition() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
