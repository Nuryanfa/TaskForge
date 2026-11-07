package job

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func TestParseCronAndNext(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		timezone string
		after    time.Time
		wantUTC  time.Time
		wantErr  bool
	}{
		{
			name:     "every 15 minutes in UTC",
			expr:     "*/15 * * * *",
			timezone: "UTC",
			after:    time.Date(2026, time.November, 5, 10, 7, 30, 0, time.UTC),
			wantUTC:  time.Date(2026, time.November, 5, 10, 15, 0, 0, time.UTC),
		},
		{
			name:     "daily at 09:30 Asia/Jakarta (UTC+7)",
			expr:     "30 9 * * *",
			timezone: "Asia/Jakarta",
			after:    time.Date(2026, time.November, 5, 2, 29, 0, 0, time.UTC),
			wantUTC:  time.Date(2026, time.November, 5, 2, 30, 0, 0, time.UTC),
		},
		{
			name:     "leap day Feb 29",
			expr:     "0 0 29 2 *",
			timezone: "UTC",
			after:    time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC),
			wantUTC:  time.Date(2028, time.February, 29, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "both dom and dow restricted uses OR semantics",
			expr:     "0 12 15 * 1",
			timezone: "UTC",
			after:    time.Date(2026, time.November, 1, 0, 0, 0, 0, time.UTC), // Sun Nov 1, 2026 -> Mon Nov 2, 2026
			wantUTC:  time.Date(2026, time.November, 2, 12, 0, 0, 0, time.UTC),
		},
		{
			name:     "sunday as 7 in range",
			expr:     "0 8 * * 5-7",
			timezone: "UTC",
			after:    time.Date(2026, time.November, 1, 0, 0, 0, 0, time.UTC), // Sun Nov 1, 2026
			wantUTC:  time.Date(2026, time.November, 1, 8, 0, 0, 0, time.UTC),
		},
		{
			name:    "impossible calendar date Feb 30 rejected",
			expr:    "0 0 30 2 *",
			wantErr: true,
		},
		{
			name:    "too few fields",
			expr:    "* * * *",
			wantErr: true,
		},
		{
			name:    "out of bounds minute",
			expr:    "60 * * * *",
			wantErr: true,
		},
		{
			name:    "invalid step",
			expr:    "*/0 * * * *",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cron, _, err := ParseCron(tt.expr)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseCron(%q) error = %v, wantErr %v", tt.expr, err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			loc, _, err := ValidateTimezone(tt.timezone)
			if err != nil {
				t.Fatalf("ValidateTimezone(%q) error = %v", tt.timezone, err)
			}
			got, err := cron.Next(tt.after, loc)
			if err != nil {
				t.Fatalf("Next() error = %v", err)
			}
			if !got.Equal(tt.wantUTC) {
				t.Fatalf("Next() = %s, want %s", got.Format(time.RFC3339), tt.wantUTC.Format(time.RFC3339))
			}
		})
	}
}

func TestNewScheduleSubmissionValidationAndFingerprint(t *testing.T) {
	first, err := NewScheduleSubmission(
		"nightly.cleanup",
		"",
		"demo.echo",
		json.RawMessage(`{"b":2,"a":1}`),
		10,
		"  0   2 * * * ",
		"",
		false,
		1024,
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewScheduleSubmission(
		"nightly.cleanup",
		"default",
		"demo.echo",
		json.RawMessage(`{"a": 1, "b": 2}`),
		10,
		"0 2 * * *",
		"UTC",
		true,
		1024,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Fingerprint[:], second.Fingerprint[:]) {
		t.Fatal("expected normalized schedule submissions to have identical fingerprints")
	}

	if _, err := NewScheduleSubmission("Bad Name", "default", "demo.echo", json.RawMessage(`{}`), 0, "* * * * *", "UTC", false, 1024); err == nil {
		t.Fatal("expected invalid schedule name error")
	}
	if _, err := NewScheduleSubmission("valid", "default", "demo.echo", json.RawMessage(`{}`), 0, "* * * * *", "Local", false, 1024); err == nil {
		t.Fatal("expected Local timezone rejection")
	}
	if _, err := NewScheduleSubmission("valid", "default", "demo.echo", json.RawMessage(`{}`), 0, "* * * * *", "Invalid/Zone", false, 1024); err == nil {
		t.Fatal("expected unknown timezone rejection")
	}
}
