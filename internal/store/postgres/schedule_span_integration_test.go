package postgres

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Nuryanfa/TaskForge/internal/job"
	"github.com/Nuryanfa/TaskForge/internal/telemetry/telemetrytest"
)

// TestScheduleDispatchSpanMarksConflictIntegration: a slot whose job already
// exists creates nothing, and its schedule.dispatch span says so.
func TestScheduleDispatchSpanMarksConflictIntegration(t *testing.T) {
	exporter := telemetrytest.Setup(t)
	store := integrationStore(t)
	ctx := context.Background()
	submission, err := job.NewScheduleSubmission("span.conflict", "default", "demo.echo", json.RawMessage(`{"message":"tick"}`), 0, "* * * * *", "UTC", false, 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	created, _, err := store.CreateSchedule(ctx, submission)
	if err != nil {
		t.Fatal(err)
	}
	const rewind = `UPDATE schedules SET next_run_at = date_trunc('minute', NOW()) - INTERVAL '1 minute' WHERE id = $1`
	for range 2 {
		if _, err := store.pool.Exec(ctx, rewind, created.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := store.DispatchDueSchedules(ctx, 10); err != nil {
			t.Fatal(err)
		}
	}
	var outcomes []string
	for _, span := range exporter.GetSpans() {
		if span.Name != "schedule.dispatch" {
			continue
		}
		value := "missing"
		for _, kv := range span.Attributes {
			if kv.Key == "taskforge.schedule.created" {
				value = kv.Value.Emit()
			}
		}
		outcomes = append(outcomes, value)
	}
	if len(outcomes) != 2 || outcomes[0] != "true" || outcomes[1] != "false" {
		t.Fatalf("schedule.dispatch created attributes=%v, want [true false]", outcomes)
	}
}
