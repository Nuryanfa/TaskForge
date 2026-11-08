package postgres

import (
	"context"
	"strings"
	"testing"

	"github.com/Nuryanfa/TaskForge/internal/job"
)

func hasViolation(violations []string, prefix string) bool {
	for _, violation := range violations {
		if strings.HasPrefix(violation, prefix) {
			return true
		}
	}
	return false
}

// TestInvariantCheckerDetectsEarlyCompensationIntegration simulates a buggy
// engine that compensates a step while its dependent is still running. The
// dependent then fails, which must not hide the ordering violation from I5.
func TestInvariantCheckerDetectsEarlyCompensationIntegration(t *testing.T) {
	store := workflowStore(t, 1)
	wf := submitWorkflow(t, store,
		withCompensation(step("a", "demo.echo"), "demo.echo"),
		withCompensation(step("b", "demo.fail", "a"), "demo.echo"))
	driveNext(t, store, "w", succeed) // a succeeds, b dispatched
	runningB := claimNext(t, store, "w")

	ctx := context.Background()
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	steps, err := loadSteps(ctx, tx, wf.ID)
	if err != nil {
		t.Fatal(err)
	}
	locked, err := lockWorkflow(ctx, tx, wf.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatchStepJob(ctx, tx, wf.ID, locked.policy, steps[0], job.RoleCompensation); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	applyOutcome(t, store, runningB, failPermanent("DEMO_FAILURE"))
	drainWorkflow(t, store, wf.ID, succeed)
	if violations := workflowInvariantViolations(t, store, wf.ID); !hasViolation(violations, "I5") {
		t.Fatalf("I5 did not detect an early compensation; violations=%v", violations)
	}
}

// TestInvariantCheckerDetectsBypassedEngineIntegration finishes a workflow job
// behind the engine's back: the workflow is left running with no live work.
func TestInvariantCheckerDetectsBypassedEngineIntegration(t *testing.T) {
	store := workflowStore(t, 1)
	wf := submitWorkflow(t, store, step("a", "demo.echo"))
	if _, err := store.pool.Exec(context.Background(), `UPDATE jobs SET status='canceled', completed_at=NOW() WHERE workflow_id=$1`, wf.ID); err != nil {
		t.Fatal(err)
	}
	violations := workflowInvariantViolations(t, store, wf.ID)
	if !hasViolation(violations, "I8") || !hasViolation(violations, "I9") {
		t.Fatalf("bypass not detected; violations=%v", violations)
	}
}
