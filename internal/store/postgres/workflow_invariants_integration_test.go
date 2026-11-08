package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

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

// TestRecoveryIsolatesAFailingWorkflowJobIntegration corrupts one workflow so
// that its engine pass fails. Lease recovery must still recover every other
// expired job in the batch instead of rolling all of them back forever.
func TestRecoveryIsolatesAFailingWorkflowJobIntegration(t *testing.T) {
	store := workflowStore(t, 1)
	ctx := context.Background()
	wf := submitWorkflow(t, store, step("a", "demo.echo"))
	standalone, _, err := store.CreateJob(ctx, submission(t, 0, nil, `{"message":"bystander"}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, worker := range []string{"doomed-1", "doomed-2"} {
		if _, err := store.ClaimNextJob(ctx, worker, time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.pool.Exec(ctx, `UPDATE workflow_steps SET status='succeeded', compensation_status='not_defined' WHERE workflow_id=$1`, wf.ID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	for range 2 {
		recovered, err := store.RecoverExpiredJobs(ctx, 10)
		if !errors.Is(err, ErrStateConflict) {
			t.Fatalf("recovery err=%v, want the poisoned job's state conflict reported", err)
		}
		if recovered > 1 {
			t.Fatalf("recovered=%d", recovered)
		}
	}
	bystander, err := store.GetJob(ctx, standalone.ID)
	if err != nil || bystander.Status != job.StatusFailed || bystander.LastErrorCode == nil || *bystander.LastErrorCode != "LEASE_EXPIRED" {
		t.Fatalf("bystander=%+v err=%v", bystander.Status, err)
	}
	poisoned, err := store.GetJob(ctx, *getWorkflow(t, store, wf.ID).Steps[0].JobID)
	if err != nil || poisoned.Status != job.StatusRunning {
		t.Fatalf("poisoned job status=%s err=%v, want left running for an operator", poisoned.Status, err)
	}
}
