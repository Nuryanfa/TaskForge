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

// TestAbortSurvivorIsCanceledOnNextEnginePassIntegration: a queued sibling
// that was row-locked elsewhere when the workflow aborted survives the abort
// (SKIP LOCKED). The next engine pass of that workflow must cancel it rather
// than let a compensating workflow start new actions.
func TestAbortSurvivorIsCanceledOnNextEnginePassIntegration(t *testing.T) {
	store := workflowStore(t, 1)
	ctx := context.Background()
	failing, slow, survivor := step("a", "demo.fail"), withCompensation(step("c", "demo.echo"), "demo.echo"), step("b", "demo.echo")
	failing.Priority, slow.Priority, survivor.Priority = 10, 8, 5
	wf := submitWorkflow(t, store, failing, survivor, slow)
	claimedA, claimedC := claimNext(t, store, "w1"), claimNext(t, store, "w2")
	if stepName(t, store, claimedA) != "a" || stepName(t, store, claimedC) != "c" {
		t.Fatal("unexpected claim order")
	}
	survivorJob := *stepByName(t, getWorkflow(t, store, wf.ID), "b").JobID
	holder, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Exec(ctx, `SELECT id FROM jobs WHERE id=$1 FOR UPDATE`, survivorJob); err != nil {
		t.Fatal(err)
	}
	applyOutcome(t, store, claimedA, failPermanent("DEMO_FAILURE"))
	if err := holder.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if b := stepByName(t, getWorkflow(t, store, wf.ID), "b"); b.Status != job.StepDispatched {
		t.Fatalf("precondition: survivor should still be dispatched, got %s", b.Status)
	}
	applyOutcome(t, store, claimedC, succeed)
	if b := stepByName(t, getWorkflow(t, store, wf.ID), "b"); b.Status != job.StepCanceled {
		t.Fatalf("survivor after next engine pass=%s, want canceled", b.Status)
	}
	final := drainWorkflow(t, store, wf.ID, succeed)
	if final.Status != job.WorkflowCompensated {
		t.Fatalf("final=%s", final.Status)
	}
	assertWorkflowInvariants(t, store, wf.ID)
}

// TestGetWorkflowReturnsAConsistentSnapshotIntegration reads workflows while
// their only step completes. The workflow status and the step status must
// always come from the same snapshot.
func TestGetWorkflowReturnsAConsistentSnapshotIntegration(t *testing.T) {
	store := workflowStore(t, 1)
	ctx := context.Background()
	for round := range 100 {
		wf := submitWorkflow(t, store, step("only", "demo.echo"))
		claimed := claimNext(t, store, "w")
		done := make(chan struct{})
		inconsistent := make(chan string, 1)
		go func() {
			defer close(done)
			for {
				read, err := store.GetWorkflow(ctx, wf.ID)
				if err != nil {
					inconsistent <- err.Error()
					return
				}
				stepDone := read.Steps[0].Status == job.StepSucceeded
				if stepDone != (read.Status == job.WorkflowSucceeded) {
					inconsistent <- string(read.Status) + "/" + string(read.Steps[0].Status)
					return
				}
				if read.Status.Terminal() {
					return
				}
			}
		}()
		applyOutcome(t, store, claimed, succeed)
		<-done
		select {
		case got := <-inconsistent:
			t.Fatalf("round %d: inconsistent read %s", round, got)
		default:
		}
	}
}

// TestDispatchedJobIsCreatedAfterItsDependenciesIntegration forces the
// interleaving the soak test found: the transaction completing d2 starts first
// but blocks, d1 completes meanwhile, and then d2's transaction dispatches the
// dependent. The dependent's created_at must not precede d1's completion even
// though NOW() in d2's transaction is older.
func TestDispatchedJobIsCreatedAfterItsDependenciesIntegration(t *testing.T) {
	store := workflowStore(t, 1)
	ctx := context.Background()
	wf := submitWorkflow(t, store, step("d1", "demo.echo"), step("d2", "demo.echo"), step("s", "demo.echo", "d1", "d2"))
	first, second := claimNext(t, store, "w1"), claimNext(t, store, "w2")
	d1, d2 := first, second
	if stepName(t, store, first) == "d2" {
		d1, d2 = second, first
	}
	holder, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Exec(ctx, `SELECT id FROM jobs WHERE id=$1 FOR UPDATE`, d2.Job.ID); err != nil {
		t.Fatal(err)
	}
	blocked := make(chan error, 1)
	go func() { blocked <- store.CompleteJob(ctx, d2.Execution, []byte(`{"ok":true}`)) }()
	time.Sleep(200 * time.Millisecond) // d2's transaction has started and is waiting
	applyOutcome(t, store, d1, succeed)
	if err := holder.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-blocked; err != nil {
		t.Fatal(err)
	}
	if violations := workflowInvariantViolations(t, store, wf.ID); hasViolation(violations, "I2") {
		t.Fatalf("dependent job timestamped before its dependency completed: %v", violations)
	}
}
