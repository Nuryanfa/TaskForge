package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/job"
)

// TestWorkflowFanInRaceIntegration completes 16 parallel roots at the same
// instant from 16 goroutines. The sink must be dispatched exactly once.
func TestWorkflowFanInRaceIntegration(t *testing.T) {
	store := workflowStore(t, 1)
	steps := make([]job.WorkflowStepRequest, 0, 17)
	roots := make([]string, 0, 16)
	for i := range 16 {
		name := fmt.Sprintf("r%02d", i)
		roots = append(roots, name)
		steps = append(steps, step(name, "demo.echo"))
	}
	steps = append(steps, step("z", "demo.echo", roots...))
	wf := submitWorkflow(t, store, steps...)

	claims := make([]job.Claimed, 16)
	for i := range claims {
		claims[i] = claimNext(t, store, fmt.Sprintf("worker-%02d", i))
	}
	start := make(chan struct{})
	errs := make([]error, len(claims))
	var wait sync.WaitGroup
	for i, claimed := range claims {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			errs[i] = store.CompleteJob(context.Background(), claimed.Execution, []byte(`{"ok":true}`))
		}()
	}
	close(start)
	wait.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("completion %d: %v", i, err)
		}
	}
	var sinkJobs, remaining int
	if err := store.pool.QueryRow(context.Background(), `SELECT
		(SELECT count(*) FROM jobs j JOIN workflow_steps s ON s.id = j.workflow_step_id WHERE s.workflow_id=$1 AND s.name='z'),
		(SELECT remaining_dependencies FROM workflow_steps WHERE workflow_id=$1 AND name='z')`, wf.ID).Scan(&sinkJobs, &remaining); err != nil {
		t.Fatal(err)
	}
	if sinkJobs != 1 || remaining != 0 {
		t.Fatalf("sink jobs=%d remaining=%d", sinkJobs, remaining)
	}
	final := drainWorkflow(t, store, wf.ID, succeed)
	if final.Status != job.WorkflowSucceeded {
		t.Fatalf("final=%s", final.Status)
	}
	assertWorkflowInvariants(t, store, wf.ID)
}

// TestConcurrentRecoveryAcrossWorkflowsNoDeadlockIntegration runs small
// recovery batches concurrently over expired jobs of two workflows, so batches
// see the workflows in different orders. Workflow rows must be locked in a
// global order or PostgreSQL reports a deadlock (SQLSTATE 40P01).
func TestConcurrentRecoveryAcrossWorkflowsNoDeadlockIntegration(t *testing.T) {
	for round := range 10 {
		store := workflowStore(t, 1)
		// Priorities interleave the claim (and so lease-expiry) order as
		// w1, w2, w2, w1, w1, w2: batches of two then hold the workflows in
		// opposite orders, [w1 w2] and [w2 w1].
		var ids []string
		for _, priorities := range [][3]int{{10, 7, 6}, {9, 8, 5}} {
			var steps []job.WorkflowStepRequest
			for i, name := range []string{"a", "b", "c"} {
				request := withCompensation(step(name, "demo.echo"), "demo.echo")
				request.Priority = priorities[i]
				steps = append(steps, request)
			}
			ids = append(ids, submitWorkflow(t, store, steps...).ID)
		}
		for i := range 6 {
			if _, err := store.ClaimNextJob(context.Background(), fmt.Sprintf("doomed-%d", i), time.Second); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(1100 * time.Millisecond)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		var wait sync.WaitGroup
		errs := make(chan error, 64)
		for range 4 {
			wait.Add(1)
			go func() {
				defer wait.Done()
				for ctx.Err() == nil {
					recovered, err := store.RecoverExpiredJobs(ctx, 2)
					if err != nil {
						errs <- err
						return
					}
					if recovered == 0 {
						return
					}
				}
			}()
		}
		wait.Wait()
		cancel()
		close(errs)
		for err := range errs {
			t.Fatalf("round %d: recovery error: %v", round, err)
		}
		for _, id := range ids {
			final := drainWorkflow(t, store, id, succeed)
			if final.Status != job.WorkflowCompensated || final.ErrorCode == nil || *final.ErrorCode != "LEASE_EXPIRED" {
				t.Fatalf("round %d: workflow %s final=%+v", round, id, final)
			}
			assertWorkflowInvariants(t, store, id)
		}
	}
}

// TestCrashDuringCompensationIntegration kills a worker in the middle of a
// compensation. The lease expires, recovery requeues the compensation, and a
// second worker completes it. The compensation must run against the same
// action execution key both times, the dead worker must be fenced out, and the
// saga must still finish consistently.
func TestCrashDuringCompensationIntegration(t *testing.T) {
	store := workflowStore(t, 3)
	ctx := context.Background()
	wf := submitWorkflow(t, store,
		withCompensation(step("a", "demo.echo"), "demo.echo"),
		withCompensation(step("b", "demo.fail", "a"), "demo.echo"))
	action := driveNext(t, store, "w0", succeed)
	driveNext(t, store, "w0", failPermanent("DEMO_FAILURE"))

	crashed, err := store.ClaimNextJob(ctx, "w1-crashes", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if crashed.Execution.Workflow == nil || crashed.Execution.Workflow.Role != job.RoleCompensation {
		t.Fatalf("w1 claimed %+v, want a's compensation", crashed.Execution.Workflow)
	}
	time.Sleep(1100 * time.Millisecond) // w1 is dead: no heartbeat, no outcome
	if recovered, err := store.RecoverExpiredJobs(ctx, 10); err != nil || recovered != 1 {
		t.Fatalf("recovered=%d err=%v", recovered, err)
	}
	survivor := claimNext(t, store, "w2")
	if survivor.Job.ID != crashed.Job.ID || survivor.Execution.FencingToken <= crashed.Execution.FencingToken {
		t.Fatalf("w2 claimed %s token %d; crashed %s token %d", survivor.Job.ID, survivor.Execution.FencingToken, crashed.Job.ID, crashed.Execution.FencingToken)
	}
	applyOutcome(t, store, survivor, succeed)
	if err := store.CompleteJob(ctx, crashed.Execution, []byte(`{"ok":true}`)); !errors.Is(err, ErrOwnershipLost) {
		t.Fatalf("stale worker completion err=%v, want ErrOwnershipLost", err)
	}

	for name, claimed := range map[string]job.Claimed{"crashed": crashed, "survivor": survivor} {
		if key := claimed.Execution.Workflow.CompensatesExecutionKey; key != action.Execution.ExecutionKey {
			t.Fatalf("%s compensated key %q, want action key %q", name, key, action.Execution.ExecutionKey)
		}
	}
	if countJobs(t, store, wf.ID, job.RoleCompensation) != 1 {
		t.Fatalf("compensation jobs=%d", countJobs(t, store, wf.ID, job.RoleCompensation))
	}
	rows, err := store.pool.Query(ctx, `SELECT outcome, COALESCE(error_code, '') FROM job_attempts WHERE job_id=$1 ORDER BY attempt`, crashed.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	var attempts []string
	for rows.Next() {
		var outcome, code string
		if err := rows.Scan(&outcome, &code); err != nil {
			t.Fatal(err)
		}
		attempts = append(attempts, outcome+":"+code)
	}
	rows.Close()
	if len(attempts) != 2 || attempts[0] != "abandoned:LEASE_EXPIRED" || attempts[1] != "succeeded:" {
		t.Fatalf("attempts=%v", attempts)
	}
	if final := getWorkflow(t, store, wf.ID); final.Status != job.WorkflowCompensated {
		t.Fatalf("final=%s", final.Status)
	}
	assertWorkflowInvariants(t, store, wf.ID)
}
