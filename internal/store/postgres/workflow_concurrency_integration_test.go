package postgres

import (
	"context"
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
