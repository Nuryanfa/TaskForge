package postgres

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/job"
	"github.com/google/uuid"
)

func TestWorkflowCancelWhileStepRunningIntegration(t *testing.T) {
	store := workflowStore(t, 1)
	ctx := context.Background()
	wf := submitWorkflow(t, store, withCompensation(step("a", "demo.sleep"), "demo.echo"), withCompensation(step("b", "demo.echo", "a"), "demo.echo"))
	runningA := claimNext(t, store, "w")
	canceled, err := store.CancelWorkflow(ctx, wf.ID)
	if err != nil {
		t.Fatal(err)
	}
	if canceled.Status != job.WorkflowCompensating || canceled.AbortReason == nil || *canceled.AbortReason != job.AbortCanceled || canceled.FailedStep != nil {
		t.Fatalf("after cancel=%+v", canceled)
	}
	if b := stepByName(t, canceled, "b"); b.Status != job.StepSkipped {
		t.Fatalf("b=%s", b.Status)
	}
	applyOutcome(t, store, runningA, succeed)
	if a := stepByName(t, getWorkflow(t, store, wf.ID), "a"); a.CompensationStatus != job.CompensationDispatched {
		t.Fatalf("a compensation=%s", a.CompensationStatus)
	}
	final := drainWorkflow(t, store, wf.ID, succeed)
	if final.Status != job.WorkflowCanceled || final.CompletedAt == nil {
		t.Fatalf("final=%+v", final)
	}
	assertWorkflowInvariants(t, store, wf.ID)
}

func TestWorkflowCancelBeforeAnyRunIntegration(t *testing.T) {
	store := workflowStore(t, 1)
	wf := submitWorkflow(t, store, withCompensation(step("a", "demo.echo"), "demo.echo"), step("b", "demo.echo", "a"))
	canceled, err := store.CancelWorkflow(context.Background(), wf.ID)
	if err != nil || canceled.Status != job.WorkflowCanceled {
		t.Fatalf("canceled=%+v err=%v", canceled, err)
	}
	if a := stepByName(t, canceled, "a"); a.Status != job.StepCanceled {
		t.Fatalf("a=%s", a.Status)
	}
	if countJobs(t, store, wf.ID, job.RoleCompensation) != 0 {
		t.Fatal("canceled-before-run workflow created compensations")
	}
	assertWorkflowInvariants(t, store, wf.ID)
}

func TestWorkflowCancelConflictIntegration(t *testing.T) {
	store := workflowStore(t, 1)
	ctx := context.Background()
	done := submitWorkflow(t, store, step("a", "demo.echo"))
	drainWorkflow(t, store, done.ID, succeed)
	if _, err := store.CancelWorkflow(ctx, done.ID); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("cancel succeeded workflow err=%v", err)
	}
	compensating := submitWorkflow(t, store, step("x", "demo.sleep"))
	claimNext(t, store, "w")
	if _, err := store.CancelWorkflow(ctx, compensating.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CancelWorkflow(ctx, compensating.ID); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("second cancel err=%v", err)
	}
	if _, err := store.CancelWorkflow(ctx, uuid.NewString()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown workflow err=%v", err)
	}
}

func TestWorkflowCancelRacesCompletionIntegration(t *testing.T) {
	store := workflowStore(t, 1)
	ctx := context.Background()
	outcomes := map[job.WorkflowStatus]int{}
	for range 20 {
		wf := submitWorkflow(t, store, withCompensation(step("a", "demo.echo"), "demo.echo"))
		claimed := claimNext(t, store, "w")
		start := make(chan struct{})
		var cancelErr, completeErr error
		var wait sync.WaitGroup
		wait.Add(2)
		go func() {
			defer wait.Done()
			<-start
			_, cancelErr = store.CancelWorkflow(ctx, wf.ID)
		}()
		go func() {
			defer wait.Done()
			<-start
			completeErr = store.CompleteJob(ctx, claimed.Execution, []byte(`{"ok":true}`))
		}()
		close(start)
		wait.Wait()
		if completeErr != nil {
			t.Fatalf("complete err=%v", completeErr)
		}
		final := drainWorkflow(t, store, wf.ID, succeed)
		switch final.Status {
		case job.WorkflowSucceeded:
			if !errors.Is(cancelErr, ErrStateConflict) {
				t.Fatalf("succeeded but cancel err=%v", cancelErr)
			}
		case job.WorkflowCanceled:
			if cancelErr != nil || stepByName(t, final, "a").CompensationStatus != job.CompensationSucceeded {
				t.Fatalf("canceled final=%+v cancel err=%v", final, cancelErr)
			}
		default:
			t.Fatalf("unexpected final status %s", final.Status)
		}
		outcomes[final.Status]++
		assertWorkflowInvariants(t, store, wf.ID)
	}
	t.Logf("race outcomes: %v", outcomes)
}

func TestClaimLoadsWorkflowContextIntegration(t *testing.T) {
	store := workflowStore(t, 1)
	ctx := context.Background()
	if _, _, err := store.CreateJob(ctx, submission(t, -10, nil, `{"message":"standalone"}`)); err != nil {
		t.Fatal(err)
	}
	wf := submitWorkflow(t, store, withCompensation(step("a", "demo.echo"), "demo.echo"), step("b", "demo.fail", "a"))
	action := claimNext(t, store, "w")
	if action.Execution.Workflow == nil || action.Execution.Workflow.WorkflowID != wf.ID ||
		action.Execution.Workflow.Step != "a" || action.Execution.Workflow.Role != job.RoleAction ||
		action.Execution.Workflow.CompensatesJobID != "" || action.Execution.Workflow.CompensatesExecutionKey != "" {
		t.Fatalf("action context=%+v", action.Execution.Workflow)
	}
	applyOutcome(t, store, action, succeed)
	driveNext(t, store, "w", failPermanent("DEMO_FAILURE"))
	compensation := claimNext(t, store, "w")
	ctxW := compensation.Execution.Workflow
	if ctxW == nil || ctxW.Role != job.RoleCompensation || ctxW.Step != "a" ||
		ctxW.CompensatesJobID != action.Job.ID || ctxW.CompensatesExecutionKey != action.Execution.ExecutionKey {
		t.Fatalf("compensation context=%+v action job=%s key=%s", ctxW, action.Job.ID, action.Execution.ExecutionKey)
	}
	if compensation.Execution.ExecutionKey == action.Execution.ExecutionKey {
		t.Fatal("compensation reused the action's own execution key")
	}
	applyOutcome(t, store, compensation, succeed)
	standalone := claimNext(t, store, "w")
	if standalone.Job.WorkflowID != nil || standalone.Execution.Workflow != nil {
		t.Fatalf("standalone context=%+v", standalone.Execution.Workflow)
	}
}

// TestAbortedWorkflowDoesNotRetryItsRunningStepIntegration: a step running
// when its workflow is canceled fails retryably. It must not be re-queued and
// executed again; the failure is final because the workflow has stopped.
func TestAbortedWorkflowDoesNotRetryItsRunningStepIntegration(t *testing.T) {
	store := workflowStore(t, 3)
	ctx := context.Background()
	wf := submitWorkflow(t, store, withCompensation(step("a", "demo.flaky"), "demo.echo"))
	running := claimNext(t, store, "w")
	if _, err := store.CancelWorkflow(ctx, wf.ID); err != nil {
		t.Fatal(err)
	}
	applyOutcome(t, store, running, failRetryable("DEMO_TRANSIENT"))
	final := getWorkflow(t, store, wf.ID)
	if final.Status != job.WorkflowCanceled || stepByName(t, final, "a").Status != job.StepFailed {
		t.Fatalf("final=%s step=%s", final.Status, stepByName(t, final, "a").Status)
	}
	if _, err := store.ClaimNextJob(ctx, "w", time.Minute); !errors.Is(err, ErrNotFound) {
		t.Fatalf("aborted workflow's step was re-queued: %v", err)
	}
	assertWorkflowInvariants(t, store, wf.ID)
}

// TestRedrivenCompensationIsStandaloneWithItsPayloadIntegration pins the
// behavior the operations runbook relies on: a redriven compensation runs as
// a standalone job without workflow context, so it must find what to undo in
// its own payload, which redrive preserves.
func TestRedrivenCompensationIsStandaloneWithItsPayloadIntegration(t *testing.T) {
	store := workflowStore(t, 1)
	ctx := context.Background()
	wf := submitWorkflow(t, store, withCompensation(step("a", "demo.echo"), "demo.echo"), step("b", "demo.fail", "a"))
	driveNext(t, store, "w", succeed)
	driveNext(t, store, "w", failPermanent("DEMO_FAILURE"))
	failedCompensation := driveNext(t, store, "w", failPermanent("REFUND_REJECTED"))
	if getWorkflow(t, store, wf.ID).Status != job.WorkflowCompensationFailed {
		t.Fatal("precondition: workflow should be compensation_failed")
	}
	redriven, _, err := store.RedriveDeadLetter(ctx, failedCompensation.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	claimed := claimNext(t, store, "operator-worker")
	if claimed.Job.ID != redriven.ID || claimed.Execution.Workflow != nil {
		t.Fatalf("claimed %s with workflow context %+v", claimed.Job.ID, claimed.Execution.Workflow)
	}
	if string(claimed.Job.Payload) != string(failedCompensation.Job.Payload) {
		t.Fatalf("redrive changed the compensation payload: %s vs %s", claimed.Job.Payload, failedCompensation.Job.Payload)
	}
	if getWorkflow(t, store, wf.ID).Status != job.WorkflowCompensationFailed {
		t.Fatal("redrive changed the workflow status")
	}
}
