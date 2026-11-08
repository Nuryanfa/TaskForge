package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/job"
)

// byStep picks an outcome by step: action jobs by step name, compensation jobs
// by "undo:<name>". Anything not listed succeeds.
func byStep(t *testing.T, s *Store, plan map[string]outcomeFunc) outcomeFunc {
	return func(claimed job.Claimed) (json.RawMessage, *job.Failure) {
		key := stepName(t, s, claimed)
		if claimed.Job.WorkflowRole != nil && *claimed.Job.WorkflowRole == job.RoleCompensation {
			key = "undo:" + key
		}
		if outcome, ok := plan[key]; ok {
			return outcome(claimed)
		}
		return succeed(claimed)
	}
}

func compensationOrder(t *testing.T, s *Store, workflowID string) []string {
	t.Helper()
	rows, err := s.pool.Query(context.Background(), `SELECT st.name FROM jobs j
		JOIN workflow_steps st ON st.id = j.workflow_step_id
		WHERE j.workflow_id=$1 AND j.workflow_role='compensation' ORDER BY j.created_at, st.name`, workflowID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	return names
}

func countJobs(t *testing.T, s *Store, workflowID, role string) int {
	t.Helper()
	var count int
	if err := s.pool.QueryRow(context.Background(), `SELECT count(*) FROM jobs WHERE workflow_id=$1 AND workflow_role=$2`, workflowID, role).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestWorkflowLinearSuccessIntegration(t *testing.T) {
	store := workflowStore(t, 3)
	wf := submitWorkflow(t, store, withCompensation(step("a", "demo.echo"), "demo.echo"), step("b", "demo.echo", "a"), step("c", "demo.echo", "b"))
	final := drainWorkflow(t, store, wf.ID, succeed)
	if final.Status != job.WorkflowSucceeded || final.CompletedAt == nil || final.AbortReason != nil {
		t.Fatalf("final=%+v", final)
	}
	if countJobs(t, store, wf.ID, job.RoleAction) != 3 || countJobs(t, store, wf.ID, job.RoleCompensation) != 0 {
		t.Fatal("unexpected job counts")
	}
	if a := stepByName(t, final, "a"); a.CompensationStatus != job.CompensationWaiting {
		t.Fatalf("succeeded workflow leaves compensation waiting, got %s", a.CompensationStatus)
	}
	assertWorkflowInvariants(t, store, wf.ID)
}

func TestWorkflowDiamondSuccessIntegration(t *testing.T) {
	store := workflowStore(t, 3)
	wf := submitWorkflow(t, store, diamondSteps()...)
	driveNext(t, store, "w", succeed) // a
	first := driveNext(t, store, "w", succeed)
	if name := stepName(t, store, first); name != "b" && name != "c" {
		t.Fatalf("second job was %q", name)
	}
	if d := stepByName(t, getWorkflow(t, store, wf.ID), "d"); d.Status != job.StepPending {
		t.Fatalf("d dispatched before both dependencies succeeded: %+v", d)
	}
	final := drainWorkflow(t, store, wf.ID, succeed)
	if final.Status != job.WorkflowSucceeded {
		t.Fatalf("final=%s", final.Status)
	}
	assertWorkflowInvariants(t, store, wf.ID)
}

func compensatedDiamond() []job.WorkflowStepRequest {
	return []job.WorkflowStepRequest{
		withCompensation(step("a", "demo.echo"), "demo.echo"),
		withCompensation(step("b", "demo.echo", "a"), "demo.echo"),
		withCompensation(step("c", "demo.echo", "a"), "demo.echo"),
		withCompensation(step("d", "demo.fail", "b", "c"), "demo.echo"),
	}
}

func TestWorkflowDiamondFailureCompensatesInReverseIntegration(t *testing.T) {
	store := workflowStore(t, 1)
	wf := submitWorkflow(t, store, compensatedDiamond()...)
	plan := byStep(t, store, map[string]outcomeFunc{"d": failPermanent("DEMO_FAILURE")})
	for range 4 {
		driveNext(t, store, "w", plan)
	}
	mid := getWorkflow(t, store, wf.ID)
	if mid.Status != job.WorkflowCompensating || mid.FailedStep == nil || *mid.FailedStep != "d" ||
		mid.ErrorCode == nil || *mid.ErrorCode != "DEMO_FAILURE" || *mid.AbortReason != job.AbortStepFailed {
		t.Fatalf("after failure=%+v", mid)
	}
	if got := compensationOrder(t, store, wf.ID); len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Fatalf("first compensation wave=%v", got)
	}
	final := drainWorkflow(t, store, wf.ID, plan)
	if final.Status != job.WorkflowCompensated || final.CompletedAt == nil {
		t.Fatalf("final=%+v", final)
	}
	if got := compensationOrder(t, store, wf.ID); len(got) != 3 || got[2] != "a" {
		t.Fatalf("compensation order=%v", got)
	}
	if d := stepByName(t, final, "d"); d.Status != job.StepFailed || d.CompensationJobID != nil {
		t.Fatalf("failed step was compensated: %+v", d)
	}
	assertWorkflowInvariants(t, store, wf.ID)
}

func TestWorkflowNotDefinedCompensationIsSkippedIntegration(t *testing.T) {
	store := workflowStore(t, 1)
	wf := submitWorkflow(t, store,
		withCompensation(step("a", "demo.echo"), "demo.echo"),
		step("b", "demo.echo", "a"),
		step("c", "demo.fail", "b"))
	plan := byStep(t, store, map[string]outcomeFunc{"c": failPermanent("DEMO_FAILURE")})
	final := drainWorkflow(t, store, wf.ID, plan)
	if final.Status != job.WorkflowCompensated {
		t.Fatalf("final=%s", final.Status)
	}
	if b := stepByName(t, final, "b"); b.CompensationStatus != job.CompensationNotDefined {
		t.Fatalf("b compensation=%s", b.CompensationStatus)
	}
	if a := stepByName(t, final, "a"); a.CompensationStatus != job.CompensationSucceeded {
		t.Fatalf("a compensation=%s", a.CompensationStatus)
	}
	assertWorkflowInvariants(t, store, wf.ID)
}

func TestWorkflowRetryableStepDoesNotDuplicateIntegration(t *testing.T) {
	store := workflowStore(t, 3)
	wf := submitWorkflow(t, store, step("a", "demo.echo"), step("b", "demo.echo", "a"))
	driveNext(t, store, "w", failRetryable("DEMO_TRANSIENT"))
	driveNext(t, store, "w", failRetryable("DEMO_TRANSIENT"))
	final := drainWorkflow(t, store, wf.ID, succeed)
	if final.Status != job.WorkflowSucceeded || countJobs(t, store, wf.ID, job.RoleAction) != 2 {
		t.Fatalf("final=%s jobs=%d", final.Status, countJobs(t, store, wf.ID, job.RoleAction))
	}
	var attempts int
	if err := store.pool.QueryRow(context.Background(), `SELECT count(*) FROM job_attempts WHERE job_id=$1`, *stepByName(t, final, "a").JobID).Scan(&attempts); err != nil || attempts != 3 {
		t.Fatalf("attempts=%d err=%v", attempts, err)
	}
	assertWorkflowInvariants(t, store, wf.ID)
}

func TestWorkflowCompensationFailureIntegration(t *testing.T) {
	store := workflowStore(t, 1)
	wf := submitWorkflow(t, store, compensatedDiamond()...)
	plan := byStep(t, store, map[string]outcomeFunc{"d": failPermanent("DEMO_FAILURE"), "undo:b": failPermanent("REFUND_REJECTED")})
	for range 4 {
		driveNext(t, store, "w", plan)
	}
	// Two compensations (b and c) are now queued. Fail b's while c's is held.
	first := claimNext(t, store, "w")
	second := claimNext(t, store, "w")
	compB, compC := first, second
	if stepName(t, store, first) == "c" {
		compB, compC = second, first
	}
	applyOutcome(t, store, compB, plan)
	failedNow := getWorkflow(t, store, wf.ID)
	if failedNow.Status != job.WorkflowCompensationFailed || failedNow.CompletedAt == nil {
		t.Fatalf("after failed compensation=%+v", failedNow)
	}
	applyOutcome(t, store, compC, succeed)
	final := getWorkflow(t, store, wf.ID)
	if final.Status != job.WorkflowCompensationFailed {
		t.Fatalf("status changed after in-flight compensation: %s", final.Status)
	}
	if c := stepByName(t, final, "c"); c.CompensationStatus != job.CompensationSucceeded {
		t.Fatalf("in-flight compensation not recorded: %+v", c)
	}
	if a := stepByName(t, final, "a"); a.CompensationJobID != nil || a.CompensationStatus != job.CompensationWaiting {
		t.Fatalf("a was compensated after compensation failure: %+v", a)
	}
	assertWorkflowInvariants(t, store, wf.ID)
}

func TestWorkflowUnknownKindAbortsIntegration(t *testing.T) {
	store := workflowStore(t, 1)
	wf := submitWorkflow(t, store, withCompensation(step("a", "demo.echo"), "demo.echo"), step("b", "no.such.kind", "a"))
	plan := byStep(t, store, map[string]outcomeFunc{"b": failPermanent("UNKNOWN_JOB_KIND")})
	final := drainWorkflow(t, store, wf.ID, plan)
	if final.Status != job.WorkflowCompensated || final.ErrorCode == nil || *final.ErrorCode != "UNKNOWN_JOB_KIND" {
		t.Fatalf("final=%+v", final)
	}
	assertWorkflowInvariants(t, store, wf.ID)
}

func TestWorkflowFailureWithNothingToCompensateIntegration(t *testing.T) {
	store := workflowStore(t, 1)
	wf := submitWorkflow(t, store, step("only", "demo.fail"))
	driveNext(t, store, "w", failPermanent("DEMO_FAILURE"))
	final := getWorkflow(t, store, wf.ID)
	if final.Status != job.WorkflowCompensated || final.CompletedAt == nil {
		t.Fatalf("final=%+v", final)
	}
	assertWorkflowInvariants(t, store, wf.ID)
}

func TestWorkflowAbortCancelsQueuedSiblingsIntegration(t *testing.T) {
	store := workflowStore(t, 1)
	// Roots share created_at (one transaction), so priority makes a claim first.
	first := step("a", "demo.fail")
	first.Priority = 10
	wf := submitWorkflow(t, store, first, step("b", "demo.echo"), step("c", "demo.echo"), step("z", "demo.echo", "a", "b", "c"))
	claimedA := claimNext(t, store, "w")
	if name := stepName(t, store, claimedA); name != "a" {
		t.Fatalf("claimed %q first, want a", name)
	}
	applyOutcome(t, store, claimedA, failPermanent("DEMO_FAILURE"))
	final := getWorkflow(t, store, wf.ID)
	if final.Status != job.WorkflowCompensated {
		t.Fatalf("final=%s", final.Status)
	}
	for _, name := range []string{"b", "c"} {
		item := stepByName(t, final, name)
		found, err := store.GetJob(context.Background(), *item.JobID)
		if item.Status != job.StepCanceled || err != nil || found.Status != job.StatusCanceled {
			t.Fatalf("step %s=%+v job=%s err=%v", name, item, found.Status, err)
		}
	}
	if z := stepByName(t, final, "z"); z.Status != job.StepSkipped {
		t.Fatalf("z=%s", z.Status)
	}
	assertWorkflowInvariants(t, store, wf.ID)
}

func TestCancelQueuedJobRejectsWorkflowJobIntegration(t *testing.T) {
	store := workflowStore(t, 1)
	wf := submitWorkflow(t, store, step("a", "demo.echo"))
	if _, err := store.CancelQueuedJob(context.Background(), *stepByName(t, wf, "a").JobID); !errors.Is(err, ErrWorkflowOwnedJob) {
		t.Fatalf("cancel err=%v", err)
	}
	if got := getWorkflow(t, store, wf.ID); got.Status != job.WorkflowRunning || stepByName(t, got, "a").Status != job.StepDispatched {
		t.Fatalf("workflow changed: %+v", got)
	}
}

func TestRedriveWorkflowJobIsStandaloneIntegration(t *testing.T) {
	store := workflowStore(t, 1)
	wf := submitWorkflow(t, store, step("a", "demo.fail"))
	failed := driveNext(t, store, "w", failPermanent("DEMO_FAILURE"))
	before := getWorkflow(t, store, wf.ID)
	redriven, isNew, err := store.RedriveDeadLetter(context.Background(), failed.Job.ID)
	if err != nil || !isNew || redriven.WorkflowID != nil || redriven.WorkflowRole != nil {
		t.Fatalf("redriven=%+v new=%v err=%v", redriven, isNew, err)
	}
	after := getWorkflow(t, store, wf.ID)
	if after.Status != before.Status || !after.UpdatedAt.Equal(before.UpdatedAt) {
		t.Fatalf("workflow changed by redrive: before=%+v after=%+v", before, after)
	}
}

func TestRecoveryDeadLetterAdvancesWorkflowIntegration(t *testing.T) {
	store := workflowStore(t, 1)
	wf := submitWorkflow(t, store, withCompensation(step("a", "demo.echo"), "demo.echo"), step("b", "demo.sleep", "a"))
	driveNext(t, store, "w", succeed)
	if _, err := store.ClaimNextJob(context.Background(), "doomed", time.Second); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	recovered, err := store.RecoverExpiredJobs(context.Background(), 10)
	if err != nil || recovered != 1 {
		t.Fatalf("recovered=%d err=%v", recovered, err)
	}
	mid := getWorkflow(t, store, wf.ID)
	if mid.Status != job.WorkflowCompensating || *mid.ErrorCode != "LEASE_EXPIRED" || *mid.FailedStep != "b" {
		t.Fatalf("after recovery=%+v", mid)
	}
	final := drainWorkflow(t, store, wf.ID, succeed)
	if final.Status != job.WorkflowCompensated {
		t.Fatalf("final=%s", final.Status)
	}
	assertWorkflowInvariants(t, store, wf.ID)
}
