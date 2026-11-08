package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/job"
)

type recordingObserver struct {
	mu        sync.Mutex
	jobs      []string
	workflows []string
}

func (o *recordingObserver) JobOutcome(kind, outcome, errorCode string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.jobs = append(o.jobs, fmt.Sprintf("%s/%s/%s", kind, outcome, errorCode))
}

func (o *recordingObserver) WorkflowSettled(status string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.workflows = append(o.workflows, status)
}

func (o *recordingObserver) snapshot() ([]string, []string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return slices.Clone(o.jobs), slices.Clone(o.workflows)
}

func observedStore(t *testing.T, maxAttempts int) (*Store, *recordingObserver) {
	t.Helper()
	store := workflowStore(t, maxAttempts)
	observer := &recordingObserver{}
	store.SetObserver(observer)
	return store, observer
}

func TestObserverSeesCommittedOutcomesOnlyIntegration(t *testing.T) {
	store, observer := observedStore(t, 2)
	ctx := context.Background()
	if _, _, err := store.CreateJob(ctx, submission(t, 0, nil, `{"message":"a"}`)); err != nil {
		t.Fatal(err)
	}
	driveNext(t, store, "w", failRetryable("DEMO_TRANSIENT"))
	driveNext(t, store, "w", succeed)
	if _, _, err := store.CreateJob(ctx, submission(t, 0, nil, `{"message":"b"}`)); err != nil {
		t.Fatal(err)
	}
	failed := driveNext(t, store, "w", failPermanent("DEMO_FAILURE"))
	if err := store.CompleteJob(ctx, failed.Execution, json.RawMessage(`{"ok":true}`)); !errors.Is(err, ErrOwnershipLost) {
		t.Fatalf("stale completion err=%v", err)
	}
	jobs, _ := observer.snapshot()
	want := []string{"demo.echo/retry_scheduled/DEMO_TRANSIENT", "demo.echo/succeeded/", "demo.echo/dead_lettered/DEMO_FAILURE"}
	if !slices.Equal(jobs, want) {
		t.Fatalf("events=%v want %v", jobs, want)
	}
}

func TestObserverRecoveryIsolationIntegration(t *testing.T) {
	store, observer := observedStore(t, 1)
	ctx := context.Background()
	wf := submitWorkflow(t, store, step("a", "demo.sleep"))
	if _, _, err := store.CreateJob(ctx, submission(t, -1, nil, `{"message":"bystander"}`)); err != nil {
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
	if _, err := store.RecoverExpiredJobs(ctx, 10); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("recovery err=%v", err)
	}
	jobs, workflows := observer.snapshot()
	want := []string{"demo.echo/abandoned/LEASE_EXPIRED", "demo.echo/dead_lettered/LEASE_EXPIRED"}
	if !slices.Equal(jobs, want) || len(workflows) != 0 {
		t.Fatalf("events=%v workflows=%v want %v (nothing from the rolled-back savepoint)", jobs, workflows, want)
	}
}

func TestObserverWorkflowSettledIntegration(t *testing.T) {
	store, observer := observedStore(t, 1)
	ctx := context.Background()
	ok := submitWorkflow(t, store, step("a", "demo.echo"))
	drainWorkflow(t, store, ok.ID, succeed)
	failing := submitWorkflow(t, store, step("a", "demo.fail"))
	drainWorkflow(t, store, failing.ID, failPermanent("DEMO_FAILURE"))
	canceled := submitWorkflow(t, store, step("a", "demo.echo"))
	if _, err := store.CancelWorkflow(ctx, canceled.ID); err != nil {
		t.Fatal(err)
	}
	broken := submitWorkflow(t, store, withCompensation(step("a", "demo.echo"), "demo.echo"), step("b", "demo.fail", "a"))
	drainWorkflow(t, store, broken.ID, byStep(t, store, map[string]outcomeFunc{
		"b": failPermanent("DEMO_FAILURE"), "undo:a": failPermanent("REFUND_REJECTED"),
	}))
	_, workflows := observer.snapshot()
	slices.Sort(workflows)
	want := []string{"canceled", "compensated", "compensation_failed", "succeeded"}
	if !slices.Equal(workflows, want) {
		t.Fatalf("settled=%v want %v", workflows, want)
	}
}

func TestSnapshotCountsAndAgesIntegration(t *testing.T) {
	store := workflowStore(t, 1)
	ctx := context.Background()
	if _, _, err := store.CreateJob(ctx, submission(t, 0, nil, `{"message":"ready"}`)); err != nil {
		t.Fatal(err)
	}
	sixty := 60
	delayed, err := job.NewDelayedSubmission("default", "demo.echo", json.RawMessage(`{"message":"later"}`), 0, nil, nil, &sixty, time.Now(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CreateJob(ctx, delayed); err != nil {
		t.Fatal(err)
	}
	submitWorkflow(t, store, step("w", "demo.echo"))
	running, err := store.ClaimNextJob(ctx, "w", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CreateJob(ctx, submission(t, 0, nil, `{"message":"dlq"}`)); err != nil {
		t.Fatal(err)
	}
	_ = running
	time.Sleep(1100 * time.Millisecond)
	snapshot, err := store.ObservabilitySnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Ready != 2 || snapshot.Delayed != 1 || snapshot.Running != 1 || snapshot.ExpiredLease != 0 ||
		snapshot.OldestReadyAge < time.Second || snapshot.Workflows["running"] != 1 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	dead := claimNext(t, store, "w")
	applyOutcome(t, store, dead, failPermanent("DEMO_FAILURE"))
	if snapshot, err = store.ObservabilitySnapshot(ctx); err != nil || snapshot.UnredrivenDeadLetters != 1 {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
}
