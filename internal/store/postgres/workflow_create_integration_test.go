package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/Nuryanfa/TaskForge/internal/job"
)

func diamondSteps() []job.WorkflowStepRequest {
	return []job.WorkflowStepRequest{
		step("a", "demo.echo"), step("b", "demo.echo", "a"), step("c", "demo.echo", "a"), step("d", "demo.echo", "b", "c"),
	}
}

func TestCreateWorkflowDispatchesOnlyRootsIntegration(t *testing.T) {
	store := workflowStore(t, 3)
	created := submitWorkflow(t, store, diamondSteps()...)
	if created.Status != job.WorkflowRunning || created.StepCount != 4 || len(created.Steps) != 4 {
		t.Fatalf("created=%+v", created)
	}
	a := stepByName(t, created, "a")
	if a.Status != job.StepDispatched || a.JobID == nil {
		t.Fatalf("root step=%+v", a)
	}
	for _, name := range []string{"b", "c", "d"} {
		if got := stepByName(t, created, name); got.Status != job.StepPending || got.JobID != nil {
			t.Fatalf("step %s=%+v", name, got)
		}
	}
	if got := stepByName(t, created, "d").DependsOn; len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Fatalf("d depends_on=%v", got)
	}
	ctx := context.Background()
	var jobs, maxAttempts int
	var role string
	if err := store.pool.QueryRow(ctx, `SELECT count(*), max(max_attempts), max(workflow_role) FROM jobs WHERE workflow_id=$1`, created.ID).Scan(&jobs, &maxAttempts, &role); err != nil {
		t.Fatal(err)
	}
	if jobs != 1 || maxAttempts != 3 || role != job.RoleAction {
		t.Fatalf("jobs=%d max_attempts=%d role=%s", jobs, maxAttempts, role)
	}
	var remaining int
	if err := store.pool.QueryRow(ctx, `SELECT remaining_dependencies FROM workflow_steps WHERE workflow_id=$1 AND name='d'`, created.ID).Scan(&remaining); err != nil || remaining != 2 {
		t.Fatalf("remaining=%d err=%v", remaining, err)
	}
	rootJob, err := store.GetJob(ctx, *a.JobID)
	if err != nil || rootJob.WorkflowID == nil || *rootJob.WorkflowID != created.ID || *rootJob.WorkflowRole != job.RoleAction {
		t.Fatalf("root job=%+v err=%v", rootJob, err)
	}
	assertWorkflowInvariants(t, store, created.ID)
}

func TestCreateWorkflowIdempotencyIntegration(t *testing.T) {
	store := workflowStore(t, 3)
	ctx := context.Background()
	key := "order-1042"
	submission, err := job.NewWorkflowSubmission(nil, &key, diamondSteps(), workflowTestLimits)
	if err != nil {
		t.Fatal(err)
	}
	first, isNew, err := store.CreateWorkflow(ctx, submission)
	if err != nil || !isNew {
		t.Fatalf("first new=%v err=%v", isNew, err)
	}
	replay, isNew, err := store.CreateWorkflow(ctx, submission)
	if err != nil || isNew || replay.ID != first.ID {
		t.Fatalf("replay new=%v id=%s err=%v", isNew, replay.ID, err)
	}
	changed := diamondSteps()
	changed[0].Payload = json.RawMessage(`{"message":"different"}`)
	conflicting, err := job.NewWorkflowSubmission(nil, &key, changed, workflowTestLimits)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CreateWorkflow(ctx, conflicting); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflict err=%v", err)
	}
	var jobs int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM jobs`).Scan(&jobs); err != nil || jobs != 1 {
		t.Fatalf("jobs=%d err=%v", jobs, err)
	}
}

func TestConcurrentWorkflowSubmissionIntegration(t *testing.T) {
	store := workflowStore(t, 3)
	key := "concurrent-workflow"
	submission, err := job.NewWorkflowSubmission(nil, &key, diamondSteps(), workflowTestLimits)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 8)
	errs := make([]error, 8)
	var wait sync.WaitGroup
	start := make(chan struct{})
	for i := range ids {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			created, _, err := store.CreateWorkflow(context.Background(), submission)
			ids[i], errs[i] = created.ID, err
		}()
	}
	close(start)
	wait.Wait()
	for i := range ids {
		if errs[i] != nil || ids[i] != ids[0] {
			t.Fatalf("submission %d id=%s err=%v (first %s)", i, ids[i], errs[i], ids[0])
		}
	}
	var workflows, jobs int
	if err := store.pool.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM workflows), (SELECT count(*) FROM jobs)`).Scan(&workflows, &jobs); err != nil {
		t.Fatal(err)
	}
	if workflows != 1 || jobs != 1 {
		t.Fatalf("workflows=%d jobs=%d", workflows, jobs)
	}
}

func TestListWorkflowsPaginationIntegration(t *testing.T) {
	store := workflowStore(t, 3)
	ctx := context.Background()
	created := map[string]bool{}
	for range 5 {
		created[submitWorkflow(t, store, step("only", "demo.echo")).ID] = true
	}
	seen := map[string]bool{}
	sizes := []int{}
	var before *job.Workflow
	for range 5 {
		var items []job.Workflow
		var more bool
		var err error
		if before == nil {
			items, more, err = store.ListWorkflows(ctx, nil, "", 2)
		} else {
			items, more, err = store.ListWorkflows(ctx, &before.CreatedAt, before.ID, 2)
		}
		if err != nil {
			t.Fatal(err)
		}
		sizes = append(sizes, len(items))
		for _, item := range items {
			if seen[item.ID] || !created[item.ID] || item.Steps != nil || item.StepCount != 1 {
				t.Fatalf("unexpected list item %+v", item)
			}
			seen[item.ID] = true
		}
		if !more {
			break
		}
		before = &items[len(items)-1]
	}
	if len(seen) != 5 || len(sizes) != 3 || sizes[0] != 2 || sizes[1] != 2 || sizes[2] != 1 {
		t.Fatalf("pages=%v seen=%d", sizes, len(seen))
	}
}
