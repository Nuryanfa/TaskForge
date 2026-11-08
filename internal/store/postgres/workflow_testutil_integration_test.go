package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/job"
)

var workflowTestLimits = job.WorkflowLimits{MaxSteps: 100, MaxPayloadBytes: 64 * 1024}

// workflowStore is an isolated integration store whose new jobs and workflows
// snapshot maxAttempts with a short, jitter-free backoff.
func workflowStore(t *testing.T, maxAttempts int) *Store {
	t.Helper()
	store := integrationStore(t)
	store.retryPolicy = retryPolicy{maxAttempts: maxAttempts, initialBackoff: time.Millisecond, maxBackoff: time.Millisecond, jitterPercent: 0}
	return store
}

func step(name, kind string, deps ...string) job.WorkflowStepRequest {
	payload := `{}`
	if kind == "demo.echo" {
		payload = `{"message":"` + name + `"}`
	}
	return job.WorkflowStepRequest{Name: name, Kind: kind, Payload: json.RawMessage(payload), DependsOn: deps}
}

func withCompensation(request job.WorkflowStepRequest, kind string) job.WorkflowStepRequest {
	request.Compensation = &job.CompensationRequest{Kind: kind, Payload: json.RawMessage(`{"message":"undo-` + request.Name + `"}`)}
	return request
}

func submitWorkflow(t *testing.T, s *Store, steps ...job.WorkflowStepRequest) job.Workflow {
	t.Helper()
	submission, err := job.NewWorkflowSubmission(nil, nil, steps, workflowTestLimits)
	if err != nil {
		t.Fatal(err)
	}
	created, isNew, err := s.CreateWorkflow(context.Background(), submission)
	if err != nil || !isNew {
		t.Fatalf("create workflow new=%v err=%v", isNew, err)
	}
	return created
}

type outcomeFunc func(job.Claimed) (json.RawMessage, *job.Failure)

func succeed(job.Claimed) (json.RawMessage, *job.Failure) { return json.RawMessage(`{"ok":true}`), nil }

func failPermanent(code string) outcomeFunc {
	return func(job.Claimed) (json.RawMessage, *job.Failure) { return nil, &job.Failure{Code: code} }
}

func failRetryable(code string) outcomeFunc {
	return func(job.Claimed) (json.RawMessage, *job.Failure) {
		return nil, &job.Failure{Code: code, Retryable: true}
	}
}

// claimNext claims the next due job, retrying briefly to absorb retry backoff.
func claimNext(t *testing.T, s *Store, worker string) job.Claimed {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		claimed, err := s.ClaimNextJob(context.Background(), worker, 30*time.Second)
		if err == nil {
			return claimed
		}
		if !errors.Is(err, ErrNotFound) || time.Now().After(deadline) {
			t.Fatalf("claim next job: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func applyOutcome(t *testing.T, s *Store, claimed job.Claimed, outcome outcomeFunc) {
	t.Helper()
	result, failure := outcome(claimed)
	var err error
	if failure == nil {
		err = s.CompleteJob(context.Background(), claimed.Execution, result)
	} else {
		err = s.FailJob(context.Background(), claimed.Execution, *failure)
	}
	if err != nil {
		t.Fatalf("apply outcome to %s: %v", claimed.Job.ID, err)
	}
}

// driveNext claims the next due job as worker and applies outcome to it.
func driveNext(t *testing.T, s *Store, worker string, outcome outcomeFunc) job.Claimed {
	t.Helper()
	claimed := claimNext(t, s, worker)
	applyOutcome(t, s, claimed, outcome)
	return claimed
}

// stepName returns the workflow step name a claimed job belongs to.
func stepName(t *testing.T, s *Store, claimed job.Claimed) string {
	t.Helper()
	if claimed.Job.WorkflowStepID == nil {
		return ""
	}
	var name string
	if err := s.pool.QueryRow(context.Background(), `SELECT name FROM workflow_steps WHERE id=$1`, *claimed.Job.WorkflowStepID).Scan(&name); err != nil {
		t.Fatal(err)
	}
	return name
}

// drainWorkflow runs jobs until the workflow is terminal. decide picks the
// outcome per claimed job; the loop is bounded.
func drainWorkflow(t *testing.T, s *Store, workflowID string, decide outcomeFunc) job.Workflow {
	t.Helper()
	for range 200 {
		current := getWorkflow(t, s, workflowID)
		if current.Status.Terminal() {
			return current
		}
		driveNext(t, s, "drain-worker", decide)
	}
	t.Fatal("workflow did not reach a terminal state")
	return job.Workflow{}
}

func getWorkflow(t *testing.T, s *Store, id string) job.Workflow {
	t.Helper()
	found, err := s.GetWorkflow(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return found
}

func stepByName(t *testing.T, wf job.Workflow, name string) job.WorkflowStep {
	t.Helper()
	for _, item := range wf.Steps {
		if item.Name == name {
			return item
		}
	}
	t.Fatalf("step %q not found", name)
	return job.WorkflowStep{}
}

// assertWorkflowInvariants checks I1–I8 from the v0.6 design using only the
// database. Call it when the workflow is quiescent (no job mid-transition).
func assertWorkflowInvariants(t *testing.T, s *Store, workflowID string) {
	t.Helper()
	for _, violation := range workflowInvariantViolations(t, s, workflowID) {
		t.Errorf("%s in workflow %s", violation, workflowID)
	}
}

// workflowInvariantViolations returns one entry per violated invariant.
func workflowInvariantViolations(t *testing.T, s *Store, workflowID string) []string {
	t.Helper()
	var violated []string
	checks := []struct {
		name  string
		query string
	}{
		{"I1 at most one action and one compensation job per step", `
			SELECT count(*) FROM (SELECT workflow_step_id, workflow_role FROM jobs WHERE workflow_id=$1
			GROUP BY 1, 2 HAVING count(*) > 1) duplicated`},
		{"I2 action job created only after every dependency succeeded", `
			SELECT count(*) FROM workflow_steps s
			JOIN jobs j ON j.id = s.job_id
			JOIN workflow_steps d ON d.workflow_id = s.workflow_id AND d.name = ANY(s.depends_on)
			LEFT JOIN jobs dj ON dj.id = d.job_id
			WHERE s.workflow_id=$1 AND (dj.status IS DISTINCT FROM 'succeeded' OR dj.completed_at > j.created_at)`},
		{"I3 succeeded workflow has only succeeded steps and no compensations", `
			SELECT (SELECT count(*) FROM workflows w JOIN workflow_steps s ON s.workflow_id = w.id
				WHERE w.id=$1 AND w.status='succeeded' AND s.status <> 'succeeded')
			+ (SELECT count(*) FROM workflows w JOIN jobs j ON j.workflow_id = w.id
				WHERE w.id=$1 AND w.status='succeeded' AND j.workflow_role='compensation')`},
		{"I4 compensated/canceled workflow compensated every succeeded step", `
			SELECT count(*) FROM workflows w JOIN workflow_steps s ON s.workflow_id = w.id
			WHERE w.id=$1 AND w.status IN ('compensated','canceled') AND s.status='succeeded'
			AND s.compensation_status NOT IN ('succeeded','not_defined')`},
		// A dependent's action job must have finished before the compensation
		// was created (judged by timestamps, so a dependent that later failed
		// cannot hide the violation), and a succeeded dependent's compensation
		// must have completed first.
		{"I5 compensation created only after dependents finished and were compensated", `
			SELECT count(*) FROM workflow_steps s
			JOIN jobs c ON c.id = s.compensation_job_id
			JOIN workflow_steps d ON d.workflow_id = s.workflow_id AND s.name = ANY(d.depends_on)
			LEFT JOIN jobs dj ON dj.id = d.job_id
			LEFT JOIN jobs dc ON dc.id = d.compensation_job_id
			WHERE s.workflow_id=$1 AND (
				(d.job_id IS NOT NULL AND (dj.completed_at IS NULL OR dj.completed_at > c.created_at))
				OR (d.status = 'succeeded' AND d.compensation_status <> 'not_defined'
					AND (dc.status IS DISTINCT FROM 'succeeded' OR dc.completed_at > c.created_at)))`},
		{"I6 terminal workflow has no live jobs and a completion time", `
			SELECT count(*) FROM workflows w
			WHERE w.id=$1 AND w.status IN ('succeeded','compensated','canceled','compensation_failed')
			AND (w.completed_at IS NULL OR EXISTS (
				SELECT 1 FROM jobs j WHERE j.workflow_id = w.id AND j.status IN ('queued','running')))`},
		{"I7 compensation_failed has a dead-lettered compensation", `
			SELECT count(*) FROM workflows w WHERE w.id=$1 AND w.status='compensation_failed' AND NOT EXISTS (
				SELECT 1 FROM workflow_steps s JOIN jobs c ON c.id = s.compensation_job_id
				WHERE s.workflow_id = w.id AND s.compensation_status='failed'
				AND c.status='failed' AND c.dead_lettered_at IS NOT NULL)`},
		{"I8 step state agrees with its jobs", `
			SELECT (SELECT count(*) FROM workflow_steps s
			LEFT JOIN jobs j ON j.id = s.job_id
			LEFT JOIN jobs c ON c.id = s.compensation_job_id
			WHERE s.workflow_id=$1 AND (
				(s.status='dispatched' AND j.status NOT IN ('queued','running'))
				OR (s.status IN ('succeeded','failed','canceled') AND j.status::text <> s.status)
				OR (s.compensation_status='dispatched' AND c.status NOT IN ('queued','running'))
				OR (s.compensation_status IN ('succeeded','failed') AND c.status::text <> s.compensation_status)
				OR (s.status <> 'succeeded' AND s.compensation_status <> 'none')
				OR (s.status = 'succeeded' AND s.compensation_status = 'none')))
			+ (SELECT count(*) FROM jobs j LEFT JOIN workflow_steps js ON js.id = j.workflow_step_id
				WHERE j.workflow_id=$1 AND (js.id IS NULL OR js.workflow_id <> j.workflow_id
					OR (j.workflow_role='action' AND js.job_id IS DISTINCT FROM j.id)
					OR (j.workflow_role='compensation' AND js.compensation_job_id IS DISTINCT FROM j.id)))`},
		{"I9 a non-terminal workflow has live work", `
			SELECT count(*) FROM workflows w
			WHERE w.id=$1 AND w.status IN ('running','compensating') AND NOT EXISTS (
				SELECT 1 FROM jobs j WHERE j.workflow_id = w.id AND j.status IN ('queued','running'))`},
	}
	for _, check := range checks {
		var violations int
		if err := s.pool.QueryRow(context.Background(), check.query, workflowID).Scan(&violations); err != nil {
			t.Fatalf("%s: %v", check.name, err)
		}
		if violations != 0 {
			violated = append(violated, fmt.Sprintf("%s: %d violation(s)", check.name, violations))
		}
	}
	return violated
}
