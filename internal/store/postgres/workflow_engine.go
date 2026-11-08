package postgres

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/Nuryanfa/TaskForge/internal/job"
	"github.com/jackc/pgx/v5"
)

// terminalJob describes a workflow-owned job that just reached a terminal
// status inside the caller's transaction.
type terminalJob struct {
	ID         string
	WorkflowID *string
	StepID     *int64
	Role       *string
	Status     job.Status
	ErrorCode  *string
}

type workflowState struct {
	id          string
	status      job.WorkflowStatus
	abortReason *job.AbortReason
	policy      workflowPolicy
}

func lockWorkflow(ctx context.Context, tx pgx.Tx, id string) (workflowState, error) {
	var state workflowState
	err := tx.QueryRow(ctx, `SELECT id, status, abort_reason, max_attempts, retry_initial_backoff_ms,
		retry_max_backoff_ms, retry_jitter_percent FROM workflows WHERE id=$1 FOR UPDATE`, id).
		Scan(&state.id, &state.status, &state.abortReason, &state.policy.maxAttempts,
			&state.policy.initialBackoffMS, &state.policy.maxBackoffMS, &state.policy.jitterPercent)
	if errors.Is(err, pgx.ErrNoRows) {
		return workflowState{}, ErrNotFound
	}
	if err != nil {
		return workflowState{}, classify("lock workflow", err)
	}
	return state, nil
}

// advanceWorkflows runs the engine for jobs that became terminal in tx. Lock
// order: the caller already holds the job rows; workflow rows are locked here
// in ascending id order so that transactions touching several workflows (lease
// recovery batches) cannot deadlock with each other.
func (s *Store) advanceWorkflows(ctx context.Context, tx pgx.Tx, terminal []terminalJob) error {
	var owned []terminalJob
	seen := map[string]bool{}
	var ids []string
	for _, item := range terminal {
		if item.WorkflowID == nil {
			continue
		}
		owned = append(owned, item)
		if !seen[*item.WorkflowID] {
			seen[*item.WorkflowID] = true
			ids = append(ids, *item.WorkflowID)
		}
	}
	if len(owned) == 0 {
		return nil
	}
	sort.Strings(ids)
	for _, id := range ids {
		if _, err := lockWorkflow(ctx, tx, id); err != nil {
			return err
		}
	}
	for _, item := range owned {
		if err := s.onJobTerminal(ctx, tx, item); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) onJobTerminal(ctx context.Context, tx pgx.Tx, item terminalJob) error {
	wf, err := lockWorkflow(ctx, tx, *item.WorkflowID)
	if err != nil {
		return err
	}
	var name string
	if *item.Role == job.RoleAction {
		stepStatus := job.StepFailed
		compensation := job.CompensationNone
		if item.Status == job.StatusSucceeded {
			stepStatus = job.StepSucceeded
			compensation = job.CompensationNotDefined
		}
		err := tx.QueryRow(ctx, `UPDATE workflow_steps SET status=$2,
			compensation_status = CASE WHEN $2 = 'succeeded' AND compensation_kind IS NOT NULL
				THEN 'waiting' ELSE $3 END
			WHERE id=$1 AND status='dispatched' RETURNING name`, *item.StepID, stepStatus, compensation).Scan(&name)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("record step outcome for job %s: %w", item.ID, ErrStateConflict)
		}
		if err != nil {
			return classify("record step outcome", err)
		}
		switch {
		case stepStatus == job.StepSucceeded && wf.status == job.WorkflowRunning:
			if err := dispatchReadyDependents(ctx, tx, wf, name); err != nil {
				return err
			}
		case stepStatus == job.StepFailed && wf.status == job.WorkflowRunning:
			if err := abortWorkflow(ctx, tx, wf.id, job.AbortStepFailed, &name, item.ErrorCode); err != nil {
				return err
			}
		}
	} else {
		compensation := job.CompensationFailed
		if item.Status == job.StatusSucceeded {
			compensation = job.CompensationSucceeded
		}
		command, err := tx.Exec(ctx, `UPDATE workflow_steps SET compensation_status=$2
			WHERE id=$1 AND compensation_status='dispatched'`, *item.StepID, compensation)
		if err != nil {
			return classify("record compensation outcome", err)
		}
		if command.RowsAffected() != 1 {
			return fmt.Errorf("record compensation outcome for job %s: %w", item.ID, ErrStateConflict)
		}
		if compensation == job.CompensationFailed && wf.status == job.WorkflowCompensating {
			if _, err := tx.Exec(ctx, `UPDATE workflows SET status='compensation_failed',
				completed_at=NOW(), updated_at=NOW() WHERE id=$1`, wf.id); err != nil {
				return classify("mark compensation failed", err)
			}
		}
	}
	return settleWorkflow(ctx, tx, wf.id)
}

// dispatchReadyDependents decrements the fan-in counter of every direct
// dependent of the succeeded step and dispatches those that reach zero. The
// workflow row lock serializes concurrent completions of sibling steps.
func dispatchReadyDependents(ctx context.Context, tx pgx.Tx, wf workflowState, name string) error {
	rows, err := tx.Query(ctx, `UPDATE workflow_steps SET remaining_dependencies = remaining_dependencies - 1
		WHERE workflow_id=$1 AND $2 = ANY(depends_on) RETURNING `+stepColumns, wf.id, name)
	if err != nil {
		return classify("release workflow dependents", err)
	}
	var ready []stepRow
	for rows.Next() {
		item, err := scanStep(rows)
		if err != nil {
			rows.Close()
			return classify("scan released dependent", err)
		}
		if item.remaining == 0 && item.status == job.StepPending {
			ready = append(ready, item)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return classify("read released dependents", err)
	}
	sort.Slice(ready, func(i, j int) bool { return ready[i].position < ready[j].position })
	for _, item := range ready {
		if err := dispatchStepJob(ctx, tx, wf.id, wf.policy, item, job.RoleAction); err != nil {
			return err
		}
	}
	return nil
}

// abortWorkflow moves a running workflow to compensating: pending steps are
// skipped and still-queued action jobs are canceled. Queued jobs are locked
// with SKIP LOCKED because the caller already holds the workflow row; a job
// that is mid-claim is left to run and reaches the engine on its own.
func abortWorkflow(ctx context.Context, tx pgx.Tx, workflowID string, reason job.AbortReason, failedStep, errorCode *string) error {
	command, err := tx.Exec(ctx, `UPDATE workflows SET status='compensating', abort_reason=$2,
		failed_step=$3, error_code=$4, updated_at=NOW() WHERE id=$1 AND status='running'`,
		workflowID, reason, failedStep, errorCode)
	if err != nil {
		return classify("abort workflow", err)
	}
	if command.RowsAffected() != 1 {
		return fmt.Errorf("abort workflow %s: %w", workflowID, ErrStateConflict)
	}
	if _, err := tx.Exec(ctx, `UPDATE workflow_steps SET status='skipped' WHERE workflow_id=$1 AND status='pending'`, workflowID); err != nil {
		return classify("skip pending steps", err)
	}
	rows, err := tx.Query(ctx, `SELECT id FROM jobs WHERE workflow_id=$1 AND workflow_role='action'
		AND status='queued' ORDER BY id FOR UPDATE SKIP LOCKED`, workflowID)
	if err != nil {
		return classify("lock queued workflow jobs", err)
	}
	var queued []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return classify("scan queued workflow job", err)
		}
		queued = append(queued, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return classify("read queued workflow jobs", err)
	}
	if len(queued) == 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, `UPDATE jobs SET status='canceled', updated_at=NOW(), completed_at=NOW()
		WHERE id = ANY($1) AND status='queued'`, queued); err != nil {
		return classify("cancel queued workflow jobs", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE workflow_steps SET status='canceled'
		WHERE job_id = ANY($1) AND status='dispatched'`, queued); err != nil {
		return classify("cancel queued workflow steps", err)
	}
	return nil
}

// settleWorkflow dispatches compensations that became ready and moves the
// workflow to a terminal status when nothing is left in flight.
func settleWorkflow(ctx context.Context, tx pgx.Tx, workflowID string) error {
	wf, err := lockWorkflow(ctx, tx, workflowID)
	if err != nil {
		return err
	}
	if wf.status.Terminal() {
		return nil
	}
	steps, err := loadSteps(ctx, tx, workflowID)
	if err != nil {
		return err
	}
	states := make([]job.StepState, len(steps))
	byName := make(map[string]int, len(steps))
	for i, item := range steps {
		states[i] = item.state()
		byName[item.name] = i
	}
	if wf.status == job.WorkflowCompensating {
		for _, name := range job.ReadyCompensations(states) {
			index := byName[name]
			if err := dispatchStepJob(ctx, tx, wf.id, wf.policy, steps[index], job.RoleCompensation); err != nil {
				return err
			}
			states[index].Compensation = job.CompensationDispatched
		}
	}
	next, ok := job.Settle(wf.status, wf.abortReason, states)
	if !ok {
		return nil
	}
	if _, err := tx.Exec(ctx, `UPDATE workflows SET status=$2, completed_at=NOW(), updated_at=NOW()
		WHERE id=$1`, wf.id, next); err != nil {
		return classify("settle workflow", err)
	}
	return nil
}
