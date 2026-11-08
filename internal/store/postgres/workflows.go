package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/job"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const workflowColumns = `id, label, status, abort_reason, failed_step, error_code, step_count,
created_at, updated_at, completed_at`

const stepColumns = `id, position, name, queue, kind, priority, payload, depends_on,
remaining_dependencies, status, job_id, compensation_queue, compensation_kind,
compensation_priority, compensation_payload, compensation_status, compensation_job_id`

type querier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// workflowPolicy is the retry snapshot every workflow job inherits.
type workflowPolicy struct {
	maxAttempts      int
	initialBackoffMS int64
	maxBackoffMS     int64
	jitterPercent    int
}

type stepRow struct {
	id                  int64
	position            int
	name                string
	queue               string
	kind                string
	priority            int16
	payload             json.RawMessage
	dependsOn           []string
	remaining           int
	status              job.StepStatus
	jobID               *string
	compensationQueue   *string
	compensationKind    *string
	compensationPrio    *int16
	compensationPayload json.RawMessage
	compensationStatus  job.CompensationStatus
	compensationJobID   *string
}

func (r stepRow) public() job.WorkflowStep {
	return job.WorkflowStep{
		ID: r.id, Position: r.position, Name: r.name, Queue: r.queue, Kind: r.kind,
		DependsOn: r.dependsOn, Status: r.status, JobID: r.jobID,
		CompensationKind: r.compensationKind, CompensationStatus: r.compensationStatus,
		CompensationJobID: r.compensationJobID,
	}
}

func (r stepRow) state() job.StepState {
	return job.StepState{Name: r.name, DependsOn: r.dependsOn, Status: r.status, Compensation: r.compensationStatus}
}

func scanWorkflow(row rowScanner) (job.Workflow, error) {
	var found job.Workflow
	var stepCount int16
	err := row.Scan(&found.ID, &found.Label, &found.Status, &found.AbortReason, &found.FailedStep,
		&found.ErrorCode, &stepCount, &found.CreatedAt, &found.UpdatedAt, &found.CompletedAt)
	found.StepCount = int(stepCount)
	return found, err
}

func scanStep(row rowScanner) (stepRow, error) {
	var found stepRow
	var position, remaining int16
	err := row.Scan(&found.id, &position, &found.name, &found.queue, &found.kind, &found.priority,
		&found.payload, &found.dependsOn, &remaining, &found.status, &found.jobID,
		&found.compensationQueue, &found.compensationKind, &found.compensationPrio,
		&found.compensationPayload, &found.compensationStatus, &found.compensationJobID)
	found.position, found.remaining = int(position), int(remaining)
	if found.dependsOn == nil {
		found.dependsOn = []string{}
	}
	return found, err
}

func loadSteps(ctx context.Context, q querier, workflowID string) ([]stepRow, error) {
	rows, err := q.Query(ctx, `SELECT `+stepColumns+` FROM workflow_steps WHERE workflow_id=$1 ORDER BY position`, workflowID)
	if err != nil {
		return nil, classify("load workflow steps", err)
	}
	defer rows.Close()
	var steps []stepRow
	for rows.Next() {
		item, err := scanStep(rows)
		if err != nil {
			return nil, classify("scan workflow step", err)
		}
		steps = append(steps, item)
	}
	if err := rows.Err(); err != nil {
		return nil, classify("read workflow steps", err)
	}
	return steps, nil
}

func (s *Store) CreateWorkflow(ctx context.Context, submission job.WorkflowSubmission) (job.Workflow, bool, error) {
	queryCtx, cancel := s.queryContext(ctx)
	defer cancel()
	tx, err := s.pool.BeginTx(queryCtx, pgx.TxOptions{})
	if err != nil {
		return job.Workflow{}, false, classify("begin workflow creation", err)
	}
	defer tx.Rollback(queryCtx)
	policy := workflowPolicy{
		maxAttempts: s.retryPolicy.maxAttempts, initialBackoffMS: s.retryPolicy.initialBackoff.Milliseconds(),
		maxBackoffMS: s.retryPolicy.maxBackoff.Milliseconds(), jitterPercent: s.retryPolicy.jitterPercent,
	}
	id := uuid.NewString()
	err = tx.QueryRow(queryCtx, `INSERT INTO workflows
		(id, label, status, idempotency_key, fingerprint, step_count, max_attempts,
		retry_initial_backoff_ms, retry_max_backoff_ms, retry_jitter_percent)
		VALUES ($1, $2, 'running', $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (idempotency_key) DO NOTHING RETURNING id`,
		id, submission.Label, submission.IdempotencyKey, submission.Fingerprint[:], len(submission.Steps),
		policy.maxAttempts, policy.initialBackoffMS, policy.maxBackoffMS, policy.jitterPercent).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) && submission.IdempotencyKey != nil {
		_ = tx.Rollback(queryCtx)
		return s.resolveIdempotentWorkflow(ctx, submission)
	}
	if err != nil {
		return job.Workflow{}, false, classify("insert workflow", err)
	}
	steps := make([]stepRow, 0, len(submission.Steps))
	for position, spec := range submission.Steps {
		inserted, err := insertStep(queryCtx, tx, id, position, spec)
		if err != nil {
			return job.Workflow{}, false, err
		}
		steps = append(steps, inserted)
	}
	for _, item := range steps {
		if item.remaining == 0 {
			if err := dispatchStepJob(queryCtx, tx, id, policy, item, job.RoleAction); err != nil {
				return job.Workflow{}, false, err
			}
		}
	}
	if err := tx.Commit(queryCtx); err != nil {
		return job.Workflow{}, false, classify("commit workflow creation", err)
	}
	created, err := s.GetWorkflow(ctx, id)
	return created, true, err
}

func (s *Store) resolveIdempotentWorkflow(ctx context.Context, submission job.WorkflowSubmission) (job.Workflow, bool, error) {
	queryCtx, cancel := s.queryContext(ctx)
	defer cancel()
	var id string
	var fingerprint []byte
	err := s.pool.QueryRow(queryCtx, `SELECT id, fingerprint FROM workflows WHERE idempotency_key=$1`,
		*submission.IdempotencyKey).Scan(&id, &fingerprint)
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Workflow{}, false, fmt.Errorf("resolve idempotent workflow: %w", ErrStateConflict)
	}
	if err != nil {
		return job.Workflow{}, false, classify("resolve idempotent workflow", err)
	}
	if !bytes.Equal(fingerprint, submission.Fingerprint[:]) {
		return job.Workflow{}, false, ErrIdempotencyConflict
	}
	existing, err := s.GetWorkflow(ctx, id)
	return existing, false, err
}

func insertStep(ctx context.Context, tx pgx.Tx, workflowID string, position int, spec job.StepSpec) (stepRow, error) {
	var compQueue, compKind *string
	var compPriority *int16
	var compPayload json.RawMessage
	if spec.Compensation != nil {
		compQueue, compKind = &spec.Compensation.Queue, &spec.Compensation.Kind
		compPriority, compPayload = &spec.Compensation.Priority, spec.Compensation.Payload
	}
	inserted, err := scanStep(tx.QueryRow(ctx, `INSERT INTO workflow_steps
		(workflow_id, position, name, queue, kind, priority, payload, depends_on, remaining_dependencies,
		status, compensation_queue, compensation_kind, compensation_priority, compensation_payload)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'pending', $10, $11, $12, $13)
		RETURNING `+stepColumns,
		workflowID, position, spec.Name, spec.Queue, spec.Kind, spec.Priority, spec.Payload, spec.DependsOn,
		len(spec.DependsOn), compQueue, compKind, compPriority, compPayload))
	if err != nil {
		return stepRow{}, classify("insert workflow step", err)
	}
	return inserted, nil
}

// dispatchStepJob inserts the action or compensation job of a step and links
// it. The unique (workflow_step_id, workflow_role) index guarantees a step is
// never dispatched twice for the same role.
func dispatchStepJob(ctx context.Context, tx pgx.Tx, workflowID string, policy workflowPolicy, item stepRow, role string) error {
	queue, kind, priority, payload := item.queue, item.kind, item.priority, item.payload
	if role == job.RoleCompensation {
		queue, kind, priority, payload = *item.compensationQueue, *item.compensationKind, *item.compensationPrio, item.compensationPayload
	}
	hash := sha256.Sum256(payload)
	jobID := uuid.NewString()
	if _, err := tx.Exec(ctx, `INSERT INTO jobs
		(id, queue, kind, payload, payload_hash, status, priority, available_at,
		execution_idempotency_key, max_attempts, retry_initial_backoff_ms, retry_max_backoff_ms,
		retry_jitter_percent, workflow_id, workflow_step_id, workflow_role)
		VALUES ($1, $2, $3, $4, $5, 'queued', $6, NOW(), $7, $8, $9, $10, $11, $12, $13, $14)`,
		jobID, queue, kind, payload, hash[:], priority, uuid.NewString(), policy.maxAttempts,
		policy.initialBackoffMS, policy.maxBackoffMS, policy.jitterPercent, workflowID, item.id, role); err != nil {
		var pgError *pgconn.PgError
		if errors.As(err, &pgError) && pgError.Code == "23505" {
			return fmt.Errorf("dispatch step %s %s: %w", item.name, role, ErrStateConflict)
		}
		return classify("insert workflow job", err)
	}
	query := `UPDATE workflow_steps SET job_id=$2, status='dispatched' WHERE id=$1 AND status='pending'`
	if role == job.RoleCompensation {
		query = `UPDATE workflow_steps SET compensation_job_id=$2, compensation_status='dispatched'
			WHERE id=$1 AND compensation_status='waiting'`
	}
	command, err := tx.Exec(ctx, query, item.id, jobID)
	if err != nil {
		return classify("link workflow job", err)
	}
	if command.RowsAffected() != 1 {
		return fmt.Errorf("link step %s %s: %w", item.name, role, ErrStateConflict)
	}
	return nil
}

func (s *Store) GetWorkflow(ctx context.Context, id string) (job.Workflow, error) {
	queryCtx, cancel := s.queryContext(ctx)
	defer cancel()
	// One snapshot for the workflow and its steps, so a reader never sees a
	// workflow status that disagrees with its steps.
	tx, err := s.pool.BeginTx(queryCtx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return job.Workflow{}, classify("begin workflow read", err)
	}
	defer tx.Rollback(queryCtx)
	found, err := scanWorkflow(tx.QueryRow(queryCtx, `SELECT `+workflowColumns+` FROM workflows WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Workflow{}, ErrNotFound
	}
	if err != nil {
		return job.Workflow{}, classify("get workflow", err)
	}
	steps, err := loadSteps(queryCtx, tx, id)
	if err != nil {
		return job.Workflow{}, err
	}
	found.Steps = make([]job.WorkflowStep, 0, len(steps))
	for _, item := range steps {
		found.Steps = append(found.Steps, item.public())
	}
	return found, nil
}

func (s *Store) ListWorkflows(ctx context.Context, before *time.Time, beforeID string, limit int) ([]job.Workflow, bool, error) {
	queryCtx, cancel := s.queryContext(ctx)
	defer cancel()
	rows, err := s.pool.Query(queryCtx, `SELECT `+workflowColumns+` FROM workflows
		WHERE ($1::timestamptz IS NULL OR (created_at, id) < ($1, $2::uuid))
		ORDER BY created_at DESC, id DESC LIMIT $3`, before, nullableUUID(before, beforeID), limit+1)
	if err != nil {
		return nil, false, classify("list workflows", err)
	}
	defer rows.Close()
	items := make([]job.Workflow, 0, limit+1)
	for rows.Next() {
		item, err := scanWorkflow(rows)
		if err != nil {
			return nil, false, classify("scan workflow", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, false, classify("read workflows", err)
	}
	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}
	return items, hasMore, nil
}
