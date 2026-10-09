package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	"github.com/Nuryanfa/TaskForge/internal/job"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const jobColumns = `id, queue, kind, payload, payload_hash, status, priority,
idempotency_key, available_at, attempt_count, result, last_error_code,
created_at, updated_at, started_at, completed_at`

var errorCodePattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,63}$`)

type rowScanner interface {
	Scan(dest ...any) error
}

func (s *Store) CreateJob(ctx context.Context, submission job.Submission) (job.Job, bool, error) {
	queryCtx, cancel := s.queryContext(ctx)
	defer cancel()

	id := uuid.NewString()
	query := `INSERT INTO jobs
        (id, queue, kind, payload, payload_hash, status, priority, idempotency_key)
        VALUES ($1, $2, $3, $4, $5, 'queued', $6, $7)`
	if submission.IdempotencyKey != nil {
		query += ` ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING`
	}
	query += ` RETURNING ` + jobColumns

	created, err := scanJob(s.pool.QueryRow(queryCtx, query, id, submission.Queue, submission.Kind,
		submission.Payload, submission.Fingerprint[:], submission.Priority, submission.IdempotencyKey))
	if err == nil {
		return created, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) || submission.IdempotencyKey == nil {
		return job.Job{}, false, classify("create job", err)
	}

	existing, err := scanJob(s.pool.QueryRow(queryCtx,
		`SELECT `+jobColumns+` FROM jobs WHERE queue = $1 AND idempotency_key = $2`,
		submission.Queue, *submission.IdempotencyKey))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return job.Job{}, false, fmt.Errorf("resolve idempotent job: %w", ErrStateConflict)
		}
		return job.Job{}, false, classify("resolve idempotent job", err)
	}
	if !bytes.Equal(existing.PayloadHash, submission.Fingerprint[:]) {
		return job.Job{}, false, ErrIdempotencyConflict
	}
	return existing, false, nil
}

func (s *Store) GetJob(ctx context.Context, id string) (job.Job, error) {
	queryCtx, cancel := s.queryContext(ctx)
	defer cancel()
	found, err := scanJob(s.pool.QueryRow(queryCtx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, ErrNotFound
	}
	if err != nil {
		return job.Job{}, classify("get job", err)
	}
	return found, nil
}

func (s *Store) CancelQueuedJob(ctx context.Context, id string) (job.Job, error) {
	queryCtx, cancel := s.queryContext(ctx)
	defer cancel()
	canceled, err := scanJob(s.pool.QueryRow(queryCtx, `UPDATE jobs
        SET status = 'canceled', updated_at = NOW(), completed_at = NOW()
        WHERE id = $1 AND status = 'queued' RETURNING `+jobColumns, id))
	if err == nil {
		return canceled, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, classify("cancel job", err)
	}
	var exists bool
	if err := s.pool.QueryRow(queryCtx, `SELECT EXISTS (SELECT 1 FROM jobs WHERE id = $1)`, id).Scan(&exists); err != nil {
		return job.Job{}, classify("check job after cancel conflict", err)
	}
	if !exists {
		return job.Job{}, ErrNotFound
	}
	return job.Job{}, ErrStateConflict
}

func (s *Store) ClaimNextJob(ctx context.Context, workerID string) (job.Job, error) {
	queryCtx, cancel := s.queryContext(ctx)
	defer cancel()
	tx, err := s.pool.BeginTx(queryCtx, pgx.TxOptions{})
	if err != nil {
		return job.Job{}, classify("begin claim transaction", err)
	}
	defer tx.Rollback(queryCtx)

	// The row lock and conditional update make claiming atomic and retain safe
	// semantics when v0.2 introduces more than one worker.
	var id string
	err = tx.QueryRow(queryCtx, `SELECT id FROM jobs
        WHERE status = 'queued' AND available_at <= NOW()
        ORDER BY priority DESC, created_at, id
        FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, ErrNotFound
	}
	if err != nil {
		return job.Job{}, classify("select job for claim", err)
	}
	claimed, err := scanJob(tx.QueryRow(queryCtx, `UPDATE jobs
        SET status = 'running', attempt_count = attempt_count + 1,
            started_at = NOW(), updated_at = NOW()
        WHERE id = $1 AND status = 'queued' RETURNING `+jobColumns, id))
	if err != nil {
		return job.Job{}, classify("mark job running", err)
	}
	if _, err := tx.Exec(queryCtx, `INSERT INTO job_attempts
        (job_id, attempt, worker_id, started_at) VALUES ($1, $2, $3, $4)`,
		claimed.ID, claimed.AttemptCount, workerID, *claimed.StartedAt); err != nil {
		return job.Job{}, classify("insert job attempt", err)
	}
	if err := tx.Commit(queryCtx); err != nil {
		return job.Job{}, classify("commit job claim", err)
	}
	return claimed, nil
}

func (s *Store) CompleteJob(ctx context.Context, id string, attempt int, result json.RawMessage) error {
	if int64(len(result)) > s.maxResultBytes || !json.Valid(result) {
		return errors.New("result is invalid or exceeds configured limit")
	}
	return s.finishJob(ctx, id, attempt, job.StatusSucceeded, result, "")
}

func (s *Store) FailJob(ctx context.Context, id string, attempt int, errorCode string) error {
	if !errorCodePattern.MatchString(errorCode) {
		return errors.New("error code must be a sanitized stable machine code")
	}
	return s.finishJob(ctx, id, attempt, job.StatusFailed, nil, errorCode)
}

func (s *Store) finishJob(ctx context.Context, id string, attempt int, status job.Status, result json.RawMessage, errorCode string) error {
	queryCtx, cancel := s.queryContext(ctx)
	defer cancel()
	tx, err := s.pool.BeginTx(queryCtx, pgx.TxOptions{})
	if err != nil {
		return classify("begin completion transaction", err)
	}
	defer tx.Rollback(queryCtx)

	command, err := tx.Exec(queryCtx, `UPDATE jobs SET status = $3, result = $4,
        last_error_code = NULLIF($5, ''), updated_at = NOW(), completed_at = NOW()
        WHERE id = $1 AND attempt_count = $2 AND status = 'running'`, id, attempt, status, result, errorCode)
	if err != nil {
		return classify("update job outcome", err)
	}
	if command.RowsAffected() != 1 {
		return ErrStateConflict
	}
	command, err = tx.Exec(queryCtx, `UPDATE job_attempts
        SET finished_at = NOW(), outcome = $3, error_code = NULLIF($4, '')
        WHERE job_id = $1 AND attempt = $2 AND finished_at IS NULL`, id, attempt, status, errorCode)
	if err != nil {
		return classify("update job attempt outcome", err)
	}
	if command.RowsAffected() != 1 {
		return ErrStateConflict
	}
	if err := tx.Commit(queryCtx); err != nil {
		return classify("commit job outcome", err)
	}
	return nil
}

func scanJob(row rowScanner) (job.Job, error) {
	var found job.Job
	err := row.Scan(
		&found.ID, &found.Queue, &found.Kind, &found.Payload, &found.PayloadHash,
		&found.Status, &found.Priority, &found.IdempotencyKey, &found.AvailableAt,
		&found.AttemptCount, &found.Result, &found.LastErrorCode, &found.CreatedAt,
		&found.UpdatedAt, &found.StartedAt, &found.CompletedAt,
	)
	return found, err
}
