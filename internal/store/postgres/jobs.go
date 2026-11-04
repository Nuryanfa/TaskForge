package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/job"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const jobColumns = `id, queue, kind, payload, payload_hash, status, priority,
idempotency_key, available_at, attempt_count, result, last_error_code,
created_at, updated_at, started_at, completed_at, lease_owner, lease_expires_at,
fencing_token, execution_idempotency_key, max_attempts, retry_initial_backoff_ms,
retry_max_backoff_ms, retry_jitter_percent, dead_lettered_at`

var errorCodePattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,63}$`)

type rowScanner interface{ Scan(dest ...any) error }

func (s *Store) CreateJob(ctx context.Context, submission job.Submission) (job.Job, bool, error) {
	queryCtx, cancel := s.queryContext(ctx)
	defer cancel()
	id := uuid.NewString()
	query := `INSERT INTO jobs
		(id, queue, kind, payload, payload_hash, status, priority, idempotency_key,
		execution_idempotency_key, max_attempts, retry_initial_backoff_ms,
		retry_max_backoff_ms, retry_jitter_percent)
		VALUES ($1, $2, $3, $4, $5, 'queued', $6, $7, $8, $9, $10, $11, $12)`
	if submission.IdempotencyKey != nil {
		query += ` ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING`
	}
	query += ` RETURNING ` + jobColumns
	created, err := scanJob(s.pool.QueryRow(queryCtx, query, id, submission.Queue, submission.Kind,
		submission.Payload, submission.Fingerprint[:], submission.Priority, submission.IdempotencyKey,
		uuid.NewString(), s.retryPolicy.maxAttempts, s.retryPolicy.initialBackoff.Milliseconds(),
		s.retryPolicy.maxBackoff.Milliseconds(), s.retryPolicy.jitterPercent))
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

func (s *Store) ClaimNextJob(ctx context.Context, workerID string, leaseDuration time.Duration) (job.Claimed, error) {
	queryCtx, cancel := s.queryContext(ctx)
	defer cancel()
	tx, err := s.pool.BeginTx(queryCtx, pgx.TxOptions{})
	if err != nil {
		return job.Claimed{}, classify("begin claim transaction", err)
	}
	defer tx.Rollback(queryCtx)
	var id string
	err = tx.QueryRow(queryCtx, `SELECT id FROM jobs
        WHERE status = 'queued' AND available_at <= NOW()
        ORDER BY priority DESC, created_at, id
        FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Claimed{}, ErrNotFound
	}
	if err != nil {
		return job.Claimed{}, classify("select job for claim", err)
	}
	claimedJob, err := scanJob(tx.QueryRow(queryCtx, `UPDATE jobs
        SET status = 'running', attempt_count = attempt_count + 1,
            fencing_token = fencing_token + 1, lease_owner = $2,
            lease_expires_at = NOW() + ($3 * INTERVAL '1 microsecond'),
            started_at = NOW(), updated_at = NOW(), completed_at = NULL
        WHERE id = $1 AND status = 'queued' RETURNING `+jobColumns,
		id, workerID, leaseDuration.Microseconds()))
	if err != nil {
		return job.Claimed{}, classify("mark job running", err)
	}
	execution := job.Execution{JobID: claimedJob.ID, Attempt: claimedJob.AttemptCount,
		WorkerID: workerID, FencingToken: claimedJob.FencingToken, ExecutionKey: claimedJob.ExecutionKey}
	if _, err := tx.Exec(queryCtx, `INSERT INTO job_attempts
        (job_id, attempt, worker_id, started_at, fencing_token)
        VALUES ($1, $2, $3, $4, $5)`, execution.JobID, execution.Attempt,
		execution.WorkerID, *claimedJob.StartedAt, execution.FencingToken); err != nil {
		return job.Claimed{}, classify("insert job attempt", err)
	}
	if err := tx.Commit(queryCtx); err != nil {
		return job.Claimed{}, classify("commit job claim", err)
	}
	return job.Claimed{Job: claimedJob, Execution: execution}, nil
}

func (s *Store) RenewLease(ctx context.Context, execution job.Execution, leaseDuration time.Duration) error {
	queryCtx, cancel := s.queryContext(ctx)
	defer cancel()
	command, err := s.pool.Exec(queryCtx, `UPDATE jobs
        SET lease_expires_at = NOW() + ($4 * INTERVAL '1 microsecond'), updated_at = NOW()
        WHERE id = $1 AND status = 'running' AND lease_owner = $2
          AND fencing_token = $3 AND lease_expires_at > NOW()`, execution.JobID,
		execution.WorkerID, execution.FencingToken, leaseDuration.Microseconds())
	if err != nil {
		return classify("renew job lease", err)
	}
	if command.RowsAffected() != 1 {
		return ErrOwnershipLost
	}
	return nil
}

func (s *Store) CompleteJob(ctx context.Context, execution job.Execution, result json.RawMessage) error {
	if int64(len(result)) > s.maxResultBytes || !json.Valid(result) {
		return errors.New("result is invalid or exceeds configured limit")
	}
	return s.finishJob(ctx, execution, job.StatusSucceeded, result, "")
}

func (s *Store) FailJob(ctx context.Context, execution job.Execution, failure job.Failure) error {
	if !errorCodePattern.MatchString(failure.Code) {
		return errors.New("error code must be a sanitized stable machine code")
	}
	queryCtx, cancel := s.queryContext(ctx)
	defer cancel()
	tx, err := s.pool.BeginTx(queryCtx, pgx.TxOptions{})
	if err != nil {
		return classify("begin failure transaction", err)
	}
	defer tx.Rollback(queryCtx)
	var current job.Job
	current, err = scanJob(tx.QueryRow(queryCtx, `SELECT `+jobColumns+` FROM jobs
		WHERE id=$1 AND status='running' AND lease_owner=$2 AND fencing_token=$3
		AND lease_expires_at > NOW() FOR UPDATE`, execution.JobID, execution.WorkerID, execution.FencingToken))
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrOwnershipLost
	}
	if err != nil {
		return classify("lock failed job", err)
	}
	if err := s.finalizeFailure(queryCtx, tx, current, execution, failure, "failed"); err != nil {
		return err
	}
	if err := tx.Commit(queryCtx); err != nil {
		return classify("commit job failure", err)
	}
	return nil
}

func (s *Store) finalizeFailure(ctx context.Context, tx pgx.Tx, current job.Job, execution job.Execution, failure job.Failure, outcome string) error {
	command, err := tx.Exec(ctx, `UPDATE job_attempts SET finished_at=NOW(), outcome=$4, error_code=$5
		WHERE job_id=$1 AND attempt=$2 AND fencing_token=$3 AND finished_at IS NULL`,
		execution.JobID, execution.Attempt, execution.FencingToken, outcome, failure.Code)
	if err != nil {
		return classify("finalize failed attempt", err)
	}
	if command.RowsAffected() != 1 {
		return ErrStateConflict
	}
	if failure.Retryable && current.AttemptCount < current.MaxAttempts {
		delay := job.RetryDelay(current.ID, current.AttemptCount, current.InitialBackoff, current.MaxBackoff, current.JitterPercent)
		command, err = tx.Exec(ctx, `UPDATE jobs SET status='queued', available_at=NOW()+($4*INTERVAL '1 microsecond'),
			last_error_code=$5, updated_at=NOW(), completed_at=NULL, lease_owner=NULL, lease_expires_at=NULL
			WHERE id=$1 AND status='running' AND lease_owner=$2 AND fencing_token=$3`, current.ID,
			execution.WorkerID, execution.FencingToken, delay.Microseconds(), failure.Code)
		if err != nil {
			return classify("schedule job retry", err)
		}
		if command.RowsAffected() != 1 {
			return ErrOwnershipLost
		}
		return nil
	}
	command, err = tx.Exec(ctx, `UPDATE jobs SET status='failed', last_error_code=$4, updated_at=NOW(),
		completed_at=NOW(), dead_lettered_at=NOW(), lease_owner=NULL, lease_expires_at=NULL
		WHERE id=$1 AND status='running' AND lease_owner=$2 AND fencing_token=$3`, current.ID,
		execution.WorkerID, execution.FencingToken, failure.Code)
	if err != nil {
		return classify("dead-letter failed job", err)
	}
	if command.RowsAffected() != 1 {
		return ErrOwnershipLost
	}
	_, err = tx.Exec(ctx, `INSERT INTO dead_letters(job_id,queue,kind,final_attempt,error_code,dead_lettered_at)
		VALUES($1,$2,$3,$4,$5,NOW()) ON CONFLICT(job_id) DO NOTHING`, current.ID, current.Queue,
		current.Kind, current.AttemptCount, failure.Code)
	if err != nil {
		return classify("insert dead letter", err)
	}
	return nil
}

func (s *Store) finishJob(ctx context.Context, execution job.Execution, status job.Status, result json.RawMessage, errorCode string) error {
	queryCtx, cancel := s.queryContext(ctx)
	defer cancel()
	tx, err := s.pool.BeginTx(queryCtx, pgx.TxOptions{})
	if err != nil {
		return classify("begin completion transaction", err)
	}
	defer tx.Rollback(queryCtx)
	command, err := tx.Exec(queryCtx, `UPDATE jobs SET status = $4, result = $5,
        last_error_code = NULLIF($6, ''), updated_at = NOW(), completed_at = NOW(),
        lease_owner = NULL, lease_expires_at = NULL
        WHERE id = $1 AND status = 'running' AND lease_owner = $2
          AND fencing_token = $3 AND lease_expires_at > NOW()`, execution.JobID,
		execution.WorkerID, execution.FencingToken, status, result, errorCode)
	if err != nil {
		return classify("update job outcome", err)
	}
	if command.RowsAffected() != 1 {
		return ErrOwnershipLost
	}
	command, err = tx.Exec(queryCtx, `UPDATE job_attempts
        SET finished_at = NOW(), outcome = $4, error_code = NULLIF($5, '')
        WHERE job_id = $1 AND attempt = $2 AND fencing_token = $3
          AND finished_at IS NULL`, execution.JobID, execution.Attempt,
		execution.FencingToken, status, errorCode)
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

func (s *Store) RecoverExpiredJobs(ctx context.Context, batchSize int) (int, error) {
	queryCtx, cancel := s.queryContext(ctx)
	defer cancel()
	tx, err := s.pool.BeginTx(queryCtx, pgx.TxOptions{})
	if err != nil {
		return 0, classify("begin lease recovery transaction", err)
	}
	defer tx.Rollback(queryCtx)
	rows, err := tx.Query(queryCtx, `SELECT `+jobColumns+` FROM jobs
        WHERE status = 'running' AND lease_expires_at <= NOW()
        ORDER BY lease_expires_at, id FOR UPDATE SKIP LOCKED LIMIT $1`, batchSize)
	if err != nil {
		return 0, classify("select expired leases", err)
	}
	var selected []job.Job
	for rows.Next() {
		item, err := scanJob(rows)
		if err != nil {
			rows.Close()
			return 0, classify("scan expired lease", err)
		}
		selected = append(selected, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, classify("read expired leases", err)
	}
	rows.Close()
	for _, item := range selected {
		exec := job.Execution{JobID: item.ID, Attempt: item.AttemptCount, WorkerID: *item.LeaseOwner,
			FencingToken: item.FencingToken, ExecutionKey: item.ExecutionKey}
		if err := s.finalizeFailure(queryCtx, tx, item, exec, job.Failure{Code: "LEASE_EXPIRED", Retryable: true}, "abandoned"); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(queryCtx); err != nil {
		return 0, classify("commit lease recovery", err)
	}
	return len(selected), nil
}

func scanJob(row rowScanner) (job.Job, error) {
	var found job.Job
	var initialMS, maxMS int64
	err := row.Scan(&found.ID, &found.Queue, &found.Kind, &found.Payload, &found.PayloadHash,
		&found.Status, &found.Priority, &found.IdempotencyKey, &found.AvailableAt,
		&found.AttemptCount, &found.Result, &found.LastErrorCode, &found.CreatedAt,
		&found.UpdatedAt, &found.StartedAt, &found.CompletedAt, &found.LeaseOwner,
		&found.LeaseExpiresAt, &found.FencingToken, &found.ExecutionKey, &found.MaxAttempts,
		&initialMS, &maxMS, &found.JitterPercent, &found.DeadLetteredAt)
	found.InitialBackoff = time.Duration(initialMS) * time.Millisecond
	found.MaxBackoff = time.Duration(maxMS) * time.Millisecond
	return found, err
}
