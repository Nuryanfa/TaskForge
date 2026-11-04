package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/job"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (s *Store) ListDeadLetters(ctx context.Context, before *time.Time, beforeID string, limit int) ([]job.DeadLetter, bool, error) {
	queryCtx, cancel := s.queryContext(ctx)
	defer cancel()
	rows, err := s.pool.Query(queryCtx, `SELECT job_id,queue,kind,final_attempt,error_code,
		dead_lettered_at,redriven_job_id,redriven_at FROM dead_letters
		WHERE ($1::timestamptz IS NULL OR (dead_lettered_at,job_id) < ($1,$2::uuid))
		ORDER BY dead_lettered_at DESC,job_id DESC LIMIT $3`, before, nullableUUID(before, beforeID), limit+1)
	if err != nil {
		return nil, false, classify("list dead letters", err)
	}
	defer rows.Close()
	items := make([]job.DeadLetter, 0, limit+1)
	for rows.Next() {
		var item job.DeadLetter
		if err := rows.Scan(&item.JobID, &item.Queue, &item.Kind, &item.FinalAttempt, &item.ErrorCode, &item.DeadLetteredAt, &item.RedrivenJobID, &item.RedrivenAt); err != nil {
			return nil, false, classify("scan dead letter", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, false, classify("read dead letters", err)
	}
	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}
	return items, hasMore, nil
}

func nullableUUID(before *time.Time, id string) any {
	if before == nil {
		return nil
	}
	return id
}

func (s *Store) GetDeadLetter(ctx context.Context, id string) (job.DeadLetter, error) {
	queryCtx, cancel := s.queryContext(ctx)
	defer cancel()
	var item job.DeadLetter
	err := s.pool.QueryRow(queryCtx, `SELECT job_id,queue,kind,final_attempt,error_code,
		dead_lettered_at,redriven_job_id,redriven_at FROM dead_letters WHERE job_id=$1`, id).
		Scan(&item.JobID, &item.Queue, &item.Kind, &item.FinalAttempt, &item.ErrorCode, &item.DeadLetteredAt, &item.RedrivenJobID, &item.RedrivenAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return job.DeadLetter{}, ErrNotFound
	}
	if err != nil {
		return job.DeadLetter{}, classify("get dead letter", err)
	}
	return item, nil
}

func (s *Store) RedriveDeadLetter(ctx context.Context, id string) (job.Job, bool, error) {
	queryCtx, cancel := s.queryContext(ctx)
	defer cancel()
	tx, err := s.pool.BeginTx(queryCtx, pgx.TxOptions{})
	if err != nil {
		return job.Job{}, false, classify("begin redrive", err)
	}
	defer tx.Rollback(queryCtx)
	var existing *string
	err = tx.QueryRow(queryCtx, `SELECT redriven_job_id FROM dead_letters WHERE job_id=$1 FOR UPDATE`, id).Scan(&existing)
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, false, ErrNotFound
	}
	if err != nil {
		return job.Job{}, false, classify("lock dead letter", err)
	}
	if existing != nil {
		found, err := scanJob(tx.QueryRow(queryCtx, `SELECT `+jobColumns+` FROM jobs WHERE id=$1`, *existing))
		if err != nil {
			return job.Job{}, false, classify("get redriven job", err)
		}
		if err := tx.Commit(queryCtx); err != nil {
			return job.Job{}, false, classify("commit repeated redrive", err)
		}
		return found, false, nil
	}
	newID := uuid.NewString()
	created, err := scanJob(tx.QueryRow(queryCtx, `INSERT INTO jobs
		(id,queue,kind,payload,payload_hash,status,priority,idempotency_key,available_at,
		execution_idempotency_key,max_attempts,retry_initial_backoff_ms,retry_max_backoff_ms,retry_jitter_percent)
		SELECT $2,queue,kind,payload,payload_hash,'queued',priority,NULL,NOW(),execution_idempotency_key,
		max_attempts,retry_initial_backoff_ms,retry_max_backoff_ms,retry_jitter_percent FROM jobs WHERE id=$1 RETURNING `+jobColumns, id, newID))
	if err != nil {
		return job.Job{}, false, classify("create redriven job", err)
	}
	command, err := tx.Exec(queryCtx, `UPDATE dead_letters SET redriven_job_id=$2,redriven_at=NOW() WHERE job_id=$1 AND redriven_job_id IS NULL`, id, newID)
	if err != nil || command.RowsAffected() != 1 {
		if err == nil {
			err = ErrStateConflict
		}
		return job.Job{}, false, classify("link redriven job", err)
	}
	if err := tx.Commit(queryCtx); err != nil {
		return job.Job{}, false, classify("commit redrive", err)
	}
	return created, true, nil
}
