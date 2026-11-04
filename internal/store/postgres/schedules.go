package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/job"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const scheduleColumns = `id, name, queue, kind, payload, payload_hash, priority,
cron_expr, timezone, paused, next_run_at, last_run_at, last_job_id, created_at, updated_at`

func (s *Store) CreateSchedule(ctx context.Context, submission job.ScheduleSubmission) (job.Schedule, bool, error) {
	queryCtx, cancel := s.queryContext(ctx)
	defer cancel()

	var dbNow time.Time
	if err := s.pool.QueryRow(queryCtx, `SELECT NOW()`).Scan(&dbNow); err != nil {
		return job.Schedule{}, false, classify("read clock for schedule", err)
	}
	nextRunAt, err := submission.Cron.Next(dbNow, submission.Location)
	if err != nil {
		return job.Schedule{}, false, fmt.Errorf("compute initial schedule run: %w", err)
	}

	id := uuid.NewString()
	created, err := scanSchedule(s.pool.QueryRow(queryCtx, `INSERT INTO schedules
		(id, name, queue, kind, payload, payload_hash, priority, cron_expr, timezone, paused, next_run_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (name) DO NOTHING
		RETURNING `+scheduleColumns,
		id, submission.Name, submission.Queue, submission.Kind, submission.Payload,
		submission.Fingerprint[:], submission.Priority, submission.CronExpr,
		submission.Timezone, submission.Paused, nextRunAt))
	if err == nil {
		return created, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return job.Schedule{}, false, classify("create schedule", err)
	}

	existing, err := scanSchedule(s.pool.QueryRow(queryCtx,
		`SELECT `+scheduleColumns+` FROM schedules WHERE name = $1`, submission.Name))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return job.Schedule{}, false, fmt.Errorf("resolve idempotent schedule: %w", ErrStateConflict)
		}
		return job.Schedule{}, false, classify("resolve idempotent schedule", err)
	}
	if !bytes.Equal(existing.PayloadHash, submission.Fingerprint[:]) || existing.Paused != submission.Paused {
		return job.Schedule{}, false, ErrIdempotencyConflict
	}
	return existing, false, nil
}

func (s *Store) GetSchedule(ctx context.Context, id string) (job.Schedule, error) {
	queryCtx, cancel := s.queryContext(ctx)
	defer cancel()
	found, err := scanSchedule(s.pool.QueryRow(queryCtx, `SELECT `+scheduleColumns+` FROM schedules WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Schedule{}, ErrNotFound
	}
	if err != nil {
		return job.Schedule{}, classify("get schedule", err)
	}
	return found, nil
}

func (s *Store) ListSchedules(ctx context.Context, before *time.Time, beforeID string, limit int) ([]job.Schedule, bool, error) {
	queryCtx, cancel := s.queryContext(ctx)
	defer cancel()
	rows, err := s.pool.Query(queryCtx, `SELECT `+scheduleColumns+` FROM schedules
		WHERE ($1::timestamptz IS NULL OR (created_at, id) < ($1, $2::uuid))
		ORDER BY created_at DESC, id DESC LIMIT $3`, before, nullableUUID(before, beforeID), limit+1)
	if err != nil {
		return nil, false, classify("list schedules", err)
	}
	defer rows.Close()
	items := make([]job.Schedule, 0, limit+1)
	for rows.Next() {
		item, err := scanSchedule(rows)
		if err != nil {
			return nil, false, classify("scan schedule", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, false, classify("read schedules", err)
	}
	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}
	return items, hasMore, nil
}

func (s *Store) SetSchedulePaused(ctx context.Context, id string, paused bool) (job.Schedule, error) {
	queryCtx, cancel := s.queryContext(ctx)
	defer cancel()
	if paused {
		updated, err := scanSchedule(s.pool.QueryRow(queryCtx, `UPDATE schedules
			SET paused = TRUE, updated_at = NOW()
			WHERE id = $1 RETURNING `+scheduleColumns, id))
		if errors.Is(err, pgx.ErrNoRows) {
			return job.Schedule{}, ErrNotFound
		}
		if err != nil {
			return job.Schedule{}, classify("pause schedule", err)
		}
		return updated, nil
	}

	tx, err := s.pool.BeginTx(queryCtx, pgx.TxOptions{})
	if err != nil {
		return job.Schedule{}, classify("begin resume schedule", err)
	}
	defer tx.Rollback(queryCtx)

	var dbNow time.Time
	var current job.Schedule
	err = tx.QueryRow(queryCtx, `SELECT NOW(), `+scheduleColumns+` FROM schedules WHERE id = $1 FOR UPDATE`, id).
		Scan(&dbNow, &current.ID, &current.Name, &current.Queue, &current.Kind, &current.Payload,
			&current.PayloadHash, &current.Priority, &current.CronExpr, &current.Timezone,
			&current.Paused, &current.NextRunAt, &current.LastRunAt, &current.LastJobID,
			&current.CreatedAt, &current.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Schedule{}, ErrNotFound
	}
	if err != nil {
		return job.Schedule{}, classify("lock schedule for resume", err)
	}

	nextRunAt := current.NextRunAt
	if !nextRunAt.After(dbNow) {
		cron, _, err := job.ParseCron(current.CronExpr)
		if err != nil {
			return job.Schedule{}, fmt.Errorf("parse stored cron: %w", err)
		}
		loc, _, err := job.ValidateTimezone(current.Timezone)
		if err != nil {
			return job.Schedule{}, fmt.Errorf("parse stored timezone: %w", err)
		}
		nextRunAt, err = cron.Next(dbNow, loc)
		if err != nil {
			return job.Schedule{}, fmt.Errorf("compute resumed schedule next run: %w", err)
		}
	}

	updated, err := scanSchedule(tx.QueryRow(queryCtx, `UPDATE schedules
		SET paused = FALSE, next_run_at = $2, updated_at = NOW()
		WHERE id = $1 RETURNING `+scheduleColumns, id, nextRunAt))
	if err != nil {
		return job.Schedule{}, classify("resume schedule", err)
	}
	if err := tx.Commit(queryCtx); err != nil {
		return job.Schedule{}, classify("commit resume schedule", err)
	}
	return updated, nil
}

func (s *Store) DeleteSchedule(ctx context.Context, id string) error {
	queryCtx, cancel := s.queryContext(ctx)
	defer cancel()
	command, err := s.pool.Exec(queryCtx, `DELETE FROM schedules WHERE id = $1`, id)
	if err != nil {
		return classify("delete schedule", err)
	}
	if command.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) DispatchDueSchedules(ctx context.Context, batchSize int) (int, error) {
	queryCtx, cancel := s.queryContext(ctx)
	defer cancel()
	tx, err := s.pool.BeginTx(queryCtx, pgx.TxOptions{})
	if err != nil {
		return 0, classify("begin schedule dispatch transaction", err)
	}
	defer tx.Rollback(queryCtx)

	rows, err := tx.Query(queryCtx, `SELECT NOW(), `+scheduleColumns+` FROM schedules
		WHERE NOT paused AND next_run_at <= NOW()
		ORDER BY next_run_at, id
		FOR UPDATE SKIP LOCKED LIMIT $1`, batchSize)
	if err != nil {
		return 0, classify("select due schedules", err)
	}
	type dueSchedule struct {
		dbNow    time.Time
		schedule job.Schedule
	}
	var selected []dueSchedule
	for rows.Next() {
		var item dueSchedule
		if err := rows.Scan(&item.dbNow, &item.schedule.ID, &item.schedule.Name, &item.schedule.Queue,
			&item.schedule.Kind, &item.schedule.Payload, &item.schedule.PayloadHash, &item.schedule.Priority,
			&item.schedule.CronExpr, &item.schedule.Timezone, &item.schedule.Paused, &item.schedule.NextRunAt,
			&item.schedule.LastRunAt, &item.schedule.LastJobID, &item.schedule.CreatedAt, &item.schedule.UpdatedAt); err != nil {
			rows.Close()
			return 0, classify("scan due schedule", err)
		}
		selected = append(selected, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, classify("read due schedules", err)
	}
	rows.Close()

	dispatched := 0
	for _, entry := range selected {
		item := entry.schedule
		cron, _, err := job.ParseCron(item.CronExpr)
		if err != nil {
			return 0, fmt.Errorf("parse schedule cron %s: %w", item.ID, err)
		}
		loc, _, err := job.ValidateTimezone(item.Timezone)
		if err != nil {
			return 0, fmt.Errorf("validate schedule timezone %s: %w", item.ID, err)
		}
		slot := item.NextRunAt.UTC()
		reference := entry.dbNow.UTC()
		if slot.After(reference) {
			reference = slot
		}
		nextRunAt, err := cron.Next(reference, loc)
		if err != nil {
			return 0, fmt.Errorf("advance schedule %s: %w", item.ID, err)
		}

		newJobID := uuid.NewString()
		var insertedID *string
		var createdID string
		err = tx.QueryRow(queryCtx, `INSERT INTO jobs
			(id, queue, kind, payload, payload_hash, status, priority, available_at,
			execution_idempotency_key, max_attempts, retry_initial_backoff_ms,
			retry_max_backoff_ms, retry_jitter_percent, schedule_id, scheduled_for)
			VALUES ($1, $2, $3, $4, $5, 'queued', $6, NOW(), $7, $8, $9, $10, $11, $12, $13)
			ON CONFLICT (schedule_id, scheduled_for) WHERE schedule_id IS NOT NULL DO NOTHING
			RETURNING id`,
			newJobID, item.Queue, item.Kind, item.Payload, item.PayloadHash, item.Priority,
			uuid.NewString(), s.retryPolicy.maxAttempts, s.retryPolicy.initialBackoff.Milliseconds(),
			s.retryPolicy.maxBackoff.Milliseconds(), s.retryPolicy.jitterPercent, item.ID, slot).Scan(&createdID)
		if err == nil {
			insertedID = &createdID
			dispatched++
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return 0, classify("enqueue scheduled job", err)
		}

		if _, err := tx.Exec(queryCtx, `UPDATE schedules
			SET next_run_at = $2, last_run_at = $3, last_job_id = COALESCE($4, last_job_id), updated_at = NOW()
			WHERE id = $1`, item.ID, nextRunAt, slot, insertedID); err != nil {
			return 0, classify("advance schedule state", err)
		}
	}

	if err := tx.Commit(queryCtx); err != nil {
		return 0, classify("commit schedule dispatch", err)
	}
	return dispatched, nil
}

func scanSchedule(row rowScanner) (job.Schedule, error) {
	var found job.Schedule
	err := row.Scan(&found.ID, &found.Name, &found.Queue, &found.Kind, &found.Payload,
		&found.PayloadHash, &found.Priority, &found.CronExpr, &found.Timezone,
		&found.Paused, &found.NextRunAt, &found.LastRunAt, &found.LastJobID,
		&found.CreatedAt, &found.UpdatedAt)
	return found, err
}

