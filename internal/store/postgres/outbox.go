package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/outbox"
	"github.com/jackc/pgx/v5"
)

// outboxRelayLockClass and the current schema name form the transaction-scoped
// advisory lock that allows only one relay per TaskForge schema to publish at
// a time, which preserves per-job event order.
const outboxRelayLockClass int32 = 0x54465242 // "TFRB"

// PublishOutboxBatch locks the relay role, reads up to limit unpublished
// events in id order, and calls publish for each until one fails. Only the
// acknowledged prefix is marked published, under a fresh query timeout. The
// caller bounds reading and publishing with ctx because publish performs
// network I/O inside the transaction.
// It returns ErrOutboxBusy when another relay holds the role.
func (s *Store) PublishOutboxBatch(ctx context.Context, limit int, publish func(context.Context, outbox.Event) error) (int, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, classify("begin outbox transaction", err)
	}
	defer tx.Rollback(ctx)
	var locked bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1, hashtext(current_schema()))`, outboxRelayLockClass).Scan(&locked); err != nil {
		return 0, classify("lock outbox relay", err)
	}
	if !locked {
		return 0, ErrOutboxBusy
	}
	rows, err := tx.Query(ctx, `SELECT id, job_id, event_type, payload, created_at FROM outbox_events
		WHERE published_at IS NULL ORDER BY id LIMIT $1`, limit)
	if err != nil {
		return 0, classify("select outbox events", err)
	}
	var events []outbox.Event
	for rows.Next() {
		var event outbox.Event
		if err := rows.Scan(&event.ID, &event.JobID, &event.Type, &event.Payload, &event.CreatedAt); err != nil {
			rows.Close()
			return 0, classify("scan outbox event", err)
		}
		events = append(events, event)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, classify("read outbox events", err)
	}
	acked := make([]int64, 0, len(events))
	var publishErr error
	for _, event := range events {
		if err := publish(ctx, event); err != nil {
			publishErr = err
			break
		}
		acked = append(acked, event.ID)
	}
	if len(acked) > 0 {
		// Marking gets its own bound: a slow broker may have spent the batch
		// budget, and losing the acknowledged prefix would stall progress.
		markCtx, cancel := s.queryContext(context.WithoutCancel(ctx))
		defer cancel()
		if _, err := tx.Exec(markCtx, `UPDATE outbox_events SET published_at = NOW() WHERE id = ANY($1)`, acked); err != nil {
			return 0, classify("mark outbox events published", err)
		}
		if err := tx.Commit(markCtx); err != nil {
			return 0, classify("commit outbox batch", err)
		}
	}
	if publishErr != nil {
		return len(acked), fmt.Errorf("publish outbox event: %w", publishErr)
	}
	return len(acked), nil
}

// PurgePublishedOutbox deletes at most limit events published before
// retention ago. Unpublished events are never deleted.
func (s *Store) PurgePublishedOutbox(ctx context.Context, retention time.Duration, limit int) (int, error) {
	queryCtx, cancel := s.queryContext(ctx)
	defer cancel()
	command, err := s.pool.Exec(queryCtx, `DELETE FROM outbox_events WHERE id IN (
		SELECT id FROM outbox_events
		WHERE published_at IS NOT NULL AND published_at < NOW() - ($1 * INTERVAL '1 microsecond')
		ORDER BY published_at LIMIT $2)`, retention.Microseconds(), limit)
	if err != nil {
		return 0, classify("purge published outbox events", err)
	}
	return int(command.RowsAffected()), nil
}

// OutboxBacklog counts events not yet published.
func (s *Store) OutboxBacklog(ctx context.Context) (int64, error) {
	queryCtx, cancel := s.queryContext(ctx)
	defer cancel()
	var count int64
	if err := s.pool.QueryRow(queryCtx, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`).Scan(&count); err != nil {
		return 0, classify("count outbox backlog", err)
	}
	return count, nil
}
