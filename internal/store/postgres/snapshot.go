package postgres

import (
	"context"
	"time"
)

// Snapshot is the database state exported as gauges. All ages use the
// database clock.
type Snapshot struct {
	Ready, Delayed, Running, ExpiredLease int64
	OldestReadyAge                        time.Duration
	UnredrivenDeadLetters                 int64
	OldestUnpublishedOutboxAge            time.Duration
	Workflows                             map[string]int64 // non-terminal statuses
}

// ObservabilitySnapshot reads the gauges with bounded queries on live rows
// (queued, running, unpublished, non-terminal) so its cost tracks the backlog,
// not the table's history.
func (s *Store) ObservabilitySnapshot(ctx context.Context) (Snapshot, error) {
	queryCtx, cancel := s.queryContext(ctx)
	defer cancel()
	snapshot := Snapshot{Workflows: map[string]int64{"running": 0, "compensating": 0}}
	var oldestReady float64
	if err := s.pool.QueryRow(queryCtx, `SELECT
		count(*) FILTER (WHERE status = 'queued' AND available_at <= NOW()),
		count(*) FILTER (WHERE status = 'queued' AND available_at > NOW()),
		count(*) FILTER (WHERE status = 'running' AND lease_expires_at > NOW()),
		count(*) FILTER (WHERE status = 'running' AND lease_expires_at <= NOW()),
		COALESCE(EXTRACT(EPOCH FROM NOW() - min(available_at) FILTER (
			WHERE status = 'queued' AND available_at <= NOW())), 0)
		FROM jobs WHERE status IN ('queued', 'running')`).Scan(
		&snapshot.Ready, &snapshot.Delayed, &snapshot.Running, &snapshot.ExpiredLease, &oldestReady); err != nil {
		return Snapshot{}, classify("snapshot jobs", err)
	}
	snapshot.OldestReadyAge = time.Duration(oldestReady * float64(time.Second))
	if err := s.pool.QueryRow(queryCtx, `SELECT count(*) FROM dead_letters WHERE redriven_job_id IS NULL`).
		Scan(&snapshot.UnredrivenDeadLetters); err != nil {
		return Snapshot{}, classify("snapshot dead letters", err)
	}
	var oldestOutbox float64
	if err := s.pool.QueryRow(queryCtx, `SELECT COALESCE((SELECT EXTRACT(EPOCH FROM NOW() - created_at)
		FROM outbox_events WHERE published_at IS NULL ORDER BY id LIMIT 1), 0)`).Scan(&oldestOutbox); err != nil {
		return Snapshot{}, classify("snapshot outbox", err)
	}
	snapshot.OldestUnpublishedOutboxAge = time.Duration(oldestOutbox * float64(time.Second))
	rows, err := s.pool.Query(queryCtx, `SELECT status, count(*) FROM workflows
		WHERE status IN ('running', 'compensating') GROUP BY status`)
	if err != nil {
		return Snapshot{}, classify("snapshot workflows", err)
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return Snapshot{}, classify("scan workflow snapshot", err)
		}
		snapshot.Workflows[status] = count
	}
	if err := rows.Err(); err != nil {
		return Snapshot{}, classify("read workflow snapshot", err)
	}
	return snapshot, nil
}
