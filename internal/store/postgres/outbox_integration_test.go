package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/job"
	"github.com/Nuryanfa/TaskForge/internal/outbox"
)

type recordedEvent struct {
	Type    string
	Payload map[string]any
}

func outboxEventsFor(t *testing.T, store *Store, jobID string) []recordedEvent {
	t.Helper()
	rows, err := store.pool.Query(context.Background(),
		`SELECT event_type, payload FROM outbox_events WHERE job_id=$1 ORDER BY id`, jobID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var events []recordedEvent
	for rows.Next() {
		var event recordedEvent
		var raw []byte
		if err := rows.Scan(&event.Type, &raw); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, &event.Payload); err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return events
}

func eventTypes(events []recordedEvent) []string {
	types := make([]string, len(events))
	for i, event := range events {
		types[i] = event.Type
	}
	return types
}

func assertEventTypes(t *testing.T, events []recordedEvent, want ...string) {
	t.Helper()
	if got := eventTypes(events); !reflect.DeepEqual(got, want) {
		t.Fatalf("event types=%v want %v", got, want)
	}
}

func TestOutboxRecordsSuccessfulLifecycleIntegration(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	key := "outbox-success"
	created, _, err := store.CreateJob(ctx, submission(t, 7, &key, `{"message":"secret payload"}`))
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimNextJob(ctx, "worker-secret", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteJob(ctx, claimed.Execution, json.RawMessage(`{"message":"secret result"}`)); err != nil {
		t.Fatal(err)
	}
	events := outboxEventsFor(t, store, created.ID)
	assertEventTypes(t, events, "job.queued", "job.started", "job.succeeded")

	wantKeys := []string{"attempt", "available_at", "dead_lettered", "error_code", "job_id",
		"kind", "max_attempts", "priority", "queue", "schedule_id", "status"}
	for _, event := range events {
		var keys []string
		for k := range event.Payload {
			keys = append(keys, k)
		}
		if len(keys) != len(wantKeys) {
			t.Fatalf("%s payload keys=%v want %v", event.Type, keys, wantKeys)
		}
		for _, k := range wantKeys {
			if _, ok := event.Payload[k]; !ok {
				t.Fatalf("%s payload missing %q: %v", event.Type, k, event.Payload)
			}
		}
		raw, _ := json.Marshal(event.Payload)
		for _, secret := range []string{"secret payload", "secret result", key, created.ExecutionKey, "worker-secret"} {
			if secret != "" && strings.Contains(string(raw), secret) {
				t.Fatalf("%s payload leaks %q: %s", event.Type, secret, raw)
			}
		}
	}
	queued, started, succeeded := events[0].Payload, events[1].Payload, events[2].Payload
	if queued["job_id"] != created.ID || queued["queue"] != "default" || queued["kind"] != "demo.echo" ||
		queued["status"] != "queued" || queued["attempt"] != float64(0) || queued["priority"] != float64(7) {
		t.Fatalf("queued payload=%v", queued)
	}
	if started["status"] != "running" || started["attempt"] != float64(1) {
		t.Fatalf("started payload=%v", started)
	}
	if succeeded["status"] != "succeeded" || succeeded["error_code"] != nil || succeeded["dead_lettered"] != false {
		t.Fatalf("succeeded payload=%v", succeeded)
	}
}

func TestOutboxRecordsRetryAndDeadLetterIntegration(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	created, _, err := store.CreateJob(ctx, submission(t, 0, nil, `{"message":"retry"}`))
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.ClaimNextJob(ctx, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.FailJob(ctx, first.Execution, job.Failure{Code: "TEMPORARY", Retryable: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `UPDATE jobs SET available_at=NOW() WHERE id=$1`, created.ID); err != nil {
		t.Fatal(err)
	}
	second, err := store.ClaimNextJob(ctx, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.FailJob(ctx, second.Execution, job.Failure{Code: "PERMANENT", Retryable: false}); err != nil {
		t.Fatal(err)
	}
	events := outboxEventsFor(t, store, created.ID)
	assertEventTypes(t, events, "job.queued", "job.started", "job.retry_scheduled", "job.started", "job.failed")
	if retry := events[2].Payload; retry["status"] != "queued" || retry["error_code"] != "TEMPORARY" || retry["attempt"] != float64(1) {
		t.Fatalf("retry payload=%v", retry)
	}
	if failed := events[4].Payload; failed["status"] != "failed" || failed["error_code"] != "PERMANENT" || failed["dead_lettered"] != true {
		t.Fatalf("failed payload=%v", failed)
	}

	redriven, _, err := store.RedriveDeadLetter(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertEventTypes(t, outboxEventsFor(t, store, redriven.ID), "job.queued")
}

func TestOutboxRecordsLeaseRecoveryCancelAndScheduleIntegration(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	recovered, _, err := store.CreateJob(ctx, submission(t, 0, nil, `{"message":"crash"}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNextJob(ctx, "crasher", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `UPDATE jobs SET lease_expires_at=NOW()-INTERVAL '1 second' WHERE id=$1`, recovered.ID); err != nil {
		t.Fatal(err)
	}
	if count, err := store.RecoverExpiredJobs(ctx, 10); err != nil || count != 1 {
		t.Fatalf("recover count=%d err=%v", count, err)
	}
	events := outboxEventsFor(t, store, recovered.ID)
	assertEventTypes(t, events, "job.queued", "job.started", "job.retry_scheduled")
	if events[2].Payload["error_code"] != "LEASE_EXPIRED" {
		t.Fatalf("recovery payload=%v", events[2].Payload)
	}

	canceled, _, err := store.CreateJob(ctx, submission(t, 0, nil, `{"message":"cancel"}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CancelQueuedJob(ctx, canceled.ID); err != nil {
		t.Fatal(err)
	}
	assertEventTypes(t, outboxEventsFor(t, store, canceled.ID), "job.queued", "job.canceled")

	sub, err := job.NewScheduleSubmission("outbox.tick", "default", "demo.echo", json.RawMessage(`{"message":"tick"}`), 0, "* * * * *", "UTC", false, 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	schedule, _, err := store.CreateSchedule(ctx, sub)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `UPDATE schedules SET next_run_at=NOW()-INTERVAL '1 minute' WHERE id=$1`, schedule.ID); err != nil {
		t.Fatal(err)
	}
	if count, err := store.DispatchDueSchedules(ctx, 10); err != nil || count != 1 {
		t.Fatalf("dispatch count=%d err=%v", count, err)
	}
	updated, err := store.GetSchedule(ctx, schedule.ID)
	if err != nil || updated.LastJobID == nil {
		t.Fatalf("schedule=%+v err=%v", updated, err)
	}
	scheduled := outboxEventsFor(t, store, *updated.LastJobID)
	assertEventTypes(t, scheduled, "job.queued")
	if scheduled[0].Payload["schedule_id"] != schedule.ID {
		t.Fatalf("scheduled payload=%v", scheduled[0].Payload)
	}
}

func TestOutboxRowIsRolledBackWithTransitionIntegration(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	created, _, err := store.CreateJob(ctx, submission(t, 0, nil, `{"message":"rollback"}`))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE jobs SET status='canceled', completed_at=NOW() WHERE id=$1`, created.ID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	assertEventTypes(t, outboxEventsFor(t, store, created.ID), "job.queued")

	// Updates that do not change status (heartbeats) emit nothing.
	claimed, err := store.ClaimNextJob(ctx, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RenewLease(ctx, claimed.Execution, time.Minute); err != nil {
		t.Fatal(err)
	}
	assertEventTypes(t, outboxEventsFor(t, store, created.ID), "job.queued", "job.started")
}

func TestPublishOutboxBatchPublishesInOrderAndMarksAckedPrefixIntegration(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	var jobIDs []string
	for i := 0; i < 3; i++ {
		created, _, err := store.CreateJob(ctx, submission(t, 0, nil, `{"message":"batch"}`))
		if err != nil {
			t.Fatal(err)
		}
		jobIDs = append(jobIDs, created.ID)
	}
	var seen []outbox.Event
	failing := errors.New("broker unavailable")
	published, err := store.PublishOutboxBatch(ctx, 10, func(_ context.Context, event outbox.Event) error {
		if len(seen) == 1 {
			return failing
		}
		seen = append(seen, event)
		return nil
	})
	if published != 1 || !errors.Is(err, failing) {
		t.Fatalf("published=%d err=%v", published, err)
	}
	if seen[0].JobID != jobIDs[0] || seen[0].Type != "job.queued" || seen[0].ID <= 0 || seen[0].CreatedAt.IsZero() {
		t.Fatalf("first event=%+v", seen[0])
	}
	if _, err := seen[0].Encode(); err != nil {
		t.Fatalf("stored event does not encode: %v", err)
	}
	if backlog, err := store.OutboxBacklog(ctx); err != nil || backlog != 2 {
		t.Fatalf("backlog=%d err=%v", backlog, err)
	}

	var retried []outbox.Event
	published, err = store.PublishOutboxBatch(ctx, 10, func(_ context.Context, event outbox.Event) error {
		retried = append(retried, event)
		return nil
	})
	if err != nil || published != 2 || retried[0].JobID != jobIDs[1] || retried[1].JobID != jobIDs[2] ||
		retried[0].ID >= retried[1].ID || retried[0].ID <= seen[0].ID {
		t.Fatalf("published=%d err=%v retried=%+v", published, err, retried)
	}
	published, err = store.PublishOutboxBatch(ctx, 10, func(context.Context, outbox.Event) error {
		t.Fatal("nothing should remain to publish")
		return nil
	})
	if err != nil || published != 0 {
		t.Fatalf("empty batch published=%d err=%v", published, err)
	}
}

func TestPublishOutboxBatchHonorsLimitIntegration(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if _, _, err := store.CreateJob(ctx, submission(t, 0, nil, `{"message":"limit"}`)); err != nil {
			t.Fatal(err)
		}
	}
	published, err := store.PublishOutboxBatch(ctx, 2, func(context.Context, outbox.Event) error { return nil })
	if err != nil || published != 2 {
		t.Fatalf("published=%d err=%v", published, err)
	}
	if backlog, err := store.OutboxBacklog(ctx); err != nil || backlog != 3 {
		t.Fatalf("backlog=%d err=%v", backlog, err)
	}
}

func TestPublishOutboxBatchAllowsOneActiveRelayIntegration(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateJob(ctx, submission(t, 0, nil, `{"message":"exclusive"}`)); err != nil {
		t.Fatal(err)
	}
	inside := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := store.PublishOutboxBatch(ctx, 10, func(context.Context, outbox.Event) error {
			close(inside)
			<-release
			return nil
		})
		done <- err
	}()
	<-inside
	published, err := store.PublishOutboxBatch(ctx, 10, func(context.Context, outbox.Event) error {
		t.Fatal("standby relay must not publish")
		return nil
	})
	if published != 0 || !errors.Is(err, ErrOutboxBusy) {
		t.Fatalf("standby published=%d err=%v", published, err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if backlog, err := store.OutboxBacklog(ctx); err != nil || backlog != 0 {
		t.Fatalf("backlog=%d err=%v", backlog, err)
	}
}

func TestPurgePublishedOutboxKeepsUnpublishedIntegration(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	for i := 0; i < 4; i++ {
		if _, _, err := store.CreateJob(ctx, submission(t, 0, nil, `{"message":"purge"}`)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.PublishOutboxBatch(ctx, 3, func(context.Context, outbox.Event) error { return nil }); err != nil {
		t.Fatal(err)
	}
	// Two old published rows, one recent published row, one unpublished row.
	if _, err := store.pool.Exec(ctx, `UPDATE outbox_events SET published_at=NOW()-INTERVAL '2 hours'
		WHERE id IN (SELECT id FROM outbox_events WHERE published_at IS NOT NULL ORDER BY id LIMIT 2)`); err != nil {
		t.Fatal(err)
	}
	purged, err := store.PurgePublishedOutbox(ctx, time.Hour, 1)
	if err != nil || purged != 1 {
		t.Fatalf("bounded purge=%d err=%v", purged, err)
	}
	purged, err = store.PurgePublishedOutbox(ctx, time.Hour, 100)
	if err != nil || purged != 1 {
		t.Fatalf("second purge=%d err=%v", purged, err)
	}
	var total, unpublished int
	if err := store.pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE published_at IS NULL) FROM outbox_events`).Scan(&total, &unpublished); err != nil {
		t.Fatal(err)
	}
	if total != 2 || unpublished != 1 {
		t.Fatalf("remaining total=%d unpublished=%d", total, unpublished)
	}
}

func TestPublishOutboxBatchMarksAckedPrefixWhenBatchDeadlineExpiresIntegration(t *testing.T) {
	store := integrationStore(t)
	for i := 0; i < 2; i++ {
		if _, _, err := store.CreateJob(context.Background(), submission(t, 0, nil, `{"message":"slow broker"}`)); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	calls := 0
	published, err := store.PublishOutboxBatch(ctx, 10, func(ctx context.Context, _ outbox.Event) error {
		calls++
		if calls == 1 {
			return nil
		}
		<-ctx.Done() // a slow broker consumes the rest of the batch budget
		return ctx.Err()
	})
	if published != 1 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("published=%d err=%v", published, err)
	}
	if backlog, err := store.OutboxBacklog(context.Background()); err != nil || backlog != 1 {
		t.Fatalf("acked event must stay marked: backlog=%d err=%v", backlog, err)
	}
}
