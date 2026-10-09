package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/config"
	"github.com/Nuryanfa/TaskForge/internal/job"
	"github.com/Nuryanfa/TaskForge/internal/migrations"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func integrationStore(t *testing.T) *Store {
	t.Helper()
	baseURL := os.Getenv("TASKFORGE_TEST_DATABASE_URL")
	if baseURL == "" {
		t.Skip("TASKFORGE_TEST_DATABASE_URL is not set")
	}
	databaseURL := isolatedDatabaseURL(t, baseURL)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	if err := migrations.Run(ctx, databaseURL); err != nil {
		t.Fatal("apply integration migrations")
	}
	// Running migrations twice verifies that an up-to-date database is success.
	if err := migrations.Run(ctx, databaseURL); err != nil {
		t.Fatal("reapply integration migrations")
	}
	store, err := Open(ctx, config.Config{
		DatabaseURL: databaseURL, DatabaseMaxConns: 10, DatabaseMinConns: 0,
		DatabaseConnectTimeout: 5 * time.Second, DatabaseQueryTimeout: 3 * time.Second,
		JobResultMaxBytes: 64 * 1024,
	})
	if err != nil {
		t.Fatal("open integration store")
	}
	t.Cleanup(store.Close)
	if _, err := store.pool.Exec(ctx, `TRUNCATE job_attempts, jobs RESTART IDENTITY CASCADE`); err != nil {
		t.Fatal("clean integration tables")
	}
	return store
}

func isolatedDatabaseURL(t *testing.T, baseURL string) string {
	t.Helper()
	schema := "taskforge_test_" + strings.ReplaceAll(uuid.NewString(), "-", "_")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, baseURL)
	if err != nil {
		t.Fatal("connect to create isolated schema")
	}
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err := conn.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		conn.Close(ctx)
		t.Fatal("create isolated schema")
	}
	conn.Close(ctx)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		cleanupConn, err := pgx.Connect(cleanupCtx, baseURL)
		if err == nil {
			_, _ = cleanupConn.Exec(cleanupCtx, "DROP SCHEMA IF EXISTS "+identifier+" CASCADE")
			_ = cleanupConn.Close(cleanupCtx)
		}
	})
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme == "" {
		t.Fatal("TASKFORGE_TEST_DATABASE_URL must be a PostgreSQL URL")
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func submission(t *testing.T, priority int, key *string, payload string) job.Submission {
	t.Helper()
	value, err := job.NewSubmission("default", "demo.echo", json.RawMessage(payload), priority, key, 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestCreateGetAndIdempotencyIntegration(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	key := "same-request"
	request := submission(t, 0, &key, `{"message":"hello"}`)
	created, isNew, err := store.CreateJob(ctx, request)
	if err != nil || !isNew {
		t.Fatalf("create job: isNew=%v err=%v", isNew, err)
	}
	found, err := store.GetJob(ctx, created.ID)
	if err != nil || !bytes.Equal(found.PayloadHash, request.Fingerprint[:]) {
		t.Fatalf("get job: err=%v", err)
	}
	replayed, isNew, err := store.CreateJob(ctx, request)
	if err != nil || isNew || replayed.ID != created.ID {
		t.Fatalf("replay: id=%s isNew=%v err=%v", replayed.ID, isNew, err)
	}
	conflict := submission(t, 0, &key, `{"message":"different"}`)
	if _, _, err := store.CreateJob(ctx, conflict); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting replay error = %v", err)
	}
}

func TestConcurrentIdempotentSubmissionIntegration(t *testing.T) {
	store := integrationStore(t)
	key := "concurrent-request"
	request := submission(t, 0, &key, `{"message":"hello"}`)
	const callers = 12
	ids := make(chan string, callers)
	errs := make(chan error, callers)
	var wait sync.WaitGroup
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			created, _, err := store.CreateJob(context.Background(), request)
			if err != nil {
				errs <- err
				return
			}
			ids <- created.ID
		}()
	}
	wait.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var first string
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatalf("concurrent submissions returned different IDs: %s and %s", first, id)
		}
	}
	var count int
	if err := store.pool.QueryRow(context.Background(), `SELECT count(*) FROM jobs`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("job count=%d err=%v", count, err)
	}
}

func TestClaimOrderingAttemptsCancellationAndOutcomesIntegration(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	firstHigh, _, err := store.CreateJob(ctx, submission(t, 10, nil, `{"message":"first"}`))
	if err != nil {
		t.Fatal(err)
	}
	secondHigh, _, err := store.CreateJob(ctx, submission(t, 10, nil, `{"message":"second"}`))
	if err != nil {
		t.Fatal(err)
	}
	low, _, err := store.CreateJob(ctx, submission(t, -10, nil, `{"message":"low"}`))
	if err != nil {
		t.Fatal(err)
	}
	canceled, err := store.CancelQueuedJob(ctx, low.ID)
	if err != nil || canceled.Status != job.StatusCanceled {
		t.Fatalf("cancel queued job: status=%s err=%v", canceled.Status, err)
	}
	if _, err := store.CancelQueuedJob(ctx, low.ID); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("cancel terminal job error=%v", err)
	}

	claimed, err := store.ClaimNextJob(ctx, "integration-worker")
	if err != nil || claimed.ID != firstHigh.ID || claimed.AttemptCount != 1 {
		t.Fatalf("first claim=%s attempt=%d err=%v", claimed.ID, claimed.AttemptCount, err)
	}
	if _, err := store.CancelQueuedJob(ctx, claimed.ID); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("cancel running job error=%v", err)
	}
	var runningAttempts int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM job_attempts
        WHERE job_id = $1 AND attempt = $2 AND finished_at IS NULL`, claimed.ID, claimed.AttemptCount).Scan(&runningAttempts); err != nil || runningAttempts != 1 {
		t.Fatalf("running attempt count=%d err=%v", runningAttempts, err)
	}
	if err := store.CompleteJob(ctx, claimed.ID, claimed.AttemptCount, json.RawMessage(`{"message":"first"}`)); err != nil {
		t.Fatal(err)
	}
	var succeededOutcome string
	if err := store.pool.QueryRow(ctx, `SELECT outcome FROM job_attempts
        WHERE job_id = $1 AND attempt = $2`, claimed.ID, claimed.AttemptCount).Scan(&succeededOutcome); err != nil || succeededOutcome != "succeeded" {
		t.Fatalf("completion attempt outcome=%s err=%v", succeededOutcome, err)
	}
	if err := store.CompleteJob(ctx, claimed.ID, claimed.AttemptCount, json.RawMessage(`{}`)); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("duplicate completion error=%v", err)
	}

	claimed, err = store.ClaimNextJob(ctx, "integration-worker")
	if err != nil || claimed.ID != secondHigh.ID {
		t.Fatalf("second claim=%s err=%v", claimed.ID, err)
	}
	if err := store.FailJob(ctx, claimed.ID, claimed.AttemptCount, "HANDLER_FAILED"); err != nil {
		t.Fatal(err)
	}
	if err := store.FailJob(ctx, claimed.ID, claimed.AttemptCount, "raw unsafe error"); err == nil {
		t.Fatal("expected unsafe error code rejection")
	}
	if _, err := store.ClaimNextJob(ctx, "integration-worker"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("canceled job was claimable: %v", err)
	}

	var outcome, errorCode string
	var finishedAt *time.Time
	if err := store.pool.QueryRow(ctx, `SELECT outcome, error_code, finished_at FROM job_attempts
        WHERE job_id = $1 AND attempt = 1`, secondHigh.ID).Scan(&outcome, &errorCode, &finishedAt); err != nil {
		t.Fatal(err)
	}
	if outcome != "failed" || errorCode != "HANDLER_FAILED" || finishedAt == nil {
		t.Fatalf("unexpected attempt outcome=%s code=%s finished=%v", outcome, errorCode, finishedAt)
	}
}
