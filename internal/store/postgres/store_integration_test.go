package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

	claimed, err := store.ClaimNextJob(ctx, "integration-worker", time.Minute)
	if err != nil || claimed.Job.ID != firstHigh.ID || claimed.Job.AttemptCount != 1 {
		t.Fatalf("first claim=%s attempt=%d err=%v", claimed.Job.ID, claimed.Job.AttemptCount, err)
	}
	if _, err := store.CancelQueuedJob(ctx, claimed.Job.ID); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("cancel running job error=%v", err)
	}
	var runningAttempts int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM job_attempts
		WHERE job_id = $1 AND attempt = $2 AND finished_at IS NULL`, claimed.Job.ID, claimed.Job.AttemptCount).Scan(&runningAttempts); err != nil || runningAttempts != 1 {
		t.Fatalf("running attempt count=%d err=%v", runningAttempts, err)
	}
	if err := store.CompleteJob(ctx, claimed.Execution, json.RawMessage(`{"message":"first"}`)); err != nil {
		t.Fatal(err)
	}
	var succeededOutcome string
	if err := store.pool.QueryRow(ctx, `SELECT outcome FROM job_attempts
		WHERE job_id = $1 AND attempt = $2`, claimed.Job.ID, claimed.Job.AttemptCount).Scan(&succeededOutcome); err != nil || succeededOutcome != "succeeded" {
		t.Fatalf("completion attempt outcome=%s err=%v", succeededOutcome, err)
	}
	if err := store.CompleteJob(ctx, claimed.Execution, json.RawMessage(`{}`)); !errors.Is(err, ErrOwnershipLost) {
		t.Fatalf("duplicate completion error=%v", err)
	}

	claimed, err = store.ClaimNextJob(ctx, "integration-worker", time.Minute)
	if err != nil || claimed.Job.ID != secondHigh.ID {
		t.Fatalf("second claim=%s err=%v", claimed.Job.ID, err)
	}
	if err := store.FailJob(ctx, claimed.Execution, job.Failure{Code: "HANDLER_FAILED"}); err != nil {
		t.Fatal(err)
	}
	if err := store.FailJob(ctx, claimed.Execution, job.Failure{Code: "raw unsafe error"}); err == nil {
		t.Fatal("expected unsafe error code rejection")
	}
	if _, err := store.ClaimNextJob(ctx, "integration-worker", time.Minute); !errors.Is(err, ErrNotFound) {
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

func TestConcurrentClaimsHaveExclusiveOwnersIntegration(t *testing.T) {
	store := integrationStore(t)
	const total = 8
	for i := 0; i < total; i++ {
		if _, _, err := store.CreateJob(context.Background(), submission(t, 0, nil, `{"message":"work"}`)); err != nil {
			t.Fatal(err)
		}
	}
	claims := make(chan job.Claimed, total)
	errs := make(chan error, total)
	var wait sync.WaitGroup
	for i := 0; i < total; i++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			claimed, err := store.ClaimNextJob(context.Background(), fmt.Sprintf("worker-%d", worker), time.Minute)
			if err != nil {
				errs <- err
				return
			}
			claims <- claimed
		}(i)
	}
	wait.Wait()
	close(claims)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	seen := make(map[string]struct{}, total)
	for claimed := range claims {
		if _, exists := seen[claimed.Job.ID]; exists {
			t.Fatalf("job %s claimed twice", claimed.Job.ID)
		}
		seen[claimed.Job.ID] = struct{}{}
		if claimed.Job.LeaseOwner == nil || *claimed.Job.LeaseOwner != claimed.Execution.WorkerID {
			t.Fatalf("claim owner mismatch: %+v", claimed)
		}
	}
	if len(seen) != total {
		t.Fatalf("claimed %d jobs, want %d", len(seen), total)
	}
}

func TestLeaseHeartbeatRecoveryAndFencingIntegration(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	created, _, err := store.CreateJob(ctx, submission(t, 0, nil, `{"message":"recover"}`))
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.ClaimNextJob(ctx, "worker-one", 150*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	initialExpiration := *first.Job.LeaseExpiresAt
	if err := store.RenewLease(ctx, first.Execution, 400*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	renewed, err := store.GetJob(ctx, created.ID)
	if err != nil || renewed.LeaseExpiresAt == nil || !renewed.LeaseExpiresAt.After(initialExpiration) {
		t.Fatalf("lease was not extended: %+v err=%v", renewed.LeaseExpiresAt, err)
	}
	staleOwner := first.Execution
	staleOwner.WorkerID = "worker-other"
	if err := store.RenewLease(ctx, staleOwner, time.Second); !errors.Is(err, ErrOwnershipLost) {
		t.Fatalf("stale heartbeat error=%v", err)
	}
	time.Sleep(450 * time.Millisecond)
	recovered, err := store.RecoverExpiredJobs(ctx, 1)
	if err != nil || recovered != 1 {
		t.Fatalf("recovered=%d err=%v", recovered, err)
	}
	if err := store.RenewLease(ctx, first.Execution, time.Second); !errors.Is(err, ErrOwnershipLost) {
		t.Fatalf("recovered execution heartbeat error=%v", err)
	}
	if err := store.CompleteJob(ctx, first.Execution, json.RawMessage(`{}`)); !errors.Is(err, ErrOwnershipLost) {
		t.Fatalf("stale completion error=%v", err)
	}
	if err := store.FailJob(ctx, first.Execution, job.Failure{Code: "STALE_FAILURE"}); !errors.Is(err, ErrOwnershipLost) {
		t.Fatalf("stale failure error=%v", err)
	}
	if _, err := store.pool.Exec(ctx, `UPDATE jobs SET available_at=NOW() WHERE id=$1`, created.ID); err != nil {
		t.Fatal(err)
	}
	second, err := store.ClaimNextJob(ctx, "worker-two", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if second.Job.ID != created.ID || second.Execution.Attempt != 2 || second.Execution.FencingToken <= first.Execution.FencingToken {
		t.Fatalf("unexpected reclaimed execution: first=%+v second=%+v", first.Execution, second.Execution)
	}
	var outcome, code string
	var finishedAt time.Time
	if err := store.pool.QueryRow(ctx, `SELECT outcome, error_code, finished_at FROM job_attempts
		WHERE job_id = $1 AND attempt = 1`, created.ID).Scan(&outcome, &code, &finishedAt); err != nil {
		t.Fatal(err)
	}
	if outcome != "abandoned" || code != "LEASE_EXPIRED" || finishedAt.IsZero() {
		t.Fatalf("attempt outcome=%s code=%s finished=%v", outcome, code, finishedAt)
	}
}

func TestHealthyHeartbeatPreventsRecoveryIntegration(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateJob(ctx, submission(t, 0, nil, `{"message":"long"}`)); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimNextJob(ctx, "healthy-worker", 180*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		time.Sleep(60 * time.Millisecond)
		if err := store.RenewLease(ctx, claimed.Execution, 180*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		if count, err := store.RecoverExpiredJobs(ctx, 10); err != nil || count != 0 {
			t.Fatalf("healthy lease recovered: count=%d err=%v", count, err)
		}
	}
	if err := store.CompleteJob(ctx, claimed.Execution, json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentRecoveryOnlyRecoversLeaseOnceIntegration(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateJob(ctx, submission(t, 0, nil, `{"message":"once"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNextJob(ctx, "crashed-worker", 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	counts := make(chan int, 2)
	errs := make(chan error, 2)
	var wait sync.WaitGroup
	for i := 0; i < 2; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			count, err := store.RecoverExpiredJobs(context.Background(), 10)
			counts <- count
			errs <- err
		}()
	}
	wait.Wait()
	close(counts)
	close(errs)
	total := 0
	for count := range counts {
		total += count
	}
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if total != 1 {
		t.Fatalf("total recoveries=%d, want 1", total)
	}
}

func TestRetryDeadLetterAndRedriveIntegration(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	created, _, err := store.CreateJob(ctx, submission(t, 0, nil, `{"message":"retry"}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `UPDATE jobs SET max_attempts=2,retry_initial_backoff_ms=80,
		retry_max_backoff_ms=80,retry_jitter_percent=0 WHERE id=$1`, created.ID); err != nil {
		t.Fatal(err)
	}
	first, err := store.ClaimNextJob(ctx, "retry-worker", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.FailJob(ctx, first.Execution, job.Failure{Code: "TEMPORARY_DEPENDENCY", Retryable: true}); err != nil {
		t.Fatal(err)
	}
	retrying, err := store.GetJob(ctx, created.ID)
	if err != nil || retrying.Status != job.StatusQueued || !retrying.AvailableAt.After(time.Now().Add(-20*time.Millisecond)) {
		t.Fatalf("retry state=%+v err=%v", retrying, err)
	}
	if _, err := store.ClaimNextJob(ctx, "early-worker", time.Second); !errors.Is(err, ErrNotFound) {
		t.Fatalf("retry claimed early: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	var second job.Claimed
	for {
		second, err = store.ClaimNextJob(ctx, "retry-worker", time.Second)
		if err == nil {
			break
		}
		if !errors.Is(err, ErrNotFound) || time.Now().After(deadline) {
			t.Fatalf("retry not claimable: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if second.Execution.Attempt != 2 || second.Execution.ExecutionKey != first.Execution.ExecutionKey {
		t.Fatalf("execution identity changed: first=%+v second=%+v", first.Execution, second.Execution)
	}
	if err := store.FailJob(ctx, second.Execution, job.Failure{Code: "TEMPORARY_DEPENDENCY", Retryable: true}); err != nil {
		t.Fatal(err)
	}
	failed, err := store.GetJob(ctx, created.ID)
	if err != nil || failed.Status != job.StatusFailed || failed.DeadLetteredAt == nil {
		t.Fatalf("failed=%+v err=%v", failed, err)
	}
	letter, err := store.GetDeadLetter(ctx, created.ID)
	if err != nil || letter.FinalAttempt != 2 {
		t.Fatalf("letter=%+v err=%v", letter, err)
	}
	redriven, isNew, err := store.RedriveDeadLetter(ctx, created.ID)
	if err != nil || !isNew {
		t.Fatalf("redrive new=%v err=%v", isNew, err)
	}
	replayed, isNew, err := store.RedriveDeadLetter(ctx, created.ID)
	if err != nil || isNew || replayed.ID != redriven.ID {
		t.Fatalf("redrive replay=%s/%s new=%v err=%v", redriven.ID, replayed.ID, isNew, err)
	}
	if redriven.ExecutionKey != created.ExecutionKey || redriven.IdempotencyKey != nil {
		t.Fatalf("redrive key/idempotency mismatch")
	}
}

func TestPermanentFailureAndConcurrentRedriveIntegration(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	created, _, err := store.CreateJob(ctx, submission(t, 0, nil, `{"message":"permanent"}`))
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimNextJob(ctx, "worker", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.FailJob(ctx, claimed.Execution, job.Failure{Code: "INVALID_JOB_PAYLOAD", Retryable: false}); err != nil {
		t.Fatal(err)
	}
	ids := make(chan string, 8)
	errs := make(chan error, 8)
	var wait sync.WaitGroup
	for i := 0; i < 8; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			redriven, _, err := store.RedriveDeadLetter(context.Background(), created.ID)
			if err != nil {
				errs <- err
				return
			}
			ids <- redriven.ID
		}()
	}
	wait.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	first := ""
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatalf("duplicate redrives %s/%s", first, id)
		}
	}
	var count int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE execution_idempotency_key=$1`, created.ExecutionKey).Scan(&count); err != nil || count != 2 {
		t.Fatalf("logical job count=%d err=%v", count, err)
	}
}

func TestExpiredAttemptsReachDeadLetterIntegration(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	created, _, err := store.CreateJob(ctx, submission(t, 0, nil, `{"message":"crash-loop"}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `UPDATE jobs SET max_attempts=2,retry_initial_backoff_ms=1,retry_max_backoff_ms=1,retry_jitter_percent=0 WHERE id=$1`, created.ID); err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 2; attempt++ {
		claimed, err := store.ClaimNextJob(ctx, "crasher", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.pool.Exec(ctx, `UPDATE jobs SET lease_expires_at=NOW()-INTERVAL '1 second' WHERE id=$1`, created.ID); err != nil {
			t.Fatal(err)
		}
		count, err := store.RecoverExpiredJobs(ctx, 10)
		if err != nil || count != 1 {
			t.Fatalf("recovery %d count=%d err=%v", attempt, count, err)
		}
		_ = claimed
		if attempt == 1 {
			if _, err := store.pool.Exec(ctx, `UPDATE jobs SET available_at=NOW() WHERE id=$1`, created.ID); err != nil {
				t.Fatal(err)
			}
		}
	}
	letter, err := store.GetDeadLetter(ctx, created.ID)
	if err != nil || letter.FinalAttempt != 2 || letter.ErrorCode != "LEASE_EXPIRED" {
		t.Fatalf("letter=%+v err=%v", letter, err)
	}
	var abandoned int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM job_attempts WHERE job_id=$1 AND outcome='abandoned'`, created.ID).Scan(&abandoned); err != nil || abandoned != 2 {
		t.Fatalf("abandoned=%d err=%v", abandoned, err)
	}
}

func TestDeadLetterPaginationIsStableAndBoundedIntegration(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		created, _, err := store.CreateJob(ctx, submission(t, 0, nil, `{"message":"page"}`))
		if err != nil {
			t.Fatal(err)
		}
		claimed, err := store.ClaimNextJob(ctx, "worker", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.FailJob(ctx, claimed.Execution, job.Failure{Code: "PERMANENT", Retryable: false}); err != nil {
			t.Fatal(err)
		}
		_ = created
	}
	first, more, err := store.ListDeadLetters(ctx, nil, "", 2)
	if err != nil || len(first) != 2 || !more {
		t.Fatalf("first page=%d more=%v err=%v", len(first), more, err)
	}
	last := first[len(first)-1]
	second, more, err := store.ListDeadLetters(ctx, &last.DeadLetteredAt, last.JobID, 2)
	if err != nil || len(second) != 1 || more {
		t.Fatalf("second page=%d more=%v err=%v", len(second), more, err)
	}
	if second[0].JobID == first[0].JobID || second[0].JobID == first[1].JobID {
		t.Fatal("pagination duplicated a dead letter")
	}
}

func TestSuccessfulRetryHasOneTerminalSuccessIntegration(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	created, _, err := store.CreateJob(ctx, submission(t, 0, nil, `{"message":"eventual"}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `UPDATE jobs SET max_attempts=2,retry_initial_backoff_ms=1,retry_max_backoff_ms=1,retry_jitter_percent=0 WHERE id=$1`, created.ID); err != nil {
		t.Fatal(err)
	}
	first, err := store.ClaimNextJob(ctx, "worker", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.FailJob(ctx, first.Execution, job.Failure{Code: "TEMPORARY", Retryable: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `UPDATE jobs SET available_at=NOW() WHERE id=$1`, created.ID); err != nil {
		t.Fatal(err)
	}
	second, err := store.ClaimNextJob(ctx, "worker", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if second.Execution.ExecutionKey != first.Execution.ExecutionKey {
		t.Fatal("execution key changed")
	}
	if err := store.CompleteJob(ctx, second.Execution, json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	final, err := store.GetJob(ctx, created.ID)
	if err != nil || final.Status != job.StatusSucceeded || final.AttemptCount != 2 || final.DeadLetteredAt != nil {
		t.Fatalf("final=%+v err=%v", final, err)
	}
	var successes int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM job_attempts WHERE job_id=$1 AND outcome='succeeded'`, created.ID).Scan(&successes); err != nil || successes != 1 {
		t.Fatalf("successes=%d err=%v", successes, err)
	}
}

func TestConcurrentFailureCreatesOneDeadLetterIntegration(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	created, _, err := store.CreateJob(ctx, submission(t, 0, nil, `{"message":"race"}`))
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimNextJob(ctx, "worker", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 2)
	var wait sync.WaitGroup
	for i := 0; i < 2; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			errs <- store.FailJob(context.Background(), claimed.Execution, job.Failure{Code: "PERMANENT", Retryable: false})
		}()
	}
	wait.Wait()
	close(errs)
	successes, lost := 0, 0
	for err := range errs {
		if err == nil {
			successes++
		} else if errors.Is(err, ErrOwnershipLost) {
			lost++
		} else {
			t.Fatal(err)
		}
	}
	if successes != 1 || lost != 1 {
		t.Fatalf("successes=%d lost=%d", successes, lost)
	}
	var count int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM dead_letters WHERE job_id=$1`, created.ID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("dead letters=%d err=%v", count, err)
	}
}
