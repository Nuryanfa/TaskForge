package worker

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/job"
	"github.com/Nuryanfa/TaskForge/internal/store/postgres"
)

type workerStore struct {
	mu                sync.Mutex
	jobs              []job.Job
	completed         int
	failed            int
	failures          []job.Failure
	completeErr       error
	claimsAfterCancel int
	recovered         int
	recoverErr        error
	completeCtx       context.Context
	renewErr          error
}

func (s *workerStore) ClaimNextJob(ctx context.Context, workerID string, _ time.Duration) (job.Claimed, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx.Err() != nil {
		s.claimsAfterCancel++
		return job.Claimed{}, ctx.Err()
	}
	if len(s.jobs) == 0 {
		return job.Claimed{}, postgres.ErrNotFound
	}
	claimedJob := s.jobs[0]
	s.jobs = s.jobs[1:]
	execution := job.Execution{JobID: claimedJob.ID, Attempt: claimedJob.AttemptCount,
		WorkerID: workerID, FencingToken: claimedJob.FencingToken}
	return job.Claimed{Job: claimedJob, Execution: execution}, nil
}
func (s *workerStore) RenewLease(context.Context, job.Execution, time.Duration) error { return s.renewErr }
func (s *workerStore) CompleteJob(ctx context.Context, _ job.Execution, _ json.RawMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completeCtx = ctx
	if s.completeErr != nil {
		return s.completeErr
	}
	s.completed++
	return nil
}
func (s *workerStore) FailJob(_ context.Context, _ job.Execution, failure job.Failure) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failed++
	s.failures = append(s.failures, failure)
	return nil
}
func (s *workerStore) RecoverExpiredJobs(context.Context, int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recovered, s.recoverErr
}
func (s *workerStore) DispatchDueSchedules(context.Context, int) (int, error) { return 0, nil }

func testOptions() Options {
	return Options{WorkerID: "worker", Concurrency: 2, PollInterval: time.Millisecond,
		ExecutionTimeout: time.Second, ShutdownTimeout: 50 * time.Millisecond,
		LeaseDuration: time.Second, HeartbeatInterval: 10 * time.Millisecond,
		RecoveryInterval: 10 * time.Millisecond, RecoveryBatchSize: 10}
}

func TestRunnerNeverExceedsConfiguredConcurrency(t *testing.T) {
	jobs := make([]job.Job, 8)
	for i := range jobs {
		jobs[i] = job.Job{ID: string(rune('a' + i)), Kind: "test", AttemptCount: 1, FencingToken: 1}
	}
	store := &workerStore{jobs: jobs}
	var mu sync.Mutex
	active, maximum := 0, 0
	registry := newRegistry(map[string]Handler{"test": HandlerFunc(func(context.Context, job.Execution, json.RawMessage) (json.RawMessage, *job.Failure) {
		mu.Lock()
		active++
		if active > maximum {
			maximum = active
		}
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		mu.Lock()
		active--
		mu.Unlock()
		return json.RawMessage(`{}`), nil
	})})
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	runner := New(store, registry, slog.New(slog.NewTextHandler(io.Discard, nil)), testOptions())
	if err := runner.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if maximum != 2 || store.completed != len(jobs) {
		t.Fatalf("maximum active=%d completed=%d", maximum, store.completed)
	}
}

func TestRunnerGracefulShutdownIsBoundedAndStopsClaims(t *testing.T) {
	store := &workerStore{jobs: []job.Job{{ID: "one", Kind: "test", AttemptCount: 1, FencingToken: 1}}}
	started := make(chan struct{})
	registry := newRegistry(map[string]Handler{"test": HandlerFunc(func(ctx context.Context, _ job.Execution, _ json.RawMessage) (json.RawMessage, *job.Failure) {
		close(started)
		<-ctx.Done()
		return nil, &job.Failure{Code: ErrorShutdownTimeout, Retryable: true}
	})})
	options := testOptions()
	options.Concurrency = 1
	options.ShutdownTimeout = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	runner := New(store, registry, slog.New(slog.NewTextHandler(io.Discard, nil)), options)
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	<-started
	startedAt := time.Now()
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(startedAt); elapsed > 200*time.Millisecond {
		t.Fatalf("shutdown took %s", elapsed)
	}
	if store.completed != 0 || store.failed != 1 || store.claimsAfterCancel != 0 {
		t.Fatalf("completed=%d failed=%d claims-after-cancel=%d", store.completed, store.failed, store.claimsAfterCancel)
	}
	if got := store.failures[0]; got.Code != ErrorShutdownTimeout || !got.Retryable {
		t.Fatalf("shutdown failure=%+v", got)
	}
}

func TestRunnerFailsJobWhenStoreRejectsResult(t *testing.T) {
	store := &workerStore{jobs: []job.Job{{ID: "one", Kind: "test", AttemptCount: 1, FencingToken: 1}},
		completeErr: postgres.ErrInvalidResult}
	registry := newRegistry(map[string]Handler{"test": HandlerFunc(func(context.Context, job.Execution, json.RawMessage) (json.RawMessage, *job.Failure) {
		return json.RawMessage(`not json`), nil
	})})
	options := testOptions()
	options.Concurrency = 1
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	runner := New(store, registry, slog.New(slog.NewTextHandler(io.Discard, nil)), options)
	if err := runner.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if store.completed != 0 || store.failed != 1 {
		t.Fatalf("completed=%d failed=%d", store.completed, store.failed)
	}
	if got := store.failures[0]; got.Code != ErrorInvalidResult || got.Retryable {
		t.Fatalf("invalid result failure=%+v", got)
	}
}

func TestRunnerClaimsPromptlyOnWakeSignal(t *testing.T) {
	store := &workerStore{}
	registry := newRegistry(map[string]Handler{"test": HandlerFunc(func(context.Context, job.Execution, json.RawMessage) (json.RawMessage, *job.Failure) {
		return json.RawMessage(`{}`), nil
	})})
	wake := make(chan struct{}, 1)
	options := testOptions()
	options.Concurrency = 1
	options.PollInterval = time.Hour
	options.Wake = wake
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := New(store, registry, slog.New(slog.NewTextHandler(io.Discard, nil)), options)
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	time.Sleep(20 * time.Millisecond) // the first claim finds nothing and sleeps for the poll interval
	store.mu.Lock()
	store.jobs = append(store.jobs, job.Job{ID: "woken", Kind: "test", AttemptCount: 1, FencingToken: 1})
	store.mu.Unlock()
	wake <- struct{}{}

	deadline := time.Now().Add(time.Second)
	for {
		store.mu.Lock()
		completed := store.completed
		store.mu.Unlock()
		if completed == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("wake signal did not trigger a claim")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
