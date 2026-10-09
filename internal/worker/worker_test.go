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
	claimsAfterCancel int
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
func (s *workerStore) RenewLease(context.Context, job.Execution, time.Duration) error { return nil }
func (s *workerStore) CompleteJob(_ context.Context, _ job.Execution, _ json.RawMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completed++
	return nil
}
func (s *workerStore) FailJob(_ context.Context, _ job.Execution, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failed++
	return nil
}
func (s *workerStore) RecoverExpiredJobs(context.Context, int) (int, error) { return 0, nil }

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
	registry := newRegistry(map[string]Handler{"test": HandlerFunc(func(context.Context, json.RawMessage) (json.RawMessage, error) {
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
	registry := newRegistry(map[string]Handler{"test": HandlerFunc(func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
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
	if store.completed != 0 || store.failed != 0 || store.claimsAfterCancel != 0 {
		t.Fatalf("completed=%d failed=%d claims-after-cancel=%d", store.completed, store.failed, store.claimsAfterCancel)
	}
}
