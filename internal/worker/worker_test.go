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
	mu        sync.Mutex
	jobs      []job.Job
	completed int
	failed    int
}

func (s *workerStore) ClaimNextJob(context.Context, string) (job.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.jobs) == 0 {
		return job.Job{}, postgres.ErrNotFound
	}
	claimed := s.jobs[0]
	s.jobs = s.jobs[1:]
	return claimed, nil
}
func (s *workerStore) CompleteJob(context.Context, string, int, json.RawMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completed++
	return nil
}
func (s *workerStore) FailJob(context.Context, string, int, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failed++
	return nil
}

func TestRunnerProcessesSequentially(t *testing.T) {
	store := &workerStore{jobs: []job.Job{
		{ID: "one", Kind: "test", AttemptCount: 1},
		{ID: "two", Kind: "test", AttemptCount: 1},
	}}
	var mu sync.Mutex
	active, maximum := 0, 0
	registry := newRegistry(map[string]Handler{"test": HandlerFunc(func(context.Context, json.RawMessage) (json.RawMessage, error) {
		mu.Lock()
		active++
		if active > maximum {
			maximum = active
		}
		mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		mu.Lock()
		active--
		mu.Unlock()
		return json.RawMessage(`{}`), nil
	})})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	runner := New(store, registry, slog.New(slog.NewTextHandler(io.Discard, nil)), "worker", time.Millisecond, time.Second, time.Second)
	if err := runner.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if maximum != 1 || store.completed != 2 {
		t.Fatalf("maximum active=%d completed=%d", maximum, store.completed)
	}
}

func TestRunnerShutdownTimeoutPersistsFailure(t *testing.T) {
	store := &workerStore{jobs: []job.Job{{ID: "one", Kind: "test", AttemptCount: 1}}}
	started := make(chan struct{})
	registry := newRegistry(map[string]Handler{"test": HandlerFunc(func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	})})
	ctx, cancel := context.WithCancel(context.Background())
	runner := New(store, registry, slog.New(slog.NewTextHandler(io.Discard, nil)), "worker", time.Millisecond, time.Second, 10*time.Millisecond)
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	<-started
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if store.failed != 1 {
		t.Fatalf("failed outcomes=%d, want 1", store.failed)
	}
}
