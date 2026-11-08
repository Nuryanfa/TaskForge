package relay

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/outbox"
	"github.com/Nuryanfa/TaskForge/internal/store/postgres"
)

type fakeStore struct {
	mu        sync.Mutex
	pending   []outbox.Event
	published []outbox.Event
	batches   int
	busy      bool
	purges    int
}

func (s *fakeStore) PublishOutboxBatch(ctx context.Context, limit int, publish func(context.Context, outbox.Event) error) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches++
	if s.busy {
		return 0, postgres.ErrOutboxBusy
	}
	count := 0
	for count < limit && count < len(s.pending) {
		if err := publish(ctx, s.pending[count]); err != nil {
			s.published = append(s.published, s.pending[:count]...)
			s.pending = s.pending[count:]
			return count, err
		}
		count++
	}
	s.published = append(s.published, s.pending[:count]...)
	s.pending = s.pending[count:]
	return count, nil
}

func (s *fakeStore) PurgePublishedOutbox(context.Context, time.Duration, int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purges++
	return 0, nil
}

func (s *fakeStore) OutboxBacklog(context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.pending)), nil
}

type fakePublisher struct {
	mu          sync.Mutex
	ensureCalls int
	ensureErr   error
	failNext    int
	sent        []int64
}

func (p *fakePublisher) EnsureStream(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ensureCalls++
	return p.ensureErr
}

func (p *fakePublisher) Publish(_ context.Context, event outbox.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failNext > 0 {
		p.failNext--
		return errors.New("no ack")
	}
	p.sent = append(p.sent, event.ID)
	return nil
}

func events(n int) []outbox.Event {
	out := make([]outbox.Event, n)
	for i := range out {
		out[i] = outbox.Event{ID: int64(i + 1), Type: "job.queued"}
	}
	return out
}

func testOptions() Options {
	return Options{PollInterval: time.Hour, BatchSize: 2, BatchTimeout: time.Second,
		Retention: time.Hour, PurgeInterval: time.Hour, PurgeBatchSize: 10, StreamRetryInterval: 10 * time.Millisecond}
}

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func runFor(t *testing.T, runner *Runner, d time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	if err := runner.Run(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerDrainsBacklogWithoutWaitingForPollInterval(t *testing.T) {
	store := &fakeStore{pending: events(5)}
	publisher := &fakePublisher{}
	runFor(t, New(store, publisher, discard(), testOptions()), 100*time.Millisecond)
	// Full batches (2, 2) re-run immediately; the short batch (1) waits for the
	// one-hour poll interval, so exactly three batches run.
	if len(publisher.sent) != 5 || store.batches != 3 {
		t.Fatalf("sent=%v batches=%d", publisher.sent, store.batches)
	}
	for i, id := range publisher.sent {
		if id != int64(i+1) {
			t.Fatalf("out of order: %v", publisher.sent)
		}
	}
	if store.purges != 1 {
		t.Fatalf("purges=%d want one on startup", store.purges)
	}
}

func TestRunnerRetriesStreamSetupBeforePublishing(t *testing.T) {
	store := &fakeStore{pending: events(1)}
	publisher := &fakePublisher{ensureErr: errors.New("nats down")}
	runner := New(store, publisher, discard(), testOptions())
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = runner.Run(ctx)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	publisher.mu.Lock()
	calls := publisher.ensureCalls
	publisher.ensureErr = nil
	publisher.mu.Unlock()
	store.mu.Lock()
	batches := store.batches
	store.mu.Unlock()
	if calls < 2 || batches != 0 {
		t.Fatalf("ensure calls=%d batches=%d before stream ready", calls, batches)
	}
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done
	if len(publisher.sent) != 1 {
		t.Fatalf("sent=%v after stream became ready", publisher.sent)
	}
}

func TestRunnerReensuresStreamAfterPublishFailure(t *testing.T) {
	store := &fakeStore{pending: events(3)}
	publisher := &fakePublisher{failNext: 1}
	options := testOptions()
	options.PollInterval = 10 * time.Millisecond
	runFor(t, New(store, publisher, discard(), options), 150*time.Millisecond)
	if len(publisher.sent) != 3 || publisher.ensureCalls != 2 {
		t.Fatalf("sent=%v ensure calls=%d", publisher.sent, publisher.ensureCalls)
	}
	for i, id := range publisher.sent {
		if id != int64(i+1) {
			t.Fatalf("out of order after failure: %v", publisher.sent)
		}
	}
}

func TestRunnerStandsByWhileAnotherRelayIsActive(t *testing.T) {
	store := &fakeStore{pending: events(1), busy: true}
	publisher := &fakePublisher{}
	options := testOptions()
	options.PollInterval = 10 * time.Millisecond
	runFor(t, New(store, publisher, discard(), options), 60*time.Millisecond)
	if len(publisher.sent) != 0 || store.batches < 2 {
		t.Fatalf("sent=%v batches=%d", publisher.sent, store.batches)
	}
}

func TestRunnerFinishesInFlightBatchOnShutdown(t *testing.T) {
	store := &blockingStore{started: make(chan struct{})}
	runner := New(store, &fakePublisher{}, discard(), testOptions())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	<-store.started
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown not bounded by batch timeout")
	}
	if !store.sawLiveContext {
		t.Fatal("in-flight batch context was canceled by shutdown signal")
	}
}

type blockingStore struct {
	fakeStore
	started        chan struct{}
	sawLiveContext bool
}

func (s *blockingStore) PublishOutboxBatch(ctx context.Context, _ int, _ func(context.Context, outbox.Event) error) (int, error) {
	close(s.started)
	time.Sleep(50 * time.Millisecond)
	s.sawLiveContext = ctx.Err() == nil
	return 0, nil
}
