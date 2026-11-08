package postgres

import (
	"context"
	"sync"
)

// Observer receives committed job outcomes and workflow settlements, for
// metrics. Events are delivered only after the transaction that produced them
// commits, so rolled-back work is never counted. A crash between commit and
// delivery loses that one event; the database state is still correct.
type Observer interface {
	JobOutcome(kind, outcome, errorCode string)
	WorkflowSettled(status string)
}

// SetObserver installs o; nil removes any observer.
func (s *Store) SetObserver(o Observer) {
	s.observerMu.Lock()
	defer s.observerMu.Unlock()
	s.observer = o
}

func (s *Store) currentObserver() Observer {
	s.observerMu.RLock()
	defer s.observerMu.RUnlock()
	return s.observer
}

type jobEvent struct{ kind, outcome, code string }

// eventBuffer collects events of one transaction (or one savepoint).
type eventBuffer struct {
	mu        sync.Mutex
	jobs      []jobEvent
	workflows []string
}

type eventBufferKey struct{}

// withEvents returns a context that records events into a new buffer.
func withEvents(ctx context.Context) (context.Context, *eventBuffer) {
	buffer := &eventBuffer{}
	return context.WithValue(ctx, eventBufferKey{}, buffer), buffer
}

func eventsFrom(ctx context.Context) *eventBuffer {
	buffer, _ := ctx.Value(eventBufferKey{}).(*eventBuffer)
	return buffer
}

func recordJob(ctx context.Context, kind, outcome, code string) {
	if buffer := eventsFrom(ctx); buffer != nil {
		buffer.mu.Lock()
		buffer.jobs = append(buffer.jobs, jobEvent{kind, outcome, code})
		buffer.mu.Unlock()
	}
}

func recordWorkflow(ctx context.Context, status string) {
	if buffer := eventsFrom(ctx); buffer != nil {
		buffer.mu.Lock()
		buffer.workflows = append(buffer.workflows, status)
		buffer.mu.Unlock()
	}
}

// merge appends a released savepoint's events to its parent transaction.
func (b *eventBuffer) merge(child *eventBuffer) {
	child.mu.Lock()
	jobs, workflows := child.jobs, child.workflows
	child.mu.Unlock()
	b.mu.Lock()
	b.jobs = append(b.jobs, jobs...)
	b.workflows = append(b.workflows, workflows...)
	b.mu.Unlock()
}

// flush delivers a committed transaction's events to the observer.
func (s *Store) flush(buffer *eventBuffer) {
	observer := s.currentObserver()
	if observer == nil || buffer == nil {
		return
	}
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	for _, event := range buffer.jobs {
		observer.JobOutcome(event.kind, event.outcome, event.code)
	}
	for _, status := range buffer.workflows {
		observer.WorkflowSettled(status)
	}
}
