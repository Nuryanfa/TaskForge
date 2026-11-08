// Package relay moves committed outbox events from PostgreSQL to NATS
// JetStream with at-least-once semantics.
package relay

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/outbox"
	"github.com/Nuryanfa/TaskForge/internal/store/postgres"
)

type Store interface {
	PublishOutboxBatch(context.Context, int, func(context.Context, outbox.Event) error) (int, error)
	PurgePublishedOutbox(context.Context, time.Duration, int) (int, error)
	OutboxBacklog(context.Context) (int64, error)
}

type Publisher interface {
	EnsureStream(context.Context) error
	Publish(context.Context, outbox.Event) error
}

type Options struct {
	PollInterval        time.Duration
	BatchSize           int
	BatchTimeout        time.Duration
	Retention           time.Duration
	PurgeInterval       time.Duration
	PurgeBatchSize      int
	StreamRetryInterval time.Duration
}

type Runner struct {
	store     Store
	publisher Publisher
	logger    *slog.Logger
	options   Options
}

func New(store Store, publisher Publisher, logger *slog.Logger, options Options) *Runner {
	if options.PurgeInterval <= 0 {
		options.PurgeInterval = time.Minute
	}
	if options.PurgeBatchSize <= 0 {
		options.PurgeBatchSize = 1000
	}
	if options.StreamRetryInterval <= 0 {
		options.StreamRetryInterval = time.Second
	}
	return &Runner{store: store, publisher: publisher, logger: logger, options: options}
}

// Run publishes until ctx is canceled. A batch in flight when shutdown starts
// is allowed to finish within BatchTimeout so acknowledged events are marked.
func (r *Runner) Run(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	streamReady := false
	var lastPurge time.Time
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		}
		if !streamReady {
			if err := r.publisher.EnsureStream(ctx); err != nil {
				if ctx.Err() == nil {
					r.logger.Error("relay_stream_failed")
				}
				timer.Reset(r.options.StreamRetryInterval)
				continue
			}
			streamReady = true
			r.logger.Info("relay_stream_ready")
		}
		if time.Since(lastPurge) >= r.options.PurgeInterval {
			r.purge(ctx)
			lastPurge = time.Now()
		}
		published, err := r.publishBatch(ctx)
		switch {
		case errors.Is(err, postgres.ErrOutboxBusy):
		case err != nil:
			if published > 0 {
				r.logger.Info("relay_batch_published", "count", published)
			}
			r.logger.Error("relay_batch_failed")
			// The stream may have been deleted or reconfigured; verify it
			// before the next attempt.
			streamReady = false
		case published > 0:
			r.logger.Info("relay_batch_published", "count", published)
		}
		if err == nil && published == r.options.BatchSize {
			timer.Reset(0)
		} else {
			timer.Reset(r.options.PollInterval)
		}
	}
}

func (r *Runner) publishBatch(ctx context.Context) (int, error) {
	batchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.options.BatchTimeout)
	defer cancel()
	return r.store.PublishOutboxBatch(batchCtx, r.options.BatchSize, r.publisher.Publish)
}

func (r *Runner) purge(ctx context.Context) {
	purged, err := r.store.PurgePublishedOutbox(ctx, r.options.Retention, r.options.PurgeBatchSize)
	if err != nil {
		if ctx.Err() == nil {
			r.logger.Error("relay_outbox_purge_failed")
		}
		return
	}
	if purged > 0 {
		r.logger.Info("relay_outbox_purged", "count", purged)
	}
	if backlog, err := r.store.OutboxBacklog(ctx); err == nil {
		r.logger.Info("relay_outbox_backlog", "count", backlog)
	}
}
