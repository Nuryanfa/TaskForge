package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/job"
	"github.com/Nuryanfa/TaskForge/internal/store/postgres"
)

type Store interface {
	ClaimNextJob(context.Context, string) (job.Job, error)
	CompleteJob(context.Context, string, int, json.RawMessage) error
	FailJob(context.Context, string, int, string) error
}

type Runner struct {
	store            Store
	registry         *Registry
	logger           *slog.Logger
	workerID         string
	pollInterval     time.Duration
	executionTimeout time.Duration
	shutdownTimeout  time.Duration
}

func New(store Store, registry *Registry, logger *slog.Logger, workerID string, pollInterval, executionTimeout, shutdownTimeout time.Duration) *Runner {
	return &Runner{
		store: store, registry: registry, logger: logger, workerID: workerID,
		pollInterval: pollInterval, executionTimeout: executionTimeout, shutdownTimeout: shutdownTimeout,
	}
}

func (r *Runner) Run(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		}

		claimed, err := r.store.ClaimNextJob(ctx, r.workerID)
		if err != nil {
			if !errors.Is(err, postgres.ErrNotFound) {
				r.logger.Error("worker_claim_failed")
			}
			resetTimer(timer, r.pollInterval)
			continue
		}
		if err := r.execute(ctx, claimed); err != nil {
			return err
		}
		resetTimer(timer, 0)
	}
}

type executionResult struct {
	result    json.RawMessage
	errorCode string
}

func (r *Runner) execute(runCtx context.Context, claimed job.Job) error {
	executionCtx, cancelExecution := context.WithTimeout(context.Background(), r.executionTimeout)
	defer cancelExecution()
	done := make(chan executionResult, 1)
	go func() {
		result, code := r.registry.Execute(executionCtx, claimed.Kind, claimed.Payload)
		done <- executionResult{result: result, errorCode: code}
	}()

	select {
	case outcome := <-done:
		if errors.Is(executionCtx.Err(), context.DeadlineExceeded) {
			outcome = executionResult{errorCode: ErrorExecutionTimeout}
		}
		return r.persistOutcome(claimed, outcome)
	case <-executionCtx.Done():
		return r.persistOutcome(claimed, executionResult{errorCode: ErrorExecutionTimeout})
	case <-runCtx.Done():
		shutdownTimer := time.NewTimer(r.shutdownTimeout)
		defer shutdownTimer.Stop()
		select {
		case outcome := <-done:
			if errors.Is(executionCtx.Err(), context.DeadlineExceeded) {
				outcome = executionResult{errorCode: ErrorExecutionTimeout}
			}
			return r.persistOutcome(claimed, outcome)
		case <-executionCtx.Done():
			return r.persistOutcome(claimed, executionResult{errorCode: ErrorExecutionTimeout})
		case <-shutdownTimer.C:
			cancelExecution()
			return r.persistOutcome(claimed, executionResult{errorCode: ErrorShutdownTimeout})
		}
	}
}

func (r *Runner) persistOutcome(claimed job.Job, outcome executionResult) error {
	persistCtx, cancel := context.WithTimeout(context.Background(), r.shutdownTimeout)
	defer cancel()
	var err error
	if outcome.errorCode == "" {
		err = r.store.CompleteJob(persistCtx, claimed.ID, claimed.AttemptCount, outcome.result)
	} else {
		err = r.store.FailJob(persistCtx, claimed.ID, claimed.AttemptCount, outcome.errorCode)
	}
	if err != nil {
		return fmt.Errorf("persist job outcome: %w", err)
	}
	return nil
}

func resetTimer(timer *time.Timer, duration time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(duration)
}
