package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/job"
	"github.com/Nuryanfa/TaskForge/internal/store/postgres"
)

type Store interface {
	ClaimNextJob(context.Context, string, time.Duration) (job.Claimed, error)
	RenewLease(context.Context, job.Execution, time.Duration) error
	CompleteJob(context.Context, job.Execution, json.RawMessage) error
	FailJob(context.Context, job.Execution, string) error
	RecoverExpiredJobs(context.Context, int) (int, error)
}

type Options struct {
	WorkerID          string
	Concurrency       int
	PollInterval      time.Duration
	ExecutionTimeout  time.Duration
	ShutdownTimeout   time.Duration
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	RecoveryInterval  time.Duration
	RecoveryBatchSize int
}

type Runner struct {
	store    Store
	registry *Registry
	logger   *slog.Logger
	options  Options
}

func New(store Store, registry *Registry, logger *slog.Logger, options Options) *Runner {
	return &Runner{store: store, registry: registry, logger: logger, options: options}
}

// Run starts a fixed-size executor pool and one bounded recovery loop. No job
// can create more than one handler and one heartbeat goroutine-equivalent loop.
func (r *Runner) Run(ctx context.Context) error {
	var wait sync.WaitGroup
	wait.Add(r.options.Concurrency + 1)
	for slot := 0; slot < r.options.Concurrency; slot++ {
		go func() {
			defer wait.Done()
			r.runExecutor(ctx)
		}()
	}
	go func() {
		defer wait.Done()
		r.runRecovery(ctx)
	}()
	wait.Wait()
	return nil
}

func (r *Runner) runExecutor(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		if ctx.Err() != nil {
			return
		}
		claimed, err := r.store.ClaimNextJob(ctx, r.options.WorkerID, r.options.LeaseDuration)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if !errors.Is(err, postgres.ErrNotFound) {
				r.logger.Error("worker_claim_failed")
			}
			resetTimer(timer, r.options.PollInterval)
			continue
		}
		r.execute(ctx, claimed)
		resetTimer(timer, 0)
	}
}

func (r *Runner) runRecovery(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		recovered, err := r.store.RecoverExpiredJobs(ctx, r.options.RecoveryBatchSize)
		if err != nil && ctx.Err() == nil {
			r.logger.Error("worker_recovery_failed")
		} else if recovered > 0 {
			r.logger.Info("worker_jobs_recovered", "count", recovered)
		}
		resetTimer(timer, r.options.RecoveryInterval)
	}
}

type executionResult struct {
	result    json.RawMessage
	errorCode string
}

func (r *Runner) execute(runCtx context.Context, claimed job.Claimed) {
	executionCtx, cancelExecution := context.WithTimeout(context.Background(), r.options.ExecutionTimeout)
	defer cancelExecution()
	done := make(chan executionResult, 1)
	go func() {
		result, code := r.registry.Execute(executionCtx, claimed.Job.Kind, claimed.Job.Payload)
		done <- executionResult{result: result, errorCode: code}
	}()
	heartbeat := time.NewTicker(r.options.HeartbeatInterval)
	defer heartbeat.Stop()

	var shutdown <-chan time.Time
	var shutdownTimer *time.Timer
	defer func() {
		if shutdownTimer != nil {
			shutdownTimer.Stop()
		}
	}()
	for {
		select {
		case outcome := <-done:
			if errors.Is(executionCtx.Err(), context.DeadlineExceeded) {
				outcome = executionResult{errorCode: ErrorExecutionTimeout}
			}
			r.persistOutcome(claimed.Execution, outcome)
			return
		case <-executionCtx.Done():
			if errors.Is(executionCtx.Err(), context.DeadlineExceeded) {
				r.persistOutcome(claimed.Execution, executionResult{errorCode: ErrorExecutionTimeout})
			}
			return
		case <-heartbeat.C:
			hbCtx, cancel := context.WithTimeout(context.Background(), r.options.HeartbeatInterval)
			err := r.store.RenewLease(hbCtx, claimed.Execution, r.options.LeaseDuration)
			cancel()
			if err != nil {
				cancelExecution()
				if !errors.Is(err, postgres.ErrOwnershipLost) {
					r.logger.Error("worker_heartbeat_failed")
				}
				return
			}
		case <-runCtx.Done():
			if shutdown == nil {
				shutdownTimer = time.NewTimer(r.options.ShutdownTimeout)
				shutdown = shutdownTimer.C
				runCtx = context.Background()
			}
		case <-shutdown:
			cancelExecution()
			return
		}
	}
}

func (r *Runner) persistOutcome(execution job.Execution, outcome executionResult) {
	persistCtx, cancel := context.WithTimeout(context.Background(), r.options.ShutdownTimeout)
	defer cancel()
	var err error
	if outcome.errorCode == "" {
		err = r.store.CompleteJob(persistCtx, execution, outcome.result)
	} else {
		err = r.store.FailJob(persistCtx, execution, outcome.errorCode)
	}
	if err != nil && !errors.Is(err, postgres.ErrOwnershipLost) {
		r.logger.Error("worker_persist_outcome_failed")
	}
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
