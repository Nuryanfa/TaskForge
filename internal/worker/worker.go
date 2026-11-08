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
	"github.com/Nuryanfa/TaskForge/internal/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type Store interface {
	ClaimNextJob(context.Context, string, time.Duration) (job.Claimed, error)
	RenewLease(context.Context, job.Execution, time.Duration) error
	CompleteJob(context.Context, job.Execution, json.RawMessage) error
	FailJob(context.Context, job.Execution, job.Failure) error
	RecoverExpiredJobs(context.Context, int) (int, error)
	DispatchDueSchedules(context.Context, int) (int, error)
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
	ScheduleInterval  time.Duration
	ScheduleBatchSize int
	// Wake, when non-nil, delivers hints that a job may be claimable now. It
	// only shortens the poll wait; PostgreSQL still decides every claim.
	Wake <-chan struct{}
	// Metrics, when non-nil, records start delay and attempt durations.
	Metrics *telemetry.Metrics
}

type Runner struct {
	store    Store
	registry *Registry
	logger   *slog.Logger
	options  Options
}

func New(store Store, registry *Registry, logger *slog.Logger, options Options) *Runner {
	if options.ScheduleInterval <= 0 {
		if options.RecoveryInterval > 0 {
			options.ScheduleInterval = options.RecoveryInterval
		} else {
			options.ScheduleInterval = time.Second
		}
	}
	if options.ScheduleBatchSize <= 0 {
		if options.RecoveryBatchSize > 0 {
			options.ScheduleBatchSize = options.RecoveryBatchSize
		} else {
			options.ScheduleBatchSize = 50
		}
	}
	return &Runner{store: store, registry: registry, logger: logger, options: options}
}

// Run starts a fixed-size executor pool, one bounded recovery loop, and one
// bounded schedule dispatch loop. No job can create more than one handler and
// one heartbeat goroutine-equivalent loop.
func (r *Runner) Run(ctx context.Context) error {
	var wait sync.WaitGroup
	wait.Add(r.options.Concurrency + 2)
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
	go func() {
		defer wait.Done()
		r.runScheduler(ctx)
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
		case <-r.options.Wake:
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
		if recovered > 0 {
			r.logger.Info("worker_jobs_recovered", "count", recovered)
		}
		if r.options.Metrics != nil {
			r.options.Metrics.LeaseRecovery("recovered", recovered)
			var partial *postgres.RecoveryError
			if errors.As(err, &partial) {
				r.options.Metrics.LeaseRecovery("failed", len(partial.JobIDs))
			}
		}
		if err != nil && ctx.Err() == nil {
			// Job IDs identify what an operator must inspect; the raw error
			// may contain database detail and is never logged.
			var partial *postgres.RecoveryError
			if errors.As(err, &partial) {
				r.logger.Error("worker_recovery_partial", "failed_count", len(partial.JobIDs), "failed_job_ids", partial.JobIDs)
			} else {
				r.logger.Error("worker_recovery_failed")
			}
		}
		resetTimer(timer, r.options.RecoveryInterval)
	}
}

func (r *Runner) runScheduler(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		dispatched, err := r.store.DispatchDueSchedules(ctx, r.options.ScheduleBatchSize)
		if err != nil && ctx.Err() == nil {
			r.logger.Error("worker_schedule_dispatch_failed")
		} else if dispatched > 0 {
			r.logger.Info("worker_schedules_dispatched", "count", dispatched)
		}
		resetTimer(timer, r.options.ScheduleInterval)
	}
}

type executionResult struct {
	result  json.RawMessage
	failure *job.Failure
}

func (r *Runner) execute(runCtx context.Context, claimed job.Claimed) {
	// The attempt span is attached to the handler and persistence contexts
	// explicitly: both derive from context.Background() so that shutdown does
	// not abort them, and the store stamps this span on any job it dispatches.
	spanCtx, span := telemetry.StartAttempt(context.Background(), claimed)
	defer span.End()
	started := time.Now()
	if r.options.Metrics != nil && claimed.Job.StartedAt != nil {
		// Both timestamps come from the claim statement's database clock.
		r.options.Metrics.StartDelay(claimed.Job.Queue, claimed.Job.Kind, claimed.Job.StartedAt.Sub(claimed.Job.AvailableAt))
	}
	attempt := attemptScope{span: span, ctx: spanCtx, kind: claimed.Job.Kind, started: started}
	executionCtx, cancelExecution := context.WithTimeout(spanCtx, r.options.ExecutionTimeout)
	defer cancelExecution()
	done := make(chan executionResult, 1)
	go func() {
		result, failure := r.registry.Execute(executionCtx, claimed.Execution, claimed.Job.Kind, claimed.Job.Payload)
		done <- executionResult{result: result, failure: failure}
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
				outcome = executionResult{failure: &job.Failure{Code: ErrorExecutionTimeout, Retryable: true}}
			}
			r.persistOutcome(attempt, claimed.Execution, outcome)
			return
		case <-executionCtx.Done():
			if errors.Is(executionCtx.Err(), context.DeadlineExceeded) {
				r.persistOutcome(attempt, claimed.Execution, executionResult{failure: &job.Failure{Code: ErrorExecutionTimeout, Retryable: true}})
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
			r.persistOutcome(attempt, claimed.Execution, executionResult{failure: &job.Failure{Code: ErrorShutdownTimeout, Retryable: true}})
			return
		}
	}
}

// attemptScope carries one attempt's span and timing to persistOutcome.
type attemptScope struct {
	span    trace.Span
	ctx     context.Context
	kind    string
	started time.Time
}

func (r *Runner) persistOutcome(attempt attemptScope, execution job.Execution, outcome executionResult) {
	persistCtx, cancel := context.WithTimeout(attempt.ctx, r.options.ShutdownTimeout)
	defer cancel()
	defer r.recordAttempt(attempt, outcome)
	var err error
	if outcome.failure == nil {
		err = r.store.CompleteJob(persistCtx, execution, outcome.result)
		if errors.Is(err, postgres.ErrInvalidResult) {
			// Retrying would produce the same rejected result, so fail permanently
			// now instead of waiting for lease expiry.
			err = r.store.FailJob(persistCtx, execution, job.Failure{Code: ErrorInvalidResult, Retryable: false})
		}
	} else {
		err = r.store.FailJob(persistCtx, execution, *outcome.failure)
	}
	if err != nil && !errors.Is(err, postgres.ErrOwnershipLost) {
		r.logger.Error("worker_persist_outcome_failed", "trace_id", telemetry.TraceID(attempt.ctx))
	}
}

func (r *Runner) recordAttempt(attempt attemptScope, outcome executionResult) {
	label := "succeeded"
	if outcome.failure != nil {
		label = "failed"
		attempt.span.SetAttributes(attribute.String("taskforge.job.error_code", outcome.failure.Code),
			attribute.Bool("taskforge.job.retryable", outcome.failure.Retryable))
		attempt.span.SetStatus(codes.Error, outcome.failure.Code)
	}
	attempt.span.SetAttributes(attribute.String("taskforge.job.outcome", label))
	if r.options.Metrics != nil {
		r.options.Metrics.AttemptDuration(attempt.kind, label, time.Since(attempt.started))
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
