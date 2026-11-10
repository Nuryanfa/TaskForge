package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Nuryanfa/TaskForge/internal/config"
	"github.com/Nuryanfa/TaskForge/internal/natsbus"
	"github.com/Nuryanfa/TaskForge/internal/store/postgres"
	"github.com/Nuryanfa/TaskForge/internal/telemetry"
	"github.com/Nuryanfa/TaskForge/internal/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil || cfg.RequireWorker() != nil {
		logger.Error("invalid_configuration")
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	store, err := postgres.Open(ctx, cfg)
	if err != nil {
		logger.Error("database_connection_failed")
		os.Exit(1)
	}
	defer store.Close()

	process, err := telemetry.StartProcess(ctx, cfg, "taskforge-worker", "127.0.0.1:9102", logger)
	if err != nil {
		logger.Error("metrics_listen_failed")
		os.Exit(1)
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		_ = process.Shutdown(flushCtx)
	}()
	store.SetObserver(process.Metrics)
	process.Registry.MustRegister(telemetry.NewPoolCollector(store.PoolStats))

	// NATS wake-ups are optional hints; without them the worker only polls.
	var wake chan struct{}
	if cfg.NATSURL != "" {
		nc, err := natsbus.Connect(cfg.NATSURL, "taskforge-worker-"+cfg.WorkerID, cfg.NATSConnectTimeout)
		if err == nil {
			defer nc.Close()
			wake = make(chan struct{}, cfg.WorkerConcurrency)
			_, err = natsbus.SubscribeWakeups(nc, wake)
		}
		if err != nil {
			wake = nil
			logger.Error("worker_wakeup_subscribe_failed")
		} else {
			logger.Info("worker_wakeups_enabled")
		}
	}

	runner := worker.New(
		store, worker.NewRegistry(cfg.JobResultMaxBytes), logger, worker.Options{
			WorkerID: cfg.WorkerID, Concurrency: cfg.WorkerConcurrency,
			PollInterval: cfg.WorkerPollInterval, ExecutionTimeout: cfg.JobExecutionTimeout,
			ShutdownTimeout: cfg.ShutdownTimeout, LeaseDuration: cfg.JobLeaseDuration,
			HeartbeatInterval: cfg.JobHeartbeatInterval, RecoveryInterval: cfg.RecoveryInterval,
			RecoveryBatchSize: cfg.RecoveryBatchSize, ScheduleInterval: cfg.ScheduleInterval,
			ScheduleBatchSize: cfg.ScheduleBatchSize, Wake: wake, Metrics: process.Metrics,
		},
	)
	logger.Info("worker_started", "worker_id", cfg.WorkerID, "concurrency", cfg.WorkerConcurrency)
	if err := runner.Run(ctx); err != nil {
		logger.Error("worker_failed")
		os.Exit(1)
	}
	logger.Info("worker_stopped", "worker_id", cfg.WorkerID)
}
