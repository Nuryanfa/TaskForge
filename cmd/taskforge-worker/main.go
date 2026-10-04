package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Nuryanfa/TaskForge/internal/config"
	"github.com/Nuryanfa/TaskForge/internal/store/postgres"
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

	runner := worker.New(
		store, worker.NewRegistry(cfg.JobResultMaxBytes), logger, cfg.WorkerID,
		cfg.WorkerPollInterval, cfg.JobExecutionTimeout, cfg.ShutdownTimeout,
	)
	logger.Info("worker_started", "worker_id", cfg.WorkerID)
	if err := runner.Run(ctx); err != nil {
		logger.Error("worker_failed")
		os.Exit(1)
	}
	logger.Info("worker_stopped", "worker_id", cfg.WorkerID)
}
