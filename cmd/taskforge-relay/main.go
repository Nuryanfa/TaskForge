package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Nuryanfa/TaskForge/internal/config"
	"github.com/Nuryanfa/TaskForge/internal/natsbus"
	"github.com/Nuryanfa/TaskForge/internal/relay"
	"github.com/Nuryanfa/TaskForge/internal/store/postgres"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil || cfg.RequireRelay() != nil {
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

	nc, err := natsbus.Connect(cfg.NATSURL, "taskforge-relay", cfg.NATSConnectTimeout)
	if err != nil {
		logger.Error("nats_connection_failed")
		os.Exit(1)
	}
	defer nc.Close()
	publisher, err := natsbus.NewPublisher(nc, natsbus.StreamOptions{
		Name: cfg.NATSStream, MaxAge: cfg.NATSStreamMaxAge, Duplicates: cfg.NATSDuplicateWindow,
	}, cfg.NATSPublishTimeout)
	if err != nil {
		logger.Error("nats_connection_failed")
		os.Exit(1)
	}

	runner := relay.New(store, publisher, logger, relay.Options{
		PollInterval: cfg.OutboxPollInterval, BatchSize: cfg.OutboxBatchSize,
		BatchTimeout: cfg.OutboxBatchTimeout, Retention: cfg.OutboxRetention,
	})
	logger.Info("relay_started", "stream", cfg.NATSStream)
	if err := runner.Run(ctx); err != nil {
		logger.Error("relay_failed")
		os.Exit(1)
	}
	logger.Info("relay_stopped")
}
