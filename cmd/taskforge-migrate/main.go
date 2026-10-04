package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Nuryanfa/TaskForge/internal/config"
	"github.com/Nuryanfa/TaskForge/internal/migrations"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil || cfg.RequireDatabase() != nil {
		logger.Error("invalid_configuration")
		os.Exit(1)
	}
	signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(signalCtx, cfg.DatabaseConnectTimeout+cfg.DatabaseQueryTimeout)
	defer cancel()
	if err := migrations.Run(ctx, cfg.DatabaseURL); err != nil {
		logger.Error("migration_failed")
		os.Exit(1)
	}
	logger.Info("migration_complete")
}
