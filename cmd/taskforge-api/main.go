package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/config"
	"github.com/Nuryanfa/TaskForge/internal/httpapi"
	"github.com/Nuryanfa/TaskForge/internal/store/postgres"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "healthcheck" {
		runHealthcheck()
		return
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil || cfg.RequireDatabase() != nil {
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

	server := &http.Server{
		Addr: cfg.HTTPAddr, Handler: httpapi.New(store, cfg.JobPayloadMaxBytes, cfg.DatabaseQueryTimeout, cfg.DeadLetterPageSize),
		ReadTimeout: cfg.ReadTimeout, ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		WriteTimeout: cfg.WriteTimeout, IdleTimeout: cfg.IdleTimeout,
	}
	serveErr := make(chan error, 1)
	go func() {
		logger.Info("api_started", "address", cfg.HTTPAddr)
		serveErr <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("api_shutdown_failed")
			os.Exit(1)
		}
		logger.Info("api_stopped")
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("api_server_failed")
			os.Exit(1)
		}
	}
}

func runHealthcheck() {
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get("http://127.0.0.1:8080/healthz")
	if err != nil {
		os.Exit(1)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		os.Exit(1)
	}
}
