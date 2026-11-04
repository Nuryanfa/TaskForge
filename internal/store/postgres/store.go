package postgres

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/config"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/puddle/v2"
)

type Store struct {
	pool           *pgxpool.Pool
	queryTimeout   time.Duration
	maxResultBytes int64
	retryPolicy    retryPolicy
}

type retryPolicy struct {
	maxAttempts    int
	initialBackoff time.Duration
	maxBackoff     time.Duration
	jitterPercent  int
}

func Open(ctx context.Context, cfg config.Config) (*Store, error) {
	if cfg.JobMaxAttempts == 0 {
		cfg.JobMaxAttempts = 3
	}
	if cfg.RetryInitialBackoff == 0 {
		cfg.RetryInitialBackoff = time.Second
	}
	if cfg.RetryMaxBackoff == 0 {
		cfg.RetryMaxBackoff = time.Minute
	}
	poolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse postgres configuration: %w", err)
	}
	poolConfig.MaxConns = cfg.DatabaseMaxConns
	poolConfig.MinConns = cfg.DatabaseMinConns
	poolConfig.ConnConfig.ConnectTimeout = cfg.DatabaseConnectTimeout

	connectCtx, cancel := context.WithTimeout(ctx, cfg.DatabaseConnectTimeout)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(connectCtx, poolConfig)
	if err != nil {
		return nil, classify("open postgres pool", err)
	}
	store := &Store{pool: pool, queryTimeout: cfg.DatabaseQueryTimeout, maxResultBytes: cfg.JobResultMaxBytes,
		retryPolicy: retryPolicy{cfg.JobMaxAttempts, cfg.RetryInitialBackoff, cfg.RetryMaxBackoff, cfg.RetryJitterPercent}}
	if err := store.Ping(connectCtx); err != nil {
		pool.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) Ping(ctx context.Context) error {
	queryCtx, cancel := s.queryContext(ctx)
	defer cancel()
	if err := s.pool.Ping(queryCtx); err != nil {
		return classify("ping postgres", err)
	}
	return nil
}

func (s *Store) queryContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.queryTimeout)
}

func classify(operation string, err error) error {
	if err == nil {
		return nil
	}
	var connectError *pgconn.ConnectError
	var networkError net.Error
	var pgError *pgconn.PgError
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, puddle.ErrClosedPool) ||
		errors.Is(err, puddle.ErrNotAvailable) || errors.As(err, &connectError) || errors.As(err, &networkError) {
		return fmt.Errorf("%s: %w", operation, ErrUnavailable)
	}
	if errors.As(err, &pgError) && len(pgError.Code) >= 2 {
		switch pgError.Code[:2] {
		case "08", "53", "57":
			return fmt.Errorf("%s: %w", operation, ErrUnavailable)
		}
	}
	return fmt.Errorf("%s: %w", operation, err)
}
