package migrations

import (
	"context"
	"fmt"

	migrationfiles "github.com/Nuryanfa/TaskForge/db/migrations"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

// Run applies all pending embedded migrations. Goose's PostgreSQL session lock
// serializes competing migration commands on one dedicated connection.
func Run(ctx context.Context, databaseURL string) error {
	pgxConfig, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		return fmt.Errorf("parse database configuration: %w", err)
	}
	db := stdlib.OpenDB(*pgxConfig)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("connect for migrations: %w", err)
	}
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return fmt.Errorf("create migration lock: %w", err)
	}
	provider, err := goose.NewProvider(
		goose.DialectPostgres,
		db,
		migrationfiles.Files,
		goose.WithSessionLocker(locker),
		goose.WithDisableGlobalRegistry(true),
	)
	if err != nil {
		return fmt.Errorf("create migration provider: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}
