package migrations

import (
	"context"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestV01RunningJobUpgradeIntegration(t *testing.T) {
	baseURL := os.Getenv("TASKFORGE_TEST_DATABASE_URL")
	if baseURL == "" {
		t.Skip("TASKFORGE_TEST_DATABASE_URL is not set")
	}
	databaseURL := migrationTestURL(t, baseURL)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	v1 := int64(1)
	if err := run(ctx, databaseURL, &v1); err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	jobID := uuid.NewString()
	if _, err := conn.Exec(ctx, `INSERT INTO jobs
		(id, queue, kind, payload, payload_hash, status, attempt_count, started_at)
		VALUES ($1, 'default', 'demo.echo', '{"message":"orphan"}', $2, 'running', 1, NOW())`,
		jobID, make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO job_attempts
		(job_id, attempt, worker_id, started_at) VALUES ($1, 1, 'v01-worker', NOW())`, jobID); err != nil {
		t.Fatal(err)
	}
	if err := Run(ctx, databaseURL); err != nil {
		t.Fatal(err)
	}
	var status, outcome, code string
	var owner *string
	var expiration *time.Time
	var jobToken, attemptToken int64
	if err := conn.QueryRow(ctx, `SELECT j.status, j.lease_owner, j.lease_expires_at,
		j.fencing_token, a.outcome, a.error_code, a.fencing_token
		FROM jobs j JOIN job_attempts a ON a.job_id = j.id WHERE j.id = $1`, jobID).
		Scan(&status, &owner, &expiration, &jobToken, &outcome, &code, &attemptToken); err != nil {
		t.Fatal(err)
	}
	if status != "queued" || owner != nil || expiration != nil || jobToken != 1 ||
		outcome != "abandoned" || code != "V01_MIGRATION_RECOVERY" || attemptToken != 1 {
		t.Fatalf("unexpected upgraded state: status=%s owner=%v expiration=%v job-token=%d outcome=%s code=%s attempt-token=%d",
			status, owner, expiration, jobToken, outcome, code, attemptToken)
	}
	if _, err := conn.Exec(ctx, `UPDATE jobs SET fencing_token = 0 WHERE id = $1`, jobID); err == nil {
		t.Fatal("expected database to reject a decreasing fencing token")
	}
}

func migrationTestURL(t *testing.T, baseURL string) string {
	t.Helper()
	schema := "taskforge_migration_" + strings.ReplaceAll(uuid.NewString(), "-", "_")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, baseURL)
	if err != nil {
		t.Fatal(err)
	}
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err := conn.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		conn.Close(ctx)
		t.Fatal(err)
	}
	conn.Close(ctx)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		cleanup, err := pgx.Connect(cleanupCtx, baseURL)
		if err == nil {
			_, _ = cleanup.Exec(cleanupCtx, "DROP SCHEMA IF EXISTS "+identifier+" CASCADE")
			_ = cleanup.Close(cleanupCtx)
		}
	})
	parsed, err := url.Parse(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}
