package worker_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/config"
	"github.com/Nuryanfa/TaskForge/internal/httpapi"
	"github.com/Nuryanfa/TaskForge/internal/job"
	"github.com/Nuryanfa/TaskForge/internal/migrations"
	"github.com/Nuryanfa/TaskForge/internal/store/postgres"
	"github.com/Nuryanfa/TaskForge/internal/worker"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestEchoEndToEndIntegration(t *testing.T) {
	baseURL := os.Getenv("TASKFORGE_TEST_DATABASE_URL")
	if baseURL == "" {
		t.Skip("TASKFORGE_TEST_DATABASE_URL is not set")
	}
	databaseURL := isolatedURL(t, baseURL)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := migrations.Run(ctx, databaseURL); err != nil {
		t.Fatal("apply migrations")
	}
	cfg := config.Config{
		DatabaseURL: databaseURL, DatabaseMaxConns: 5, DatabaseMinConns: 0,
		DatabaseConnectTimeout: 5 * time.Second, DatabaseQueryTimeout: 2 * time.Second,
		JobPayloadMaxBytes: 64 * 1024, JobResultMaxBytes: 64 * 1024,
	}
	store, err := postgres.Open(ctx, cfg)
	if err != nil {
		t.Fatal("open store")
	}
	defer store.Close()
	api := httpapi.New(store, cfg.JobPayloadMaxBytes, time.Second)

	body := `{"kind":"demo.echo","payload":{"message":"hello"},"idempotency_key":"e2e-echo"}`
	first := performJSON(api, http.MethodPost, "/v1/jobs", body)
	if first.Code != http.StatusCreated {
		t.Fatalf("submit status=%d body=%s", first.Code, first.Body.String())
	}
	var submitted struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &submitted); err != nil {
		t.Fatal(err)
	}
	replay := performJSON(api, http.MethodPost, "/v1/jobs", body)
	if replay.Code != http.StatusOK || !strings.Contains(replay.Body.String(), submitted.ID) {
		t.Fatalf("replay status=%d body=%s", replay.Code, replay.Body.String())
	}
	conflict := performJSON(api, http.MethodPost, "/v1/jobs",
		`{"kind":"demo.echo","payload":{"message":"different"},"idempotency_key":"e2e-echo"}`)
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflict status=%d body=%s", conflict.Code, conflict.Body.String())
	}

	runCtx, stopWorker := context.WithCancel(context.Background())
	runner := worker.New(store, worker.NewRegistry(cfg.JobResultMaxBytes),
		slog.New(slog.NewTextHandler(io.Discard, nil)), "e2e-worker", 5*time.Millisecond, time.Second, time.Second)
	workerDone := make(chan error, 1)
	go func() { workerDone <- runner.Run(runCtx) }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		found, err := store.GetJob(ctx, submitted.ID)
		if err == nil && found.Status == job.StatusSucceeded {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("job did not reach succeeded before deadline")
		}
		select {
		case <-ctx.Done():
			t.Fatal("test context expired")
		case <-time.After(10 * time.Millisecond):
		}
	}
	stopWorker()
	if err := <-workerDone; err != nil {
		t.Fatal(err)
	}

	get := httptest.NewRecorder()
	api.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/v1/jobs/"+submitted.ID, nil))
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), `"status":"succeeded"`) {
		t.Fatalf("get status=%d body=%s", get.Code, get.Body.String())
	}
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal("connect to inspect attempts")
	}
	defer conn.Close(ctx)
	var attempts int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM job_attempts WHERE job_id = $1`, submitted.ID).Scan(&attempts); err != nil || attempts != 1 {
		t.Fatalf("attempt count=%d err=%v", attempts, err)
	}
}

func performJSON(handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func isolatedURL(t *testing.T, baseURL string) string {
	t.Helper()
	schema := "taskforge_e2e_" + strings.ReplaceAll(uuid.NewString(), "-", "_")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, baseURL)
	if err != nil {
		t.Fatal("connect to create isolated schema")
	}
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err := conn.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		conn.Close(ctx)
		t.Fatal("create isolated schema")
	}
	conn.Close(ctx)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		cleanupConn, err := pgx.Connect(cleanupCtx, baseURL)
		if err == nil {
			_, _ = cleanupConn.Exec(cleanupCtx, "DROP SCHEMA IF EXISTS "+identifier+" CASCADE")
			_ = cleanupConn.Close(cleanupCtx)
		}
	})
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme == "" {
		t.Fatal("TASKFORGE_TEST_DATABASE_URL must be a PostgreSQL URL")
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}
