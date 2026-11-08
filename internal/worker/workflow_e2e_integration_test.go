package worker_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/config"
	"github.com/Nuryanfa/TaskForge/internal/httpapi"
	"github.com/Nuryanfa/TaskForge/internal/migrations"
	"github.com/Nuryanfa/TaskForge/internal/store/postgres"
	"github.com/Nuryanfa/TaskForge/internal/worker"
	"github.com/jackc/pgx/v5"
)

// TestWorkflowSagaEndToEndIntegration submits the design's order example over
// HTTP and runs it with the real worker runner and handler registry: "ship"
// fails, so "charge" and "reserve" are compensated in reverse order.
func TestWorkflowSagaEndToEndIntegration(t *testing.T) {
	baseURL := os.Getenv("TASKFORGE_TEST_DATABASE_URL")
	if baseURL == "" {
		t.Skip("TASKFORGE_TEST_DATABASE_URL is not set")
	}
	databaseURL := isolatedURL(t, baseURL)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := migrations.Run(ctx, databaseURL); err != nil {
		t.Fatal("apply migrations")
	}
	cfg := config.Config{
		DatabaseURL: databaseURL, DatabaseMaxConns: 8, DatabaseMinConns: 0,
		DatabaseConnectTimeout: 5 * time.Second, DatabaseQueryTimeout: 3 * time.Second,
		JobPayloadMaxBytes: 64 * 1024, JobResultMaxBytes: 64 * 1024,
	}
	store, err := postgres.Open(ctx, cfg)
	if err != nil {
		t.Fatal("open store")
	}
	defer store.Close()
	api := httpapi.NewWithOptions(store, httpapi.Options{PayloadMaxBytes: cfg.JobPayloadMaxBytes, ReadinessTimeout: time.Second})

	body := `{"label":"order-1042","idempotency_key":"order-1042-v1","steps":[
		{"name":"reserve","kind":"demo.echo","payload":{"message":"reserve"},
		 "compensation":{"kind":"demo.echo","payload":{"message":"release"}}},
		{"name":"charge","kind":"demo.echo","payload":{"message":"charge"},"depends_on":["reserve"],
		 "compensation":{"kind":"demo.echo","payload":{"message":"refund"}}},
		{"name":"notify","kind":"demo.echo","payload":{"message":"email"},"depends_on":["reserve"]},
		{"name":"ship","kind":"demo.fail","payload":{},"depends_on":["charge","notify"]}]}`
	created := performJSON(api, http.MethodPost, "/v1/workflows", body)
	if created.Code != http.StatusCreated {
		t.Fatalf("submit status=%d body=%s", created.Code, created.Body.String())
	}
	var submitted struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &submitted); err != nil {
		t.Fatal(err)
	}

	runCtx, stopWorker := context.WithCancel(context.Background())
	runner := worker.New(store, worker.NewRegistry(cfg.JobResultMaxBytes),
		slog.New(slog.NewTextHandler(io.Discard, nil)), worker.Options{
			WorkerID: "e2e-workflow", Concurrency: 4, PollInterval: 5 * time.Millisecond,
			ExecutionTimeout: time.Second, ShutdownTimeout: time.Second,
			LeaseDuration: 2 * time.Second, HeartbeatInterval: 200 * time.Millisecond,
			RecoveryInterval: 100 * time.Millisecond, RecoveryBatchSize: 10,
		})
	workerDone := make(chan error, 1)
	go func() { workerDone <- runner.Run(runCtx) }()

	type stepView struct {
		Name         string `json:"name"`
		Status       string `json:"status"`
		Compensation *struct {
			Status string `json:"status"`
		} `json:"compensation"`
	}
	var final struct {
		Status     string     `json:"status"`
		FailedStep string     `json:"failed_step"`
		ErrorCode  string     `json:"error_code"`
		Steps      []stepView `json:"steps"`
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		get := performJSON(api, http.MethodGet, "/v1/workflows/"+submitted.ID, "")
		if err := json.Unmarshal(get.Body.Bytes(), &final); err != nil {
			t.Fatal(err)
		}
		if final.Status == "compensated" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("workflow did not compensate in time: %s", get.Body.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	stopWorker()
	if err := <-workerDone; err != nil {
		t.Fatal(err)
	}
	if final.FailedStep != "ship" || final.ErrorCode != "DEMO_FAILURE" {
		t.Fatalf("final=%+v", final)
	}
	steps := map[string]stepView{}
	for _, item := range final.Steps {
		steps[item.Name] = item
	}
	for _, name := range []string{"reserve", "charge"} {
		if steps[name].Compensation == nil || steps[name].Compensation.Status != "succeeded" {
			t.Fatalf("%s compensation=%+v", name, steps[name].Compensation)
		}
	}
	if steps["notify"].Status != "succeeded" || steps["notify"].Compensation != nil || steps["ship"].Status != "failed" {
		t.Fatalf("steps=%+v", steps)
	}

	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal("connect to inspect outbox")
	}
	defer conn.Close(ctx)
	var missingWorkflowID, compensationsBeforeFailure int
	if err := conn.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM outbox_events e JOIN jobs j ON j.id = e.job_id
			WHERE j.workflow_id = $1 AND e.payload->>'workflow_id' IS DISTINCT FROM $1::text),
		(SELECT count(*) FROM outbox_events e JOIN jobs j ON j.id = e.job_id
			WHERE j.workflow_id = $1 AND j.workflow_role = 'compensation' AND e.event_type = 'job.queued'
			AND e.id < (SELECT f.id FROM outbox_events f JOIN jobs fj ON fj.id = f.job_id
				JOIN workflow_steps s ON s.id = fj.workflow_step_id
				WHERE fj.workflow_id = $1 AND s.name = 'ship' AND f.event_type = 'job.failed'))`,
		submitted.ID).Scan(&missingWorkflowID, &compensationsBeforeFailure); err != nil {
		t.Fatal(err)
	}
	if missingWorkflowID != 0 || compensationsBeforeFailure != 0 {
		t.Fatalf("events without workflow_id=%d compensations queued before ship failed=%d", missingWorkflowID, compensationsBeforeFailure)
	}
}
