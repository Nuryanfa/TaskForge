package worker_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/config"
	"github.com/Nuryanfa/TaskForge/internal/httpapi"
	"github.com/Nuryanfa/TaskForge/internal/natsbus"
	"github.com/Nuryanfa/TaskForge/internal/relay"
	"github.com/Nuryanfa/TaskForge/internal/store/postgres"
	"github.com/Nuryanfa/TaskForge/internal/worker"
	"github.com/nats-io/nats.go/jetstream"
)

// e2eStream owns the real taskforge.events.> subjects, so only this test may
// create it; it is deleted before and after the run.
const e2eStream = "TASKFORGE_E2E"

type receivedEvent struct {
	Version int    `json:"version"`
	EventID int64  `json:"event_id"`
	Type    string `json:"type"`
	Job     struct {
		JobID  string `json:"job_id"`
		Status string `json:"status"`
	} `json:"job"`
	msgID string
}

func TestOutboxRelayAndWakeupsEndToEndIntegration(t *testing.T) {
	baseURL := os.Getenv("TASKFORGE_TEST_DATABASE_URL")
	natsURL := os.Getenv("TASKFORGE_TEST_NATS_URL")
	if baseURL == "" || natsURL == "" {
		t.Skip("TASKFORGE_TEST_DATABASE_URL and TASKFORGE_TEST_NATS_URL are required")
	}
	databaseURL := isolatedURL(t, baseURL)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	migrateIsolated(t, databaseURL)
	cfg := config.Config{
		DatabaseURL: databaseURL, DatabaseMaxConns: 8, DatabaseMinConns: 0,
		DatabaseConnectTimeout: 5 * time.Second, DatabaseQueryTimeout: 2 * time.Second,
		JobPayloadMaxBytes: 64 * 1024, JobResultMaxBytes: 64 * 1024,
	}
	store, err := postgres.Open(ctx, cfg)
	if err != nil {
		t.Fatal("open store")
	}
	defer store.Close()

	nc, err := natsbus.Connect(natsURL, "taskforge-e2e", 5*time.Second)
	if err != nil {
		t.Fatal("connect to NATS")
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	_ = js.DeleteStream(ctx, e2eStream)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_ = js.DeleteStream(cleanupCtx, e2eStream)
	})
	publisher, err := natsbus.NewPublisher(nc, natsbus.StreamOptions{
		Name: e2eStream, MaxAge: time.Hour, Duplicates: time.Minute}, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.EnsureStream(ctx); err != nil {
		t.Fatal(err)
	}
	consumer, err := js.OrderedConsumer(ctx, e2eStream, jetstream.OrderedConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	relayDone := make(chan error, 1)
	go func() {
		relayDone <- relay.New(store, publisher, logger, relay.Options{
			PollInterval: 20 * time.Millisecond, BatchSize: 50, BatchTimeout: 5 * time.Second, Retention: time.Hour,
		}).Run(runCtx)
	}()
	wake := make(chan struct{}, 1)
	sub, err := natsbus.SubscribeWakeups(nc, wake)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	// A one-hour poll interval means only a wake-up can explain a prompt claim.
	workerDone := make(chan error, 1)
	go func() {
		workerDone <- worker.New(store, worker.NewRegistry(cfg.JobResultMaxBytes), logger, worker.Options{
			WorkerID: "e2e-events-worker", Concurrency: 1, PollInterval: time.Hour,
			ExecutionTimeout: time.Second, ShutdownTimeout: time.Second,
			LeaseDuration: 2 * time.Second, HeartbeatInterval: 200 * time.Millisecond,
			RecoveryInterval: time.Hour, RecoveryBatchSize: 10, ScheduleInterval: time.Hour, Wake: wake,
		}).Run(runCtx)
	}()
	time.Sleep(200 * time.Millisecond) // let the worker's first empty claim pass

	submittedAt := time.Now()
	api := httpapi.New(store, cfg.JobPayloadMaxBytes, time.Second)
	response := performJSON(api, http.MethodPost, "/v1/jobs", `{"kind":"demo.echo","payload":{"message":"evented"}}`)
	if response.Code != http.StatusCreated {
		t.Fatalf("submit status=%d body=%s", response.Code, response.Body.String())
	}
	var submitted struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &submitted); err != nil {
		t.Fatal(err)
	}

	var received []receivedEvent
	deadline := time.Now().Add(10 * time.Second)
	for len(received) < 3 && time.Now().Before(deadline) {
		batch, err := consumer.Fetch(10, jetstream.FetchMaxWait(500*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		for msg := range batch.Messages() {
			var event receivedEvent
			if err := json.Unmarshal(msg.Data(), &event); err != nil {
				t.Fatal(err)
			}
			event.msgID = msg.Headers().Get(jetstream.MsgIDHeader)
			if event.Job.JobID == submitted.ID {
				received = append(received, event)
			}
		}
	}
	elapsed := time.Since(submittedAt)
	var types []string
	for _, event := range received {
		types = append(types, event.Type)
	}
	if want := []string{"job.queued", "job.started", "job.succeeded"}; !reflect.DeepEqual(types, want) {
		t.Fatalf("event types=%v want %v", types, want)
	}
	for i, event := range received {
		if event.Version != 1 || event.msgID == "" || (i > 0 && event.EventID <= received[i-1].EventID) {
			t.Fatalf("event %d=%+v", i, event)
		}
	}
	if elapsed > 5*time.Second {
		t.Fatalf("job took %s; wake-up did not shortcut the one-hour poll", elapsed)
	}
	if backlog, err := store.OutboxBacklog(ctx); err != nil || backlog != 0 {
		t.Fatalf("outbox backlog=%d err=%v", backlog, err)
	}

	stop()
	if err := <-workerDone; err != nil {
		t.Fatal(err)
	}
	if err := <-relayDone; err != nil {
		t.Fatal(err)
	}
}
