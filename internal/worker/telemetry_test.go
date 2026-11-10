package worker

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/job"
	"github.com/Nuryanfa/TaskForge/internal/store/postgres"
	"github.com/Nuryanfa/TaskForge/internal/telemetry"
	"github.com/Nuryanfa/TaskForge/internal/telemetry/telemetrytest"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func runOnce(t *testing.T, store *workerStore, metrics *telemetry.Metrics) {
	t.Helper()
	options := testOptions()
	options.Metrics = metrics
	runner := New(store, NewRegistry(1024), slog.New(slog.NewTextHandler(io.Discard, nil)), options)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := runner.Run(ctx); err != nil {
		t.Fatal(err)
	}
}

func readyJob(delay time.Duration) job.Job {
	started := time.Now()
	return job.Job{ID: "job-1", Kind: "demo.echo", Queue: "default", Payload: []byte(`{"message":"x"}`),
		AttemptCount: 1, MaxAttempts: 3, AvailableAt: started.Add(-delay), StartedAt: &started}
}

func TestRunnerRecordsStartDelayAndAttemptMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := telemetry.NewMetrics(registry, []string{"default"})
	runOnce(t, &workerStore{jobs: []job.Job{readyJob(3 * time.Second)}}, metrics)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var delaySum float64
	var delayCount, attemptCount uint64
	for _, family := range families {
		switch family.GetName() {
		case "taskforge_job_start_delay_seconds":
			delaySum = family.GetMetric()[0].GetHistogram().GetSampleSum()
			delayCount = family.GetMetric()[0].GetHistogram().GetSampleCount()
		case "taskforge_job_attempt_duration_seconds":
			for _, metric := range family.GetMetric() {
				attemptCount += metric.GetHistogram().GetSampleCount()
			}
		}
	}
	if delayCount != 1 || delaySum < 2.9 || delaySum > 3.1 || attemptCount != 1 {
		t.Fatalf("start delay count=%d sum=%v attempts=%d", delayCount, delaySum, attemptCount)
	}
}

func TestRunnerPersistsWithAttemptSpanContext(t *testing.T) {
	exporter := telemetrytest.Setup(t)
	store := &workerStore{jobs: []job.Job{readyJob(0)}}
	runOnce(t, store, nil)
	store.mu.Lock()
	persisted := trace.SpanContextFromContext(store.completeCtx)
	store.mu.Unlock()
	for _, span := range exporter.GetSpans() {
		if span.Name == "job.attempt" {
			if !persisted.IsValid() || persisted.SpanID() != span.SpanContext.SpanID() {
				t.Fatalf("CompleteJob ctx span=%s, attempt span=%s", persisted.SpanID(), span.SpanContext.SpanID())
			}
			return
		}
	}
	t.Fatal("no job.attempt span recorded")
}

func TestRejectedResultIsRecordedAsFailedAttempt(t *testing.T) {
	exporter := telemetrytest.Setup(t)
	registry := prometheus.NewRegistry()
	metrics := telemetry.NewMetrics(registry, []string{"default"})
	runOnce(t, &workerStore{jobs: []job.Job{readyJob(0)}, completeErr: postgres.ErrInvalidResult}, metrics)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "taskforge_job_attempt_duration_seconds" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "outcome" && label.GetValue() != "failed" {
					t.Fatalf("rejected result recorded with outcome=%s", label.GetValue())
				}
			}
		}
	}
	for _, span := range exporter.GetSpans() {
		if span.Name == "job.attempt" {
			if span.Status.Code != codes.Error {
				t.Fatalf("rejected result left span status %v", span.Status.Code)
			}
			return
		}
	}
	t.Fatal("no attempt span")
}

func TestLostLeaseIsRecordedOnAttempt(t *testing.T) {
	exporter := telemetrytest.Setup(t)
	registry := prometheus.NewRegistry()
	metrics := telemetry.NewMetrics(registry, []string{"default"})
	sleeping := readyJob(0)
	sleeping.Kind, sleeping.Payload = "demo.sleep", []byte(`{"ms":2000}`)
	runOnce(t, &workerStore{jobs: []job.Job{sleeping}, renewErr: postgres.ErrOwnershipLost}, metrics)
	for _, span := range exporter.GetSpans() {
		if span.Name == "job.attempt" {
			outcome := ""
			for _, kv := range span.Attributes {
				if kv.Key == "taskforge.job.outcome" {
					outcome = kv.Value.AsString()
				}
			}
			if outcome != "lease_lost" || span.Status.Code != codes.Error {
				t.Fatalf("lost lease recorded as outcome=%q status=%v", outcome, span.Status.Code)
			}
			return
		}
	}
	t.Fatal("no attempt span")
}
