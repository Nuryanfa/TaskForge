package worker_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/config"
	"github.com/Nuryanfa/TaskForge/internal/httpapi"
	"github.com/Nuryanfa/TaskForge/internal/job"
	"github.com/Nuryanfa/TaskForge/internal/migrations"
	"github.com/Nuryanfa/TaskForge/internal/store/postgres"
	"github.com/Nuryanfa/TaskForge/internal/telemetry"
	"github.com/Nuryanfa/TaskForge/internal/telemetry/telemetrytest"
	"github.com/Nuryanfa/TaskForge/internal/worker"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// obsEnv is one isolated database with the real API, store observer, and
// metrics; tests run the real worker runner against it.
type obsEnv struct {
	store    *postgres.Store
	api      http.Handler
	metrics  *telemetry.Metrics
	registry *prometheus.Registry
}

func newObsEnv(t *testing.T, maxAttempts int, backoff time.Duration) *obsEnv {
	t.Helper()
	baseURL := os.Getenv("TASKFORGE_TEST_DATABASE_URL")
	if baseURL == "" {
		t.Skip("TASKFORGE_TEST_DATABASE_URL is not set")
	}
	databaseURL := isolatedURL(t, baseURL)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := migrations.Run(ctx, databaseURL); err != nil {
		t.Fatal("apply migrations")
	}
	cfg := config.Config{
		DatabaseURL: databaseURL, DatabaseMaxConns: 8, DatabaseMinConns: 0,
		DatabaseConnectTimeout: 5 * time.Second, DatabaseQueryTimeout: 3 * time.Second,
		JobPayloadMaxBytes: 64 * 1024, JobResultMaxBytes: 64 * 1024,
		JobMaxAttempts: maxAttempts, RetryInitialBackoff: backoff, RetryMaxBackoff: backoff,
	}
	store, err := postgres.Open(ctx, cfg)
	if err != nil {
		t.Fatal("open store")
	}
	t.Cleanup(store.Close)
	registry := prometheus.NewRegistry()
	metrics := telemetry.NewMetrics(registry, []string{"default"})
	store.SetObserver(metrics)
	api := httpapi.NewWithOptions(store, httpapi.Options{PayloadMaxBytes: 64 * 1024, ReadinessTimeout: time.Second, Metrics: metrics})
	return &obsEnv{store: store, api: api, metrics: metrics, registry: registry}
}

// runWorkerUntil runs the real runner until done() reports true.
func (e *obsEnv) runWorkerUntil(t *testing.T, done func() bool) {
	t.Helper()
	runCtx, stop := context.WithCancel(context.Background())
	runner := worker.New(e.store, worker.NewRegistry(64*1024), slog.New(slog.NewTextHandler(io.Discard, nil)), worker.Options{
		WorkerID: "obs-worker", Concurrency: 2, PollInterval: 5 * time.Millisecond,
		ExecutionTimeout: 2 * time.Second, ShutdownTimeout: time.Second, LeaseDuration: 2 * time.Second,
		HeartbeatInterval: 200 * time.Millisecond, RecoveryInterval: 100 * time.Millisecond, RecoveryBatchSize: 10,
		Metrics: e.metrics,
	})
	finished := make(chan error, 1)
	go func() { finished <- runner.Run(runCtx) }()
	deadline := time.Now().Add(15 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			stop()
			<-finished
			t.Fatal("condition not reached before deadline")
		}
		time.Sleep(10 * time.Millisecond)
	}
	stop()
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
}

func (e *obsEnv) submit(t *testing.T, path, body string) string {
	t.Helper()
	response := performJSON(e.api, http.MethodPost, path, body)
	if response.Code != http.StatusCreated {
		t.Fatalf("POST %s status=%d body=%s", path, response.Code, response.Body.String())
	}
	var created struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	return created.ID
}

func (e *obsEnv) jobIs(t *testing.T, id string, status job.Status) func() bool {
	return func() bool {
		found, err := e.store.GetJob(context.Background(), id)
		return err == nil && found.Status == status
	}
}

func (e *obsEnv) workflowIs(t *testing.T, id string, status job.WorkflowStatus) func() bool {
	return func() bool {
		found, err := e.store.GetWorkflow(context.Background(), id)
		return err == nil && found.Status == status
	}
}

func attr(span tracetest.SpanStub, key string) string {
	for _, kv := range span.Attributes {
		if string(kv.Key) == key {
			return kv.Value.String()
		}
	}
	return ""
}

func spansNamed(exporter *tracetest.InMemoryExporter, name string) []tracetest.SpanStub {
	var found []tracetest.SpanStub
	for _, span := range exporter.GetSpans() {
		if span.Name == name {
			found = append(found, span)
		}
	}
	return found
}

func onlySpanNamed(t *testing.T, exporter *tracetest.InMemoryExporter, name string) tracetest.SpanStub {
	t.Helper()
	found := spansNamed(exporter, name)
	if len(found) != 1 {
		t.Fatalf("%d %s spans, want 1", len(found), name)
	}
	return found[0]
}

func TestImmediateJobTraceIntegration(t *testing.T) {
	exporter := telemetrytest.Setup(t)
	env := newObsEnv(t, 3, 100*time.Millisecond)
	id := env.submit(t, "/v1/jobs", `{"kind":"demo.echo","payload":{"message":"hi"}}`)
	env.runWorkerUntil(t, env.jobIs(t, id, job.StatusSucceeded))
	server := onlySpanNamed(t, exporter, "POST /v1/jobs")
	enqueue := onlySpanNamed(t, exporter, "job.enqueue")
	attempt := onlySpanNamed(t, exporter, "job.attempt")
	if enqueue.Parent.SpanID() != server.SpanContext.SpanID() {
		t.Fatal("job.enqueue is not a child of the HTTP span")
	}
	if attempt.SpanContext.TraceID() != server.SpanContext.TraceID() || attempt.Parent.SpanID() != enqueue.SpanContext.SpanID() {
		t.Fatalf("attempt trace=%s parent=%s; want trace %s parent %s",
			attempt.SpanContext.TraceID(), attempt.Parent.SpanID(), server.SpanContext.TraceID(), enqueue.SpanContext.SpanID())
	}
}

func TestRetriedJobAttemptsAreSiblingsIntegration(t *testing.T) {
	exporter := telemetrytest.Setup(t)
	env := newObsEnv(t, 3, 100*time.Millisecond)
	id := env.submit(t, "/v1/jobs", `{"kind":"demo.flaky","payload":{"fail_attempts":1}}`)
	env.runWorkerUntil(t, env.jobIs(t, id, job.StatusSucceeded))
	enqueue := onlySpanNamed(t, exporter, "job.enqueue")
	attempts := spansNamed(exporter, "job.attempt")
	if len(attempts) != 2 {
		t.Fatalf("attempt spans=%d", len(attempts))
	}
	seen := map[string]bool{}
	for _, attempt := range attempts {
		if attempt.Parent.SpanID() != enqueue.SpanContext.SpanID() {
			t.Fatal("retry attempt is not a sibling under the enqueue span")
		}
		seen[attr(attempt, "taskforge.job.attempt")] = true
	}
	if !seen["1"] || !seen["2"] {
		t.Fatalf("attempt numbers=%v", seen)
	}
}

func TestDelayedJobAttemptsAreLinkedRootsIntegration(t *testing.T) {
	exporter := telemetrytest.Setup(t)
	env := newObsEnv(t, 3, 100*time.Millisecond)
	id := env.submit(t, "/v1/jobs", `{"kind":"demo.flaky","payload":{"fail_attempts":1},"delay_seconds":1}`)
	env.runWorkerUntil(t, env.jobIs(t, id, job.StatusSucceeded))
	enqueue := onlySpanNamed(t, exporter, "job.enqueue")
	attempts := spansNamed(exporter, "job.attempt")
	if len(attempts) != 2 {
		t.Fatalf("attempt spans=%d", len(attempts))
	}
	for _, attempt := range attempts {
		if attempt.Parent.IsValid() || attempt.SpanContext.TraceID() == enqueue.SpanContext.TraceID() ||
			len(attempt.Links) != 1 || attempt.Links[0].SpanContext.SpanID() != enqueue.SpanContext.SpanID() {
			t.Fatalf("delayed attempt %s is not a new root linked to the enqueue span", attr(attempt, "taskforge.job.attempt"))
		}
	}
}

func TestWorkflowStepsFormCausalChainIntegration(t *testing.T) {
	exporter := telemetrytest.Setup(t)
	env := newObsEnv(t, 1, 100*time.Millisecond)
	id := env.submit(t, "/v1/workflows", `{"steps":[
		{"name":"reserve","kind":"demo.echo","payload":{"message":"r"},"compensation":{"kind":"demo.echo","payload":{"message":"release"}}},
		{"name":"charge","kind":"demo.echo","payload":{"message":"c"},"depends_on":["reserve"],"compensation":{"kind":"demo.echo","payload":{"message":"refund"}}},
		{"name":"notify","kind":"demo.echo","payload":{"message":"n"},"depends_on":["reserve"]},
		{"name":"ship","kind":"demo.fail","payload":{},"depends_on":["charge","notify"]}]}`)
	env.runWorkerUntil(t, env.workflowIs(t, id, job.WorkflowCompensated))
	byStep := map[string]tracetest.SpanStub{}
	for _, span := range spansNamed(exporter, "job.attempt") {
		byStep[attr(span, "taskforge.workflow.step")+"/"+attr(span, "taskforge.workflow.role")] = span
	}
	parentOf := func(child, parent string) {
		t.Helper()
		if byStep[child].Parent.SpanID() != byStep[parent].SpanContext.SpanID() {
			t.Fatalf("%s attempt is not a child of the %s attempt", child, parent)
		}
	}
	submit := onlySpanNamed(t, exporter, "workflow.submit")
	if byStep["reserve/action"].Parent.SpanID() != submit.SpanContext.SpanID() {
		t.Fatal("root step attempt is not a child of workflow.submit")
	}
	parentOf("charge/action", "reserve/action")
	parentOf("charge/compensation", "ship/action")
	parentOf("reserve/compensation", "charge/compensation")
	for key, span := range byStep {
		if span.SpanContext.TraceID() != submit.SpanContext.TraceID() {
			t.Fatalf("%s attempt is outside the workflow's trace", key)
		}
	}
}

func TestRecoveredJobRetryIsChildOfStoredContextIntegration(t *testing.T) {
	exporter := telemetrytest.Setup(t)
	env := newObsEnv(t, 3, 100*time.Millisecond)
	id := env.submit(t, "/v1/jobs", `{"kind":"demo.echo","payload":{"message":"recover me"}}`)
	if _, err := env.store.ClaimNextJob(context.Background(), "doomed", time.Second); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	if recovered, err := env.store.RecoverExpiredJobs(context.Background(), 10); err != nil || recovered != 1 {
		t.Fatalf("recovered=%d err=%v", recovered, err)
	}
	env.runWorkerUntil(t, env.jobIs(t, id, job.StatusSucceeded))
	enqueue := onlySpanNamed(t, exporter, "job.enqueue")
	attempt := onlySpanNamed(t, exporter, "job.attempt")
	if attempt.Parent.SpanID() != enqueue.SpanContext.SpanID() {
		t.Fatal("the attempt after recovery is not a child of the stored enqueue context")
	}
	recovery := onlySpanNamed(t, exporter, "lease.recovery")
	if attr(recovery, "taskforge.recovery.recovered") != "1" || attr(recovery, "taskforge.recovery.failed") != "0" {
		t.Fatalf("lease.recovery attributes recovered=%q failed=%q", attr(recovery, "taskforge.recovery.recovered"), attr(recovery, "taskforge.recovery.failed"))
	}
}

func TestTracingDisabledStillRunsIntegration(t *testing.T) {
	previous := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(previous) })
	if _, err := telemetry.SetupTracing(context.Background(), telemetry.TracingConfig{}, nil); err != nil {
		t.Fatal(err)
	}
	env := newObsEnv(t, 3, 100*time.Millisecond)
	id := env.submit(t, "/v1/jobs", `{"kind":"demo.echo","payload":{"message":"untraced"}}`)
	env.runWorkerUntil(t, env.jobIs(t, id, job.StatusSucceeded))
	found, err := env.store.GetJob(context.Background(), id)
	if err != nil || found.TraceParent != nil {
		t.Fatalf("trace_parent=%v err=%v with tracing disabled", found.TraceParent, err)
	}
}

func TestExporterDownJobsStillRunIntegration(t *testing.T) {
	previous := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(previous) })
	env := newObsEnv(t, 3, 100*time.Millisecond)
	shutdown, err := telemetry.SetupTracing(context.Background(), telemetry.TracingConfig{
		Endpoint: "http://127.0.0.1:1", SampleRatio: 1, ExportTimeout: 200 * time.Millisecond, ServiceName: "test",
	}, env.metrics.ExportFailures())
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	jobID := env.submit(t, "/v1/jobs", `{"kind":"demo.echo","payload":{"message":"x"}}`)
	workflowID := env.submit(t, "/v1/workflows", `{"steps":[{"name":"a","kind":"demo.echo","payload":{"message":"a"}},{"name":"b","kind":"demo.echo","payload":{"message":"b"},"depends_on":["a"]}]}`)
	env.runWorkerUntil(t, func() bool {
		return env.jobIs(t, jobID, job.StatusSucceeded)() && env.workflowIs(t, workflowID, job.WorkflowSucceeded)()
	})
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Fatalf("work took %s with the exporter down", elapsed)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	shutdownStarted := time.Now()
	_ = shutdown(ctx)
	if elapsed := time.Since(shutdownStarted); elapsed > 2*time.Second {
		t.Fatalf("tracing shutdown took %s", elapsed)
	}
	if counterValue(t, env.registry, "taskforge_otel_export_failures_total") < 1 {
		t.Fatal("export failures were not counted")
	}
}

func counterValue(t *testing.T, registry *prometheus.Registry, name string, labels ...string) float64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	total := 0.0
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if matches(metric, labels) {
				total += metric.GetCounter().GetValue() + float64(metric.GetHistogram().GetSampleCount())
			}
		}
	}
	return total
}

func histogramSum(t *testing.T, registry *prometheus.Registry, name string) (float64, uint64) {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var sum float64
	var count uint64
	for _, family := range families {
		if family.GetName() == name {
			for _, metric := range family.GetMetric() {
				sum += metric.GetHistogram().GetSampleSum()
				count += metric.GetHistogram().GetSampleCount()
			}
		}
	}
	return sum, count
}

// matches reports whether metric has every name=value pair in labels.
func matches(metric *dto.Metric, labels []string) bool {
	for i := 0; i+1 < len(labels); i += 2 {
		found := false
		for _, label := range metric.GetLabel() {
			if label.GetName() == labels[i] && label.GetValue() == labels[i+1] {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func TestMetricsCardinalityIntegration(t *testing.T) {
	telemetrytest.Setup(t)
	env := newObsEnv(t, 1, 100*time.Millisecond)
	ids := []string{
		env.submit(t, "/v1/jobs", `{"queue":"custom.queue","kind":"demo.echo","payload":{"message":"q"}}`),
		env.submit(t, "/v1/workflows", `{"steps":[{"name":"a","kind":"demo.echo","payload":{"message":"a"}}]}`),
	}
	legacy, err := job.NewSubmission("default", "legacy.kind", json.RawMessage(`{}`), 0, nil, 1024)
	if err != nil {
		t.Fatal(err)
	}
	created, _, err := env.store.CreateJob(context.Background(), legacy)
	if err != nil {
		t.Fatal(err)
	}
	ids = append(ids, created.ID)
	env.runWorkerUntil(t, env.jobIs(t, created.ID, job.StatusFailed))
	families, err := env.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				value := label.GetValue()
				for _, id := range ids {
					if strings.Contains(value, id) {
						t.Fatalf("%s carries an entity ID in label %s", family.GetName(), label.GetName())
					}
				}
				if value == "custom.queue" || value == "legacy.kind" {
					t.Fatalf("%s carries unbounded value %q", family.GetName(), value)
				}
			}
		}
	}
	if counterValue(t, env.registry, "taskforge_job_outcomes_total", "kind", "other", "class", "platform") != 1 {
		t.Fatal("unknown-kind dead letter is not counted as kind=other, class=platform")
	}
}

func TestStartDelayMeasuredFromReadinessIntegration(t *testing.T) {
	telemetrytest.Setup(t)
	env := newObsEnv(t, 3, time.Second)
	delayed := env.submit(t, "/v1/jobs", `{"kind":"demo.echo","payload":{"message":"later"},"delay_seconds":2}`)
	retried := env.submit(t, "/v1/jobs", `{"kind":"demo.flaky","payload":{"fail_attempts":1}}`)
	env.runWorkerUntil(t, func() bool {
		return env.jobIs(t, delayed, job.StatusSucceeded)() && env.jobIs(t, retried, job.StatusSucceeded)()
	})
	sum, count := histogramSum(t, env.registry, "taskforge_job_start_delay_seconds")
	if count != 3 || sum >= 1 {
		t.Fatalf("start delay count=%d sum=%.3fs; a 2s delay and a 1s backoff must not count as start latency", count, sum)
	}
	failing := env.submit(t, "/v1/jobs", `{"kind":"demo.fail","payload":{}}`)
	env.runWorkerUntil(t, env.jobIs(t, failing, job.StatusFailed))
	if counterValue(t, env.registry, "taskforge_job_outcomes_total", "outcome", "dead_lettered", "class", "handler") != 1 {
		t.Fatal("demo.fail dead letter is not class=handler")
	}
}

func TestLeaseExpiryDeadLetterIsPlatformIntegration(t *testing.T) {
	telemetrytest.Setup(t)
	env := newObsEnv(t, 1, 100*time.Millisecond)
	env.submit(t, "/v1/jobs", `{"kind":"demo.echo","payload":{"message":"lost"}}`)
	if _, err := env.store.ClaimNextJob(context.Background(), "doomed", time.Second); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	if _, err := env.store.RecoverExpiredJobs(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	if counterValue(t, env.registry, "taskforge_job_outcomes_total", "outcome", "dead_lettered", "class", "platform") != 1 {
		t.Fatal("lease-expiry dead letter is not class=platform")
	}
}

func TestStalledWorkersRaiseOldestReadyAgeIntegration(t *testing.T) {
	env := newObsEnv(t, 3, 100*time.Millisecond)
	for range 3 {
		env.submit(t, "/v1/jobs", `{"kind":"demo.echo","payload":{"message":"waiting"}}`)
	}
	collector, refresh := telemetry.NewDBCollector(env.store.ObservabilitySnapshot, 200*time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)))
	registry := prometheus.NewRegistry()
	registry.MustRegister(collector)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go refresh(ctx)
	time.Sleep(2300 * time.Millisecond)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == "taskforge_oldest_ready_job_age_seconds" {
			if age := family.GetMetric()[0].GetGauge().GetValue(); age < 2 {
				t.Fatalf("oldest ready age=%.2fs with no worker running", age)
			}
			return
		}
	}
	t.Fatal("oldest ready age gauge missing")
}
