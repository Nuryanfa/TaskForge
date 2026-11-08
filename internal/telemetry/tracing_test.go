package telemetry

import (
	"bytes"
	"context"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/job"
	"github.com/Nuryanfa/TaskForge/internal/telemetry/telemetrytest"
	"github.com/Nuryanfa/TaskForge/internal/tracectx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const storedParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

func claimedJob(traceParent *string, link bool) job.Claimed {
	return job.Claimed{
		Job: job.Job{ID: "11111111-1111-4111-8111-111111111111", Kind: "demo.echo", Queue: "default",
			MaxAttempts: 3, TraceParent: traceParent, TraceLink: link},
		Execution: job.Execution{JobID: "11111111-1111-4111-8111-111111111111", Attempt: 2},
	}
}

func stored() *string { value := storedParent; return &value }

func onlySpan(t *testing.T, spans int) {
	t.Helper()
	if spans != 1 {
		t.Fatalf("recorded %d spans, want 1", spans)
	}
}

func TestStartAttemptChildOfStoredContext(t *testing.T) {
	exporter := telemetrytest.Setup(t)
	_, span := StartAttempt(context.Background(), claimedJob(stored(), false))
	span.End()
	spans := exporter.GetSpans()
	onlySpan(t, len(spans))
	parent, _ := tracectx.SpanContext(storedParent)
	got := spans[0]
	if got.Name != "job.attempt" || got.SpanKind != trace.SpanKindConsumer ||
		got.SpanContext.TraceID() != parent.TraceID() || got.Parent.SpanID() != parent.SpanID() || len(got.Links) != 0 {
		t.Fatalf("span=%+v", got)
	}
	attrs := map[string]any{}
	for _, kv := range got.Attributes {
		attrs[string(kv.Key)] = kv.Value.AsInterface()
	}
	if attrs["taskforge.job.attempt"] != int64(2) || attrs["taskforge.job.kind"] != "demo.echo" ||
		attrs["taskforge.job.max_attempts"] != int64(3) || attrs["taskforge.job.id"] == nil {
		t.Fatalf("attributes=%v", attrs)
	}
}

func TestStartAttemptLinksForDelayedJob(t *testing.T) {
	exporter := telemetrytest.Setup(t)
	_, span := StartAttempt(context.Background(), claimedJob(stored(), true))
	span.End()
	spans := exporter.GetSpans()
	onlySpan(t, len(spans))
	parent, _ := tracectx.SpanContext(storedParent)
	got := spans[0]
	if got.SpanContext.TraceID() == parent.TraceID() || got.Parent.IsValid() || len(got.Links) != 1 ||
		got.Links[0].SpanContext.SpanID() != parent.SpanID() {
		t.Fatalf("delayed attempt span=%+v", got)
	}
}

func TestStartAttemptNewRootWithoutContext(t *testing.T) {
	exporter := telemetrytest.Setup(t)
	_, span := StartAttempt(context.Background(), claimedJob(nil, false))
	span.End()
	spans := exporter.GetSpans()
	onlySpan(t, len(spans))
	if spans[0].Parent.IsValid() || len(spans[0].Links) != 0 {
		t.Fatalf("span without stored context=%+v", spans[0])
	}
}

func TestStartAttemptWorkflowAttributes(t *testing.T) {
	exporter := telemetrytest.Setup(t)
	claimed := claimedJob(stored(), false)
	claimed.Execution.Workflow = &job.WorkflowContext{WorkflowID: "wf-1", Step: "charge", Role: job.RoleCompensation}
	_, span := StartAttempt(context.Background(), claimed)
	span.End()
	attrs := map[string]any{}
	for _, kv := range exporter.GetSpans()[0].Attributes {
		attrs[string(kv.Key)] = kv.Value.AsInterface()
	}
	if attrs["taskforge.workflow.id"] != "wf-1" || attrs["taskforge.workflow.step"] != "charge" || attrs["taskforge.workflow.role"] != "compensation" {
		t.Fatalf("attributes=%v", attrs)
	}
}

func TestSampledOutContextStillPropagates(t *testing.T) {
	exporter := telemetrytest.SetupWithRatio(t, 0)
	ctx, span := StartAttempt(context.Background(), claimedJob(nil, false))
	span.End()
	if len(exporter.GetSpans()) != 0 {
		t.Fatal("a sampled-out span was exported")
	}
	encoded := tracectx.FromContext(ctx)
	if encoded == nil || (*encoded)[len(*encoded)-2:] != "00" {
		t.Fatalf("sampled-out context did not propagate with flags 00: %v", encoded)
	}
}

func TestShutdownBoundedByHangingExporter(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-time.After(10 * time.Second):
		}
	}))
	defer server.Close()
	defer close(release)
	failures := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_export_failures_total"})
	shutdown, err := SetupTracing(context.Background(), TracingConfig{
		Endpoint: server.URL, SampleRatio: 1, ExportTimeout: 200 * time.Millisecond, ServiceName: "test",
	}, failures)
	if err != nil {
		t.Fatal(err)
	}
	_, span := StartAttempt(context.Background(), claimedJob(nil, false))
	span.End()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	started := time.Now()
	_ = shutdown(ctx)
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("shutdown took %s", elapsed)
	}
	if testutil.ToFloat64(failures) < 1 {
		t.Fatal("export failure was not counted")
	}
}

func TestExportErrorsAreNotLoggedRaw(t *testing.T) {
	previousProvider := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(previousProvider) })
	var raw bytes.Buffer
	log.SetOutput(&raw)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	var structured bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&structured, nil))
	shutdown, err := SetupTracing(context.Background(), TracingConfig{
		Endpoint: "http://127.0.0.1:1", SampleRatio: 1, ExportTimeout: 100 * time.Millisecond,
		BatchTimeout: 50 * time.Millisecond, ServiceName: "test", Logger: logger,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		_, span := StartAttempt(context.Background(), claimedJob(nil, false))
		span.End()
		time.Sleep(200 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = shutdown(ctx)
	if strings.Contains(raw.String(), "traces export") {
		t.Fatalf("raw export errors reached the standard logger:\n%s", raw.String())
	}
	if count := strings.Count(structured.String(), `"msg":"otel_export_failed"`); count != 1 {
		t.Fatalf("structured export failure logs=%d, want exactly 1 per minute:\n%s", count, structured.String())
	}
}

func TestNoopTracingDoesNotForwardStoredParent(t *testing.T) {
	previousProvider := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(previousProvider) })
	if _, err := SetupTracing(context.Background(), TracingConfig{}, nil); err != nil {
		t.Fatal(err)
	}
	ctx, span := StartAttempt(context.Background(), claimedJob(stored(), false))
	span.End()
	if got := tracectx.FromContext(ctx); got != nil {
		t.Fatalf("tracing disabled but the attempt forwarded stored parent %s", *got)
	}
}

// TestShutdownBoundedWithManyQueuedBatches queues several batches against a
// collector that accepts connections but never answers: the flush as a whole,
// not each batch, must stay within the export timeout.
func TestShutdownBoundedWithManyQueuedBatches(t *testing.T) {
	previousProvider := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(previousProvider) })
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-time.After(30 * time.Second):
		}
	}))
	defer server.Close()
	defer close(release)
	shutdown, err := SetupTracing(context.Background(), TracingConfig{
		Endpoint: server.URL, SampleRatio: 1, ExportTimeout: 300 * time.Millisecond,
		BatchTimeout: time.Hour, ServiceName: "test",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for range 2000 {
		_, span := StartAttempt(context.Background(), claimedJob(nil, false))
		span.End()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	started := time.Now()
	_ = shutdown(ctx)
	if elapsed := time.Since(started); elapsed > 800*time.Millisecond {
		t.Fatalf("flushing 4 batches to a hanging collector took %s; the export timeout is 300ms", elapsed)
	}
}
