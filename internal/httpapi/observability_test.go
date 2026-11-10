package httpapi

import (
	"net/http"
	"testing"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/job"
	"github.com/Nuryanfa/TaskForge/internal/telemetry"
	"github.com/Nuryanfa/TaskForge/internal/telemetry/telemetrytest"
	"github.com/Nuryanfa/TaskForge/internal/tracectx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func instrumentedHandler(t *testing.T, store *fakeStore) (http.Handler, *prometheus.Registry) {
	t.Helper()
	registry := prometheus.NewRegistry()
	metrics := telemetry.NewMetrics(registry, []string{"default"})
	return NewWithOptions(store, Options{PayloadMaxBytes: 4096, ReadinessTimeout: time.Second, Metrics: metrics}), registry
}

func submittedTotal(t *testing.T, registry *prometheus.Registry) float64 {
	t.Helper()
	return testutil.ToFloat64(collectorFor(t, registry, "taskforge_jobs_submitted_total"))
}

// collectorFor sums a gathered counter family across label sets.
func collectorFor(t *testing.T, registry *prometheus.Registry, name string) prometheus.Collector {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	total := 0.0
	for _, family := range families {
		if family.GetName() == name {
			for _, metric := range family.GetMetric() {
				total += metric.GetCounter().GetValue()
			}
		}
	}
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "sum"})
	gauge.Set(total)
	return gauge
}

func TestAPIDoesNotServeMetrics(t *testing.T) {
	handler, _ := instrumentedHandler(t, &fakeStore{})
	response := serve(handler, http.MethodGet, "/metrics", "")
	if response.Code != http.StatusNotFound || errorCode(t, response) != "ROUTE_NOT_FOUND" {
		t.Fatalf("GET /metrics on the API listener status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestReplayDoesNotCountAsSubmission(t *testing.T) {
	telemetrytest.Setup(t)
	store := &fakeStore{created: true, value: job.Job{ID: "00000000-0000-4000-8000-000000000001", Queue: "default", Kind: "demo.echo", Status: job.StatusQueued},
		workflow: sampleWorkflow(), workflowCreated: true}
	handler, registry := instrumentedHandler(t, store)
	body := `{"kind":"demo.echo","payload":{"message":"x"},"idempotency_key":"k1"}`
	if response := serve(handler, http.MethodPost, "/v1/jobs", body); response.Code != http.StatusCreated {
		t.Fatalf("first submit status=%d", response.Code)
	}
	store.created = false
	if response := serve(handler, http.MethodPost, "/v1/jobs", body); response.Code != http.StatusOK {
		t.Fatalf("replay status=%d", response.Code)
	}
	if got := submittedTotal(t, registry); got != 1 {
		t.Fatalf("jobs submitted=%v after create + replay, want 1", got)
	}
	workflowBody := `{"idempotency_key":"w1","steps":[{"name":"a","kind":"demo.echo","payload":{"message":"x"}}]}`
	serve(handler, http.MethodPost, "/v1/workflows", workflowBody)
	store.workflowCreated = false
	serve(handler, http.MethodPost, "/v1/workflows", workflowBody)
	if got := submittedTotal(t, registry); got != 1 {
		t.Fatalf("workflow submissions changed the job submission counter: %v", got)
	}
}

func TestEnqueueSpanIsStoredAsTraceParent(t *testing.T) {
	exporter := telemetrytest.Setup(t)
	store := &fakeStore{created: true, value: job.Job{ID: "00000000-0000-4000-8000-000000000001", Status: job.StatusQueued}}
	handler, _ := instrumentedHandler(t, store)
	if response := serve(handler, http.MethodPost, "/v1/jobs", `{"kind":"demo.echo","payload":{"message":"x"}}`); response.Code != http.StatusCreated {
		t.Fatalf("status=%d", response.Code)
	}
	stored := tracectx.FromContext(store.lastCtx)
	if stored == nil {
		t.Fatal("store received no span context")
	}
	storedContext, _ := tracectx.SpanContext(*stored)
	for _, span := range exporter.GetSpans() {
		if span.Name == "job.enqueue" {
			if span.SpanContext.SpanID() != storedContext.SpanID() || !span.Parent.IsValid() {
				t.Fatalf("stored context %s is not the job.enqueue span %s", storedContext.SpanID(), span.SpanContext.SpanID())
			}
			return
		}
	}
	t.Fatal("no job.enqueue span recorded")
}
