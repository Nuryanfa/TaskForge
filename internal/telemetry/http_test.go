package telemetry

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Nuryanfa/TaskForge/internal/telemetry/telemetrytest"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func testMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/jobs/{id}", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("GET /fail", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) })
	mux.HandleFunc("GET /stream", func(w http.ResponseWriter, r *http.Request) {
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNotFound) })
	return mux
}

func TestMiddlewareCapturesStatusCode(t *testing.T) {
	exporter := telemetrytest.Setup(t)
	m, _ := newTestMetrics(t)
	handler := Middleware(m, testMux())
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/fail", nil))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/jobs/abc", nil))
	if got := testutil.ToFloat64(m.httpRequests.WithLabelValues("GET /fail", "503")); got != 1 {
		t.Fatalf("503 count=%v", got)
	}
	if got := testutil.ToFloat64(m.httpRequests.WithLabelValues("GET /v1/jobs/{id}", "200")); got != 1 {
		t.Fatalf("implicit 200 count=%v", got)
	}
	var sawStatus bool
	for _, span := range exporter.GetSpans() {
		for _, kv := range span.Attributes {
			if kv.Key == "http.response.status_code" && kv.Value.AsInt64() == 503 {
				sawStatus = true
			}
		}
	}
	if !sawStatus {
		t.Fatal("status code not recorded on the server span")
	}
}

type hijackableRecorder struct {
	*httptest.ResponseRecorder
	hijacked bool
}

func (h *hijackableRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h.hijacked = true
	return nil, nil, errors.New("test hijack")
}

func TestMiddlewarePreservesFlusherAndHijacker(t *testing.T) {
	telemetrytest.Setup(t)
	m, _ := newTestMetrics(t)
	recorder := httptest.NewRecorder()
	Middleware(m, testMux()).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/stream", nil))
	if !recorder.Flushed {
		t.Fatal("Flush did not reach the underlying writer")
	}
	hijackable := &hijackableRecorder{ResponseRecorder: httptest.NewRecorder()}
	handler := Middleware(m, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Fatal("wrapped writer lost http.Hijacker")
		}
		_, _, _ = hijacker.Hijack()
	}))
	handler.ServeHTTP(hijackable, httptest.NewRequest(http.MethodGet, "/x", nil))
	if !hijackable.hijacked {
		t.Fatal("Hijack did not reach the underlying writer")
	}
}

func TestMiddlewareUntrustedTraceparentBecomesLink(t *testing.T) {
	exporter := telemetrytest.Setup(t)
	m, _ := newTestMetrics(t)
	request := httptest.NewRequest(http.MethodGet, "/v1/jobs/abc", nil)
	request.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	Middleware(m, testMux()).ServeHTTP(httptest.NewRecorder(), request)
	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("spans=%d", len(spans))
	}
	span := spans[0]
	if span.SpanContext.TraceID().String() == "4bf92f3577b34da6a3ce929d0e0e4736" || span.Parent.IsValid() {
		t.Fatal("client traceparent was used as parent")
	}
	if len(span.Links) != 1 || span.Links[0].SpanContext.TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("links=%+v", span.Links)
	}
}

func TestMiddlewareRouteLabels(t *testing.T) {
	telemetrytest.Setup(t)
	m, registry := newTestMetrics(t)
	handler := Middleware(m, testMux())
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/jobs/8f1d2c4e", nil))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/nope/123", nil))
	if got := testutil.ToFloat64(m.httpRequests.WithLabelValues("unmatched", "404")); got != 1 {
		t.Fatalf("unmatched count=%v", got)
	}
	families, _ := registry.Gather()
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if strings.Contains(label.GetValue(), "8f1d2c4e") || strings.Contains(label.GetValue(), "/nope") {
					t.Fatalf("raw path leaked into %s label %s", family.GetName(), label.GetValue())
				}
			}
		}
	}
}

func TestServeMetricsFailsOnBusyAddress(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	_, registry := newTestMetrics(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := ServeMetrics(ctx, busy.Addr().String(), registry); err == nil {
		t.Fatal("ServeMetrics succeeded on a busy address")
	}
}

func TestServeMetricsServesOnlyMetrics(t *testing.T) {
	m, registry := newTestMetrics(t)
	m.LeaseRecovery("recovered", 1)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveMetricsOn(ctx, listener, registry) }()
	base := "http://" + listener.Addr().String()
	response, err := http.Get(base + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body := new(strings.Builder)
	_, _ = bufio.NewReader(response.Body).WriteTo(body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || !strings.Contains(body.String(), "taskforge_lease_recoveries_total") {
		t.Fatalf("metrics status=%d", response.StatusCode)
	}
	other, err := http.Get(base + "/")
	if err != nil {
		t.Fatal(err)
	}
	other.Body.Close()
	if other.StatusCode != http.StatusNotFound {
		t.Fatalf("/ status=%d", other.StatusCode)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("serve returned %v after shutdown", err)
	}
}
