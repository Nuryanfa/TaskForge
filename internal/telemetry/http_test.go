package telemetry

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Nuryanfa/TaskForge/internal/config"
	"github.com/Nuryanfa/TaskForge/internal/telemetry/telemetrytest"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
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

// TestStartProcessFailsOnBusyAddress keeps one real-socket test for the
// production path: a busy metrics address fails startup with ErrMetricsListen.
func TestStartProcessFailsOnBusyAddress(t *testing.T) {
	previousProvider := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(previousProvider) })
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	t.Setenv("TASKFORGE_METRICS_ADDR", busy.Addr().String())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err = StartProcess(ctx, config.Config{}, "test", "127.0.0.1:0", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if !errors.Is(err, ErrMetricsListen) {
		t.Fatalf("StartProcess on a busy address returned %v, want ErrMetricsListen", err)
	}
}

func TestStartProcessReportsTracingSetupFailure(t *testing.T) {
	previousProvider := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(previousProvider) })
	t.Setenv("TASKFORGE_METRICS_ADDR", "")
	_, err := StartProcess(context.Background(), config.Config{OTelEndpoint: "::not a url"}, "test", "", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if !errors.Is(err, ErrTracingSetup) {
		t.Fatalf("StartProcess with a bad trace endpoint returned %v, want ErrTracingSetup", err)
	}
}

// TestMetricsHandlerServesOnlyMetrics routes in memory, without a socket.
func TestMetricsHandlerServesOnlyMetrics(t *testing.T) {
	m, registry := newTestMetrics(t)
	m.LeaseRecovery("recovered", 1)
	handler := metricsHandler(registry)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "taskforge_lease_recoveries_total") {
		t.Fatalf("GET /metrics status=%d", response.Code)
	}
	for _, path := range []string{"/", "/healthz", "/v1/jobs"} {
		other := httptest.NewRecorder()
		handler.ServeHTTP(other, httptest.NewRequest(http.MethodGet, path, nil))
		if other.Code != http.StatusNotFound {
			t.Fatalf("GET %s status=%d, want 404", path, other.Code)
		}
	}
}

func panicMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /boom", func(w http.ResponseWriter, r *http.Request) { panic("handler bug") })
	mux.HandleFunc("GET /boom-late", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		panic("handler bug after the header")
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {})
	return mux
}

// TestMiddlewareCountsPanicAs5xx: a handler panic is a platform error the
// availability SLI must see, not a silently dropped connection.
func TestMiddlewareCountsPanicAs5xx(t *testing.T) {
	exporter := telemetrytest.Setup(t)
	m, _ := newTestMetrics(t)
	response := httptest.NewRecorder()
	Middleware(m, panicMux()).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/boom", nil))
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d, want 500", response.Code)
	}
	if got := testutil.ToFloat64(m.httpRequests.WithLabelValues("GET /boom", "500")); got != 1 {
		t.Fatalf("500 count=%v", got)
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].Status.Code != codes.Error {
		t.Fatalf("spans=%d status=%+v, want one error span", len(spans), spans)
	}
}

// TestMiddlewareCountsPanicAfterHeaderAs5xx: the client already received a
// 200 header and then a truncated body, so the request is still a platform
// failure; the connection is aborted rather than counted as a success.
func TestMiddlewareCountsPanicAfterHeaderAs5xx(t *testing.T) {
	exporter := telemetrytest.Setup(t)
	m, _ := newTestMetrics(t)
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		Middleware(m, panicMux()).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/boom-late", nil))
	}()
	if recovered != http.ErrAbortHandler {
		t.Fatalf("recovered %v, want http.ErrAbortHandler", recovered)
	}
	if got := testutil.ToFloat64(m.httpRequests.WithLabelValues("GET /boom-late", "500")); got != 1 {
		t.Fatalf("500 count=%v", got)
	}
	if got := testutil.ToFloat64(m.httpRequests.WithLabelValues("GET /boom-late", "200")); got != 0 {
		t.Fatalf("200 count=%v, want 0", got)
	}
	if spans := exporter.GetSpans(); len(spans) != 1 || spans[0].Status.Code != codes.Error {
		t.Fatalf("ended spans=%+v, want one error span", spans)
	}
}

// TestMiddlewareDoesNotTraceProbes: liveness and readiness probes are still
// counted but never start a root span.
func TestMiddlewareDoesNotTraceProbes(t *testing.T) {
	exporter := telemetrytest.Setup(t)
	m, _ := newTestMetrics(t)
	handler := Middleware(m, panicMux())
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/healthz", nil))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if spans := exporter.GetSpans(); len(spans) != 0 {
		t.Fatalf("probes produced %d spans", len(spans))
	}
	if got := testutil.ToFloat64(m.httpRequests.WithLabelValues("GET /healthz", "200")); got != 1 {
		t.Fatalf("healthz count=%v", got)
	}
}

func flushThenPanicMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /stream", func(w http.ResponseWriter, r *http.Request) {
		w.(http.Flusher).Flush()
		panic("handler bug after a flush")
	})
	mux.HandleFunc("GET /abort", func(w http.ResponseWriter, r *http.Request) { panic(http.ErrAbortHandler) })
	return mux
}

func recoverServe(handler http.Handler, path string) (recovered any) {
	defer func() { recovered = recover() }()
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
	return nil
}

// TestMiddlewareLogsAndRecordsPanic: a recovered panic leaves a fixed log
// event with a stack trace and an exception event on the span, and never
// logs the panic value (it could carry request data).
func TestMiddlewareLogsAndRecordsPanic(t *testing.T) {
	exporter := telemetrytest.Setup(t)
	m, _ := newTestMetrics(t)
	var logs bytes.Buffer
	var wrote bool
	handler := MiddlewareWith(m, panicMux(), MiddlewareOptions{
		Logger: slog.New(slog.NewJSONHandler(&logs, nil)),
		OnPanic: func(w http.ResponseWriter, r *http.Request) {
			wrote = true
			w.WriteHeader(http.StatusInternalServerError)
		},
	})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/boom", nil))
	if response.Code != http.StatusInternalServerError || !wrote {
		t.Fatalf("status=%d OnPanic called=%v", response.Code, wrote)
	}
	text := logs.String()
	if !strings.Contains(text, `"msg":"http_handler_panic"`) || !strings.Contains(text, `"route":"GET /boom"`) ||
		!strings.Contains(text, `"stack":"goroutine`) || !strings.Contains(text, `"trace_id":"`) {
		t.Fatalf("panic log=%s", text)
	}
	if strings.Contains(text, "handler bug") {
		t.Fatalf("panic value leaked into the log: %s", text)
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 || len(spans[0].Events) != 1 || spans[0].Events[0].Name != "exception" {
		t.Fatalf("span events=%+v, want one exception event", spans)
	}
}

// TestMiddlewareCountsPanicAfterFlushAs5xx: a flush sends the header, so a
// later panic must abort the connection, not try to write a 500.
func TestMiddlewareCountsPanicAfterFlushAs5xx(t *testing.T) {
	telemetrytest.Setup(t)
	m, _ := newTestMetrics(t)
	if got := recoverServe(Middleware(m, flushThenPanicMux()), "/stream"); got != http.ErrAbortHandler {
		t.Fatalf("recovered %v, want http.ErrAbortHandler", got)
	}
	if got := testutil.ToFloat64(m.httpRequests.WithLabelValues("GET /stream", "500")); got != 1 {
		t.Fatalf("500 count=%v", got)
	}
}

// TestMiddlewareKeepsDeliberateAbort: a handler that aborts on purpose keeps
// aborting instead of being answered with a 500.
func TestMiddlewareKeepsDeliberateAbort(t *testing.T) {
	telemetrytest.Setup(t)
	m, _ := newTestMetrics(t)
	if got := recoverServe(Middleware(m, flushThenPanicMux()), "/abort"); got != http.ErrAbortHandler {
		t.Fatalf("recovered %v, want http.ErrAbortHandler", got)
	}
}
