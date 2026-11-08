// Package telemetry owns TaskForge's tracing and metrics plumbing: tracer
// provider setup, the job attempt span rule, bounded Prometheus metrics, the
// HTTP middleware, and the metrics listener. Telemetry never fails work: an
// unavailable exporter only increments a counter.
package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/job"
	"github.com/Nuryanfa/TaskForge/internal/tracectx"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// TracerName names every TaskForge span's instrumentation scope.
const TracerName = tracectx.TracerName

const maxQueuedSpans = 2048

// TracingConfig configures span export. An empty Endpoint disables tracing.
type TracingConfig struct {
	Endpoint      string
	SampleRatio   float64
	ExportTimeout time.Duration
	ServiceName   string
	// BatchTimeout is the batch processor's export interval (default 5s).
	BatchTimeout time.Duration
	// Logger receives the rate-limited otel_export_failed event; nil uses
	// slog.Default().
	Logger *slog.Logger
}

// SetupTracing installs the global tracer provider and the W3C TraceContext
// propagator. With no endpoint it installs a no-op provider. Export is
// asynchronous with a bounded, drop-on-full queue; every failed export
// increments failures. The returned shutdown flushes within ctx's deadline.
func SetupTracing(ctx context.Context, cfg TracingConfig, failures prometheus.Counter) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	// Export failures are counted and logged (rate-limited, structured) by
	// countingExporter; OTel's default handler would print every failed batch
	// as a raw, unstructured line.
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(error) {}))
	if cfg.Endpoint == "" {
		otel.SetTracerProvider(noop.NewTracerProvider())
		return func(context.Context) error { return nil }, nil
	}
	endpoint, err := tracesURL(cfg.Endpoint)
	if err != nil {
		return nil, err
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	batchTimeout := cfg.BatchTimeout
	if batchTimeout <= 0 {
		batchTimeout = 5 * time.Second
	}
	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(endpoint),
		otlptracehttp.WithTimeout(cfg.ExportTimeout),
		otlptracehttp.WithRetry(otlptracehttp.RetryConfig{Enabled: false}),
	)
	if err != nil {
		return nil, fmt.Errorf("create trace exporter: %w", err)
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(&countingExporter{inner: exporter, failures: failures, logger: logger},
			sdktrace.WithMaxQueueSize(maxQueuedSpans),
			sdktrace.WithExportTimeout(cfg.ExportTimeout),
			sdktrace.WithBatchTimeout(batchTimeout),
		),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))),
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", cfg.ServiceName))),
	)
	otel.SetTracerProvider(provider)
	return provider.Shutdown, nil
}

func tracesURL(endpoint string) (string, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" {
		return "", fmt.Errorf("invalid trace endpoint")
	}
	if parsed.Path == "" || parsed.Path == "/" {
		parsed.Path = "/v1/traces"
	}
	return parsed.String(), nil
}

// countingExporter counts failed exports and logs them at most once a minute.
type countingExporter struct {
	inner    sdktrace.SpanExporter
	failures prometheus.Counter
	logger   *slog.Logger
	mu       sync.Mutex
	lastLog  time.Time
}

func (e *countingExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	err := e.inner.ExportSpans(ctx, spans)
	if err != nil {
		if e.failures != nil {
			e.failures.Inc()
		}
		e.mu.Lock()
		if time.Since(e.lastLog) > time.Minute {
			e.lastLog = time.Now()
			e.logger.Warn("otel_export_failed", "spans", len(spans))
		}
		e.mu.Unlock()
	}
	return err
}

func (e *countingExporter) Shutdown(ctx context.Context) error { return e.inner.Shutdown(ctx) }

// StartAttempt starts the job.attempt span for a claimed job (ADR 0013):
// a child of the stored context, a new root linked to it for client-delayed
// jobs (TraceLink), or a new root when no valid context was stored.
func StartAttempt(ctx context.Context, claimed job.Claimed) (context.Context, trace.Span) {
	attributes := []attribute.KeyValue{
		attribute.String("taskforge.job.id", claimed.Job.ID),
		attribute.String("taskforge.job.kind", claimed.Job.Kind),
		attribute.String("taskforge.job.queue", claimed.Job.Queue),
		attribute.Int("taskforge.job.attempt", claimed.Execution.Attempt),
		attribute.Int("taskforge.job.max_attempts", claimed.Job.MaxAttempts),
	}
	if wf := claimed.Execution.Workflow; wf != nil {
		attributes = append(attributes,
			attribute.String("taskforge.workflow.id", wf.WorkflowID),
			attribute.String("taskforge.workflow.step", wf.Step),
			attribute.String("taskforge.workflow.role", wf.Role))
	}
	options := []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(attributes...)}
	parent, ok := trace.SpanContext{}, false
	if claimed.Job.TraceParent != nil {
		parent, ok = tracectx.SpanContext(*claimed.Job.TraceParent)
	}
	if _, disabled := otel.GetTracerProvider().(noop.TracerProvider); disabled {
		// With tracing off a no-op span would carry the stored parent forward
		// and the store would copy it onto dispatched jobs; stay context-free.
		ok = false
	}
	switch {
	case ok && !claimed.Job.TraceLink:
		ctx = trace.ContextWithRemoteSpanContext(ctx, parent)
	case ok:
		options = append(options, trace.WithNewRoot(), trace.WithLinks(trace.Link{SpanContext: parent}))
	default:
		options = append(options, trace.WithNewRoot())
	}
	return otel.Tracer(TracerName).Start(ctx, "job.attempt", options...)
}

// TraceID returns the hex trace ID of the span in ctx, or "" when none, for
// correlating error logs with traces.
func TraceID(ctx context.Context) string {
	spanContext := trace.SpanContextFromContext(ctx)
	if !spanContext.IsValid() {
		return ""
	}
	return strings.ToLower(spanContext.TraceID().String())
}
