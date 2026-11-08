// Package tracectx converts between OpenTelemetry span contexts and the W3C
// traceparent strings TaskForge stores with jobs and workflows. It depends
// only on the OpenTelemetry API, never on the SDK or an exporter, so the
// store can use it without pulling telemetry plumbing into the data layer.
package tracectx

import (
	"context"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// TracerName names every TaskForge span's instrumentation scope.
const TracerName = "github.com/Nuryanfa/TaskForge"

var propagator = propagation.TraceContext{}

// FromContext returns the traceparent of the span context in ctx, or nil when
// ctx carries no valid span context (for example when tracing is disabled).
func FromContext(ctx context.Context) *string {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return nil
	}
	carrier := propagation.MapCarrier{}
	propagator.Inject(ctx, carrier)
	value := carrier.Get("traceparent")
	if value == "" {
		return nil
	}
	return &value
}

// SpanContext parses a stored traceparent into a remote span context. It
// returns false for malformed values and for all-zero trace or span IDs.
func SpanContext(traceparent string) (trace.SpanContext, bool) {
	ctx := propagator.Extract(context.Background(), propagation.MapCarrier{"traceparent": traceparent})
	spanContext := trace.SpanContextFromContext(ctx)
	return spanContext, spanContext.IsValid()
}
