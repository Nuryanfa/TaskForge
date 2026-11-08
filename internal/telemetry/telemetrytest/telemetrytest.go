// Package telemetrytest installs an in-memory OpenTelemetry tracer provider
// for tests. It lives apart from internal/telemetry so production binaries do
// not link the testing package.
package telemetrytest

import (
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// Setup installs a provider that samples everything and exports
// synchronously to the returned in-memory exporter. The previous global
// provider and propagator are restored when the test ends. Tests using it
// must not run in parallel (the provider is global).
func Setup(t testing.TB) *tracetest.InMemoryExporter {
	return SetupWithRatio(t, 1)
}

// SetupWithRatio is Setup with a parent-based root sampling ratio.
func SetupWithRatio(t testing.TB, ratio float64) *tracetest.InMemoryExporter {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
	)
	previousProvider := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		_ = provider.Shutdown(t.Context())
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
	})
	return exporter
}
