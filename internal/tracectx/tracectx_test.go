package tracectx

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func fixedSpanContext(sampled bool) trace.SpanContext {
	flags := trace.TraceFlags(0)
	if sampled {
		flags = trace.FlagsSampled
	}
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e, 0x47, 0x36},
		SpanID:     trace.SpanID{0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7},
		TraceFlags: flags,
	})
}

func TestSpanContextRoundTrip(t *testing.T) {
	for _, sampled := range []bool{true, false} {
		want := fixedSpanContext(sampled)
		encoded := FromContext(trace.ContextWithSpanContext(context.Background(), want))
		if encoded == nil {
			t.Fatal("FromContext returned nil for a valid span context")
		}
		got, ok := SpanContext(*encoded)
		if !ok || got.TraceID() != want.TraceID() || got.SpanID() != want.SpanID() ||
			got.IsSampled() != sampled || !got.IsRemote() {
			t.Fatalf("round trip %q -> %+v ok=%v", *encoded, got, ok)
		}
	}
}

func TestSpanContextRejectsZeroIDs(t *testing.T) {
	for _, value := range []string{
		"00-00000000000000000000000000000000-0000000000000001-01",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
		"garbage",
		"",
	} {
		if _, ok := SpanContext(value); ok {
			t.Fatalf("SpanContext(%q) accepted an invalid context", value)
		}
	}
	if FromContext(context.Background()) != nil {
		t.Fatal("FromContext of a background context must be nil")
	}
}
