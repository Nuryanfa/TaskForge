package telemetry

import (
	"bufio"
	"errors"
	"net"
	"net/http"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const unmatchedRoute = "unmatched"

// Middleware traces and measures every request. The API is a public
// endpoint: a client-supplied traceparent is never used as the parent (a
// client could force sampling or join someone else's trace); it is attached
// as a link to a new root server span instead. It must wrap the ServeMux
// directly so the matched route pattern is visible after routing.
func Middleware(m *Metrics, next http.Handler) http.Handler {
	propagator := propagation.TraceContext{}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		options := []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindServer), trace.WithNewRoot()}
		if incoming := trace.SpanContextFromContext(propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))); incoming.IsValid() {
			options = append(options, trace.WithLinks(trace.Link{SpanContext: incoming}))
		}
		ctx, span := otel.Tracer(TracerName).Start(r.Context(), "http.request", options...)
		defer span.End()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		routed := r.WithContext(ctx)
		next.ServeHTTP(recorder, routed)

		route := routed.Pattern
		if route == "" || route == "/" {
			route = unmatchedRoute
		}
		span.SetName(route)
		span.SetAttributes(attribute.String("http.route", route), attribute.Int("http.response.status_code", recorder.status))
		if recorder.status >= http.StatusInternalServerError {
			span.SetStatus(codes.Error, http.StatusText(recorder.status))
		}
		if m != nil {
			m.httpRequests.WithLabelValues(route, strconv.Itoa(recorder.status)).Inc()
			m.httpDuration.WithLabelValues(route).Observe(time.Since(started).Seconds())
		}
	})
}

// statusRecorder captures the response status while preserving the optional
// interfaces handlers rely on (Flusher, Hijacker) and http.ResponseController
// (via Unwrap).
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (s *statusRecorder) WriteHeader(status int) {
	if !s.wroteHeader {
		s.status, s.wroteHeader = status, true
	}
	s.ResponseWriter.WriteHeader(status)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	s.wroteHeader = true
	return s.ResponseWriter.Write(b)
}

func (s *statusRecorder) Flush() {
	if flusher, ok := s.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (s *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hijacker, ok := s.ResponseWriter.(http.Hijacker); ok {
		return hijacker.Hijack()
	}
	return nil, nil, errors.New("response writer does not support hijacking")
}

func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }
