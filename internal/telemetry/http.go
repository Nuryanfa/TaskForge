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
//
// A handler panic is counted as a 500 and marks the span as an error: before
// any header was written the client gets a bare 500; after it, the panic is
// re-raised as http.ErrAbortHandler so net/http aborts the truncated response
// quietly. Liveness and readiness probes are counted but never traced, so
// frequent probes do not flood the trace store with root spans. m may be nil
// (tracing without metrics).
func Middleware(m *Metrics, next http.Handler) http.Handler {
	propagator := propagation.TraceContext{}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		ctx, span := r.Context(), trace.SpanFromContext(r.Context())
		traced := r.URL.Path != "/healthz" && r.URL.Path != "/readyz"
		if traced {
			options := []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindServer), trace.WithNewRoot()}
			if incoming := trace.SpanContextFromContext(propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))); incoming.IsValid() {
				options = append(options, trace.WithLinks(trace.Link{SpanContext: incoming}))
			}
			ctx, span = otel.Tracer(TracerName).Start(r.Context(), "http.request", options...)
		}
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		routed := r.WithContext(ctx)
		defer func() {
			recovered := recover()
			status := recorder.status
			if recovered != nil {
				status = http.StatusInternalServerError
			}
			route := routed.Pattern
			if route == "" || route == "/" {
				route = unmatchedRoute
			}
			if traced {
				span.SetName(route)
				span.SetAttributes(attribute.String("http.route", route), attribute.Int("http.response.status_code", status))
				if status >= http.StatusInternalServerError {
					span.SetStatus(codes.Error, http.StatusText(status))
				}
				span.End()
			}
			if m != nil {
				m.httpRequests.WithLabelValues(route, strconv.Itoa(status)).Inc()
				m.httpDuration.WithLabelValues(route).Observe(time.Since(started).Seconds())
			}
			if recovered == nil {
				return
			}
			if recorder.wroteHeader {
				panic(http.ErrAbortHandler)
			}
			w.WriteHeader(http.StatusInternalServerError)
		}()
		next.ServeHTTP(recorder, routed)
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
