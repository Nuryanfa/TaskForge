package telemetry

import (
	"bufio"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const unmatchedRoute = "unmatched"

// MiddlewareOptions configures MiddlewareWith.
type MiddlewareOptions struct {
	// Logger receives the http_handler_panic event; nil uses slog.Default().
	Logger *slog.Logger
	// OnPanic writes the response for a panic that happened before any
	// header was sent; nil writes a bare 500.
	OnPanic func(http.ResponseWriter, *http.Request)
}

// Middleware is MiddlewareWith with default options.
func Middleware(m *Metrics, next http.Handler) http.Handler {
	return MiddlewareWith(m, next, MiddlewareOptions{})
}

// MiddlewareWith traces and measures every request. The API is a public
// endpoint: a client-supplied traceparent is never used as the parent (a
// client could force sampling or join someone else's trace); it is attached
// as a link to a new root server span instead. It must wrap the ServeMux
// directly so the matched route pattern is visible after routing.
//
// A handler panic is counted as a 500, recorded on the span with a stack
// trace, and logged as http_handler_panic (route, trace ID, stack; never the
// panic value, which could carry request data). Before any header was sent,
// OnPanic answers; after one (or for a deliberate http.ErrAbortHandler), the
// panic is re-raised as http.ErrAbortHandler so net/http aborts the
// connection quietly. Liveness and readiness probes are counted but never
// traced, so frequent probes do not flood the trace store with root spans.
// m may be nil (tracing without metrics).
func MiddlewareWith(m *Metrics, next http.Handler, options MiddlewareOptions) http.Handler {
	propagator := propagation.TraceContext{}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		ctx, span := r.Context(), trace.SpanFromContext(r.Context())
		traced := r.URL.Path != "/healthz" && r.URL.Path != "/readyz"
		if traced {
			spanOptions := []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindServer), trace.WithNewRoot()}
			if incoming := trace.SpanContextFromContext(propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))); incoming.IsValid() {
				spanOptions = append(spanOptions, trace.WithLinks(trace.Link{SpanContext: incoming}))
			}
			ctx, span = otel.Tracer(TracerName).Start(r.Context(), "http.request", spanOptions...)
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
			if recovered != nil && recovered != http.ErrAbortHandler {
				logger.Error("http_handler_panic", "route", route, "trace_id", TraceID(ctx), "stack", string(debug.Stack()))
				if traced {
					span.RecordError(errors.New("handler panic"), trace.WithStackTrace(true))
				}
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
			if recovered == http.ErrAbortHandler || recorder.wroteHeader {
				panic(http.ErrAbortHandler)
			}
			if options.OnPanic != nil {
				options.OnPanic(w, routed)
				return
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

// Flush sends the header, so a later panic must abort, not answer.
func (s *statusRecorder) Flush() {
	if flusher, ok := s.ResponseWriter.(http.Flusher); ok {
		s.wroteHeader = true
		flusher.Flush()
	}
}

func (s *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hijacker, ok := s.ResponseWriter.(http.Hijacker); ok {
		conn, buffered, err := hijacker.Hijack()
		if err == nil {
			// The handler owns the connection now; a later panic must abort.
			s.wroteHeader = true
		}
		return conn, buffered, err
	}
	return nil, nil, errors.New("response writer does not support hijacking")
}

func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }
