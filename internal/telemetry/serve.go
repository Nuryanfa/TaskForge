package telemetry

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ServeMetrics serves only /metrics on addr until ctx is done. It returns an
// error immediately when the address cannot be bound, so a misconfigured
// process fails at startup instead of running unobservable.
func ServeMetrics(ctx context.Context, addr string, gatherer prometheus.Gatherer) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen for metrics: %w", err)
	}
	return serveMetricsOn(ctx, listener, gatherer)
}

func serveMetricsOn(ctx context.Context, listener net.Listener, gatherer prometheus.Gatherer) error {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second}
	errs := make(chan error, 1)
	go func() { errs <- server.Serve(listener) }()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		if err := <-errs; err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case err := <-errs:
		return err
	}
}
