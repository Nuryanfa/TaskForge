package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"github.com/Nuryanfa/TaskForge/internal/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Process bundles one command's telemetry: a dedicated registry with Go and
// process collectors, the TaskForge metrics, tracing, and the metrics
// listener.
type Process struct {
	Metrics  *Metrics
	Registry *prometheus.Registry
	shutdown func(context.Context) error
}

// StartProcess sets up telemetry for a command. The metrics listener is bound
// before returning, so a busy or invalid address fails startup; an empty
// TASKFORGE_METRICS_ADDR disables the listener.
func StartProcess(ctx context.Context, cfg config.Config, service, defaultMetricsAddr string, logger *slog.Logger) (*Process, error) {
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	metrics := NewMetrics(registry, cfg.MetricsQueues)
	shutdown, err := SetupTracing(ctx, TracingConfig{
		Endpoint: cfg.OTelEndpoint, SampleRatio: cfg.OTelSampleRatio,
		ExportTimeout: cfg.OTelExportTimeout, ServiceName: service,
	}, metrics.ExportFailures())
	if err != nil {
		return nil, fmt.Errorf("set up tracing: %w", err)
	}
	if addr := cfg.MetricsAddr(defaultMetricsAddr); addr != "" {
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			_ = shutdown(context.Background())
			return nil, fmt.Errorf("listen for metrics: %w", err)
		}
		go func() {
			if err := serveMetricsOn(ctx, listener, registry); err != nil {
				logger.Error("metrics_server_failed")
			}
		}()
		logger.Info("metrics_started", "address", listener.Addr().String())
	}
	return &Process{Metrics: metrics, Registry: registry, shutdown: shutdown}, nil
}

// Shutdown flushes pending spans within ctx's deadline.
func (p *Process) Shutdown(ctx context.Context) error { return p.shutdown(ctx) }
