package telemetry

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/store/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	jobsDesc = prometheus.NewDesc("taskforge_jobs",
		"Live jobs by state (database snapshot).", []string{"state"}, nil)
	oldestReadyDesc = prometheus.NewDesc("taskforge_oldest_ready_job_age_seconds",
		"Age of the oldest job that is ready but not yet claimed; grows when no worker runs.", nil, nil)
	deadLettersDesc = prometheus.NewDesc("taskforge_dead_letters_unredriven",
		"Dead letters that have not been redriven.", nil, nil)
	outboxAgeDesc = prometheus.NewDesc("taskforge_outbox_oldest_unpublished_age_seconds",
		"Age of the oldest unpublished outbox event.", nil, nil)
	workflowsDesc = prometheus.NewDesc("taskforge_workflows",
		"Non-terminal workflows by status.", []string{"status"}, nil)
)

// dbCollector exports the last successful database snapshot. A scrape never
// queries the database. After a failed refresh it exports nothing, so the
// absent() alert fires instead of dashboards showing stale numbers.
type dbCollector struct {
	mu       sync.RWMutex
	snapshot *postgres.Snapshot
}

// NewDBCollector returns the collector and the refresh loop to run in its own
// goroutine; the loop refreshes immediately and then every interval.
func NewDBCollector(snapshot func(context.Context) (postgres.Snapshot, error), interval time.Duration, logger *slog.Logger) (prometheus.Collector, func(context.Context)) {
	collector := &dbCollector{}
	refresh := func(ctx context.Context) {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			current, err := snapshot(ctx)
			collector.mu.Lock()
			if err != nil {
				collector.snapshot = nil
			} else {
				collector.snapshot = &current
			}
			collector.mu.Unlock()
			if err != nil && ctx.Err() == nil {
				logger.Error("metrics_snapshot_failed")
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}
	return collector, refresh
}

func (c *dbCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range []*prometheus.Desc{jobsDesc, oldestReadyDesc, deadLettersDesc, outboxAgeDesc, workflowsDesc} {
		ch <- desc
	}
}

func (c *dbCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.RLock()
	snapshot := c.snapshot
	c.mu.RUnlock()
	if snapshot == nil {
		return
	}
	for state, value := range map[string]int64{"ready": snapshot.Ready, "delayed": snapshot.Delayed,
		"running": snapshot.Running, "expired_lease": snapshot.ExpiredLease} {
		ch <- prometheus.MustNewConstMetric(jobsDesc, prometheus.GaugeValue, float64(value), state)
	}
	ch <- prometheus.MustNewConstMetric(oldestReadyDesc, prometheus.GaugeValue, snapshot.OldestReadyAge.Seconds())
	ch <- prometheus.MustNewConstMetric(deadLettersDesc, prometheus.GaugeValue, float64(snapshot.UnredrivenDeadLetters))
	ch <- prometheus.MustNewConstMetric(outboxAgeDesc, prometheus.GaugeValue, snapshot.OldestUnpublishedOutboxAge.Seconds())
	for status, value := range snapshot.Workflows {
		ch <- prometheus.MustNewConstMetric(workflowsDesc, prometheus.GaugeValue, float64(value), status)
	}
}

var poolDescs = map[string]*prometheus.Desc{
	"total":    prometheus.NewDesc("taskforge_db_pool_total_conns", "Connections in the pool.", nil, nil),
	"acquired": prometheus.NewDesc("taskforge_db_pool_acquired_conns", "Connections currently in use.", nil, nil),
	"idle":     prometheus.NewDesc("taskforge_db_pool_idle_conns", "Idle connections.", nil, nil),
	"max":      prometheus.NewDesc("taskforge_db_pool_max_conns", "Configured maximum connections.", nil, nil),
}

type poolCollector struct{ stat func() *pgxpool.Stat }

// NewPoolCollector exports pgxpool statistics.
func NewPoolCollector(stat func() *pgxpool.Stat) prometheus.Collector {
	return poolCollector{stat: stat}
}

func (p poolCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range poolDescs {
		ch <- desc
	}
}

func (p poolCollector) Collect(ch chan<- prometheus.Metric) {
	stat := p.stat()
	ch <- prometheus.MustNewConstMetric(poolDescs["total"], prometheus.GaugeValue, float64(stat.TotalConns()))
	ch <- prometheus.MustNewConstMetric(poolDescs["acquired"], prometheus.GaugeValue, float64(stat.AcquiredConns()))
	ch <- prometheus.MustNewConstMetric(poolDescs["idle"], prometheus.GaugeValue, float64(stat.IdleConns()))
	ch <- prometheus.MustNewConstMetric(poolDescs["max"], prometheus.GaugeValue, float64(stat.MaxConns()))
}
