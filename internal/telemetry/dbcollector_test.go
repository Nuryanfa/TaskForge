package telemetry

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/store/postgres"
	"github.com/prometheus/client_golang/prometheus"
)

func gatherValue(t *testing.T, registry *prometheus.Registry, name, labelValue string) (float64, bool) {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if labelValue == "" || (len(metric.GetLabel()) > 0 && metric.GetLabel()[0].GetValue() == labelValue) {
				return metric.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

func TestDBCollectorServesCachedValues(t *testing.T) {
	var calls atomic.Int32
	fail := atomic.Bool{}
	snapshot := func(context.Context) (postgres.Snapshot, error) {
		calls.Add(1)
		if fail.Load() {
			return postgres.Snapshot{}, errors.New("database down")
		}
		return postgres.Snapshot{Ready: 4, Delayed: 2, Running: 1, ExpiredLease: 1, OldestReadyAge: 90 * time.Second,
			UnredrivenDeadLetters: 3, OldestUnpublishedOutboxAge: 5 * time.Second,
			Workflows: map[string]int64{"running": 2, "compensating": 1}}, nil
	}
	collector, refresh := NewDBCollector(snapshot, time.Hour, slog.New(slog.NewTextHandler(io.Discard, nil)))
	registry := prometheus.NewRegistry()
	registry.MustRegister(collector)
	if _, ok := gatherValue(t, registry, "taskforge_jobs", "ready"); ok {
		t.Fatal("gauges exported before the first successful refresh")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { refresh(ctx); close(done) }()
	deadline := time.Now().Add(2 * time.Second)
	for calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	before := calls.Load()
	if value, ok := gatherValue(t, registry, "taskforge_jobs", "ready"); !ok || value != 4 {
		t.Fatalf("ready=%v ok=%v", value, ok)
	}
	if value, _ := gatherValue(t, registry, "taskforge_oldest_ready_job_age_seconds", ""); value != 90 {
		t.Fatalf("oldest ready age=%v", value)
	}
	if value, _ := gatherValue(t, registry, "taskforge_workflows", "compensating"); value != 1 {
		t.Fatalf("compensating workflows=%v", value)
	}
	_, _ = registry.Gather()
	if calls.Load() != before {
		t.Fatal("a scrape queried the database")
	}
	cancel()
	<-done

	fail.Store(true)
	refreshCtx, stop := context.WithCancel(context.Background())
	collector2, refresh2 := NewDBCollector(snapshot, time.Hour, slog.New(slog.NewTextHandler(io.Discard, nil)))
	registry2 := prometheus.NewRegistry()
	registry2.MustRegister(collector2)
	go refresh2(refreshCtx)
	time.Sleep(50 * time.Millisecond)
	stop()
	if _, ok := gatherValue(t, registry2, "taskforge_jobs", "ready"); ok {
		t.Fatal("gauges exported after a failed refresh; absent() alerting depends on their absence")
	}
}
