package telemetry

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

func newTestMetrics(t *testing.T) (*Metrics, *prometheus.Registry) {
	t.Helper()
	registry := prometheus.NewRegistry()
	return NewMetrics(registry, []string{"default", "billing"}), registry
}

func TestLabelBounding(t *testing.T) {
	m, _ := newTestMetrics(t)
	if m.KindLabel("demo.echo") != "demo.echo" || m.KindLabel("report.generate") != "other" {
		t.Fatal("kind label not bounded to registered kinds")
	}
	if m.QueueLabel("billing") != "billing" || m.QueueLabel("custom.queue") != "other" {
		t.Fatal("queue label not bounded to configured queues")
	}
}

func TestFailureClass(t *testing.T) {
	for _, code := range []string{"LEASE_EXPIRED", "SHUTDOWN_TIMEOUT", "UNKNOWN_JOB_KIND"} {
		if FailureClass(code) != "platform" {
			t.Fatalf("%s should be platform", code)
		}
	}
	for _, code := range []string{"EXECUTION_TIMEOUT", "DEMO_FAILURE", "INVALID_JOB_PAYLOAD", "INVALID_JOB_RESULT"} {
		if FailureClass(code) != "handler" {
			t.Fatalf("%s should be handler", code)
		}
	}
}

func TestJobOutcomeClassesAndBoundedLabels(t *testing.T) {
	m, registry := newTestMetrics(t)
	m.JobOutcome("demo.echo", "succeeded", "")
	m.JobOutcome("report.generate", "dead_lettered", "LEASE_EXPIRED")
	m.JobOutcome("demo.fail", "dead_lettered", "DEMO_FAILURE")
	if got := testutil.ToFloat64(m.jobOutcomes.WithLabelValues("demo.echo", "succeeded", "none")); got != 1 {
		t.Fatalf("succeeded=%v", got)
	}
	if got := testutil.ToFloat64(m.jobOutcomes.WithLabelValues("other", "dead_lettered", "platform")); got != 1 {
		t.Fatalf("platform dead letter=%v", got)
	}
	if got := testutil.ToFloat64(m.jobOutcomes.WithLabelValues("demo.fail", "dead_lettered", "handler")); got != 1 {
		t.Fatalf("handler dead letter=%v", got)
	}
	m.WorkflowSettled("compensated")
	m.LeaseRecovery("recovered", 3)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if strings.Contains(label.GetValue(), "report.generate") {
					t.Fatalf("unbounded label value in %s", family.GetName())
				}
			}
		}
	}
}

func TestStartDelayHasFiveSecondBucket(t *testing.T) {
	m, registry := newTestMetrics(t)
	m.StartDelay("default", "demo.echo", 3*time.Second)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var histogram *dto.Histogram
	for _, family := range families {
		if family.GetName() == "taskforge_job_start_delay_seconds" {
			histogram = family.GetMetric()[0].GetHistogram()
		}
	}
	if histogram == nil {
		t.Fatal("start delay histogram not registered")
	}
	for _, bucket := range histogram.GetBucket() {
		if bucket.GetUpperBound() == 5 {
			if bucket.GetCumulativeCount() != 1 {
				t.Fatalf("5s bucket count=%d", bucket.GetCumulativeCount())
			}
			return
		}
	}
	t.Fatal("start delay histogram has no 5 second bucket")
}
