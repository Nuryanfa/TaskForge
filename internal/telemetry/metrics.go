package telemetry

import (
	"slices"
	"time"

	"github.com/Nuryanfa/TaskForge/internal/job"
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds every TaskForge metric. Label values are bounded: kinds by
// job.RegisteredKinds, queues by configuration, everything else by enums.
// It implements postgres.Observer (JobOutcome, WorkflowSettled).
type Metrics struct {
	queues           []string
	httpRequests     *prometheus.CounterVec
	httpDuration     *prometheus.HistogramVec
	jobsSubmitted    *prometheus.CounterVec
	startDelay       *prometheus.HistogramVec
	attemptDuration  *prometheus.HistogramVec
	jobOutcomes      *prometheus.CounterVec
	workflowsSettled *prometheus.CounterVec
	leaseRecoveries  *prometheus.CounterVec
	exportFailures   prometheus.Counter
}

const otherLabel = "other"

// platformFailures are failure codes caused by the platform rather than the
// handler; only these burn the reliability error budget.
var platformFailures = []string{"LEASE_EXPIRED", "SHUTDOWN_TIMEOUT", "UNKNOWN_JOB_KIND"}

// NewMetrics registers all metrics on reg.
func NewMetrics(reg prometheus.Registerer, queues []string) *Metrics {
	m := &Metrics{
		queues: slices.Clone(queues),
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskforge_http_requests_total", Help: "HTTP requests by route pattern and status code.",
		}, []string{"route", "code"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "taskforge_http_request_duration_seconds", Help: "HTTP request duration by route pattern.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5},
		}, []string{"route"}),
		jobsSubmitted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskforge_jobs_submitted_total", Help: "Newly created jobs (idempotent replays excluded).",
		}, []string{"queue", "kind", "delayed"}),
		startDelay: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "taskforge_job_start_delay_seconds", Help: "Time from a job becoming ready (available_at) to being claimed, from the database clock.",
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 300},
		}, []string{"queue", "kind"}),
		attemptDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "taskforge_job_attempt_duration_seconds", Help: "Duration of one job attempt by outcome.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60},
		}, []string{"kind", "outcome"}),
		jobOutcomes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskforge_job_outcomes_total", Help: "Committed job attempt outcomes by failure class.",
		}, []string{"kind", "outcome", "class"}),
		workflowsSettled: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskforge_workflow_completions_total", Help: "Workflows that reached a terminal status.",
		}, []string{"status"}),
		leaseRecoveries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskforge_lease_recoveries_total", Help: "Expired leases recovered or failed to recover.",
		}, []string{"result"}),
		exportFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "taskforge_otel_export_failures_total", Help: "Failed trace span exports.",
		}),
	}
	reg.MustRegister(m.httpRequests, m.httpDuration, m.jobsSubmitted, m.startDelay, m.attemptDuration,
		m.jobOutcomes, m.workflowsSettled, m.leaseRecoveries, m.exportFailures)
	return m
}

// KindLabel bounds a job kind to the registered set.
func (m *Metrics) KindLabel(kind string) string {
	if job.IsRegisteredKind(kind) {
		return kind
	}
	return otherLabel
}

// QueueLabel bounds a queue name to the configured set.
func (m *Metrics) QueueLabel(queue string) string {
	if slices.Contains(m.queues, queue) {
		return queue
	}
	return otherLabel
}

// FailureClass classifies a failure code as "platform" or "handler".
func FailureClass(code string) string {
	if slices.Contains(platformFailures, code) {
		return "platform"
	}
	return "handler"
}

func (m *Metrics) JobSubmitted(queue, kind string, delayed bool) {
	label := "false"
	if delayed {
		label = "true"
	}
	m.jobsSubmitted.WithLabelValues(m.QueueLabel(queue), m.KindLabel(kind), label).Inc()
}

func (m *Metrics) StartDelay(queue, kind string, d time.Duration) {
	if d < 0 {
		d = 0
	}
	m.startDelay.WithLabelValues(m.QueueLabel(queue), m.KindLabel(kind)).Observe(d.Seconds())
}

func (m *Metrics) AttemptDuration(kind, outcome string, d time.Duration) {
	m.attemptDuration.WithLabelValues(m.KindLabel(kind), outcome).Observe(d.Seconds())
}

// JobOutcome records a committed outcome; it implements postgres.Observer.
func (m *Metrics) JobOutcome(kind, outcome, errorCode string) {
	class := "none"
	if outcome != "succeeded" {
		class = FailureClass(errorCode)
	}
	m.jobOutcomes.WithLabelValues(m.KindLabel(kind), outcome, class).Inc()
}

// WorkflowSettled records a terminal workflow; it implements postgres.Observer.
func (m *Metrics) WorkflowSettled(status string) {
	m.workflowsSettled.WithLabelValues(status).Inc()
}

func (m *Metrics) LeaseRecovery(result string, n int) {
	if n > 0 {
		m.leaseRecoveries.WithLabelValues(result).Add(float64(n))
	}
}

// ExportFailures is the counter SetupTracing increments on failed exports.
func (m *Metrics) ExportFailures() prometheus.Counter { return m.exportFailures }
