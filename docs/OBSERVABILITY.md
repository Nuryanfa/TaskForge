# TaskForge observability (v0.7)

TaskForge exports traces over OTLP/HTTP (to Jaeger in Compose) and Prometheus
metrics from every process. Telemetry never blocks work: with tracing off, a
dead collector, or Prometheus down, jobs and requests behave exactly as
without telemetry. Design rationale: [ADR 0013](adr/0013-trace-context-through-the-database.md);
targets and alerts: [SLO.md](SLO.md).

## Following one job

The trace context of the span that created a job is stored in
`jobs.trace_parent`, so a worker can continue the trace hours later in
another process.

```mermaid
sequenceDiagram
    participant C as Client
    participant A as taskforge-api
    participant D as PostgreSQL
    participant W as taskforge-worker
    C->>A: POST /v1/jobs
    Note over A: span "POST /v1/jobs" (new root; a client traceparent is only a link)
    Note over A: child span "job.enqueue"
    A->>D: INSERT job (trace_parent = job.enqueue context)
    W->>D: claim job (reads trace_parent, trace_link)
    Note over W: span "job.attempt": child of job.enqueue,<br/>or a linked root if the client delayed the job
    W->>D: complete / fail (same context)
    Note over W,D: workflow steps and compensations dispatched in that<br/>transaction store the attempt's context
```

In Jaeger (http://127.0.0.1:16686), search service `taskforge-api` for
`POST /v1/jobs` or `POST /v1/workflows`; the `taskforge-worker` attempt spans
appear in the same trace. Search by tag `taskforge.job.id=<id>` to find every
attempt of one job, including linked roots of delayed jobs.

A saga appears as a causal chain, because each step is dispatched inside the
transaction of the attempt that released it:

```text
POST /v1/workflows
└─ workflow.submit
   └─ job.attempt reserve
      ├─ job.attempt notify
      └─ job.attempt charge
         └─ job.attempt ship        (fails, dead-lettered)
            └─ job.attempt charge/compensation   (refund)
               └─ job.attempt reserve/compensation (release)
```

## Span catalog

| Span | Process | Notes |
| --- | --- | --- |
| route pattern, e.g. `POST /v1/jobs` | API | server span, always a new root; `http.route`, `http.response.status_code`; unknown paths are `unmatched` |
| `job.enqueue`, `workflow.submit` | API | producer; job or workflow ID, kind, queue, delayed |
| `job.redrive`, `workflow.cancel` | API | internal |
| `job.attempt` | worker | consumer; job ID, kind, queue, attempt, max attempts, workflow ID/step/role, outcome (`succeeded`, `failed`, `lease_lost`), error code; error status when the outcome could not be persisted; `workflow.dispatch` events for steps it released |
| `lease.recovery` | worker | only for recovery passes that found expired leases; `recovered`, `failed` |
| `schedule.dispatch` | worker | one root per dispatched cron slot; the materialized job is its child |

Span attributes never contain payloads, results, idempotency keys, execution
keys, or credentials.

## Metrics catalog

Each process serves `/metrics` on its own loopback listener, never on the API
listener: API `127.0.0.1:9101`, worker `127.0.0.1:9102`, relay
`127.0.0.1:9103` (`TASKFORGE_METRICS_ADDR`; set it empty to disable).

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `taskforge_http_requests_total` | counter | route, code | API requests |
| `taskforge_http_request_duration_seconds` | histogram | route | API latency |
| `taskforge_jobs_submitted_total` | counter | queue, kind, delayed | new jobs from `POST /v1/jobs` (replays excluded) |
| `taskforge_job_start_delay_seconds` | histogram | queue, kind | ready → claimed, database clock; has a 5 s bucket |
| `taskforge_job_attempt_duration_seconds` | histogram | kind, outcome | one attempt; outcome `succeeded`, `failed`, `lease_lost` |
| `taskforge_job_outcomes_total` | counter | kind, outcome, class | committed outcomes: `succeeded`, `retry_scheduled`, `dead_lettered`, `abandoned`; class `platform`, `handler`, `none` |
| `taskforge_workflow_completions_total` | counter | status | workflows reaching a terminal status |
| `taskforge_lease_recoveries_total` | counter | result | expired leases recovered or failed to recover |
| `taskforge_otel_export_failures_total` | counter | — | failed span exports |
| `taskforge_jobs` | gauge | state | `ready`, `delayed`, `running`, `expired_lease` (API, cached) |
| `taskforge_oldest_ready_job_age_seconds` | gauge | — | grows when no worker claims (API, cached) |
| `taskforge_dead_letters_unredriven` | gauge | — | API, cached |
| `taskforge_outbox_oldest_unpublished_age_seconds` | gauge | — | relay lag (API, cached) |
| `taskforge_workflows` | gauge | status | non-terminal workflows (API, cached) |
| `taskforge_db_pool_*_conns` | gauge | — | pgxpool statistics |
| Go runtime and process | standard | — | every process |

**Label bounds.** `kind` is one of the registered kinds or `other`; `queue` is
listed in `TASKFORGE_METRICS_QUEUES` (default `default`) or `other`; `route` is
the mux pattern or `unmatched`. No metric carries a job or workflow ID; a test
submits unlisted queues and kinds and asserts this.

**Database gauges** come from one bounded snapshot in the API process every
`TASKFORGE_METRICS_DB_REFRESH` (15 s); a scrape never queries the database.
One or two failed refreshes keep the last snapshot, so a single slow query
does not reset a pending `TaskForgeJobsNotStarting`. After three in a row
(about 45 s) the gauges disappear rather than going stale, which fires
`TaskForgeDBCollectorAbsent`. With several API replicas, use `max()`.

**Counting semantics.** Outcomes are reported only after their transaction
commits, so rolled-back or fenced-out work is never counted. A process that
crashes between commit and reporting loses that one count; the database
remains correct.

## Dashboards and tools

| Tool | URL (Compose) | What it is for |
| --- | --- | --- |
| Grafana | http://127.0.0.1:3000 | "TaskForge — what is broken?" (generated by `deployments/grafana/generate_dashboard.py`; CI rejects drift) |
| Prometheus | http://127.0.0.1:9090 | queries, rules, alerts |
| Jaeger | http://127.0.0.1:16686 | traces |
