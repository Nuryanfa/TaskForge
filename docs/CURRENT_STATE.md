# TaskForge Current Architecture

## Product

TaskForge is an educational, production-like durable job platform written in
Go. v0.7 adds connected tracing, bounded metrics, SLOs with tested burn-rate
alerts, and dashboards on top of v0.6 workflow DAGs with saga compensation and the v0.5
transactional outbox and NATS JetStream events, while PostgreSQL remains the
durable coordination authority.

## Current implemented state (v0.7)

- Commands: `taskforge-api`, `taskforge-worker`, `taskforge-migrate`, and
  `taskforge-relay`.
- PostgreSQL access uses bounded `pgxpool` connections and explicit SQL.
- Goose applies embedded SQL migrations under a PostgreSQL advisory lock.
- The HTTP API submits, inspects, and cancels jobs, manages dead letters and
  recurring schedules, and exposes health/readiness.
- Creation idempotency is scoped by `(queue, idempotency_key)` and compares a
  SHA-256 fingerprint over normalized queue, kind, payload, priority, and
  optional delayed timing (`run_at` or `delay_seconds`).
- Delayed jobs remain `queued` until `available_at <= NOW()` and can be canceled
  before claim.
- Recurring schedules use 5-field cron expressions and IANA timezones, are
  materialized transactionally with `FOR UPDATE SKIP LOCKED`, enforce per-slot
  uniqueness `(schedule_id, scheduled_for)`, and coalesce missed ticks into at
  most one catch-up run.
- Worker processes use fixed-size executor pools and may safely cooperate.
- Claims assign PostgreSQL-clock leases and monotonically increasing fencing
  tokens before handler execution.
- Active executions renew leases; expired executions are requeued in bounded,
  lock-safe batches and their attempts are marked `abandoned`.
- Executions cut off by the shutdown drain persist a retryable
  `SHUTDOWN_TIMEOUT`; results rejected by the store (invalid JSON or oversized)
  fail permanently with `INVALID_JOB_RESULT` instead of waiting for lease expiry.
- Attempts and bounded success/failure/abandoned outcomes are persisted
  transactionally.
- Retryable attempts return to `queued` with policy-snapshotted exponential
  backoff and deterministic jitter; permanent or exhausted jobs enter the DLQ.
- Execution keys remain stable across attempts and linked redrives.
- Dead letters contain operational metadata only and support cursor-based
  inspection and idempotent transactional redrive.
- Database triggers write a privacy-safe `outbox_events` row in the same
  transaction as every job insert or status change (six `job.*` event types).
- `taskforge-relay` publishes outbox rows to the `TASKFORGE_EVENTS` JetStream
  stream in id order with `Nats-Msg-Id` deduplication, marks only the
  acknowledged prefix, keeps one active relay per schema via an advisory lock,
  and purges published rows after a retention period.
- Workers optionally subscribe to `job.queued` events as wake-up hints, decided
  only from database timestamps; polling remains the correctness fallback.
- Compose orders PostgreSQL and NATS health, migration completion, then API,
  worker, and relay startup.
- Workflows (`POST /v1/workflows`) are DAGs of ordinary jobs with optional
  compensations. Creation, step dispatch, abort, and compensation happen in
  the same transaction as the job outcome that causes them; workflow rows are
  locked in ascending id order and lease recovery isolates each job under a
  savepoint.
- Fan-in steps are dispatched exactly once; failed or canceled workflows skip
  pending steps, cancel queued ones, let running ones finish, and compensate
  succeeded steps in reverse dependency order. A dead-lettered compensation
  ends the workflow as `compensation_failed`.
- Compensation handlers receive the execution key of the action they undo.
- Workflow correctness is checked by nine database invariants after scenario,
  race, crash-injection, and seeded randomized soak tests.
- CI runs unit, PostgreSQL and NATS integration, race, build, Compose, and
  image validation.

- Every job and workflow stores the trace context that caused it (migration
  7); worker attempts continue the trace as children, or as linked roots for
  client-delayed jobs, and workflow steps chain causally (ADR 0013). Client
  `traceparent` headers are links only.
- Prometheus metrics with bounded labels on a separate loopback listener per
  process; database gauges are cached snapshots; job outcomes are reported
  only after commit.
- Start-latency (99 % within 5 s of readiness) and reliability (99.5 %, platform
  failures only) SLOs with multi-window burn-rate, stalled-worker, and
  missing-signal alerts, all `promtool`-tested; a provisioned Grafana
  dashboard; Jaeger; `taskforge-loadgen` and a burn demo.
- The API rejects unregistered kinds with `400 UNSUPPORTED_JOB_KIND`.

## Architecture decisions

- Go module: `github.com/Nuryanfa/TaskForge`.
- PostgreSQL is the only durable source of truth.
- API and worker processes never run migrations automatically.
- Public APIs omit payloads, idempotency keys, DSNs, and raw internal errors.
- Submission idempotency does not imply execution-side idempotency.
- Delivery is at least once; external effects need their own idempotency or
  fencing support.
- SQL concurrency behavior is tested against real PostgreSQL, not SQL mocks.
- Development credentials remain limited to `.env.example` and Compose.

## Current limitations

- A hard crash is recoverable only after lease expiry; duplicate external side
  effects remain possible under at-least-once execution.
- Retry policy is global per API process; no per-queue, per-kind, or
  client-supplied policy, and no bulk redrive.
- No retention or archival of jobs, attempts, dead letters, or workflows.
- Workflow steps and compensations are at least once; compensations must be
  idempotent.

Not yet implemented:

- Workflow-level events, data flow between workflow steps, conditional
  branches, or resuming a workflow after redrive; events for schedule changes
- Arbitrary commands, binaries, scripts, URLs, or user-provided executable code
- Authentication, public-network exposure, multi-tenancy, or billing
- Trace propagation through NATS events, the API-availability SLO,
  Alertmanager routing, log shipping
- gRPC, Kubernetes, or a web UI beyond Grafana
- Claims of exactly-once execution or production readiness

## Further reading

[System design](DESIGN.md) · [Data model](DATA_MODEL.md) · [API](API.md) ·
[Events](EVENTS.md) · [Workflows](WORKFLOWS.md) · [Observability](OBSERVABILITY.md) · [SLOs](SLO.md) ·
[Configuration](CONFIGURATION.md) · [Operations](OPERATIONS.md) ·
[Testing](TESTING.md) · [ADRs](adr/README.md)
