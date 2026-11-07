# TaskForge Current Architecture

## Product

TaskForge is an educational, production-like durable job platform written in
Go. v0.5 adds a transactional outbox and NATS JetStream lifecycle events, plus
optional NATS wake-ups for workers, while PostgreSQL remains the durable
coordination authority.

## Current implemented state (v0.5)

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
- CI runs unit, PostgreSQL and NATS integration, race, build, Compose, and
  image validation.

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
- No retention or archival of jobs, attempts, or dead letters.

Not yet implemented:

- Workflow DAGs or saga compensation; events for schedule changes
- Arbitrary commands, binaries, scripts, URLs, or user-provided executable code
- Authentication, public-network exposure, multi-tenancy, or billing
- gRPC, Prometheus, OpenTelemetry, Kubernetes, or a web dashboard
- Claims of exactly-once execution or production readiness

## Further reading

[System design](DESIGN.md) · [Data model](DATA_MODEL.md) · [API](API.md) ·
[Events](EVENTS.md) ·
[Configuration](CONFIGURATION.md) · [Operations](OPERATIONS.md) ·
[Testing](TESTING.md) · [ADRs](adr/README.md)
