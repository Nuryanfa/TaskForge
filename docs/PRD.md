# TaskForge product requirements document

## Problem

Applications need durable asynchronous work without hiding failure behavior.
TaskForge v0.3 adds bounded retry scheduling, execution idempotency metadata,
and dead-letter operations while keeping ownership and failure explicit.

## Product principles

1. PostgreSQL is the durable source of truth.
2. Delivery and failure semantics are explicit; exactly-once is not promised.
3. Concurrency, request data, result data, and blocking operations are bounded.
4. Correctness-sensitive SQL is explicit and tested against real PostgreSQL.
5. Payloads, results, idempotency keys, and database credentials are sensitive.

## Implemented through v0.3

- Embedded, versioned PostgreSQL migrations and a dedicated migration command.
- Bounded `pgxpool` connection pooling and database operations.
- Submit, inspect, and queued-job cancellation HTTP endpoints.
- Queue-scoped creation idempotency using a canonical SHA-256 fingerprint.
- A fixed, bounded executor pool and transactional queued-to-running claims.
- Time-bounded leases, periodic heartbeat renewal, and monotonically increasing
  fencing tokens.
- Bounded expired-lease recovery with abandoned-attempt history.
- Persisted attempts and success/failure/abandoned outcomes.
- Typed retryable versus permanent failures, snapshotted attempt/backoff policy,
  capped exponential backoff, and deterministic bounded jitter.
- Stable execution idempotency keys across attempts and redrive.
- Payload-free PostgreSQL dead letters with cursor pagination and transactional,
  idempotent redrive into a new job.
- A bounded, built-in `demo.echo` handler registry.
- Process liveness and bounded database-aware readiness.
- Graceful API and worker shutdown.
- Docker Compose startup ordering and non-root application containers.
- Unit, PostgreSQL integration, race, and in-process end-to-end tests.

## API

```text
POST /v1/jobs
GET  /v1/jobs/{id}
POST /v1/jobs/{id}/cancel
GET  /v1/dead-letters
GET  /v1/dead-letters/{job-id}
POST /v1/dead-letters/{job-id}/redrive
GET  /healthz
GET  /readyz
```

Public job responses omit payloads and idempotency keys. `/healthz` reports only
process liveness. `/readyz` performs a short, bounded PostgreSQL ping. HTTP
errors use stable public codes without raw dependency details.

## State, ownership, and claim semantics

Supported states are `queued`, `running`, `succeeded`, `failed`, and
`canceled`. A claim transaction locks one eligible queued row with
`FOR UPDATE SKIP LOCKED`, changes it to running, increments its attempt number,
and fencing token, assigns a PostgreSQL-clock lease, and inserts the attempt
before commit. Heartbeat, success, and failure require the current owner and
fencing token. Outcome and attempt finalization are one transaction.

An expired running job is locked by one recovery process, its current attempt
is marked `abandoned`, and it returns to `queued` without decrementing its
attempt count. A future claim receives a higher fencing token. Recovery is not
an automatic retry policy and adds no backoff or dead-letter behavior.

Submission idempotency is not execution-side idempotency. A caller can safely
replay creation with the same queue/key and normalized immutable fields, but
TaskForge does not guarantee that external handler side effects happen once.

## Delivery semantics and exclusions

TaskForge provides at-least-once execution. A worker can perform an external
side effect and crash before persisting success; lease recovery may then run
the handler again. Fencing protects TaskForge's PostgreSQL state only. Handler
integrations need idempotency keys or downstream fencing for their own effects.

v0.3 excludes client-controlled delayed or recurring jobs, NATS, workflow DAGs,
authentication or multitenancy, Kubernetes, and full observability.

## Acceptance criteria

- A queued job and its status survive API and worker restarts.
- Concurrent identical submissions with one idempotency key produce one job.
- A conflicting idempotency replay returns HTTP 409.
- Claim ordering uses priority descending, then creation time and ID.
- Claim transition and attempt insertion are atomic.
- Only queued jobs can be canceled.
- Completion/failure and attempt finalization are atomic and duplicate-safe.
- Each process never exceeds its configured worker concurrency.
- Multiple workers cannot actively own the same job.
- Heartbeats extend only a current, unexpired lease.
- Stale fencing tokens cannot heartbeat, complete, or fail a newer execution.
- Expired attempts are abandoned and jobs become claimable with a higher token.
- Concurrent recovery processes recover an expired ownership epoch once.
- All external operations and shutdown waits are bounded.
- Integration tests use real PostgreSQL with isolated schemas.
- Credentials, payloads, results, and idempotency keys do not appear in logs.

## Roadmap

| Version | Scope |
| --- | --- |
| v0.1 | Durable PostgreSQL API, migrations, and sequential worker |
| v0.2 | Concurrent workers, leases, fencing, heartbeat, and crash recovery (implemented) |
| v0.3 | Retry scheduling, execution idempotency, and dead-letter queue (implemented) |
| v0.4 | Delayed and recurring scheduling |
| v0.5 | Transactional outbox and NATS JetStream |
| v0.6 | Workflow DAGs and saga compensation |
| v0.7 | Metrics, tracing, dashboards, and SLOs |
| v0.8 | High availability, Kubernetes, load, and chaos testing |
