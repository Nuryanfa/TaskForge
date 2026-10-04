# TaskForge product requirements document

## Problem

Applications need durable asynchronous work without hiding failure behavior.
TaskForge v0.1 proves the PostgreSQL persistence, claiming, API, and sequential
execution model before adding distributed workers.

## Product principles

1. PostgreSQL is the durable source of truth.
2. Delivery and failure semantics are explicit; exactly-once is not promised.
3. Concurrency, request data, result data, and blocking operations are bounded.
4. Correctness-sensitive SQL is explicit and tested against real PostgreSQL.
5. Payloads, results, idempotency keys, and database credentials are sensitive.

## Implemented v0.1 scope

- Embedded, versioned PostgreSQL migrations and a dedicated migration command.
- Bounded `pgxpool` connection pooling and database operations.
- Submit, inspect, and queued-job cancellation HTTP endpoints.
- Queue-scoped creation idempotency using a canonical SHA-256 fingerprint.
- One sequential worker with a transactional queued-to-running claim.
- Persisted attempts and success/failure outcomes.
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
GET  /healthz
GET  /readyz
```

Public job responses omit payloads and idempotency keys. `/healthz` reports only
process liveness. `/readyz` performs a short, bounded PostgreSQL ping. HTTP
errors use stable public codes without raw dependency details.

## v0.1 state and claim semantics

Supported states are `queued`, `running`, `succeeded`, `failed`, and
`canceled`. A claim transaction locks one eligible queued row with
`FOR UPDATE SKIP LOCKED`, changes it to running, increments its attempt number,
and inserts the attempt before commit. Success and failure conditionally update
the matching running job and attempt in one transaction.

Submission idempotency is not execution-side idempotency. A caller can safely
replay creation with the same queue/key and normalized immutable fields, but
TaskForge does not guarantee that external handler side effects happen once.

## Exclusions and failure limitation

v0.1 excludes concurrent worker goroutines, leases, heartbeat or renewal,
fencing tokens, expired-lease recovery, automatic worker-crash recovery, retry
scheduling, dead-letter queues, NATS, cron scheduling, workflow DAGs, saga
compensation, gRPC, telemetry platforms, Kubernetes, and a web dashboard.

The deployment must run at most one active sequential worker. If that process
dies while executing a job, the job remains `running`; automated recovery is
intentionally deferred to v0.2.

## Acceptance criteria

- A queued job and its status survive API and worker restarts.
- Concurrent identical submissions with one idempotency key produce one job.
- A conflicting idempotency replay returns HTTP 409.
- Claim ordering uses priority descending, then creation time and ID.
- Claim transition and attempt insertion are atomic.
- Only queued jobs can be canceled.
- Completion/failure and attempt finalization are atomic and duplicate-safe.
- The worker executes no more than one job at a time.
- All external operations and shutdown waits are bounded.
- Integration tests use real PostgreSQL with isolated schemas.
- Credentials, payloads, results, and idempotency keys do not appear in logs.

## Roadmap

| Version | Scope |
| --- | --- |
| v0.1 | Durable PostgreSQL API, migrations, and sequential worker |
| v0.2 | Concurrent workers, leases, fencing, heartbeat, and crash recovery |
| v0.3 | Retry scheduling, execution idempotency, and dead-letter queue |
| v0.4 | Delayed and recurring scheduling |
| v0.5 | Transactional outbox and NATS JetStream |
| v0.6 | Workflow DAGs and saga compensation |
| v0.7 | Metrics, tracing, dashboards, and SLOs |
| v0.8 | High availability, Kubernetes, load, and chaos testing |
