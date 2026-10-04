# TaskForge product requirements document

## Problem

Applications frequently move slow, unreliable, or scheduled work outside the
request path. A naive in-memory goroutine loses work on restart, while a naive
database poller can duplicate side effects or block under concurrent workers.

TaskForge provides a small, inspectable platform for durable job submission,
execution, retries, cancellation, and operational visibility.

## Target users

- Backend teams running asynchronous work
- Platform teams operating shared worker infrastructure
- Developers who need reliable scheduled jobs without embedding schedulers in
  every service

## Product principles

1. Durable state before distributed messaging.
2. At-least-once delivery with explicit idempotency.
3. Bounded concurrency and resource use.
4. Failure behavior is documented and tested.
5. Operational visibility is part of the product, not an afterthought.

## v0.1 requirements

- Execute PostgreSQL migrations explicitly.
- Submit a job through an HTTP API.
- Persist a validated job in PostgreSQL.
- Fetch job status without exposing its payload by default.
- Cancel a queued job.
- Run one worker process that executes one job at a time.
- Transactionally claim one eligible job from queued to running.
- Persist attempts and terminal outcomes.
- Reject invalid state transitions.
- Provide liveness and dependency-aware readiness endpoints.
- Shut down without accepting new work and with a bounded drain period.
- Include migrations, Docker Compose, CI, integration tests, and documentation.

## v0.1 exclusions and failure limitation

v0.1 does not include multiple concurrent worker goroutines, job leases, lease
heartbeat or renewal, fencing tokens, expired-lease recovery, worker crash
recovery, NATS, scheduled jobs, or workflow DAGs.

If the single worker process dies while a job is running, the running job is
not automatically recovered in v0.1. Bounded concurrent workers, leases,
fencing, heartbeat, and crash recovery are v0.2 work and will introduce their
schema through a new migration.

## Initial API

```text
POST /v1/jobs
GET  /v1/jobs/{id}
POST /v1/jobs/{id}/cancel
GET  /healthz
GET  /readyz
```

## v0.1 acceptance criteria

- A committed queued job survives API and worker restart.
- A transactional claim moves only one eligible job from queued to running.
- The single worker executes no more than one job at a time.
- A terminal job cannot return to a non-terminal state.
- All database operations have bounded contexts.
- `go test -race ./...` passes.
- Integration tests use a real PostgreSQL instance.
- No credentials or job payloads appear in logs.

## Roadmap

| Version | Scope |
| --- | --- |
| v0.1 | PostgreSQL repository, HTTP API, and sequential worker |
| v0.2 | Concurrent workers, leases, fencing, heartbeat, and crash recovery |
| v0.3 | Retries, idempotency, and dead-letter queue |
| v0.4 | Delayed and recurring scheduling |
| v0.5 | Transactional outbox and NATS JetStream |
| v0.6 | Workflow DAGs and saga compensation |
| v0.7 | Metrics, tracing, dashboards, and SLOs |
| v0.8 | High availability, Kubernetes, load, and chaos testing |
