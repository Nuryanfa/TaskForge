# TaskForge project memory

## Product

TaskForge is a distributed job and workflow orchestration platform written in
Go. It demonstrates durable background processing, concurrency control,
idempotency, retries, scheduling, failure recovery, and observability.

## Current milestone

v0.1 foundation is being prepared. The repository currently contains project
hygiene, a health endpoint, configuration parsing, the job state model, initial
PostgreSQL schema, Docker Compose, CI, and the first architecture decision.

The v0.1 implementation scope is PostgreSQL migration execution and repository
operations; submit, get, and cancel HTTP endpoints; sequential single-worker
execution; a transactional queued-to-running claim; persisted attempts and
outcomes; database-aware readiness; graceful shutdown; and real PostgreSQL
integration tests.

The current Compose setup starts PostgreSQL but does not execute migrations.
The jobs table is not initialized automatically. Migration execution and
database-aware `/readyz` are pending v0.1 work.

## Non-goals for v0.1

- NATS or another external broker
- Multiple concurrent worker goroutines
- Job leases, heartbeat, renewal, or fencing tokens
- Expired-lease recovery and worker crash recovery
- Scheduled jobs
- Workflow DAGs and saga compensation
- Kubernetes and leader election
- Multi-tenancy and billing
- Web dashboard
- Claims of exactly-once delivery or production readiness

## Stable decisions

- Go module: `github.com/Nuryanfa/TaskForge`
- PostgreSQL is the durable source of truth.
- Delivery semantics will be at least once with idempotent execution.
- SQL concurrency behavior must be tested against a real PostgreSQL instance.
- Public APIs will not expose raw internal errors.
- Secrets remain environment-only.

## v0.1 failure limitation

If the sequential worker process dies while a job is running, v0.1 does not
automatically recover that job. v0.2 introduces bounded concurrent workers,
leases, fencing, heartbeat and renewal, expired-lease recovery, and worker
crash recovery through new application code and a new database migration.
