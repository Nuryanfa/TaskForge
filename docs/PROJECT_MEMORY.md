# TaskForge project memory

## Product

TaskForge is an educational, production-like durable job platform written in
Go. v0.1 is the PostgreSQL foundation for the distributed worker model planned
for v0.2; v0.1 itself must not be described as distributed execution.

## Current implemented state

- Commands: `taskforge-api`, `taskforge-worker`, and `taskforge-migrate`.
- PostgreSQL access uses bounded `pgxpool` connections and explicit SQL.
- Goose applies embedded SQL migrations under a PostgreSQL advisory lock.
- The HTTP API submits, inspects, and cancels jobs, with health/readiness.
- Creation idempotency is scoped by `(queue, idempotency_key)` and compares a
  SHA-256 fingerprint over normalized queue, kind, payload, and priority.
- The worker claims transactionally and runs exactly one built-in `demo.echo`
  job at a time.
- Attempts and bounded success/failure outcomes are persisted transactionally.
- Compose orders PostgreSQL, migration completion, then API and worker startup.
- CI runs unit, integration, race, build, Compose, and image validation.

## Stable decisions

- Go module: `github.com/Nuryanfa/TaskForge`.
- PostgreSQL is the only durable source of truth.
- API and worker processes never run migrations automatically.
- Public APIs omit payloads, idempotency keys, DSNs, and raw internal errors.
- Submission idempotency does not imply execution-side idempotency.
- One active sequential worker per v0.1 deployment is an operational invariant.
- SQL concurrency behavior is tested against real PostgreSQL, not SQL mocks.
- Development credentials remain limited to `.env.example` and Compose.

## Known v0.1 limitation

There are no leases, fencing tokens, heartbeat, retry scheduling, or abandoned
job recovery. If the worker dies after a job becomes `running`, that job stays
`running`. v0.2 will add bounded concurrent workers and a new migration for
lease/fencing state, heartbeat, and worker-crash recovery.

## Explicit v0.1 non-goals

- Multiple concurrent worker goroutines or multiple active worker processes
- Automatic retry and dead-letter handling
- NATS, cron/recurring scheduling, workflow DAGs, or saga compensation
- Arbitrary commands, binaries, scripts, URLs, or user-provided executable code
- Authentication, public-network exposure, multi-tenancy, or billing
- gRPC, Prometheus, OpenTelemetry, Kubernetes, or a web dashboard
- Claims of exactly-once execution or production readiness
