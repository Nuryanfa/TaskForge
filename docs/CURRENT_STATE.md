# TaskForge Current Architecture

## Product

TaskForge is an educational, production-like durable job platform written in
Go. v0.2 adds a bounded concurrent worker model with lease-based crash
recovery while PostgreSQL remains the durable coordination authority.

## Current implemented state (v0.2)

- Commands: `taskforge-api`, `taskforge-worker`, and `taskforge-migrate`.
- PostgreSQL access uses bounded `pgxpool` connections and explicit SQL.
- Goose applies embedded SQL migrations under a PostgreSQL advisory lock.
- The HTTP API submits, inspects, and cancels jobs, with health/readiness.
- Creation idempotency is scoped by `(queue, idempotency_key)` and compares a
  SHA-256 fingerprint over normalized queue, kind, payload, and priority.
- Worker processes use fixed-size executor pools and may safely cooperate.
- Claims assign PostgreSQL-clock leases and monotonically increasing fencing
  tokens before handler execution.
- Active executions renew leases; expired executions are requeued in bounded,
  lock-safe batches and their attempts are marked `abandoned`.
- Attempts and bounded success/failure/abandoned outcomes are persisted
  transactionally.
- Compose orders PostgreSQL, migration completion, then API and worker startup.
- CI runs unit, integration, race, build, Compose, and image validation.

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
- Automatic retry and dead-letter handling
- NATS, cron/recurring scheduling, workflow DAGs, or saga compensation
- Arbitrary commands, binaries, scripts, URLs, or user-provided executable code
- Authentication, public-network exposure, multi-tenancy, or billing
- gRPC, Prometheus, OpenTelemetry, Kubernetes, or a web dashboard
- Claims of exactly-once execution or production readiness
