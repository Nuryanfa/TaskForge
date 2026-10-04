# TaskForge agent guide

## Mission

Build a production-like distributed job platform in small, reviewable releases.
Correctness under retries, crashes, duplicate delivery, and concurrency is more
important than feature count.

## Engineering rules

- Keep PostgreSQL as the durable source of truth.
- Document delivery semantics explicitly; never claim exactly-once execution.
- Treat job payloads and error details as potentially sensitive.
- Bound queues, goroutines, batch sizes, timeouts, and retained history.
- Propagate `context.Context` through blocking operations.
- Use explicit SQL for transactions, row locking, and job claiming.
- Add race-oriented and failure-path tests for concurrent behavior.
- Do not add concurrent worker goroutines, leases, fencing, heartbeat, crash
  recovery, NATS, scheduled jobs, workflow DAGs, Kubernetes, or a web UI before
  their roadmap phase.
- Update README, PRD, ADRs, and PROJECT_MEMORY when architectural decisions change.

## Milestone boundaries

v0.1 is limited to PostgreSQL migration execution and repository operations;
submit, get, and cancel HTTP endpoints; sequential single-worker execution; a
transactional queued-to-running claim; persisted attempts and outcomes;
database-aware readiness; graceful shutdown; and real PostgreSQL integration
tests.

v0.2 introduces bounded concurrent worker goroutines, job leases, fencing
tokens, lease heartbeat and renewal, expired-lease recovery, and worker crash
recovery. If the single worker dies while a job is running in v0.1, that job is
not automatically recovered.

## Required validation

```bash
gofmt -w .
go vet ./...
go test ./...
go test -race ./...
go build ./...
git diff --check
```
