# Contributing to TaskForge

TaskForge is developed in small, reviewable releases. Correctness under
retries, crashes, duplicate delivery, and concurrency matters more than feature
count.

## Engineering principles

- Keep PostgreSQL as the durable source of truth.
- Document delivery semantics explicitly; never claim exactly-once execution.
- Treat job payloads and error details as potentially sensitive.
- Bound queues, goroutines, batch sizes, timeouts, and retained history.
- Propagate `context.Context` through blocking operations.
- Use explicit SQL for transactions, row locking, and job claiming.
- Add race-oriented and failure-path tests for concurrent behavior.
- Keep features within their documented roadmap milestone.
- Update the README, PRD, ADRs, and
  [current architecture](docs/CURRENT_STATE.md) when architectural decisions
  change.

## Milestone boundaries

v0.1 provides PostgreSQL migrations and repository operations; submit, get,
and cancel HTTP endpoints; sequential execution by one worker; transactional
queued-to-running claims; persisted attempts and outcomes; database-aware
readiness; graceful shutdown; and real PostgreSQL integration tests.

v0.2 adds bounded concurrent worker goroutines, job leases, fencing tokens,
lease heartbeat and renewal, expired-lease recovery, and worker crash recovery.
It does not add v0.3 retry policies, dead-letter queues, NATS, scheduled jobs,
workflow DAGs, Kubernetes, or a web UI.

## Validation

Run the complete local validation suite before opening a pull request:

```text
gofmt -w .
go mod tidy
go mod verify
go vet ./...
go test ./...
go test -race ./...
go build ./...
docker compose config --quiet
git diff --check
```

When `TASKFORGE_TEST_DATABASE_URL` is available, also run:

```text
go test ./... -run Integration -count=1
```

Keep commits focused and include tests and documentation for behavior changes.
