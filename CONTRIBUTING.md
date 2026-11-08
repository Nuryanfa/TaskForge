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
  change, following the checklist in the [documentation index](docs/README.md).

## Milestone boundaries

v0.1 provides PostgreSQL migrations and repository operations; submit, get,
and cancel HTTP endpoints; sequential execution by one worker; transactional
queued-to-running claims; persisted attempts and outcomes; database-aware
readiness; graceful shutdown; and real PostgreSQL integration tests.

v0.2 provides bounded concurrent worker goroutines, job leases, fencing tokens,
lease heartbeat and renewal, expired-lease recovery, and worker crash recovery.
v0.3 provides typed failures, bounded retry/backoff, execution idempotency
metadata, and dead-letter inspection/redrive.
v0.4 provides delayed job submission (`run_at` / `delay_seconds`) and durable
recurring cron schedules.
v0.5 provides a trigger-written transactional outbox, an ordered NATS
JetStream relay, and optional worker wake-ups. NATS must never decide job
ownership.
v0.6 provides workflow DAGs with saga compensation. The workflow engine runs
inside the transaction that makes a job terminal and must follow the lock
order in ADR 0012 (job rows, then workflow rows in ascending id, then steps).
Any new code path that moves a workflow job to a terminal status must call the
engine, and workflow changes need tests that end with
`assertWorkflowInvariants`. v0.7 provides connected tracing, bounded metrics, SLOs, and dashboards. New
code paths that create jobs must pass the causing span's context to the store;
metric labels must stay bounded (no IDs or client-chosen values); new alerts
need `promtool` tests (`make slo-test`), and the dashboard is edited through
`deployments/grafana/generate_dashboard.py`. Contributions must not add
Kubernetes or a web UI.

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
