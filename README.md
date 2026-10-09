# TaskForge

TaskForge v0.2 is a durable PostgreSQL-backed background job platform written
in Go. It provides idempotent submission, bounded concurrent execution,
time-bounded leases, fencing, heartbeat renewal, and worker-crash recovery.

> TaskForge is an educational portfolio project. It is not production ready,
> does not promise exactly-once execution, and must not be exposed directly to
> an untrusted public network.

## v0.2 architecture

```mermaid
flowchart LR
    Client["Client"] --> API["taskforge-api"]
    API --> DB[("PostgreSQL")]
    Migrator["taskforge-migrate"] --> DB
    Worker -->|"claim + lease<br/>heartbeat + fenced outcome<br/>expired-lease recovery"| DB
```

PostgreSQL is the durable source of truth. The API and worker never run
migrations automatically; `taskforge-migrate` applies embedded, versioned SQL
migrations under a PostgreSQL advisory lock. Multiple worker processes can
safely cooperate; each process has a fixed, configurable executor count.

## Job lifecycle

```mermaid
stateDiagram-v2
    [*] --> Queued
    Queued --> Running
    Queued --> Canceled
    Running --> Succeeded
    Running --> Failed
    Running --> Queued: lease expired / attempt abandoned
```

`succeeded`, `failed`, and `canceled` are terminal. The queued-to-running claim
uses a transaction, `FOR UPDATE SKIP LOCKED`, a conditional state update, and
attempt insertion before commit. Every ownership assignment increments a
fencing token and establishes a lease using PostgreSQL time.

## Quick start

Requirements:

- Go 1.25.x
- Docker Desktop with Compose
- Git

Start the complete development stack from PowerShell:

```powershell
docker compose up --build
Invoke-RestMethod http://127.0.0.1:8080/healthz
Invoke-RestMethod http://127.0.0.1:8080/readyz
```

Compose waits for PostgreSQL, runs the migration service to completion, then
starts the API and worker. PostgreSQL and the API bind to loopback only. If host
port 5432 is already in use:

```powershell
$env:TASKFORGE_POSTGRES_PORT = "55432"
docker compose up --build
```

The credentials in `.env.example` and `compose.yaml` are development-only.

## Commands

Run a migration explicitly outside Compose:

```powershell
$env:TASKFORGE_DATABASE_URL = "postgres://taskforge:taskforge@127.0.0.1:5432/taskforge?sslmode=disable"
go run ./cmd/taskforge-migrate
```

Run the API and worker in separate PowerShell terminals after migration:

```powershell
go run ./cmd/taskforge-api
```

```powershell
$env:TASKFORGE_WORKER_ID = "local-worker-1"
go run ./cmd/taskforge-worker
```

The API, worker, and migrator require `TASKFORGE_DATABASE_URL`. The worker also
requires a validated `TASKFORGE_WORKER_ID`; concurrency, lease, heartbeat, and
recovery settings are documented in `.env.example`. Database URLs, payloads,
results, and idempotency keys are never intentionally logged.

## API example

Submit the safe built-in `demo.echo` job:

```powershell
curl.exe -i -X POST http://127.0.0.1:8080/v1/jobs `
  -H "Content-Type: application/json" `
  --data '{"queue":"default","kind":"demo.echo","payload":{"message":"hello"},"priority":0,"idempotency_key":"demo-001"}'
```

Inspect or cancel a job:

```powershell
Invoke-RestMethod http://127.0.0.1:8080/v1/jobs/<job-id>
Invoke-RestMethod -Method Post http://127.0.0.1:8080/v1/jobs/<job-id>/cancel
```

GET responses exclude payloads and idempotency keys. Result metadata reports
only whether a bounded result exists and its byte count. Errors use a stable
JSON envelope with a machine code and request ID; raw PostgreSQL errors are not
returned.

## Submission idempotency

Idempotency applies only to job creation and is scoped by queue:

- Without an idempotency key, each valid request creates a job (`201`).
- The first request with a key creates a job (`201`).
- Replaying the same normalized queue, kind, payload, and priority returns the
  existing job (`200`).
- Reusing the key with different immutable fields returns `409`.

The fingerprint is SHA-256 over a deterministic JSON representation. This does
not make handler side effects idempotent; execution-side idempotency is future
work.

## Worker behavior and failure semantics

Each process runs a fixed executor pool. Claims use `FOR UPDATE SKIP LOCKED`,
assign a lease, increment both attempt and fencing token, and commit before a
handler starts. A handler renews its lease periodically. Heartbeat and outcome
writes require the same job ID, worker ID, and fencing token, so a stale
execution cannot mutate TaskForge's PostgreSQL state.

The bounded recovery loop locks expired jobs with `SKIP LOCKED`, records the
expired attempt as `abandoned`, clears its lease, and returns it to `queued`.
The next claim gets a higher fencing token. This provides at-least-once—not
exactly-once—execution: a crash can occur after an external side effect but
before completion is persisted. External effects therefore need idempotency
keys or downstream fencing support.

On graceful shutdown the worker stops claiming and recovering, continues
heartbeats while active handlers receive a bounded drain window, then cancels
remaining local work. A hard crash cannot drain; recovery begins only after
the lease expires. The built-in registry still supports only bounded
`demo.echo` and never executes arbitrary user code.

## Validation

PowerShell validation without `make`:

```powershell
go mod tidy
go mod verify
gofmt -w .
go vet ./...
go test ./...
go test -race ./...
go build ./...
docker compose config --quiet
git diff --check
```

Run PostgreSQL integration tests against a dedicated development database:

```powershell
$env:TASKFORGE_TEST_DATABASE_URL = "postgres://taskforge:taskforge@127.0.0.1:5432/taskforge_test?sslmode=disable"
go test ./... -run Integration -count=1
```

Integration tests create isolated schemas and skip when the variable is absent.
CI supplies a real PostgreSQL service and runs the suite with the race detector.

## Security limitations

v0.2 has no authentication, authorization, TLS termination, multi-tenancy, or
rate limiting. Bind it to loopback or a trusted development network only. Job
payloads and results are potentially sensitive; the public GET API deliberately
does not expose them. See [SECURITY.md](SECURITY.md).

## Known v0.2 limitations

- Delivery is at least once; PostgreSQL fencing cannot fence external systems.
- There is no automatic retry policy, backoff, maximum-attempt policy, or
  dead-letter queue. Lease recovery only returns abandoned work to `queued`.
- No delayed/cron scheduling, NATS, gRPC,
  workflows, Kubernetes, web UI, Prometheus, or OpenTelemetry.
- Submission idempotency does not guarantee idempotent execution side effects.

## Roadmap

| Version | Scope | Status |
| --- | --- | --- |
| v0.1 | Durable PostgreSQL API, migrations, and sequential worker | Implemented |
| v0.2 | Concurrent workers, leases, fencing, heartbeat, and crash recovery | Implemented |
| v0.3 | Retry scheduling, execution idempotency, and dead-letter queue | Planned |
| v0.4 | Delayed and recurring scheduling | Planned |
| v0.5 | Transactional outbox and NATS JetStream | Planned |
| v0.6 | Workflow DAGs and saga compensation | Planned |
| v0.7 | Metrics, tracing, dashboards, and SLOs | Planned |
| v0.8 | High availability, Kubernetes, load, and chaos testing | Planned |

See the [product requirements](docs/PRD.md),
[current architecture](docs/CURRENT_STATE.md), and [architecture decisions](docs/adr).

## License

No license has been selected yet. Until one is added, all rights are reserved.
