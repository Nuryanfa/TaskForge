# TaskForge

TaskForge v0.1 is a durable PostgreSQL-backed job foundation written in Go. It
provides job submission, inspection, cancellation, idempotent creation, and a
single sequential demonstration worker.

> TaskForge is an educational portfolio project. It is not production ready,
> does not promise exactly-once execution, and must not be exposed directly to
> an untrusted public network.

## v0.1 architecture

```mermaid
flowchart LR
    Client["Client"] --> API["taskforge-api"]
    API --> DB[("PostgreSQL")]
    Migrator["taskforge-migrate"] --> DB
    Worker["taskforge-worker<br/>one job at a time"] --> DB
```

PostgreSQL is the durable source of truth. The API and worker never run
migrations automatically; `taskforge-migrate` applies embedded, versioned SQL
migrations under a PostgreSQL advisory lock. The v0.1 deployment runs at most
one active sequential worker.

## Job lifecycle

```mermaid
stateDiagram-v2
    [*] --> Queued
    Queued --> Running
    Queued --> Canceled
    Running --> Succeeded
    Running --> Failed
```

`succeeded`, `failed`, and `canceled` are terminal. The queued-to-running claim
uses a transaction, `FOR UPDATE SKIP LOCKED`, a conditional state update, and
attempt insertion before commit.

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
requires a validated `TASKFORGE_WORKER_ID`. Database URLs, payloads, results,
and idempotency keys are never intentionally logged.

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

The v0.1 worker polls transactionally and executes exactly one job at a time.
It supports only `demo.echo`; it never executes commands, scripts, templates,
URLs, binaries, or user-provided code. Execution and result size are bounded.

On graceful shutdown the worker stops claiming, gives its current handler a
bounded drain window, then cancels and records a safe failure when possible.
A hard crash while a job is `running` leaves that job `running`. v0.1 has no
lease, heartbeat, fencing token, retry scheduling, or automatic abandoned-job
recovery. Those correctness mechanisms arrive in v0.2.

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

v0.1 has no authentication, authorization, TLS termination, multi-tenancy, or
rate limiting. Bind it to loopback or a trusted development network only. Job
payloads and results are potentially sensitive; the public GET API deliberately
does not expose them. See [SECURITY.md](SECURITY.md).

## Known v0.1 limitations

- One active sequential worker per deployment is an operational requirement.
- Hard worker crashes can strand a job in `running`.
- No retry scheduling, dead-letter queue, delayed/cron scheduling, NATS, gRPC,
  workflows, Kubernetes, web UI, Prometheus, or OpenTelemetry.
- Submission idempotency does not guarantee idempotent execution side effects.

## Roadmap

| Version | Scope | Status |
| --- | --- | --- |
| v0.1 | Durable PostgreSQL API, migrations, and sequential worker | Implemented |
| v0.2 | Concurrent workers, leases, fencing, heartbeat, and crash recovery | Planned |
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
