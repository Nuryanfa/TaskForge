# TaskForge system design (v0.3)

This document describes how TaskForge is built: its components, package
structure, concurrency model, the transactional protocols behind every job
state change, and the failure modes those protocols are designed to survive.
For *why* individual decisions were made, see the [ADRs](adr/README.md). For
the database schema see [DATA_MODEL.md](DATA_MODEL.md), and for the HTTP
contract see [API.md](API.md).

## 1. Goals and non-goals

Goals:

- Durable job submission and state that survive process restarts and crashes.
- Safe cooperation of many worker processes without a coordinator other than
  PostgreSQL.
- Explicit, documented delivery semantics: **at least once**.
- Bounded everything: connections, goroutines, batch sizes, payloads, results,
  timeouts, retries, and page sizes.
- No leakage of payloads, results, idempotency keys, or credentials through the
  public API or logs.

Non-goals for v0.3: exactly-once execution, delayed/cron scheduling, message
brokers, workflows, authentication, multi-tenancy, metrics/tracing, and
execution of arbitrary user code.

## 2. Component view

```mermaid
flowchart LR
    subgraph Clients
        C["HTTP client"]
    end
    subgraph Processes
        API["taskforge-api<br/>(net/http, stateless)"]
        W1["taskforge-worker #1<br/>N executors + 1 recovery loop"]
        W2["taskforge-worker #k"]
        M["taskforge-migrate<br/>(one-shot)"]
    end
    DB[("PostgreSQL<br/>jobs · job_attempts · dead_letters")]

    C -->|"REST/JSON"| API
    API -->|"create · get · cancel<br/>list DLQ · redrive"| DB
    W1 -->|"claim · heartbeat · complete/fail<br/>recover expired leases"| DB
    W2 --> DB
    M -->|"goose migrations<br/>under advisory lock"| DB
```

| Process | Responsibility | Scaling | State |
| --- | --- | --- | --- |
| `taskforge-api` | Validates and persists submissions; read/cancel; DLQ inspection and redrive; health/readiness. | Horizontally, any number of replicas. | Stateless. |
| `taskforge-worker` | Claims and executes jobs through a fixed executor pool; renews leases; persists outcomes; recovers expired leases. | Horizontally; each process has a unique `TASKFORGE_WORKER_ID`. | Only in-flight executions. |
| `taskforge-migrate` | Applies embedded SQL migrations, then exits. | Run once per deploy; concurrent runs serialize on a session lock. | None. |
| PostgreSQL | Single durable source of truth and coordination point. | Single primary (HA is v0.8). | All durable state. |

The API and worker **never** run migrations. Deploy order is always
PostgreSQL → `taskforge-migrate` (to completion) → API and workers. Compose
encodes this with `service_healthy` and `service_completed_successfully`.

## 3. Package structure

```text
cmd/
  taskforge-api/       wiring: config → store → httpapi → http.Server; signal-driven shutdown
  taskforge-worker/    wiring: config → store → registry → worker.Runner
  taskforge-migrate/   wiring: config → migrations.Run
internal/
  config/              env parsing, defaults, bounds, cross-field validation
  job/                 pure domain: Job, Submission, Execution, Failure, DeadLetter,
                       status set, canonical fingerprint, RetryDelay
  httpapi/             HTTP routing, request validation, response shaping, error envelope
  store/postgres/      explicit SQL; transactions; error classification
  worker/              executor pool, heartbeat, shutdown drain, handler registry
  migrations/          goose provider with PostgreSQL session locker
db/migrations/         versioned SQL files embedded into the binaries
```

Dependency direction (no cycles; `job` depends on nothing internal):

```mermaid
flowchart TD
    cmd["cmd/*"] --> config
    cmd --> httpapi
    cmd --> worker
    cmd --> postgres["store/postgres"]
    cmd --> migrations
    httpapi --> job
    httpapi --> postgres
    worker --> job
    worker --> postgres
    postgres --> job
    postgres --> config
    migrations --> dbm["db/migrations (embed)"]
```

`httpapi` and `worker` each declare a narrow `Store` interface containing only
the methods they need. `*postgres.Store` satisfies both, and unit tests use
in-memory fakes. Concurrency-sensitive SQL is never mocked; it is covered by
integration tests against real PostgreSQL (see [TESTING.md](TESTING.md)).

## 4. Domain model

| Type | Meaning |
| --- | --- |
| `job.Job` | Full durable row, including lease, fencing, and retry snapshot fields. |
| `job.Submission` | Validated, canonicalized creation request plus its SHA-256 fingerprint. |
| `job.Execution` | One ownership epoch: `(JobID, Attempt, WorkerID, FencingToken, ExecutionKey)`. Every worker mutation must present it. |
| `job.Failure` | `{Code, Retryable}`; `Code` must match `^[A-Z][A-Z0-9_]{0,63}$`. |
| `job.DeadLetter` | Payload-free operational record of a terminally failed job. |

### Job state machine

```mermaid
stateDiagram-v2
    [*] --> queued: POST /v1/jobs
    queued --> running: claim (lease + fencing_token+1 + attempt+1)
    queued --> canceled: POST /cancel
    running --> succeeded: CompleteJob (fenced)
    running --> queued: retryable failure, attempts remain<br/>(available_at = NOW() + backoff)
    running --> queued: lease expired, attempts remain<br/>(attempt = abandoned, LEASE_EXPIRED)
    running --> failed: permanent failure or attempts exhausted<br/>(+ dead_letters row)
    failed --> [*]
    succeeded --> [*]
    canceled --> [*]
    note right of failed
        Redrive never mutates a failed job.
        It inserts a NEW queued job that
        shares the execution key.
    end note
```

Transitions are enforced by the `WHERE` clauses of the SQL statements
(`status = 'queued'`, `status = 'running' AND lease_owner = … AND
fencing_token = …`), not by application-side checks. `job.ValidateTransition`
documents the user-visible subset and is unit-tested, but the database is the
enforcement point.

## 5. Core protocols

All timestamps that affect correctness (`lease_expires_at`, `available_at`,
`started_at`, `completed_at`) come from PostgreSQL `NOW()`, so worker clock
skew cannot shorten or extend leases or backoff.

### 5.1 Submission

```mermaid
sequenceDiagram
    participant C as Client
    participant A as API
    participant D as PostgreSQL
    C->>A: POST /v1/jobs {queue, kind, payload, priority, idempotency_key?}
    A->>A: validate headers, size, unknown fields, identifiers
    A->>A: canonicalize payload, SHA-256 fingerprint
    A->>D: INSERT ... ON CONFLICT (queue, idempotency_key) DO NOTHING RETURNING *
    alt inserted
        D-->>A: new row
        A-->>C: 201 Created
    else conflict (key already used)
        A->>D: SELECT by (queue, idempotency_key)
        alt same fingerprint
            A-->>C: 200 OK (existing job)
        else different fingerprint
            A-->>C: 409 IDEMPOTENCY_CONFLICT
        end
    end
```

The retry policy (`max_attempts`, backoff bounds, jitter) and a random
`execution_idempotency_key` are written at creation time. The policy comes from
the **API process's** configuration; changing worker retry settings does not
affect already-created jobs.

### 5.2 Claim, execute, heartbeat, finish

```mermaid
sequenceDiagram
    participant E as Executor goroutine
    participant H as Handler goroutine
    participant D as PostgreSQL
    E->>D: BEGIN
    E->>D: SELECT id FROM jobs WHERE status='queued' AND available_at<=NOW()<br/>ORDER BY priority DESC, created_at, id FOR UPDATE SKIP LOCKED LIMIT 1
    E->>D: UPDATE jobs SET status='running', attempt_count+1, fencing_token+1,<br/>lease_owner, lease_expires_at=NOW()+lease
    E->>D: INSERT job_attempts (attempt, worker_id, fencing_token)
    E->>D: COMMIT
    E->>H: Execute(ctx with execution timeout, Execution, payload)
    loop every heartbeat interval
        E->>D: UPDATE jobs SET lease_expires_at=NOW()+lease<br/>WHERE owner, token match AND lease not expired
        alt 0 rows
            E->>H: cancel (ownership lost) — outcome suppressed
        end
    end
    H-->>E: result or Failure
    E->>D: CompleteJob / FailJob (fenced, single transaction)
```

Key properties:

- Claim, state change, and attempt insert are one transaction; there is never a
  `running` job without its open attempt.
- `SKIP LOCKED` lets concurrent claimers pass over each other's rows. It
  maximizes throughput and does not guarantee strict FIFO fairness.
- The fencing token is incremented on every claim and is guarded by a trigger
  that rejects any decrease.
- Success requires `lease_expires_at > NOW()`; a worker whose lease already
  expired cannot record success even if recovery has not run yet.

### 5.3 Failure, retry, and dead-lettering

`FailJob` locks the row `FOR UPDATE` with the full fenced identity, then calls
`finalizeFailure` inside the same transaction:

```mermaid
flowchart TD
    F["Failure{Code, Retryable}"] --> A["close attempt: outcome=failed|abandoned, error_code"]
    A --> Q{"Retryable AND attempt_count < max_attempts?"}
    Q -- yes --> R["status=queued<br/>available_at = NOW() + RetryDelay()<br/>clear lease, set last_error_code"]
    Q -- no --> T["status=failed, completed_at, dead_lettered_at<br/>clear lease"]
    T --> DL["INSERT dead_letters ON CONFLICT DO NOTHING"]
```

`RetryDelay(jobID, failedAttempt, initial, max, jitter%)`:

1. `delay = initial · 2^(failedAttempt−1)`, doubled step by step and capped at
   `max` before it can overflow.
2. If jitter > 0, `span = delay · jitter / 100` and
   `offset ∈ [−span, +span]` is derived from `SHA-256(jobID ":" attempt)`.
3. Result is clamped to `[0, max]`.

Jitter is deterministic so that tests can assert exact values while different
jobs still spread out. With defaults (1 s initial, 60 s max, 20 % jitter,
3 attempts) a job that always fails runs at roughly t=0, t≈1 s, t≈3 s and is
then dead-lettered.

### 5.4 Expired-lease recovery

Each worker process runs one recovery loop every `TASKFORGE_RECOVERY_INTERVAL`:

1. `BEGIN`; select up to `RECOVERY_BATCH_SIZE` rows with
   `status='running' AND lease_expires_at <= NOW()` using
   `FOR UPDATE SKIP LOCKED`.
2. For each row call `finalizeFailure` with
   `Failure{LEASE_EXPIRED, Retryable: true}` and attempt outcome `abandoned`.
3. `COMMIT`.

Because recovery reuses the retry path, an abandoned attempt consumes one of
`max_attempts`, waits for backoff, and dead-letters with `LEASE_EXPIRED` once
attempts are exhausted. `SKIP LOCKED` ensures concurrent recovery loops never
recover the same ownership epoch twice.

### 5.5 Redrive

```mermaid
sequenceDiagram
    participant C as Operator
    participant A as API
    participant D as PostgreSQL
    C->>A: POST /v1/dead-letters/{job-id}/redrive
    A->>D: BEGIN; SELECT redriven_job_id FROM dead_letters WHERE job_id=$1 FOR UPDATE
    alt already redriven
        A->>D: SELECT the linked job; COMMIT
        A-->>C: 200 OK (same new job)
    else first redrive
        A->>D: INSERT new job copying queue, kind, payload, priority,<br/>execution key and retry snapshot; idempotency_key = NULL
        A->>D: UPDATE dead_letters SET redriven_job_id, redriven_at
        A->>D: COMMIT
        A-->>C: 201 Created (new job)
    end
```

The original job and its attempt history are never modified. The row lock on
the dead letter serializes concurrent redrives so exactly one new job exists.

## 6. Worker concurrency model

```mermaid
flowchart TB
    Run["Runner.Run(ctx)"] --> E1["executor 1"]
    Run --> E2["executor …"]
    Run --> EN["executor N = TASKFORGE_WORKER_CONCURRENCY"]
    Run --> RL["recovery loop"]
    E1 --> X["execute(): handler goroutine + heartbeat ticker"]
```

- Exactly `Concurrency + 1` long-lived goroutines per process, plus at most one
  handler goroutine per executor. There is no unbounded fan-out.
- An executor that finds no eligible job sleeps `TASKFORGE_WORKER_POLL_INTERVAL`;
  after finishing a job it polls again immediately.
- The handler runs under a context derived from `context.Background()` with
  `TASKFORGE_JOB_EXECUTION_TIMEOUT`, so shutdown signals do not abort it
  instantly; the drain logic below decides when to cancel.
- Heartbeat RPCs are bounded by the heartbeat interval; outcome persistence is
  bounded by `TASKFORGE_SHUTDOWN_TIMEOUT`.

### Graceful shutdown

1. SIGINT/SIGTERM cancels the run context. Executors stop claiming and the
   recovery loop exits.
2. Each in-flight execution starts a drain timer (`TASKFORGE_SHUTDOWN_TIMEOUT`)
   and keeps heartbeating.
3. If the handler finishes in time, its outcome is persisted normally.
4. If the drain timer fires, the handler is cancelled and **no outcome is
   written**. The job stays `running` until its lease expires and another
   worker's recovery loop requeues it with `LEASE_EXPIRED`.

## 7. Handler contract

```go
type Handler interface {
    Execute(ctx context.Context, exec job.Execution, payload json.RawMessage) (json.RawMessage, *job.Failure)
}
```

- Return `(result, nil)` for success. The result must be valid JSON within
  `TASKFORGE_JOB_RESULT_MAX_BYTES`.
- Return `(nil, &job.Failure{Code, Retryable})` for failure. Codes must be
  stable, uppercase machine codes and must not contain sensitive data.
- Respect `ctx` cancellation; it fires on execution timeout, lost ownership,
  or expired shutdown drain.
- Use `exec.ExecutionKey` (stable across retries, recovery, and redrive) as the
  idempotency key for external side effects, or pass `exec.FencingToken` to a
  downstream that enforces monotonic tokens.

The registry is a fixed, compile-time map. v0.3 ships only `demo.echo`, which
expects `{"message": "<non-empty string>"}` and returns it unchanged. Unknown
kinds fail permanently with `UNKNOWN_JOB_KIND`.

## 8. Failure-mode analysis

| Scenario | What happens | Guarantee |
| --- | --- | --- |
| API crashes after `INSERT` commits but before responding | Client retries with the same idempotency key and gets `200` with the same job. | No duplicate job (when a key is used). |
| Two clients submit the same key concurrently | Unique index + `ON CONFLICT DO NOTHING`; loser reads the winner's row. | One job. |
| Two executors claim simultaneously | `SKIP LOCKED` gives each a different row. | One owner per job. |
| Worker hard-crashes mid-handler | Lease expires; recovery marks attempt `abandoned`, requeues with backoff. | Job eventually re-runs or dead-letters. |
| Worker is paused (GC, VM freeze) past its lease, then resumes | Its heartbeat or outcome write finds a different token/expired lease → `ErrOwnershipLost`; outcome suppressed. | Stale writer cannot change TaskForge state. |
| Crash after an external side effect but before `CompleteJob` | Handler runs again on a later attempt. | **At least once.** External systems must dedupe by execution key. |
| PostgreSQL unavailable | Store returns `ErrUnavailable`; API → `503`, worker logs and polls again; `/readyz` fails. | No partial transitions (transactions roll back). |
| Concurrent redrive requests | Dead-letter row lock serializes them. | One new job. |
| Concurrent migration commands | Goose session lock serializes them; the second sees an up-to-date schema. | Migrations applied once. |
| Shutdown drain expires | Handler cancelled, nothing persisted; recovery after lease expiry. | Same as crash, but bounded by drain + lease. |

## 9. Security and privacy design

- The API never returns `payload`, `result`, `idempotency_key`, or
  `execution_idempotency_key`; it reports only `has_result` and `result_bytes`.
- Dead letters copy no payloads, results, or keys.
- Error codes are validated against a strict pattern before storage, which
  prevents raw error strings (possibly containing data) from being persisted.
- Raw PostgreSQL errors are mapped to a small set of public codes.
- Logs are structured JSON with fixed event names and no request bodies, DSNs,
  or keys.
- Request bodies are size-limited, must be `application/json` (UTF-8 only),
  reject `Content-Encoding`, unknown fields, and trailing data.
- Containers run as `nonroot` on a distroless static image.
- There is still **no authentication or TLS**; see [SECURITY.md](../SECURITY.md).

## 10. Known design gaps (tracked for later versions)

- `SHUTDOWN_TIMEOUT` is defined as a failure code but the runner does not
  persist it; drained-out jobs are recovered only via lease expiry.
- If `CompleteJob` rejects a handler result (invalid JSON or too large), the
  error is logged and the job is recovered after lease expiry instead of being
  failed immediately.
- Retry policy is global per API process; there is no per-queue or per-kind
  policy and no client-provided policy.
- No retention or archival for `jobs`, `job_attempts`, or `dead_letters`.
- Polling-based claiming; NATS-based wake-ups arrive with the outbox in v0.5.
- `/healthz` container healthcheck targets the fixed address `127.0.0.1:8080`.
