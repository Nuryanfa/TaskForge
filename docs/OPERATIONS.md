# TaskForge operations guide

Operational procedures for running TaskForge v0.3 in a development or trusted
lab environment. TaskForge is not production ready and has no authentication.

## Deployment order

1. Start PostgreSQL and wait until it accepts connections.
2. Run `taskforge-migrate` and wait for it to exit with status 0
   (`migration_complete`). Concurrent runs are safe; they serialize on a
   PostgreSQL session lock.
3. Start or roll `taskforge-api` replicas.
4. Start or roll `taskforge-worker` processes, each with a unique
   `TASKFORGE_WORKER_ID`.

Compose performs these steps automatically with `docker compose up --build`.

### Rolling upgrades

- Migrations are additive and run before new binaries start. Old binaries must
  tolerate the new schema for the duration of the rollout.
- Stop workers with SIGTERM, not SIGKILL. Give the container a stop grace
  period longer than `TASKFORGE_SHUTDOWN_TIMEOUT`.
- A worker killed without draining leaves jobs `running` until their leases
  expire; another worker recovers them. No manual action is needed.

## Scaling

- **API**: stateless; add replicas behind a trusted reverse proxy.
- **Workers**: add processes or raise `TASKFORGE_WORKER_CONCURRENCY`. Each
  executor holds at most one connection at a time plus heartbeat traffic.
- PostgreSQL is the throughput ceiling. Watch connection count and lock wait
  time before adding workers.

## Health checks

| Check | Meaning | Use for |
| --- | --- | --- |
| `GET /healthz` | Process is serving HTTP. | Liveness. |
| `GET /readyz` | PostgreSQL ping succeeded within the query timeout. | Readiness / load-balancer membership. |
| `/taskforge-api healthcheck` | Container-internal liveness probe against `127.0.0.1:8080/healthz`. | Docker healthcheck. |

The worker has no HTTP endpoint in v0.3; monitor it through logs and job state.

## Log events

All processes log JSON to stdout. Messages are fixed event names; payloads,
results, keys, and DSNs are never logged.

| Event | Level | Process | Meaning / action |
| --- | --- | --- | --- |
| `invalid_configuration` | error | all | Startup aborted; check env vars against [CONFIGURATION.md](CONFIGURATION.md). |
| `database_connection_failed` | error | A W | Could not connect or ping at startup. |
| `migration_complete` / `migration_failed` | info / error | M | Outcome of the migration run. |
| `api_started` / `api_stopped` | info | A | Lifecycle. |
| `api_server_failed` / `api_shutdown_failed` | error | A | Listener error or drain timeout. |
| `worker_started` / `worker_stopped` | info | W | Lifecycle with `worker_id` and `concurrency`. |
| `worker_claim_failed` | error | W | Claim query failed (other than "no job"); usually DB availability. |
| `worker_heartbeat_failed` | error | W | Lease renewal failed for a reason other than lost ownership; execution was cancelled. |
| `worker_persist_outcome_failed` | error | W | Could not write success/failure; job will be recovered after lease expiry. |
| `worker_recovery_failed` | error | W | Recovery transaction failed. |
| `worker_jobs_recovered` | info | W | `count` expired leases were requeued or dead-lettered. Frequent occurrences mean crashes or a lease that is too short. |
| `worker_schedule_dispatch_failed` | error | W | Recurring schedule dispatch transaction failed. |
| `worker_schedules_dispatched` | info | W | `count` due schedules materialized into queued jobs. |

## Job failure codes

Codes stored in `jobs.last_error_code`, `job_attempts.error_code`, and
`dead_letters.error_code`.

| Code | Retryable | Source | Typical cause |
| --- | --- | --- | --- |
| `EXECUTION_TIMEOUT` | yes | runner / handler | Handler exceeded `TASKFORGE_JOB_EXECUTION_TIMEOUT`. |
| `LEASE_EXPIRED` | yes | recovery | Worker crashed, was killed, paused, or lost database connectivity. |
| `UNKNOWN_JOB_KIND` | no | registry | No handler registered for `kind`. |
| `INVALID_JOB_PAYLOAD` | no | `demo.echo` | Payload missing `message` or has extra fields. |
| `RESULT_TOO_LARGE` | no | `demo.echo` | Result exceeds `TASKFORGE_JOB_RESULT_MAX_BYTES`. |
| `RESULT_ENCODING_FAILED` | no | `demo.echo` | Result could not be encoded. |
| `V01_MIGRATION_RECOVERY` | — | migration 2 | Attempt closed while upgrading from v0.1. |

## Runbooks

### Dead-letter queue is growing

1. List recent entries: `GET /v1/dead-letters?limit=50`.
2. Group by `kind` and `error_code`.
   - Permanent codes (`UNKNOWN_JOB_KIND`, `INVALID_JOB_PAYLOAD`) indicate a
     client or deployment bug. Fix the cause first; redriving will fail again.
   - `EXECUTION_TIMEOUT` suggests a slow dependency or a timeout that is too
     low.
   - `LEASE_EXPIRED` suggests worker crashes or a lease shorter than real
     heartbeat latency.
3. After fixing the cause, redrive individual entries with
   `POST /v1/dead-letters/{job-id}/redrive`. Redrive is idempotent; retrying
   the call returns the same new job.
4. Remember that redrive can execute the logical work again. Confirm the
   handler's downstream honours the execution key.

### Jobs stuck in `queued`

- Is any worker running and logging `worker_started`?
- Is `available_at` in the future? The job may be in retry backoff.
- Does any worker log `worker_claim_failed`? Check database health.

### Jobs stuck in `running`

- Check `lease_expires_at` in the database. If it is in the past, recovery
  should pick it up within `TASKFORGE_RECOVERY_INTERVAL`; verify at least one
  worker is running.
- If the lease keeps getting renewed, the handler is still executing; it is
  bounded by `TASKFORGE_JOB_EXECUTION_TIMEOUT`.

### Useful read-only SQL

```sql
-- Queue depth and state distribution
SELECT queue, status, count(*) FROM jobs GROUP BY 1, 2 ORDER BY 1, 2;

-- Jobs waiting on retry backoff
SELECT id, kind, attempt_count, max_attempts, last_error_code, available_at
FROM jobs WHERE status = 'queued' AND available_at > NOW()
ORDER BY available_at LIMIT 50;

-- Running jobs and their leases
SELECT id, lease_owner, fencing_token, lease_expires_at, lease_expires_at - NOW() AS remaining
FROM jobs WHERE status = 'running' ORDER BY lease_expires_at;

-- Attempt history for one job
SELECT attempt, worker_id, fencing_token, started_at, finished_at, outcome, error_code
FROM job_attempts WHERE job_id = $1 ORDER BY attempt;
```

Avoid manual `UPDATE`s on `jobs`. They can violate the lease invariant or the
fencing trigger and bypass the attempt history.

## Backup and retention

- All state is in PostgreSQL; standard PostgreSQL backups (for example
  `pg_dump` or base backups with WAL archiving) cover TaskForge entirely.
- v0.3 has no automatic retention. `jobs`, `job_attempts`, and `dead_letters`
  grow without bound. Archive or delete terminal rows manually only when no
  dead-letter row references them.
- Backups contain payloads and results; treat them as sensitive.
