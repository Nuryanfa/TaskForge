# TaskForge operations guide

Operational procedures for running TaskForge v0.5 in a development or trusted
lab environment. TaskForge is not production ready and has no authentication.

## Deployment order

1. Start PostgreSQL and NATS (with JetStream, `-js`) and wait until both are
   healthy.
2. Run `taskforge-migrate` and wait for it to exit with status 0
   (`migration_complete`). Concurrent runs are safe; they serialize on a
   PostgreSQL session lock.
3. Start or roll `taskforge-api` replicas.
4. Start or roll `taskforge-worker` processes, each with a unique
   `TASKFORGE_WORKER_ID` (and optionally `TASKFORGE_NATS_URL` for wake-ups).
5. Start `taskforge-relay`. Running two gives a warm standby; only one
   publishes at a time.

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
- **Relay**: one publishes at a time by design. Raise
  `TASKFORGE_OUTBOX_BATCH_SIZE` before anything else if the backlog grows.
- PostgreSQL is the throughput ceiling. Watch connection count and lock wait
  time before adding workers.

## Health checks

| Check | Meaning | Use for |
| --- | --- | --- |
| `GET /healthz` | Process is serving HTTP. | Liveness. |
| `GET /readyz` | PostgreSQL ping succeeded within the query timeout. | Readiness / load-balancer membership. |
| `/taskforge-api healthcheck` | Container-internal liveness probe against `127.0.0.1:8080/healthz`. | Docker healthcheck. |

The worker and relay have no HTTP endpoint; monitor them through logs, job
state, and the outbox backlog. NATS exposes `/healthz?js-enabled-only=true`
on its monitoring port (8222) for container health checks.

## Log events

All processes log JSON to stdout. Messages are fixed event names; payloads,
results, keys, and DSNs are never logged.

| Event | Level | Process | Meaning / action |
| --- | --- | --- | --- |
| `invalid_configuration` | error | all | Startup aborted; check env vars against [CONFIGURATION.md](CONFIGURATION.md). |
| `database_connection_failed` | error | A W R | Could not connect or ping at startup. |
| `nats_connection_failed` | error | R | NATS client could not be created at startup. |
| `relay_started` / `relay_stopped` | info | R | Lifecycle with stream name. |
| `relay_stream_ready` | info | R | Stream created or verified. |
| `relay_stream_failed` | error | R | Stream setup failed (NATS down or JetStream disabled); retried every second. |
| `relay_batch_published` | info | R | `count` events acknowledged and marked. |
| `relay_batch_failed` | error | R | A publish or batch transaction failed; acknowledged prefix kept, rest retried. |
| `relay_outbox_purged` | info | R | `count` old published rows deleted. |
| `relay_outbox_purge_failed` | error | R | Retention purge failed; retried next minute. |
| `relay_outbox_backlog` | info | R | Unpublished events (logged every minute). A rising value means NATS or the relay is unhealthy. |
| `worker_wakeups_enabled` | info | W | Worker subscribed to `job.queued` wake-ups. |
| `worker_wakeup_subscribe_failed` | error | W | Wake-ups disabled; worker continues with polling only. |
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
| `SHUTDOWN_TIMEOUT` | yes | runner | Worker shut down and the handler did not finish within `TASKFORGE_SHUTDOWN_TIMEOUT`. |
| `INVALID_JOB_RESULT` | no | runner | Handler returned invalid JSON or a result larger than `TASKFORGE_JOB_RESULT_MAX_BYTES`. |
| `LEASE_EXPIRED` | yes | recovery | Worker crashed, was killed, paused, or lost database connectivity. |
| `UNKNOWN_JOB_KIND` | no | registry | No handler registered for `kind`. |
| `INVALID_JOB_PAYLOAD` | no | `demo.echo` | Payload missing `message` or has extra fields. |
| `RESULT_TOO_LARGE` | no | `demo.echo` | Result exceeds `TASKFORGE_JOB_RESULT_MAX_BYTES`. |
| `RESULT_ENCODING_FAILED` | no | `demo.echo` | Result could not be encoded. |
| `DEMO_FAILURE` | no | `demo.fail` | Deliberate permanent failure (or the payload's custom `code`). |
| `DEMO_TRANSIENT` | yes | `demo.flaky` | Deliberate retryable failure for the first `fail_attempts` attempts. |
| `V01_MIGRATION_RECOVERY` | — | migration 2 | Attempt closed while upgrading from v0.1. |

## Runbooks

### Dead-letter queue is growing

1. List recent entries: `GET /v1/dead-letters?limit=50`.
2. Group by `kind` and `error_code`.
   - Permanent codes (`UNKNOWN_JOB_KIND`, `INVALID_JOB_PAYLOAD`,
     `INVALID_JOB_RESULT`) indicate a client, handler, or deployment bug. Fix the cause first; redriving will fail again.
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

### Workflow ended `compensation_failed`

A compensation job was dead-lettered. TaskForge stops starting further
compensations, so some succeeded steps may still be uncompensated.

1. `GET /v1/workflows/{id}`: steps whose `compensation.status` is `failed` are
   the cause; steps still `waiting` were never compensated.
2. Find the failed compensation in the DLQ by its `job_id`
   (`GET /v1/dead-letters/{job-id}`) and fix the cause.
3. Redrive it (`POST /v1/dead-letters/{job-id}/redrive`). This creates a
   **standalone** job with the same execution key; the workflow status does not
   change.
4. For steps still `waiting`, undo their effects by submitting standalone
   compensation jobs (or in the downstream system directly). Use the original
   action's execution key so the downstream can deduplicate.

### Workflow seems stuck in `running` or `compensating`

- A non-terminal workflow always has at least one `queued` or `running` job.
  Check them with the SQL below; they follow the normal job runbooks.
- A step waiting in retry backoff keeps the workflow `running` until it
  succeeds or is dead-lettered.
- `worker_recovery_failed` with `record step outcome … state conflict` means a
  workflow's rows were edited by hand. Recovery skips that job and continues
  with the rest; restore the workflow rows or leave the job for an operator.

### Outbox backlog is growing

1. Check relay logs for `relay_stream_failed` or `relay_batch_failed`.
2. Check NATS health: `GET http://<nats>:8222/healthz?js-enabled-only=true`.
3. Confirm a relay is running for this database; standby relays log nothing.
4. Inspect the oldest unpublished event:
   `SELECT id, event_type, created_at FROM outbox_events WHERE published_at IS NULL ORDER BY id LIMIT 5;`
5. Job processing is unaffected; events resume in order once NATS recovers.

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

-- Outbox backlog and age of the oldest unpublished event
SELECT count(*), NOW() - min(created_at) AS oldest
FROM outbox_events WHERE published_at IS NULL;

-- Attempt history for one job
SELECT attempt, worker_id, fencing_token, started_at, finished_at, outcome, error_code
FROM job_attempts WHERE job_id = $1 ORDER BY attempt;
```

Avoid manual `UPDATE`s on `jobs`. They can violate the lease invariant or the
fencing trigger and bypass the attempt history.

Workflow state and its live jobs:

```sql
SELECT s.position, s.name, s.status, s.compensation_status, j.status AS job_status, c.status AS compensation_job_status
FROM workflow_steps s
LEFT JOIN jobs j ON j.id = s.job_id
LEFT JOIN jobs c ON c.id = s.compensation_job_id
WHERE s.workflow_id = $1 ORDER BY s.position;
```

## Backup and retention

- All state is in PostgreSQL; standard PostgreSQL backups (for example
  `pg_dump` or base backups with WAL archiving) cover TaskForge entirely.
- Published `outbox_events` rows are purged after `TASKFORGE_OUTBOX_RETENTION`;
  JetStream keeps messages for `TASKFORGE_NATS_STREAM_MAX_AGE`.
- v0.6 has no other automatic retention. `jobs`, `job_attempts`,
  `dead_letters`, `workflows`, and `workflow_steps` grow without bound. Archive
  or delete terminal rows manually only when no dead-letter row or workflow
  step references them.
- Backups contain payloads and results; treat them as sensitive.
