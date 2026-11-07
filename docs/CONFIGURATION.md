# TaskForge configuration reference

All configuration comes from environment variables, parsed and validated by
[`internal/config`](../internal/config/config.go). Invalid values stop the
process at startup with the log event `invalid_configuration` (the offending
value is not logged). Durations use Go syntax such as `500ms`, `10s`, `2m`.

Process legend: **A** = `taskforge-api`, **W** = `taskforge-worker`,
**M** = `taskforge-migrate`, **R** = `taskforge-relay`.

## Database

| Variable | Default | Bounds | Used by | Notes |
| --- | --- | --- | --- | --- |
| `TASKFORGE_DATABASE_URL` | — (required) | `postgres://` or `postgresql://` with host and database | A W M R | Never logged. |
| `TASKFORGE_DATABASE_MAX_CONNS` | `10` | `1..100` | A W R | `pgxpool` maximum. |
| `TASKFORGE_DATABASE_MIN_CONNS` | `1` | `0..100`, ≤ max | A W R | |
| `TASKFORGE_DATABASE_CONNECT_TIMEOUT` | `5s` | `> 0`, ≤ `5m` | A W M R | |
| `TASKFORGE_DATABASE_QUERY_TIMEOUT` | `3s` | `> 0`, ≤ `5m` | A W M R | Bounds each store call and `/readyz`. The relay's publish batch is bounded by `TASKFORGE_OUTBOX_BATCH_TIMEOUT` instead. |

## HTTP server (API)

| Variable | Default | Bounds | Notes |
| --- | --- | --- | --- |
| `TASKFORGE_HTTP_ADDR` | `127.0.0.1:8080` | valid `host:port` | Use `:8080` inside containers. |
| `TASKFORGE_READ_TIMEOUT` | `15s` | ≤ `5m` | |
| `TASKFORGE_READ_HEADER_TIMEOUT` | `5s` | ≤ `5m` | |
| `TASKFORGE_WRITE_TIMEOUT` | `15s` | ≤ `5m` | |
| `TASKFORGE_IDLE_TIMEOUT` | `60s` | ≤ `5m` | |
| `TASKFORGE_DEAD_LETTER_PAGE_SIZE` | `50` | `1..100` | Default and maximum `limit`. |

## Shared

| Variable | Default | Bounds | Used by | Notes |
| --- | --- | --- | --- | --- |
| `TASKFORGE_SHUTDOWN_TIMEOUT` | `10s` | ≤ `5m` | A W | API: HTTP drain. Worker: handler drain and outcome-write bound. |
| `TASKFORGE_JOB_PAYLOAD_MAX_BYTES` | `65536` | `1..1048576` | A | Canonical payload limit. |
| `TASKFORGE_JOB_RESULT_MAX_BYTES` | `65536` | `1..1048576` | W | Handler result limit. |

## Worker

| Variable | Default | Bounds | Notes |
| --- | --- | --- | --- |
| `TASKFORGE_WORKER_ID` | — (required) | `^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$` | Must be **unique per running process**; it is the lease owner. |
| `TASKFORGE_WORKER_CONCURRENCY` | `4` | `1..64` | Executor count. Keep ≤ `DATABASE_MAX_CONNS` minus headroom for heartbeat and recovery. |
| `TASKFORGE_WORKER_POLL_INTERVAL` | `500ms` | ≤ `1m` | Idle sleep between empty claims. |
| `TASKFORGE_JOB_EXECUTION_TIMEOUT` | `30s` | ≤ `30m` | Handler deadline; timeout fails with retryable `EXECUTION_TIMEOUT`. |
| `TASKFORGE_JOB_LEASE_DURATION` | `30s` | `1s..10m` | Time a worker may go without a heartbeat. |
| `TASKFORGE_JOB_HEARTBEAT_INTERVAL` | `10s` | `≥ 100ms`, **< lease** | Recommended ≤ lease/3. |
| `TASKFORGE_RECOVERY_INTERVAL` | `5s` | `100ms..5m` | Expired-lease scan frequency. |
| `TASKFORGE_RECOVERY_BATCH_SIZE` | `100` | `1..1000` | Rows recovered per transaction. |
| `TASKFORGE_SCHEDULE_INTERVAL` | `1s` | `100ms..1m` | Due-schedule scan frequency. |
| `TASKFORGE_SCHEDULE_BATCH_SIZE` | `50` | `1..500` | Due schedules dispatched per transaction. |

## NATS and outbox relay

| Variable | Default | Bounds | Used by | Notes |
| --- | --- | --- | --- | --- |
| `TASKFORGE_NATS_URL` | unset | `nats://` or `tls://` with host | R (required), W (optional) | Unset on a worker means polling only. May contain credentials; never logged. |
| `TASKFORGE_NATS_CONNECT_TIMEOUT` | `5s` | ≤ `5m` | R W | Per connection attempt; clients reconnect forever in the background. |
| `TASKFORGE_NATS_PUBLISH_TIMEOUT` | `2s` | ≤ `5m` | R | Per event, and for stream setup. |
| `TASKFORGE_NATS_STREAM` | `TASKFORGE_EVENTS` | `^[A-Za-z0-9_-]{1,64}$` | R | Stream bound to `taskforge.events.>`. |
| `TASKFORGE_NATS_STREAM_MAX_AGE` | `168h` | `1m..8760h` | R | JetStream message retention. |
| `TASKFORGE_NATS_DUPLICATE_WINDOW` | `2m` | `1s..1h`, ≤ max age | R | Window in which republished events are dropped by `Nats-Msg-Id`. |
| `TASKFORGE_OUTBOX_POLL_INTERVAL` | `200ms` | `10ms..1m` | R | Wait after a non-full batch; also bounds wake-up latency. |
| `TASKFORGE_OUTBOX_BATCH_SIZE` | `100` | `1..1000` | R | Events per publish transaction. |
| `TASKFORGE_OUTBOX_BATCH_TIMEOUT` | `10s` | ≤ `5m` | R | Bounds one batch transaction including network publishes. Keep ≥ batch size × typical publish latency. |
| `TASKFORGE_OUTBOX_RETENTION` | `24h` | `1m..720h` | R | Published rows older than this are purged; unpublished rows never are. |

## Retry policy

These values are **snapshotted into each job when it is created**, so they take
effect in the process that creates jobs: the API (and redrive copies the
original snapshot). Changing them affects only new jobs.

| Variable | Default | Bounds | Notes |
| --- | --- | --- | --- |
| `TASKFORGE_JOB_MAX_ATTEMPTS` | `3` | `1..100` | Includes the first run. `1` disables retry. |
| `TASKFORGE_RETRY_INITIAL_BACKOFF` | `1s` | `1ms..30m`, ≤ max | Delay after the first failed attempt. |
| `TASKFORGE_RETRY_MAX_BACKOFF` | `60s` | `1ms..30m` | Cap for exponential growth. |
| `TASKFORGE_RETRY_JITTER_PERCENT` | `20` | `0..100` | Deterministic ± jitter; result never exceeds max. |

## Compose and tests only

| Variable | Default | Notes |
| --- | --- | --- |
| `TASKFORGE_POSTGRES_PORT` | `5432` | Host port mapped by `compose.yaml` (loopback only). |
| `TASKFORGE_TEST_DATABASE_URL` | unset | Enables PostgreSQL integration tests; each test creates an isolated schema. |
| `TASKFORGE_TEST_NATS_URL` | unset | Enables NATS JetStream integration and event end-to-end tests (also needs the database URL). |
| `TASKFORGE_NATS_PORT` | `4222` | Host port for NATS mapped by `compose.yaml` (loopback only). |

## Tuning guidance

- **Lease vs. heartbeat**: a lease must survive at least two missed heartbeats.
  Short leases detect crashes faster but raise the chance of a false expiry
  under database latency or GC pauses.
- **Execution timeout vs. lease**: the execution timeout can exceed the lease
  because heartbeats renew it; it bounds runaway handlers.
- **Crash detection time** ≈ lease duration + recovery interval.
- **Shutdown**: container stop grace periods should exceed
  `TASKFORGE_SHUTDOWN_TIMEOUT` so in-flight outcomes can be persisted.
- **Connections**: total connections ≈ Σ(API max conns) + Σ(worker max conns)
  must stay below PostgreSQL `max_connections`.
