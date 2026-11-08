# TaskForge job lifecycle events

TaskForge v0.5 publishes one event for every job insert or status change to a
NATS JetStream stream. This document is the contract for consumers. For why
it works this way see [ADR 0011](adr/0011-transactional-outbox-and-nats-jetstream.md).

## Guarantees

- **Atomic with state.** An event exists if and only if the job transition
  that caused it committed in PostgreSQL.
- **At least once.** The relay may publish an event more than once. JetStream
  discards duplicates with the same `Nats-Msg-Id` within the duplicate window
  (`TASKFORGE_NATS_DUPLICATE_WINDOW`, default 2 minutes). Consumers must
  deduplicate by `event_id` to be safe beyond that window.
- **Per-job order.** Events for one job arrive in transition order and their
  `event_id` values strictly increase. Order **across different jobs** is not
  guaranteed.
- **Privacy.** Events never contain job payloads, results, idempotency keys,
  execution keys, or lease owners.
- **Not a command channel.** Events describe what already happened. Job
  ownership is decided only by PostgreSQL.

## Stream

| Setting | Default | Variable |
| --- | --- | --- |
| Stream name | `TASKFORGE_EVENTS` | `TASKFORGE_NATS_STREAM` |
| Subjects | `taskforge.events.>` | fixed |
| Storage | file, limits retention, discard old | fixed |
| Max age | `168h` | `TASKFORGE_NATS_STREAM_MAX_AGE` |
| Duplicate window | `2m` | `TASKFORGE_NATS_DUPLICATE_WINDOW` |

The relay creates or updates the stream at startup and after publish failures.

## Subjects and event types

| Subject | Emitted when |
| --- | --- |
| `taskforge.events.job.queued` | A job is created: submission, schedule dispatch, or dead-letter redrive (the new job). |
| `taskforge.events.job.started` | A worker claims the job (`queued → running`). |
| `taskforge.events.job.retry_scheduled` | An attempt failed retryably or its lease expired and attempts remain (`running → queued`). |
| `taskforge.events.job.succeeded` | The handler succeeded. |
| `taskforge.events.job.failed` | The job failed permanently or exhausted attempts; it is dead-lettered. |
| `taskforge.events.job.canceled` | A queued job was canceled. |

Queue names may contain dots, so the queue is not a subject token; filter on
the `TaskForge-Queue` header or the body.

## Message

Headers:

| Header | Example |
| --- | --- |
| `Nats-Msg-Id` | `taskforge-outbox-42` |
| `TaskForge-Event-Type` | `job.succeeded` |
| `TaskForge-Queue` | `default` |

Body:

```json
{
  "version": 1,
  "event_id": 42,
  "type": "job.succeeded",
  "occurred_at": "2026-11-07T13:59:56.889088Z",
  "job": {
    "job_id": "b39b7078-29de-40d4-9999-c1f45b98deee",
    "queue": "default",
    "kind": "demo.echo",
    "status": "succeeded",
    "attempt": 1,
    "max_attempts": 3,
    "priority": 0,
    "error_code": null,
    "available_at": "2026-11-07T13:59:56.701213+00:00",
    "schedule_id": null,
    "workflow_id": null,
    "dead_lettered": false
  }
}
```

| Field | Meaning |
| --- | --- |
| `version` | Body schema version. Additive fields do not change it. |
| `event_id` | Unique outbox identity; use it for deduplication. |
| `occurred_at` | PostgreSQL time of the transition. |
| `job.attempt` | Attempts started so far (`0` before the first claim). |
| `job.error_code` | Last failure code, e.g. `LEASE_EXPIRED` for a recovered crash. |
| `job.available_at` | When the job is or was next claimable. |
| `job.dead_lettered` | `true` on `job.failed` events. |
| `job.workflow_id` | Workflow that owns the job (action or compensation), or `null`. Added in v0.6; additive, so `version` stays `1`. |

Workflow-level transitions (`compensating`, `compensated`, …) are not
published in v0.6. Consumers can follow a workflow through the job events of
its steps, or read `GET /v1/workflows/{id}`. Jobs canceled by a workflow abort
emit ordinary `job.canceled` events.

## Consuming

Example with the `nats` CLI (development only):

```powershell
nats stream view TASKFORGE_EVENTS
nats sub "taskforge.events.job.failed"
```

Durable consumers should use a JetStream consumer with explicit acks and
store the last processed `event_id` per job.
