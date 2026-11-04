# TaskForge HTTP API reference (v0.4)

Base URL in development: `http://127.0.0.1:8080`. All request and response
bodies are JSON. There is no authentication in v0.4; do not expose the API to
untrusted networks.

## Conventions

### Request ID

- Clients may send `X-Request-ID` matching `^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`.
- If absent, the server generates a UUID. The value is echoed in the
  `X-Request-ID` response header and in every error body.
- An invalid value yields `400 INVALID_REQUEST_ID` with a newly generated ID.

### Request bodies (`POST /v1/jobs`)

- `Content-Type` must be `application/json`; the only allowed parameter is
  `charset=utf-8`. Otherwise `415 UNSUPPORTED_MEDIA_TYPE`.
- `Content-Encoding` other than `identity` is rejected with `415`.
- Exactly one JSON object; unknown fields and trailing data are rejected.
- Body limit is `TASKFORGE_JOB_PAYLOAD_MAX_BYTES + 4096` bytes.

### Error envelope

```json
{
  "error": {
    "code": "JOB_NOT_FOUND",
    "message": "job was not found",
    "request_id": "6f1c5c0e-..."
  }
}
```

`code` is stable and intended for programmatic handling. `message` is
human-readable and may change. Raw database errors are never returned.

### Job resource

Returned by every job endpoint and by redrive.

```json
{
  "id": "0b8e7a8e-3a43-4a25-9a6e-0f1c2b8d9e10",
  "queue": "default",
  "kind": "demo.echo",
  "status": "queued",
  "priority": 0,
  "attempt_count": 0,
  "available_at": "2026-10-09T10:00:00Z",
  "created_at": "2026-10-09T10:00:00Z",
  "updated_at": "2026-10-09T10:00:00Z",
  "started_at": "2026-10-09T10:00:01Z",
  "completed_at": "2026-10-09T10:00:01Z",
  "last_error_code": "EXECUTION_TIMEOUT",
  "has_result": false,
  "result_bytes": 0,
  "max_attempts": 3,
  "dead_lettered": false
}
```

| Field | Notes |
| --- | --- |
| `status` | `queued`, `running`, `succeeded`, `failed`, `canceled`. |
| `available_at` | Earliest claim time; moves forward during retry backoff. |
| `started_at`, `completed_at`, `last_error_code` | Omitted when null. |
| `has_result`, `result_bytes` | The result body itself is never returned. `result_bytes` is omitted when 0. |
| `dead_lettered` | `true` once the job has a dead-letter record. |
| `schedule_id`, `scheduled_for` | Included when the job was materialized by a recurring schedule. |

Never returned: `payload`, `result`, `idempotency_key`, execution key, lease
owner, fencing token.

## Endpoints

### `POST /v1/jobs` — submit a job

Request:

| Field | Type | Required | Rules |
| --- | --- | --- | --- |
| `queue` | string | no | Default `default`. `^[a-z0-9][a-z0-9._-]{0,63}$`. |
| `kind` | string | yes | Same pattern. Must be a registered handler to succeed at run time. |
| `payload` | object | yes | JSON object, valid UTF-8, ≤ payload limit after canonicalization. |
| `priority` | integer | no | `-100..100`, default 0. |
| `idempotency_key` | string | no | 1–128 printable ASCII characters (`0x21–0x7E`), no spaces. |
| `run_at` | RFC3339 timestamp | no | Earliest execution time (≤ 30 days ahead). Mutually exclusive with `delay_seconds`. |
| `delay_seconds` | integer | no | `0..2592000` (30 days), added to PostgreSQL `NOW()`. Mutually exclusive with `run_at`. |

Responses:

| Status | When |
| --- | --- |
| `201 Created` | A new job was created. |
| `200 OK` | Idempotent replay: same queue + key + identical normalized fields. |
| `400 INVALID_JSON` | Malformed body, unknown fields, or trailing data. |
| `400 VALIDATION_FAILED` | Field rules violated. |
| `409 IDEMPOTENCY_CONFLICT` | Key reused in the same queue with different fields. |
| `413 REQUEST_TOO_LARGE` / `413 PAYLOAD_TOO_LARGE` | Body or canonical payload too large. |
| `415 UNSUPPORTED_MEDIA_TYPE` | Wrong content type or encoding. |
| `503 DEPENDENCY_UNAVAILABLE` | Database unavailable. |

### `GET /v1/jobs/{id}` — get a job

`200` with the job resource; `400 INVALID_JOB_ID` if not a UUID;
`404 JOB_NOT_FOUND`.

### `POST /v1/jobs/{id}/cancel` — cancel a queued job

Only `queued` jobs can be cancelled; the result is terminal `canceled`.

`200` with the job; `400 INVALID_JOB_ID`; `404 JOB_NOT_FOUND`;
`409 JOB_STATE_CONFLICT` if the job is running or terminal.

### `GET /v1/dead-letters` — list dead letters

Query parameters (each at most once; any other parameter is rejected):

| Parameter | Rules |
| --- | --- |
| `limit` | `1..TASKFORGE_DEAD_LETTER_PAGE_SIZE` (default page size 50, max 100). |
| `cursor` | Opaque value from a previous `next_cursor`. |

Ordering is newest first by `(dead_lettered_at, job_id)`; keyset pagination is
stable under concurrent inserts.

```json
{
  "items": [
    {
      "job_id": "…",
      "queue": "default",
      "kind": "demo.echo",
      "final_attempt": 3,
      "error_code": "EXECUTION_TIMEOUT",
      "dead_lettered_at": "2026-10-09T10:00:05Z",
      "redriven_job_id": "…",
      "redriven_at": "2026-10-09T11:00:00Z"
    }
  ],
  "next_cursor": "eyJhdCI6Ii4uLiIsImlkIjoiLi4uIn0"
}
```

`next_cursor` is omitted on the last page. Errors: `400 INVALID_QUERY`,
`400 INVALID_CURSOR`.

### `GET /v1/dead-letters/{job-id}` — get one dead letter

`200` with a single item; `400 INVALID_JOB_ID`; `404 JOB_NOT_FOUND`.

### `POST /v1/dead-letters/{job-id}/redrive` — redrive

Creates a new queued job from the dead-lettered one, reusing its payload,
priority, execution key, and retry snapshot (no submission idempotency key).

| Status | When |
| --- | --- |
| `201 Created` | First redrive; body is the **new** job. |
| `200 OK` | Already redriven; body is the same previously created job. |
| `404 JOB_NOT_FOUND` | No dead letter for this job ID. |

### `GET /healthz` — liveness

Always `200 {"status":"ok"}` while the process serves HTTP. No dependencies
are checked.

### `GET /readyz` — readiness

`200 {"status":"ready"}` if a PostgreSQL ping succeeds within
`TASKFORGE_DATABASE_QUERY_TIMEOUT`; otherwise `503 DEPENDENCY_UNAVAILABLE`.

### `POST /v1/schedules` — create a recurring schedule

| Field | Type | Required | Rules |
| --- | --- | --- | --- |
| `name` | string | yes | Unique identifier (`^[a-z0-9][a-z0-9._-]{0,63}$`); also scopes creation idempotency. |
| `queue` | string | no | Default `default`. |
| `kind` | string | yes | Handler identifier. |
| `payload` | object | yes | JSON object within payload limit. Never returned on read. |
| `priority` | integer | no | `-100..100`, default 0. |
| `cron_expr` | string | yes | Standard 5-field POSIX cron (`minute hour day-of-month month day-of-week`). |
| `timezone` | string | no | Default `UTC`. Valid IANA timezone name (e.g. `Asia/Jakarta`). |
| `paused` | boolean | no | Default `false`. |

Returns `201 Created` on first creation, `200 OK` on identical replay for the
same `name`, and `409 IDEMPOTENCY_CONFLICT` if `name` is reused with different
fields.

### `GET /v1/schedules` — list schedules

Supports `limit` (`1..TASKFORGE_DEAD_LETTER_PAGE_SIZE`) and `cursor` ordered by
`(created_at DESC, id DESC)`.

### `GET /v1/schedules/{id}` — get a schedule

`200 OK` with the schedule resource; `400 INVALID_SCHEDULE_ID`;
`404 SCHEDULE_NOT_FOUND`.

### `POST /v1/schedules/{id}/pause` and `POST /v1/schedules/{id}/resume`

Idempotently pauses or resumes a schedule (`200 OK`). Resuming a schedule whose
`next_run_at` is in the past advances `next_run_at` to the next tick after
PostgreSQL `NOW()`.

### `DELETE /v1/schedules/{id}` — delete a schedule

`204 No Content`, or `404 SCHEDULE_NOT_FOUND`. Historical jobs materialized by
the schedule are preserved with `schedule_id = NULL`.

## Error code catalog

| HTTP | Code | Meaning |
| --- | --- | --- |
| 400 | `INVALID_REQUEST_ID` | `X-Request-ID` is malformed. |
| 400 | `INVALID_JSON` | Body is not exactly one valid JSON object with known fields. |
| 400 | `VALIDATION_FAILED` | A field violates its rules. |
| 400 | `INVALID_JOB_ID` | Path job ID is not a UUID. |
| 400 | `INVALID_SCHEDULE_ID` | Path schedule ID is not a UUID. |
| 400 | `INVALID_QUERY` | Unsupported, repeated, or out-of-range query parameter. |
| 400 | `INVALID_CURSOR` | Cursor cannot be decoded. |
| 404 | `JOB_NOT_FOUND` | Job or dead letter does not exist. |
| 404 | `SCHEDULE_NOT_FOUND` | Schedule does not exist. |
| 404 | `ROUTE_NOT_FOUND` | Unknown path. |
| 405 | `METHOD_NOT_ALLOWED` | Known path, wrong method. |
| 409 | `JOB_STATE_CONFLICT` | Operation not allowed in the current state. |
| 409 | `IDEMPOTENCY_CONFLICT` | Idempotency key reused with different fields. |
| 413 | `REQUEST_TOO_LARGE` | HTTP body exceeds the limit. |
| 413 | `PAYLOAD_TOO_LARGE` | Canonical payload exceeds the limit. |
| 415 | `UNSUPPORTED_MEDIA_TYPE` | Content type or encoding not accepted. |
| 500 | `INTERNAL_ERROR` | Unexpected error. |
| 503 | `DEPENDENCY_UNAVAILABLE` | PostgreSQL is unavailable or timed out. |

Job failure codes stored in `last_error_code` and dead letters are listed in
[OPERATIONS.md](OPERATIONS.md#job-failure-codes).

## Examples (PowerShell)

```powershell
# Submit with an idempotency key
curl.exe -i -X POST http://127.0.0.1:8080/v1/jobs `
  -H "Content-Type: application/json" `
  --data '{"kind":"demo.echo","payload":{"message":"hello"},"idempotency_key":"demo-001"}'

# Produce a permanent failure (missing "message") to see the DLQ
curl.exe -s -X POST http://127.0.0.1:8080/v1/jobs `
  -H "Content-Type: application/json" `
  --data '{"kind":"demo.echo","payload":{"wrong":"field"}}'

# Page through dead letters
$page = Invoke-RestMethod "http://127.0.0.1:8080/v1/dead-letters?limit=10"
Invoke-RestMethod "http://127.0.0.1:8080/v1/dead-letters?limit=10&cursor=$($page.next_cursor)"
```
