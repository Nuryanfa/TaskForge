# ADR 0010: Delayed jobs and transactional recurring schedules

- Status: Accepted for v0.4
- Date: 2026-11-05

## Context

v0.3 used `jobs.available_at` only for internal retry backoff; clients could not
submit work for future execution or define recurring jobs. Supporting delayed
and recurring execution requires clear time authority, idempotency rules, and
bounded recovery when workers or schedules fall behind so a paused or offline
system never floods the queue with historical backlog.

## Decision

`POST /v1/jobs` accepts either `run_at` (RFC3339 timestamp) or `delay_seconds`
(bounded up to 30 days), never both. `run_at` is normalized to UTC microseconds
and included in the submission idempotency fingerprint; `delay_seconds` is
included in the fingerprint and evaluated relative to PostgreSQL `NOW()` on
insert. Delayed jobs remain in `queued` state until `available_at <= NOW()` and
can be canceled while `queued`.

Recurring schedules live in a dedicated `schedules` table keyed by a unique
`name` (`^[a-z0-9][a-z0-9._-]{0,63}$`) with a 5-field POSIX cron expression and
an IANA timezone (embedded via Go's `time/tzdata`). `POST /v1/schedules` uses a
SHA-256 fingerprint over normalized immutable fields for idempotent creation by
`name`.

Workers run a bounded schedule dispatch loop that locks due unpaused rows
(`next_run_at <= NOW()`) with `FOR UPDATE SKIP LOCKED`, materializes at most one
job for the due slot (`(schedule_id, scheduled_for)` enforced by a partial
unique index on `jobs`), and advances `next_run_at` to the next tick strictly
after `GREATEST(next_run_at, NOW())`. Resuming a paused schedule whose
`next_run_at` is in the past advances `next_run_at` past `NOW()`.

## Consequences

- Delayed and recurring jobs reuse the existing claim, lease, fencing, retry,
  and dead-letter pipeline without a separate broker.
- Missed ticks while a schedule is paused or workers are offline coalesce into
  at most one catch-up run ("skip missed"), preventing thundering herds.
- Deleting a schedule preserves historical jobs (`ON DELETE SET NULL` on
  `jobs.schedule_id`) while retaining `scheduled_for` audit timestamps.
- Public schedule responses omit `payload`, matching job and dead-letter privacy
  guarantees.

