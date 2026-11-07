# ADR 0011: Transactional outbox, single active relay, and NATS wake-ups

- Status: Accepted for v0.5
- Date: 2026-11-07

## Context

Through v0.4 the only way to observe job progress was to poll the HTTP API,
and workers found new work only by polling PostgreSQL. Publishing lifecycle
events directly to a broker from the API or worker would be a dual write: a
crash between the database commit and the publish either loses an event or
announces a transition that rolled back. ADR 0001 keeps PostgreSQL as the only
source of truth, so the broker must never decide job state.

## Decision

1. **Outbox written by triggers.** Migration 5 adds `outbox_events` and two
   `AFTER` triggers on `jobs` (insert, and status change) that insert one
   privacy-safe event row inside the same transaction as the change. Six event
   types map from transitions: `job.queued`, `job.started`,
   `job.retry_scheduled`, `job.succeeded`, `job.failed`, `job.canceled`.
   Triggers were chosen over explicit Go inserts because eight store methods
   create or transition jobs; a trigger cannot be forgotten by a new code path.
2. **Separate relay process.** `taskforge-relay` reads unpublished rows in id
   order and publishes them to a JetStream stream (`TASKFORGE_EVENTS`,
   subjects `taskforge.events.>`) with `Nats-Msg-Id: taskforge-outbox-<id>`.
3. **One active relay per schema.** Each batch takes
   `pg_try_advisory_xact_lock(class, hashtext(current_schema()))`; other relays
   stand by. Combined with synchronous, in-order publishing and marking only
   the acknowledged prefix, this preserves per-job event order.
4. **Wake-ups are hints.** Workers may make a core-NATS subscription to
   `taskforge.events.job.queued` and claim immediately when the event shows the
   job was due when it occurred (`available_at <= occurred_at`, both from the
   database clock). Claims are still decided only by PostgreSQL.

## Consequences

- An event exists if and only if its transition committed.
- Delivery to the stream is at least once; JetStream drops duplicates within
  the duplicate window, and consumers deduplicate by `event_id` beyond it.
- Per-job order is preserved; global order across jobs is not.
- Synchronous publishing limits relay throughput to one broker round trip per
  event. Async batching is a possible later optimization.
- NATS outages delay events and wake-ups but never block job processing;
  unpublished events accumulate in PostgreSQL until the relay catches up.
- Published outbox rows are deleted after a retention period; unpublished
  rows are never deleted.
- Jobs that existed before migration 5 get events only for later transitions.
- Comparing the worker's clock with database timestamps was rejected after it
  failed under ~3 seconds of real host/VM clock skew.
