# ADR 0001: PostgreSQL is the durable source of truth

- Status: Accepted
- Date: 2026-10-04

## Context

TaskForge must eventually recover job state after process and worker failures. Later
releases may use NATS JetStream to wake workers and distribute notifications,
but introducing two authoritative stores would create ambiguous recovery and
dual-write failure modes.

## Decision

PostgreSQL owns job definitions, state transitions, attempts, retry deadlines,
and terminal results. State changes occur in explicit transactions. The v0.1
sequential worker claims one eligible row from queued to running with row-level
locking.

Bounded concurrent workers, leases, fencing tokens, heartbeat and renewal, and
crash recovery are deferred to v0.2. Lease columns and their invariants will be
introduced in a new migration. If the v0.1 worker dies while a job is running,
that job is not automatically recovered.

When messaging is introduced, events will be derived from committed database
state through a transactional outbox. Broker delivery will be at least once,
and consumers must be idempotent.

## Consequences

- Recovery can be reasoned about from one durable state store.
- Database transactions and indexes become part of the core correctness model.
- PostgreSQL availability limits job mutation availability.
- Queue throughput will be benchmarked before introducing additional machinery.
- Exactly-once execution is not promised.
