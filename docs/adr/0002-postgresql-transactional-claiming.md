# ADR 0002: Claim jobs transactionally in PostgreSQL

- Status: Accepted
- Date: 2026-10-04

## Context

Claiming must not create a running job without an attempt record. The v0.1
worker is sequential, but its repository should preserve correctness when v0.2
adds concurrency.

## Decision

`ClaimNextJob` opens a transaction, selects one eligible queued row ordered by
priority descending, creation time, and ID, and locks it with
`FOR UPDATE SKIP LOCKED`. It conditionally changes the row to `running`,
increments `attempt_count`, inserts the matching attempt, and commits.

Completion and failure also use transactions and require the job to remain
`running` at the expected attempt number. Duplicate or stale outcomes fail with
a state conflict.

## Consequences

- A claim and its attempt history are atomic.
- The SQL remains safe if concurrent claimers are introduced later.
- `SKIP LOCKED` favors throughput and does not promise strict fairness.
- v0.1 still permits only one active worker process operationally.
