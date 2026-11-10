# Architecture decision records

Each ADR records one decision, its context, and its consequences. ADRs are
immutable once accepted; a later decision supersedes or amends an earlier one
by reference instead of rewriting it.

| ADR | Title | Release | Status |
| --- | --- | --- | --- |
| [0001](0001-postgresql-source-of-truth.md) | PostgreSQL is the durable source of truth | v0.1 | Accepted |
| [0002](0002-postgresql-transactional-claiming.md) | Claim jobs transactionally in PostgreSQL | v0.1 | Accepted; amended by 0005 |
| [0003](0003-submission-idempotency.md) | Scope submission idempotency by queue and request fingerprint | v0.1 | Accepted |
| [0004](0004-v0.1-sequential-worker.md) | Limit v0.1 to one sequential worker | v0.1 | Superseded by 0005, 0006 |
| [0005](0005-leases-heartbeats-and-fencing.md) | Leases, heartbeat, and fencing | v0.2 | Accepted |
| [0006](0006-expired-lease-recovery-and-delivery.md) | Expired-lease recovery and at-least-once delivery | v0.2 | Accepted; amended by 0007 |
| [0007](0007-retry-classification-and-backoff.md) | Typed failures and bounded retry backoff | v0.3 | Accepted |
| [0008](0008-execution-idempotency-contract.md) | Execution idempotency is a propagated contract | v0.3 | Accepted |
| [0009](0009-dead-letter-storage-and-redrive.md) | Payload-free dead letters and immutable redrive | v0.3 | Accepted |
| [0010](0010-delayed-jobs-and-recurring-schedules.md) | Delayed jobs and transactional recurring schedules | v0.4 | Accepted |
| [0011](0011-transactional-outbox-and-nats-jetstream.md) | Transactional outbox, single active relay, and NATS wake-ups | v0.5 | Accepted |
| [0012](0012-workflow-dags-and-saga-compensation.md) | Workflow DAGs advance in the job's transaction, with saga compensation | v0.6 | Accepted |
| [0013](0013-trace-context-through-the-database.md) | Trace context travels with the job through the database | v0.7 | Accepted |

## Writing a new ADR

Copy this template to `NNNN-short-title.md` using the next number:

```markdown
# ADR NNNN: Title

- Status: Proposed | Accepted | Superseded by ADR XXXX
- Date: YYYY-MM-DD

## Context

What forces are at play and why a decision is needed.

## Decision

What we decided, stated precisely enough to verify in code.

## Consequences

What becomes easier, what becomes harder, and what is explicitly not solved.
```

Add the new ADR to the table above in the same change.
