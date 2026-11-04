# ADR 0009: Payload-free dead letters and immutable redrive

- Status: Accepted for v0.3
- Date: 2026-10-09

## Context

Jobs that fail permanently or exhaust their attempts need to be visible to
operators and re-runnable after the cause is fixed. Copying payloads into a
separate DLQ would duplicate sensitive data, and mutating the failed job back
to `queued` would destroy its history and make concurrent redrives ambiguous.

## Decision

Permanent and attempt-exhausted jobs become terminal and receive one
PostgreSQL dead-letter record in the same transaction. The record references
the original job and stores bounded operational metadata only; payloads,
results, submission keys, execution keys, and raw errors are not duplicated.

Redrive locks the dead-letter record, creates a new queued job with a null
submission key and the same execution key and retry snapshot, then records the
new job reference. Repeated and concurrent redrive requests return that same
job. Original jobs and attempt history remain immutable.

## Consequences

- Sensitive data lives in exactly one row (`jobs.payload`).
- Full history of the failed job is preserved for audit and debugging.
- Each dead letter can be redriven once; a redriven job that fails again gets
  its own dead-letter record and can be redriven in turn.
- Operators inspect metadata only; investigating payload content requires
  direct, privileged database access.
- There is no bulk redrive or DLQ retention policy yet.
