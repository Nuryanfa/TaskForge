# ADR 0008: Execution idempotency is a propagated contract

- Status: Accepted for v0.3
- Date: 2026-10-09

## Context

With retries, crash recovery, and redrive, one logical job can run several
times. The submission idempotency key (ADR 0003) only deduplicates creation and
is optional, so handlers had no reliable identifier to deduplicate their
external side effects. TaskForge cannot fence systems it does not own.

## Decision

Every logical job receives a random execution idempotency key at creation. It
is passed to handlers in structured execution metadata and remains stable over
retries, crash recovery, and dead-letter redrive. It is distinct from the
queue-scoped submission idempotency key and is not part of the existing
submission fingerprint.

TaskForge does not make downstream side effects exactly once. External systems
must accept and enforce the execution key or a fencing token. The key is not
returned by public APIs or intentionally logged.

## Consequences

- Handlers have one stable key per logical job to pass to payment providers,
  email services, or databases with unique constraints.
- Redrive is safe only if downstream systems honour the key; a downstream that
  ignores it can observe duplicates.
- Because the key is never returned by the API, clients cannot correlate
  external records with it directly; correlation must use the job ID.
