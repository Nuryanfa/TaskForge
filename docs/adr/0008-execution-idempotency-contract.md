# ADR 0008: Execution idempotency is a propagated contract

- Status: Accepted for v0.3

Every logical job receives a random execution idempotency key at creation. It
is passed to handlers in structured execution metadata and remains stable over
retries, crash recovery, and dead-letter redrive. It is distinct from the
queue-scoped submission idempotency key and is not part of the existing
submission fingerprint.

TaskForge does not make downstream side effects exactly once. External systems
must accept and enforce the execution key or a fencing token. The key is not
returned by public APIs or intentionally logged.
