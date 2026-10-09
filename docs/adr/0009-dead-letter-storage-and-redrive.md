# ADR 0009: Payload-free dead letters and immutable redrive

- Status: Accepted for v0.3

Permanent and attempt-exhausted jobs become terminal and receive one
PostgreSQL dead-letter record in the same transaction. The record references
the original job and stores bounded operational metadata only; payloads,
results, submission keys, execution keys, and raw errors are not duplicated.

Redrive locks the dead-letter record, creates a new queued job with a null
submission key and the same execution key and retry snapshot, then records the
new job reference. Repeated and concurrent redrive requests return that same
job. Original jobs and attempt history remain immutable.
