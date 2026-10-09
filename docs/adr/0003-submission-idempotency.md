# ADR 0003: Scope submission idempotency by queue and request fingerprint

- Status: Accepted
- Date: 2026-10-04

## Context

Clients can lose an HTTP response and retry job creation. Blindly inserting on
every request produces duplicate jobs, while treating every reuse of a key as
equivalent can hide a changed request.

## Decision

The database enforces a partial unique index on `(queue, idempotency_key)` when
the key is present. TaskForge computes SHA-256 over a deterministic
representation of normalized queue, kind, JSON object payload, and priority.

The first request creates a job. A replay with the same fingerprint returns the
existing job. A replay with different immutable fields returns a conflict.

## Consequences

- Concurrent identical submissions produce one durable job.
- Keys can be reused independently in different queues.
- Keys are not logged or exposed by the read API.
- This protects job creation only; it does not make handler side effects
  idempotent or imply exactly-once execution.
