# ADR 0005: Leases, heartbeat, and fencing

## Status

Accepted for v0.2.

## Context

A v0.1 worker could crash after changing a job to `running`, leaving no safe
way to reassign it. Concurrent processes also need protection against a stale
execution writing after ownership changes.

## Decision

Every claim assigns `lease_owner`, sets `lease_expires_at` from PostgreSQL's
clock, and increments a per-job `fencing_token`. The claim also stores that
token on the corresponding attempt and commits before handler execution.

The default lease is 30 seconds and the default heartbeat interval is 10
seconds. Configuration requires heartbeat to be shorter than the lease and
bounds both values. Renewal succeeds only for the running job's current owner
and fencing token while its lease is still valid.

Completion and failure use the same fenced identity and atomically clear the
lease while closing the attempt. Losing ownership cancels local execution and
suppresses its normal outcome.

## Consequences

Old executions cannot modify TaskForge's PostgreSQL state after reassignment.
Fencing does not automatically protect external systems: handlers must use an
idempotency key or pass a fencing token to a downstream system that enforces
it. Delivery remains at least once.
