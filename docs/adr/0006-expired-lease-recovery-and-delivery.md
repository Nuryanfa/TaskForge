# ADR 0006: Expired-lease recovery and at-least-once delivery

## Status

Accepted for v0.2.

## Context

A hard process crash cannot participate in graceful shutdown. Its jobs must
eventually become eligible for another worker without two recovery processes
reassigning the same ownership epoch.

## Decision

Each worker process runs one recovery loop. A recovery transaction selects no
more than the configured batch size of expired `running` rows using
`FOR UPDATE SKIP LOCKED`. It marks each open attempt `abandoned` with the stable
`LEASE_EXPIRED` code, clears lease ownership, and changes the job to `queued`.
The attempt count and fencing token are retained; the next claim increments
both ownership history values.

Graceful shutdown stops new claims and recovery work, keeps heartbeats active
during a bounded handler drain, and then cancels remaining local handlers. A
hard crash skips that drain and relies on lease expiry and recovery.

## Consequences

Recovery is bounded and safe across multiple processes, but it may execute a
handler more than once. v0.2 deliberately has no retry policy, backoff,
maximum-attempt rule, or dead-letter queue; those are v0.3 concerns.
