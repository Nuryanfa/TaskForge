# ADR 0007: Typed failures and bounded retry backoff

- Status: Accepted for v0.3; amends ADR 0006
- Date: 2026-10-09

## Context

v0.2 recovered expired leases but had no retry policy: every handler failure
was terminal and recovery requeued work immediately without limit. Transient
failures (timeouts, lost workers) need automatic retry, while permanent
failures (bad payloads, unknown kinds) should not be retried at all. Retries
must be bounded, must not synchronize into thundering herds, and must stay
deterministic enough to test.

## Decision

Handlers return a stable safe code plus an explicit retryable flag. Invalid
payloads and unknown kinds are permanent; timeouts and expired leases are
retryable. Each job snapshots maximum attempts, initial and maximum backoff,
and jitter percentage when created. Maximum attempts includes the first run.

Retry delay grows exponentially and is capped before overflow. Jitter is
bounded and deterministic from job identity and attempt, keeping concurrent
behavior testable. PostgreSQL `NOW()` remains authoritative when setting
`available_at`. Attempt finalization, retry scheduling, lease clearing, and
terminal dead-lettering are atomic.

Expired-lease recovery (ADR 0006) now uses the same path with the retryable
code `LEASE_EXPIRED`, so abandoned attempts consume the attempt budget, wait for
backoff, and can be dead-lettered.

## Consequences

- Transient failures recover automatically; permanent failures fail fast.
- Changing retry configuration affects only newly created jobs; existing and
  redriven jobs keep their snapshot.
- The policy is taken from the job-creating process (the API) and is global;
  per-queue or per-kind policies are not supported yet.
- Deterministic jitter is predictable per job but still spreads different
  jobs apart.
- Retry is still at least once; handlers must be idempotent (ADR 0008).
