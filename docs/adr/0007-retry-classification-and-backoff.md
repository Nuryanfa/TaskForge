# ADR 0007: Typed failures and bounded retry backoff

- Status: Accepted for v0.3

Handlers return a stable safe code plus an explicit retryable flag. Invalid
payloads and unknown kinds are permanent; timeouts and expired leases are
retryable. Each job snapshots maximum attempts, initial and maximum backoff,
and jitter percentage when created. Maximum attempts includes the first run.

Retry delay grows exponentially and is capped before overflow. Jitter is
bounded and deterministic from job identity and attempt, keeping concurrent
behavior testable. PostgreSQL `NOW()` remains authoritative when setting
`available_at`. Attempt finalization, retry scheduling, lease clearing, and
terminal dead-lettering are atomic.
