# ADR 0013: Trace context travels with the job through the database

- Status: Accepted for v0.7
- Date: 2026-11-08

## Context

A TaskForge job is submitted by one process (the API), stored in PostgreSQL,
and executed later, possibly much later, by another process (a worker), which
may in turn create more jobs (workflow steps and compensations, ADR 0012).
Standard OpenTelemetry propagation works on request headers and message
metadata; TaskForge has neither between submission and execution, because
PostgreSQL is the only transport (ADR 0001). Without a decision, every
process would produce disconnected traces, and the most valuable question,
"what happened to this job, end to end?", could not be answered.

Three further constraints shaped the decision:

- Observability must never fail or slow work: a dead collector, a full span
  queue, or a misconfigured endpoint cannot affect jobs or requests.
- The data layer must stay free of exporters and SDK configuration.
- The public API is untrusted: a client must not be able to force sampling or
  attach its spans to someone else's trace.

## Decision

1. **Store the context with the work.** Migration 7 adds `jobs.trace_parent`
   and `workflows.trace_parent` (a W3C `traceparent`, nullable) and
   `jobs.trace_link`. Every path that creates a job stores the span context of
   whatever caused it: the API's `job.enqueue` or `workflow.submit` span, the
   attempt span whose transaction dispatched a workflow step or compensation,
   the `workflow.cancel`, `job.redrive`, `lease.recovery`, or
   `schedule.dispatch` span. The store reads the context from `ctx` through
   `internal/tracectx`, which uses only the OpenTelemetry API.
2. **Parent or link, decided once.** `trace_link` is set in SQL at insertion:
   true only when the client delayed the job (`run_at`/`delay_seconds` in the
   future). A worker's `job.attempt` span is a child of the stored context when
   `trace_link` is false, and a new root with a link to it when true, so a job
   delayed by hours never stretches one trace across that time. Retry backoff
   changes `available_at` but never the rule. Retries of an immediate job are
   sibling attempt spans in the same trace; retries of a delayed job are
   separate linked roots, correlated by job ID and attempt number.
3. **Causal workflow traces.** Because the engine runs inside the attempt's
   transaction (ADR 0012), a dispatched step or compensation is stamped with
   the attempt span that caused it. A saga therefore appears as one causal
   chain: submit → reserve → charge → ship (fails) → refund → release.
4. **Untrusted headers become links.** The API's server span is always a new
   root; a client `traceparent` is attached as a link only.
5. **Failure isolation.** Export is asynchronous with a bounded, drop-on-full
   queue, a bounded timeout, and no retries; failures increment
   `taskforge_otel_export_failures_total` and log one structured event per
   minute. The worker carries the attempt span explicitly into its handler and
   persistence contexts, which deliberately derive from
   `context.Background()` so shutdown does not abort them. With tracing
   disabled, no context is forwarded and `trace_parent` stays NULL.
6. **Metrics after commit.** Job outcomes and workflow settlements are
   reported to an observer only after the transaction (or recovery savepoint)
   that produced them commits, so rolled-back work is never counted.

## Consequences

- One job, one trace (or one linked set for delayed jobs), across API,
  PostgreSQL, worker, and workflow steps, without a message broker.
- Every job row carries ~55 bytes of trace context; it is not sensitive and is
  never returned by the API or added to outbox events.
- Trace propagation through NATS events is not implemented (out of scope for
  v0.7).
- If a process crashes between commit and reporting, one outcome is missing
  from the counters; the database state is still correct.
- Sampling is parent-based from the stored context: a job sampled out at
  submission stays sampled out on every attempt.
