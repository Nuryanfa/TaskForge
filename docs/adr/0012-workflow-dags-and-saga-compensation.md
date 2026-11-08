# ADR 0012: Workflow DAGs advance in the job's transaction, with saga compensation

- Status: Accepted for v0.6
- Date: 2026-11-08

## Context

Through v0.5 every job is independent. Real work often needs several jobs that
depend on each other ("reserve, then charge, then ship") and a defined way to
undo completed work when a later step fails. Two designs were available for
driving such workflows:

1. A separate engine that reacts to job lifecycle events (for example from
   NATS, ADR 0011) and creates the next jobs.
2. Advancing the workflow inside the same PostgreSQL transaction that makes a
   job terminal.

Option 1 reintroduces the dual-write and ordering problems ADR 0001 and ADR
0011 were written to avoid: an event can be delayed, duplicated, or processed
after a newer transition, and the broker would effectively decide job state.

## Decision

1. **Same-transaction engine.** A Go engine in `internal/store/postgres` runs
   inside every transaction that moves a workflow-owned job to a terminal
   status: completion (`finishJob`), dead-lettering (`finalizeFailure`, used by
   `FailJob` and lease recovery), and the engine's own cancellation of queued
   jobs. A job outcome and its workflow reaction commit or roll back together.
   Go was chosen over a PL/pgSQL trigger because the DAG logic is easier to
   read and unit test; the risk of a future path forgetting the engine is
   covered by invariant I8 (step state agrees with its jobs) in every workflow
   test.
2. **Lock order.** Job rows, then `workflows` rows in ascending id order, then
   `workflow_steps` rows. Lease recovery locks every involved workflow in that
   order before recovering any job, and recovers each job under its own
   savepoint so that one job whose workflow cannot advance does not block
   recovery of the rest. Queued jobs canceled by the engine are locked with
   `SKIP LOCKED`; a survivor is swept on the next engine pass.
3. **Exactly-once dispatch.** The workflow row lock serializes fan-in counter
   updates; dispatch is guarded by step status and by a unique index on
   `(workflow_step_id, workflow_role)`. Dispatched jobs are stamped with
   `clock_timestamp()` so `created_at` never precedes the dependency
   completion the dispatching transaction waited for.
4. **Saga compensation.** When a step fails terminally or the workflow is
   canceled: pending steps are skipped, queued actions (including ones waiting
   in retry backoff) are canceled, and running actions are left to finish.
   Succeeded steps are compensated in reverse dependency order: a step's
   compensation starts only after every succeeded dependent's compensation has
   succeeded and no dependent is still running. Failed steps are never
   compensated. The first dead-lettered compensation makes the workflow
   `compensation_failed` and stops further compensation; in-flight work still
   records its outcome.
5. **No data flow between steps.** Payloads are static at submission. A
   compensation receives the execution key of the action it undoes
   (`WorkflowContext.CompensatesExecutionKey`, extending ADR 0008).
6. **Retry policy snapshot on the workflow,** so jobs dispatched later by
   workers use the creating API's policy.
7. **Redrive detaches.** Redriving a dead-lettered workflow job creates a
   standalone job and does not change the workflow. Individual workflow jobs
   cannot be canceled; only the whole workflow can.

## Consequences

- Workflow state is exactly as consistent as job state; there is no window in
  which a job is terminal but its workflow has not reacted.
- The workflow row is a serialization point; very wide workflows contend on
  it. Steps are bounded (default 32, maximum 100), so this is acceptable.
- At-least-once execution still applies to every step and compensation.
  Compensations must be idempotent and should use the action's execution key
  downstream.
- Passing results between steps, conditional branches, templates, and
  resuming a workflow after redrive are not supported.
- Correctness is demonstrated by invariants I1–I9 checked from the database
  after scenario, race, crash, and randomized soak tests (see
  `docs/TESTING.md`).
