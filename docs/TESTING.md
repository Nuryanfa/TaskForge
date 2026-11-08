# TaskForge testing strategy

TaskForge's correctness depends on PostgreSQL transaction and locking behavior,
so the test suite deliberately mixes fast unit tests with integration tests
against a real database. SQL is never mocked.

## Test layers

| Layer | Location | Needs PostgreSQL | Purpose |
| --- | --- | --- | --- |
| Unit | `internal/job`, `internal/config`, `internal/httpapi`, `internal/worker`, `internal/outbox`, `internal/relay`, `internal/natsbus` | no | Pure logic, validation, HTTP contract, event encoding, runner and relay loops with fakes. |
| Store integration | `internal/store/postgres/*_integration_test.go` | yes | Claiming, fencing, recovery, retry, DLQ, concurrency races. |
| Migration integration | `internal/migrations/*_integration_test.go` | yes | Up-to-date reruns and v0.1→…→v0.6 upgrade paths with existing data. |
| Workflow integration | `internal/store/postgres/workflow_*_integration_test.go` | yes | Engine scenarios, cancel, fan-in race, deadlock freedom, crash injection, randomized soak, invariant checker self-tests. |
| Outbox integration | `internal/store/postgres/outbox_integration_test.go` | yes | Trigger-emitted events for every transition, rollback, relay batching and exclusivity, purge. |
| NATS integration | `internal/natsbus/*_integration_test.go` | NATS | Stream setup idempotency, `Nats-Msg-Id` deduplication, wake-up subscription. |
| HTTP integration | `internal/httpapi/readiness_integration_test.go` | yes | Readiness against a real, then closed, pool. |
| End-to-end | `internal/worker/e2e_integration_test.go` | yes | API → PostgreSQL → worker → result, in-process. |
| Workflow end-to-end | `internal/worker/workflow_e2e_integration_test.go` | yes | Order saga over HTTP with the real worker runner and registry; checks outbox events. |
| Event end-to-end | `internal/worker/events_e2e_integration_test.go` | yes + NATS | API → outbox → relay → JetStream (ordered events) and wake-up claim with a one-hour poll interval. |
| Race detector | all packages with `-race` | optional | Data races in runner and store code. |

Integration tests are named `…Integration` and skip when
`TASKFORGE_TEST_DATABASE_URL` (or, for NATS tests, `TASKFORGE_TEST_NATS_URL`)
is unset. Each test creates an isolated schema so
tests can run in parallel against one database.

## What is covered

| Property | Test(s) |
| --- | --- |
| Valid state transitions only | `TestValidateTransitionExhaustive`, `TestTerminal` |
| Payload canonicalization and fingerprint stability | `TestSubmissionFingerprintCanonicalizesObjectKeysAndWhitespace` |
| Input validation | `TestNewSubmissionValidation`, `TestNewSubmissionRejectsInvalidIdempotencyKeysAndPayloadBounds`, `TestSubmitRejectsInvalidRequests` |
| Backoff growth, cap, deterministic jitter | `TestRetryDelayGrowthCapAndDeterministicJitter` |
| Config defaults, overrides, bounds | `TestLoad*`, `TestCommandRequirements` |
| API privacy and status codes | `TestSubmitStatusAndPrivacy`, `TestDeadLetterAPIPrivacyPaginationAndRedrive`, `TestStoreErrorMapping` |
| Request ID and JSON error envelope | `TestRequestIDAndRoutingErrorsUseJSONEnvelope` |
| Health/readiness | `TestHealthAndReadiness`, `TestReadinessFailsAfterDatabasePoolClosesIntegration` |
| Submission idempotency under concurrency | `TestCreateGetAndIdempotencyIntegration`, `TestConcurrentIdempotentSubmissionIntegration` |
| Claim ordering, cancellation, outcomes | `TestClaimOrderingAttemptsCancellationAndOutcomesIntegration` |
| Exclusive ownership | `TestConcurrentClaimsHaveExclusiveOwnersIntegration` |
| Heartbeat, recovery, fencing | `TestLeaseHeartbeatRecoveryAndFencingIntegration`, `TestHealthyHeartbeatPreventsRecoveryIntegration`, `TestConcurrentRecoveryOnlyRecoversLeaseOnceIntegration` |
| Retry → DLQ → redrive | `TestRetryDeadLetterAndRedriveIntegration`, `TestPermanentFailureAndConcurrentRedriveIntegration`, `TestExpiredAttemptsReachDeadLetterIntegration`, `TestSuccessfulRetryHasOneTerminalSuccessIntegration`, `TestConcurrentFailureCreatesOneDeadLetterIntegration` |
| Stable DLQ pagination | `TestDeadLetterPaginationIsStableAndBoundedIntegration` |
| Delayed job eligibility and idempotency | `TestNewDelayedSubmissionValidationAndFingerprint`, `TestDelayedJobClaimEligibilityAndCancellationIntegration` |
| Cron parsing, timezone evaluation, and schedule dispatch | `TestParseCronAndNext`, `TestNewScheduleSubmissionValidationAndFingerprint`, `TestScheduleAPICreateListPauseResumeDeleteAndPrivacy`, `TestScheduleCreateReplayPauseResumeDeleteAndConcurrentDispatchIntegration` |
| Schema upgrades with live data | `TestV01RunningJobUpgradeIntegration`, `TestV02SchemaUpgradeIntegration`, `TestV03SchemaUpgradeIntegration`, `TestV04SchemaUpgradeIntegration`, `TestV05SchemaUpgradeIntegration` |
| Bounded concurrency and shutdown | `TestRunnerNeverExceedsConfiguredConcurrency`, `TestRunnerGracefulShutdownIsBoundedAndStopsClaims` |
| Outbox events for every transition, privacy, rollback | `TestOutboxRecordsSuccessfulLifecycleIntegration`, `TestOutboxRecordsRetryAndDeadLetterIntegration`, `TestOutboxRecordsLeaseRecoveryCancelAndScheduleIntegration`, `TestOutboxRowIsRolledBackWithTransitionIntegration` |
| Relay batching, acked prefix, single active relay, purge | `TestPublishOutboxBatch*Integration`, `TestPurgePublishedOutboxKeepsUnpublishedIntegration`, `TestRunner*` in `internal/relay` |
| Event contract and JetStream deduplication | `TestSubjectMsgIDAndEncode`, `TestEncodeRejectsInvalidEvents`, `TestPublisherEnsuresStreamAndDeduplicatesIntegration`, `TestPublishFailsWithoutStreamIntegration` |
| Clock-independent wake-ups | `TestDueOnOccurrenceComparesDatabaseTimestampsOnly`, `TestWakeHandlerSignalsOnlyForDueJobsWithoutBlocking`, `TestRunnerClaimsPromptlyOnWakeSignal`, `TestSubscribeWakeupsReceivesQueuedEventsIntegration` |
| Full event pipeline | `TestOutboxRelayAndWakeupsEndToEndIntegration` |
| Rejected handler results fail permanently | `TestRunnerFailsJobWhenStoreRejectsResult`, `TestRejectedResultLeavesJobOwnedForPermanentFailureIntegration` |
| Execution metadata reaches handlers | `TestHandlerReceivesStableExecutionMetadata`, `TestRegistry` |
| Full pipeline | `TestEchoEndToEndIntegration`, `TestRecurringScheduleEndToEndIntegration`, `TestWorkflowSagaEndToEndIntegration` |
| Workflow validation, fingerprint, decisions | `TestNewWorkflowSubmissionValidation`, `TestWorkflowFingerprintIsOrderAndFormattingIndependent`, `TestReadyCompensations*`, `TestSettle` |
| Workflow engine scenarios | `TestWorkflow*Integration` in `workflow_engine_integration_test.go` and `workflow_cancel_integration_test.go` |
| Workflow API privacy and errors | `TestWorkflowAPISubmitStatusesAndPrivacy`, `TestWorkflowAPIGetListCancel`, `TestJobCancelWorkflowOwned409` |
| Demo handlers | `TestDemoFail`, `TestDemoFlaky`, `TestDemoSleep` |

## Workflow correctness

Every workflow test ends with `assertWorkflowInvariants`, which checks nine
invariants (I1–I9, listed in [WORKFLOWS.md](WORKFLOWS.md#correctness-evidence))
using only SQL against the database. The checker itself is tested:
`TestInvariantCheckerDetectsEarlyCompensationIntegration` and
`TestInvariantCheckerDetectsBypassedEngineIntegration` inject violations and
require the checker to report them.

| Test | What it proves |
| --- | --- |
| `TestWorkflowFanInRaceIntegration` | 16 parallel steps completed at the same instant from 16 goroutines dispatch their common dependent exactly once. |
| `TestConcurrentRecoveryAcrossWorkflowsNoDeadlockIntegration` | Concurrent recovery batches holding two workflows in opposite orders never deadlock. Removing the ascending lock order makes it fail with SQLSTATE `40P01`. |
| `TestRecoveryIsolatesAFailingWorkflowJobIntegration` | A workflow that cannot advance does not block lease recovery of unrelated jobs. |
| `TestCrashDuringCompensationIntegration` | A worker killed mid-compensation is fenced out; the compensation reruns once with the same action execution key; the saga completes. |
| `TestDispatchedJobIsCreatedAfterItsDependenciesIntegration` | A forced lock-wait interleaving cannot make a dependent look older than its dependency. |
| `TestGetWorkflowReturnsAConsistentSnapshotIntegration` | Readers never see a workflow status that disagrees with its steps. |
| `TestAbortedWorkflowDoesNotRetryItsRunningStepIntegration` | A step that fails retryably after its workflow stopped is not executed again. |
| `TestRedrivenCompensationIsStandaloneWithItsPayloadIntegration` | A redriven compensation runs standalone with its payload intact (the runbook's recovery path). |
| `TestRecoveryLogsPartialFailuresWithJobIDs` | Partial recovery failures are logged with the failed job IDs and never the raw database error. |
| `TestWorkflowSoakIntegration` | 25 seeded random DAGs with random failures, cancels, and 5 % worker crashes across 4 concurrent workers and a recovery loop; all invariants hold once quiescent. |

The soak test prints its seed. Reproduce a run with
`TASKFORGE_SOAK_SEED=<seed> go test ./internal/store/postgres -run Soak -count=1 -v`.
It is bounded to 60 seconds (about 3 seconds in practice) and is skipped with
`-short`.

## Running the suites

```powershell
# Unit tests only
go test ./...

# Integration tests (dedicated, disposable database)
$env:TASKFORGE_TEST_DATABASE_URL = "postgres://taskforge:taskforge@127.0.0.1:5432/taskforge_test?sslmode=disable"
$env:TASKFORGE_TEST_NATS_URL = "nats://127.0.0.1:4222"
go test ./... -run Integration -count=1

# Everything with the race detector
go test -race ./...
```

Create the test database once with
`docker compose exec postgres createdb -U taskforge taskforge_test`.

## Observability evidence (v0.7)

| Test | What it proves |
| --- | --- |
| `TestImmediateJobTraceIntegration` | HTTP span → `job.enqueue` → `job.attempt` share one trace across API, database, and worker. |
| `TestRetriedJobAttemptsAreSiblingsIntegration` | Retries are sibling attempt spans with attempt numbers. |
| `TestDelayedJobAttemptsAreLinkedRootsIntegration` | Client-delayed jobs start linked roots; mutation-checked (ignoring `trace_link` fails it). |
| `TestWorkflowStepsFormCausalChainIntegration` | A saga's steps and compensations form one causal chain. |
| `TestRecoveredJobRetryIsChildOfStoredContextIntegration` | Recovery keeps the stored parent; `lease.recovery` records counts. |
| `TestTracingDisabledStillRunsIntegration`, `TestExporterDownJobsStillRunIntegration` | Telemetry never blocks work; export failures are counted; shutdown stays bounded. |
| `TestMetricsCardinalityIntegration` | No entity IDs, unlisted queues, or unregistered kinds in labels. |
| `TestStartDelayMeasuredFromReadinessIntegration`, `TestLeaseExpiryDeadLetterIsPlatformIntegration` | SLI semantics: readiness clock, handler vs platform class. |
| `TestStalledWorkersRaiseOldestReadyAgeIntegration` | The stall gauge rises with no worker. |
| `TestObserver*Integration` | Outcomes are reported only after commit, savepoint-isolated. |
| `deployments/prometheus/tests/slo_test.yml` (`make slo-test`) | Every alert fires on burn and stays silent otherwise, including below minimum volume; inputs use Prometheus 3's stored `le="5.0"` form. |

The SLO burn demo (`make demo`, [SLO.md](SLO.md)) is a manual end-to-end
check against real Prometheus, Grafana, and Jaeger. It found that Prometheus 3
stores bucket bounds as `le="5.0"`, which hand-written `promtool` inputs had
hidden.

## CI

[`.github/workflows/ci.yml`](../.github/workflows/ci.yml) runs on every push
and pull request with a PostgreSQL 18 service and a NATS 2.15 JetStream
container: module verification, `gofmt`
check, `go vet`, unit tests, integration tests, race tests (with the database),
build, Compose validation, container build, and a clean-tree check.

## Guidelines for new tests

- Every new SQL path that changes job state needs an integration test,
  including a concurrent variant when two actors can race.
- Prefer asserting on database state (attempt rows, tokens, timestamps) over
  log output.
- Use deterministic inputs (for example the deterministic jitter) instead of
  sleeps where possible; when waiting is unavoidable, bound it.
- New migrations need an upgrade test that starts from the previous version
  with representative rows.
- Tests must not print payloads, keys, or DSNs.
- Telemetry tests that install a global tracer provider (`telemetrytest.Setup`,
  `telemetry.SetupTracing`) must not run in parallel and must restore it.
- Workflow tests end with `assertWorkflowInvariants`, called when the workflow
  is quiescent (no workflow job queued or running).
- A concurrency test that passes on its first run proves nothing until a
  deliberate mutation of the code under test makes it fail.

On Windows, `go test -race` needs a working cgo toolchain. If the local gcc
cannot build cgo, run the race suite in Linux as CI does, for example
`docker run --rm -v "<repo>:/src" -w /src -e TASKFORGE_TEST_DATABASE_URL=… golang:1.25 go test -race ./...`.
