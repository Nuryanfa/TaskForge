# TaskForge testing strategy

TaskForge's correctness depends on PostgreSQL transaction and locking behavior,
so the test suite deliberately mixes fast unit tests with integration tests
against a real database. SQL is never mocked.

## Test layers

| Layer | Location | Needs PostgreSQL | Purpose |
| --- | --- | --- | --- |
| Unit | `internal/job`, `internal/config`, `internal/httpapi`, `internal/worker` | no | Pure logic, validation, HTTP contract, runner concurrency with fakes. |
| Store integration | `internal/store/postgres/*_integration_test.go` | yes | Claiming, fencing, recovery, retry, DLQ, concurrency races. |
| Migration integration | `internal/migrations/*_integration_test.go` | yes | Up-to-date reruns and v0.1→v0.2→v0.3 upgrade paths with existing data. |
| HTTP integration | `internal/httpapi/readiness_integration_test.go` | yes | Readiness against a real, then closed, pool. |
| End-to-end | `internal/worker/e2e_integration_test.go` | yes | API → PostgreSQL → worker → result, in-process. |
| Race detector | all packages with `-race` | optional | Data races in runner and store code. |

Integration tests are named `…Integration` and skip when
`TASKFORGE_TEST_DATABASE_URL` is unset. Each test creates an isolated schema so
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
| Schema upgrades with live data | `TestV01RunningJobUpgradeIntegration`, `TestV02SchemaUpgradeIntegration`, `TestV03SchemaUpgradeIntegration` |
| Bounded concurrency and shutdown | `TestRunnerNeverExceedsConfiguredConcurrency`, `TestRunnerGracefulShutdownIsBoundedAndStopsClaims` |
| Rejected handler results fail permanently | `TestRunnerFailsJobWhenStoreRejectsResult`, `TestRejectedResultLeavesJobOwnedForPermanentFailureIntegration` |
| Execution metadata reaches handlers | `TestHandlerReceivesStableExecutionMetadata`, `TestRegistry` |
| Full pipeline | `TestEchoEndToEndIntegration`, `TestRecurringScheduleEndToEndIntegration` |

## Running the suites

```powershell
# Unit tests only
go test ./...

# Integration tests (dedicated, disposable database)
$env:TASKFORGE_TEST_DATABASE_URL = "postgres://taskforge:taskforge@127.0.0.1:5432/taskforge_test?sslmode=disable"
go test ./... -run Integration -count=1

# Everything with the race detector
go test -race ./...
```

Create the test database once with
`docker compose exec postgres createdb -U taskforge taskforge_test`.

## CI

[`.github/workflows/ci.yml`](../.github/workflows/ci.yml) runs on every push
and pull request with a PostgreSQL 18 service: module verification, `gofmt`
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
