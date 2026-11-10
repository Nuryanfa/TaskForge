# TaskForge service level objectives (v0.7)

The SLIs below were defined before any dashboard existed. Every latency
sample is computed from PostgreSQL timestamps, so host clock skew cannot
distort it; Prometheus evaluates the SLOs over the resulting counters and
histograms. Rules: [`deployments/prometheus/rules/slo.yml`](../deployments/prometheus/rules/slo.yml),
tests: [`deployments/prometheus/tests/slo_test.yml`](../deployments/prometheus/tests/slo_test.yml)
(`make slo-test`).

## SLIs and targets

| SLO | Good event | Valid event | Target | Window |
| --- | --- | --- | --- | --- |
| **Start latency** | a claimed job whose start delay is ≤ 5 s | every claimed job | 99 % | 28 days |
| **Reliability** | a terminal outcome that is not a dead letter caused by the platform | every `succeeded` or `dead_lettered` outcome (`canceled` excluded) | 99.5 % | 28 days |

**Start delay** is `started_at − available_at` from the claim statement. It is
measured from when the job became **ready**, never from submission, so a job
delayed by `run_at`, a cron slot, retry backoff, or waiting for its workflow
dependencies does not count against the SLO.

**Platform failures** are `LEASE_EXPIRED`, `SHUTDOWN_TIMEOUT`, and
`UNKNOWN_JOB_KIND`. Every other failure code (for example `DEMO_FAILURE`,
`INVALID_JOB_PAYLOAD`, `EXECUTION_TIMEOUT`) belongs to the handler owner and
does not burn TaskForge's budget. Because the API rejects unregistered kinds
at submission (`400 UNSUPPORTED_JOB_KIND`), a run-time `UNKNOWN_JOB_KIND` can
only mean a worker is missing a handler the API accepts: a deployment error.

The API-availability SLO is deferred to v0.7.1; the HTTP metrics it needs
already exist.

## Alert policy

| Alert | Fires when | Severity |
| --- | --- | --- |
| `TaskForgeStartLatencyFastBurn` | bad ratio > 14.4 × budget over 1 h **and** 5 m | page |
| `TaskForgeStartLatencySlowBurn` | bad ratio > 6 × budget over 6 h **and** 30 m | ticket |
| `TaskForgeReliabilityFastBurn` / `SlowBurn` | same windows, 0.5 % budget, platform dead letters only | page / ticket |
| `TaskForgeJobsNotStarting` | oldest ready job older than 60 s for 5 m | page |
| `TaskForgeDBCollectorAbsent` | `taskforge_jobs` missing for 5 m | page |
| `TaskForgeTargetDown` | a TaskForge scrape target is down for 2 m | page |

- **Minimum volume.** Burn alerts need at least 20 valid events in the last
  hour, so a handful of events at low traffic cannot page anyone.
- **Blind spot covered.** Start latency has no samples while nothing is
  claimed (for example every worker is down). `TaskForgeJobsNotStarting` reads
  the oldest ready job's age from the database instead.
- **Missing signals.** If the API or its database connection is down, the
  database gauges disappear (the collector exports nothing after a failed
  refresh) and `TaskForgeDBCollectorAbsent` fires.

Every alert has a `promtool` case where it fires and one where it stays
silent. The silent cases cover: burn within budget (both SLOs), traffic below
the minimum volume (both SLOs), a slow burn that must not page (both SLOs), a
five-minute reliability spike that the 1h window does not confirm, handler
failures (never platform budget), ready jobs younger than 60 s, and every
target up with database gauges present. The thresholds and volume gates were
mutation-checked: loosening any of them makes the suite fail.

## Burn the budget (portfolio demo)

`compose.demo.yaml` runs one worker and loads a demo-only rule,
`TaskForgeStartLatencyFastBurnDemo`, with windows scaled down (3 m and 1 m,
`for: 30s`). Production rules are unchanged.

```powershell
$env:TASKFORGE_POSTGRES_PORT = "55432"   # only if 5432 is taken
docker compose -f compose.yaml -f compose.demo.yaml up -d --build
go run ./cmd/taskforge-loadgen -rate 2 -duration 4m -mix 'sleep:8000=80,echo=20' -seed 7
```

With one worker and 8-second jobs arriving twice a second, the queue grows and
start delay passes 5 seconds within the first minute.

1. **Grafana** (http://127.0.0.1:3000, opens on "TaskForge — what is
   broken?"): the start-latency SLI turns red, "How long has the oldest ready
   job waited?" climbs past 60 s, and p95/p99 start delay cross the 5 s line.
   *Screenshot: the SLO row and the latency row.*
2. **Prometheus alerts** (http://127.0.0.1:9090/alerts):
   `TaskForgeStartLatencyFastBurnDemo` goes pending, then firing within about
   two to three minutes; `TaskForgeJobsNotStarting` follows after five.
   *Screenshot: the firing alert.*
3. **Jaeger** (http://127.0.0.1:16686, service `taskforge-api`): open a
   `POST /v1/jobs` trace for a sleep job. The `job.attempt` span from
   `taskforge-worker` sits in the same trace under `job.enqueue`, and the gap
   before it is the queueing the SLO measured. *Screenshot: that trace.*

Stop with `docker compose -f compose.yaml -f compose.demo.yaml down`.
