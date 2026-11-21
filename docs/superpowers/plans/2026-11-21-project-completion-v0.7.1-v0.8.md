# TaskForge Completion Plan: PR #1 (`v0.7.1`) & PR #2 (`v0.8.0`)

> **Status:** Ready for execution (Revised with full `v0.7` spec audit, 2-PR split, and Kubernetes `kind` CI verification)  
> **Strategy:** Dua Pull Request terpisah agar setiap rilis fokus, mudah di-review, dan sesuai aturan `CONTRIBUTING.md`  
> **Starting Baseline:** `origin/main` at `f6442c1` (`v0.7.0` merged via PR #7)

---

## Keputusan Arsitektur & Eksekusi

1. **Lisensi — MIT License:**
   - Default rencana menggunakan **MIT License** (file `LICENSE` di root repo + pembaruan bagian `## License` di `README.md`), paling ramah untuk proyek portofolio *open-source*. Mudah diganti ke **Apache-2.0** sebelum PR #1 jika Anda lebih memilih Apache-2.0.
2. **Strategi Branch & Pull Request — 2 PR Terpisah (Sesuai Rekomendasi):**
   - **PR #1 (`feature/v0.7.1-observability-completion`):** Menuntaskan seluruh item `v0.7` yang ditunda pada *Owner Decision 12* di [`docs/superpowers/specs/2026-11-08-v0.7-observability-design.md`](../specs/2026-11-08-v0.7-observability-design.md) + perapian hygiene & sinkronisasi dokumentasi.
   - **PR #2 (`feature/v0.8-ha-kubernetes-chaos-load`):** Dibuat dari `main` setelah PR #1 di-*merge*, fokus penuh pada High Availability, Kubernetes (`deployments/k8s/` + `kind` cluster smoke test di CI), Chaos & Global Invariant Testing (`G1–G6` + `I1–I9`), serta Load & Benchmark Testing.
3. **Tingkat Pengujian Kubernetes (`v0.8.0`) — Lapis Ganda (Static + Live `kind` Cluster di CI):**
   - **Lapis 1 (Fast Static & Kustomize Check):** Go unit test `TestKubernetesManifestsStructureAndSecurityInvariants` di `internal/config/k8s_manifests_test.go` + `kubectl kustomize deployments/k8s`.
   - **Lapis 2 (Live `kind` Cluster Smoke Test di `.github/workflows/ci.yml`):** Menjalankan cluster Kubernetes nyata menggunakan `kind`, memuat image `taskforge:dev`, men-deploy PostgreSQL + NATS + `migration-job` + `api-deployment` (2 replika) + `worker-statefulset` (3 replika) + `relay-deployment` (2 replika), menjalankan end-to-end smoke test (`POST /v1/jobs` & `POST /v1/workflows`), menghapus salah satu pod worker untuk membuktikan pemulihan failover otomatis, dan memastikan seluruh pod & workload sehat.
4. **Koreksi & Kelengkapan Item `v0.7.1` Berdasarkan Spec `2026-11-08-v0.7-observability-design.md`:**
   - **Target API-Availability SLO = `99.9 %`** (error budget `0.1 %` = `0.001` dalam 28 hari, sesuai spec baris 418), bukan `99.5 %`.
   - **Extra Fan-In Span Links** pada workflow DAG (`Migration 8`): attempt span dari setiap dependency yang bukan pemicu terakhir ikut ditautkan sebagai OpenTelemetry Span Links pada step fan-in (sesuai *Decision 12* dan spec baris 145–148).
   - **Workflow Mode di `taskforge-loadgen`**: mendukung token `workflow` dan `workflow:fail` pada `POST /v1/workflows`.
   - **Dashboard Polish**: menampilkan seluruh 3 kartu SLI vs target, panel **Error Budget Remaining (%)** untuk ketiga SLO, burn-rate 3 SLO, tabel alerts, dan panduan korelasi Jaeger.

---

## Global Constraints & Engineering Rules

1. **Go Module & Dependencies:** `github.com/Nuryanfa/TaskForge`, `go 1.25.0`. Tidak menambah dependency eksternal yang tidak perlu.
2. **Boundary Isolation:** `internal/store/postgres` hanya boleh mengimpor `internal/tracectx` dan OpenTelemetry **API** (`otel`, `otel/trace`, `otel/propagation`), dilarang mengimpor OTel SDK atau exporter.
3. **Privacy & Cardinality:** Tidak ada payload, result, idempotency key, execution key, atau DSN dalam span attribute, metric label, log, maupun HTTP response. Metric labels dibatasi ketat (`kind`, `queue`, `route`, `code`, `outcome`, `class`, `status`).
4. **Code Quality & Hygiene:** `gofmt -l .` wajib kosong, `go mod verify` bersih, dan `go vet ./...` zero warnings sebelum setiap commit.
5. **Real Database Tests:** SQL yang sensitif terhadap konkurensi dan migrasi diuji langsung terhadap PostgreSQL nyata dengan schema terisolasi (`TASKFORGE_TEST_DATABASE_URL`).

---

# Bagian I — PR #1: `v0.7.1` Observability Completion & Hygiene

**Branch:** `feature/v0.7.1-observability-completion` (dari `origin/main` `f6442c1`)

### Task 1.1 — Git Sync, Hygiene (`.dockerignore`, `.gitignore`, `Dockerfile`, `LICENSE`) & Perbaikan Drift Dokumentasi
- **Files:**
  - Modify: `.dockerignore`, `.gitignore`, `deployments/docker/Dockerfile`, `README.md`, `docs/API.md`, `docs/OPERATIONS.md`, `docs/DESIGN.md`, `docs/DATA_MODEL.md`, `docs/TESTING.md`
  - Create: `LICENSE`
- **Langkah:**
  1. Checkout `main` dan fast-forward ke `origin/main` (`f6442c1`), lalu buat branch `feature/v0.7.1-observability-completion`.
  2. Tambahkan `.superpowers/` ke `.dockerignore` dan `.gitignore` agar log lokal dan binary `.superpowers/taskforge-api.exe` tidak pernah masuk Docker build context atau Git status.
  3. Rapikan `deployments/docker/Dockerfile` baris 9 agar perintah build `taskforge-migrate` dan `taskforge-relay` berada di baris terpisah dengan `\`.
  4. Buat `LICENSE` (MIT License) dan perbarui bagian `## License` di `README.md`.
  5. Perbaiki *documentation drift* yang teridentifikasi pada audit `v0.7.0`:
     - `README.md`: ubah baris 3 `"TaskForge v0.6 is a durable..."` → `"TaskForge v0.7 is a durable..."`; perbarui baris 215–216 agar mencantumkan seluruh 4 built-in handler (`demo.echo`, `demo.fail`, `demo.flaky`, `demo.sleep`).
     - `docs/API.md`: ubah baris 4 `"no authentication in v0.5"` → `"no authentication in v0.7"`.
     - `docs/OPERATIONS.md`: ubah baris 3 `"running TaskForge v0.5"` → `"running TaskForge v0.7"`; perbarui baris 48 untuk menjelaskan bahwa worker (`:9102`) dan relay (`:9103`) mengekspos `/metrics` pada listener loopback terpisah.
     - `docs/DESIGN.md`: lengkapi *Package structure* & diagram dependensi Mermaid dengan `cmd/taskforge-loadgen`, `internal/loadgen`, `internal/telemetry`, dan `internal/tracectx`.
     - `docs/DATA_MODEL.md` & `docs/TESTING.md`: lengkapi daftar upgrade test dengan `TestV05SchemaUpgradeIntegration` dan `TestV06SchemaUpgradeIntegration`.

---

### Task 1.2 — Extra Fan-In Span Links pada Workflow DAG (Migration 8)
- **Konteks Spec (`2026-11-08-v0.7-observability-design.md` Decision 12 & baris 145–148):**
  Ketika sebuah step fan-in (memiliki `> 1` dependency di `depends_on`) dilepaskan saat dependency terakhir selesai:
  - `jobs.trace_parent` tetap menyimpan span context dari attempt dependency terakhir yang melepaskannya (menjadi **parent** dari attempt step fan-in).
  - Span context attempt dari dependency-dependency lainnya dalam `depends_on` disimpan di `jobs.trace_links` dan dipasang sebagai **OpenTelemetry Span Links** saat worker memulai `job.attempt`.
- **Files:**
  - Create: `db/migrations/000008_add_fanin_trace_links.sql`
  - Modify: `internal/job/job.go`, `internal/store/postgres/jobs.go`, `internal/store/postgres/workflows.go`, `internal/store/postgres/workflow_engine.go`, `internal/telemetry/tracing.go`
  - Test: `internal/telemetry/tracing_test.go`, `internal/migrations/migrations_integration_test.go`, `internal/worker/observability_integration_test.go`
- **Migration 8 (`db/migrations/000008_add_fanin_trace_links.sql`):**
  ```sql
  -- +goose Up
  ALTER TABLE workflow_steps ADD COLUMN last_attempt_trace_parent TEXT
      CHECK (last_attempt_trace_parent IS NULL OR last_attempt_trace_parent ~ '^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$');
  ALTER TABLE jobs ADD COLUMN trace_links TEXT[] NOT NULL DEFAULT '{}';

  -- +goose Down
  ALTER TABLE jobs DROP COLUMN IF EXISTS trace_links;
  ALTER TABLE workflow_steps DROP COLUMN IF EXISTS last_attempt_trace_parent;
  ```
- **Perubahan Kode:**
  1. `job.Job` menambah field `TraceLinks []string`; `jobColumns` dan `scanJob` di `internal/store/postgres/jobs.go` membaca `trace_links`.
  2. Di `onJobTerminal` (`internal/store/postgres/workflow_engine.go`), saat action step mencapai `succeeded`, tulis `tracectx.FromContext(ctx)` ke `workflow_steps.last_attempt_trace_parent` pada step tersebut.
  3. Di `dispatchReadyDependents` (`workflow_engine.go`) dan `dispatchStepJob` (`workflows.go`), untuk setiap step ready dengan `len(item.dependsOn) > 1`, baca `last_attempt_trace_parent` dari semua dependency dalam `item.dependsOn` yang bukan step pemicu terakhir (atau yang trace context-nya valid dan berbeda dari parent), lalu simpan ke `jobs.trace_links`.
  4. Di `telemetry.StartAttempt` (`internal/telemetry/tracing.go`), loop `claimed.Job.TraceLinks`: parse dengan `tracectx.SpanContext` dan lampirkan sebagai `trace.WithLinks(trace.Link{SpanContext: sc})`.
- **Pengujian Wajib:**
  - `TestV07SchemaUpgradeIntegration` (`internal/migrations/migrations_integration_test.go`): upgrade schema v7 → v8 dengan baris job & workflow eksisting; verifikasi `trace_links = '{}'` dan `last_attempt_trace_parent IS NULL` serta validasi `CHECK` menolak string malformed.
  - `TestStartAttemptIncludesFanInTraceLinks` (`internal/telemetry/tracing_test.go`): unit test memverifikasi `TraceLinks` masuk ke `span.Links()` tanpa mengganti parent span.
  - `TestWorkflowFanInSpanLinksIntegration` (`internal/worker/observability_integration_test.go`): jalankan diamond workflow (`reserve` → `charge` + `notify` → `ship`); verifikasi attempt span `ship` memiliki parent = attempt span dependency yang selesai terakhir **dan** memiliki link ke attempt span dependency pasangannya.

---

### Task 1.3 — API-Availability SLO (`99.9 %` Target) & `promtool` Tests
- **Files:**
  - Modify: `deployments/prometheus/rules/slo.yml`, `deployments/prometheus/tests/slo_test.yml`, `docs/SLO.md`, `docs/OPERATIONS.md`
- **Spesifikasi SLI & Target (sesuai Spec baris 418):**
  - **Valid events:** `taskforge_http_requests_total{route!="unmatched",route!~"GET /healthz|GET /readyz"}`.
  - **Bad events:** response `5xx` (`code=~"5.."`) pada route valid tersebut. Response `4xx` milik klien dan probe `/healthz`/`/readyz` tidak membakar error budget.
  - **Target:** **`99.9 %`** dalam jendela 28 hari (`error budget = 0.001`).
- **Recording & Alerting Rules (`deployments/prometheus/rules/slo.yml`):**
  - Recording rules:
    - `taskforge:api_availability_valid:rate5m`, `rate30m`, `rate1h`, `rate6h`
    - `taskforge:api_availability_bad:ratio_rate5m`, `ratio_rate30m`, `ratio_rate1h`, `ratio_rate6h`
  - Alerting rules (multi-window burn rate atas budget `0.001`, gated oleh `taskforge:slo_min_events_per_hour`):
    - `TaskForgeAPIAvailabilityFastBurn`: `ratio_rate1h > (14.4 * 0.001)` AND `ratio_rate5m > (14.4 * 0.001)` AND `(valid:rate1h * 3600) >= min_events`, `for: 2m`, `labels: {severity: page, slo: api_availability}`.
    - `TaskForgeAPIAvailabilitySlowBurn`: `ratio_rate6h > (6 * 0.001)` AND `ratio_rate30m > (6 * 0.001)` AND `(valid:rate1h * 3600) >= min_events`, `for: 15m`, `labels: {severity: ticket, slo: api_availability}`.
- **Unit Tests (`deployments/prometheus/tests/slo_test.yml`):**
  1. **Fast burn fires:** `5 %` response `500` pada `POST /v1/jobs` di 600 req/jam (> `1.44 %` fast-burn threshold) → `TaskForgeAPIAvailabilityFastBurn` firing.
  2. **Slow burn fires, fast silent:** `0.8 %` response `500` selama 6 jam (> `0.6 %` slow-burn, < `1.44 %` fast-burn) → `TaskForgeAPIAvailabilitySlowBurn` firing, fast burn silent.
  3. **Silent within budget:** `0.05 %` response `500` (< `0.1 %` budget) → kedua alert silent.
  4. **Silent below minimum volume:** `100 %` response `503` pada 6 req/jam (< 20 req/jam) → kedua alert silent.
  5. **Silent on `4xx` flood & `unmatched` routes:** `50 %` error `400`/`409`/`422` atau `route="unmatched"` → kedua alert silent.
  6. **Silent on unconfirmed 5m spike:** lonjakan `500` hanya 5 menit sementara rata-rata 1 jam tetap di bawah `1.44 %` → silent.

---

### Task 1.4 — Dashboard Polish (`deployments/grafana/generate_dashboard.py` & `taskforge.json`)
- **Files:**
  - Modify: `deployments/grafana/generate_dashboard.py`
  - Regenerate: `deployments/grafana/dashboards/taskforge.json`
- **Perubahan Spesifik (sesuai Spec baris 274–285):**
  1. Row **"Are we meeting our SLOs?"**:
     - Tiga panel stat SLI vs target: **Start latency (`99 %`)**, **Reliability (`99.5 %`)**, dan **API availability (`99.9 %`)**.
     - Panel **Error Budget Remaining (%)** untuk ketiga SLO (menunjukkan sisa budget 28 hari relatif terhadap burn rate saat ini / window).
     - Panel **Burn Rate** menampilkan ketiga garis (`start latency 1h`, `reliability 1h`, `api availability 1h`) terhadap garis threshold `14.4×`.
     - Tabel **Which alerts are firing?**.
  2. Menambahkan field `description` pada panel latency, failures, dan API routes yang menjelaskan cara melompat ke datasource **Jaeger** (`taskforge-api` atau tag `taskforge.job.id`).
  3. Verifikasi deterministik: `python3 deployments/grafana/generate_dashboard.py` menghasilkan `taskforge.json` yang valid JSON dan bersih dari drift di CI.

---

### Task 1.5 — Workflow Mode di `taskforge-loadgen` & Perbaikan Unit Test Ramah Sandbox
- **Files:**
  - Modify: `internal/loadgen/loadgen.go`, `internal/loadgen/loadgen_test.go`, `cmd/taskforge-loadgen/main.go`, `internal/telemetry/http_test.go`
- **Perubahan Spesifik:**
  1. Perluas `loadgen.Entry` dengan `Endpoint string` (`"/v1/jobs"` atau `"/v1/workflows"`).
  2. Dukung token `workflow` (3-step DAG `reserve` → `charge` & `notify`) dan `workflow:fail` (saga kompensasi `reserve` → `ship` `demo.fail`) pada `ParseMix` dan `Run`.
  3. Perbaiki `TestRunPacesSubmissions` di `internal/loadgen/loadgen_test.go` agar menggunakan in-memory `http.RoundTripper` (tidak memanggil raw TCP loopback socket yang terblokir pada Windows sandbox) sehingga pacing, statistik, dan pembatalan `ctx` selalu lolos deterministik di lingkungan lokal maupun CI.
  4. Perbarui `internal/telemetry/http_test.go` agar `TestServeMetricsServesOnlyMetrics` mengetes handler mux in-memory via `httptest.NewRecorder()` dan hanya melakukan TCP dial jika izin socket loopback tersedia.

---

### Task 1.6 — Pembaruan Dokumentasi & Verifikasi Penuh PR #1 (`v0.7.1`)
- **Files:**
  - Modify: `README.md`, `docs/PRD.md`, `docs/CURRENT_STATE.md`, `docs/SLO.md`, `docs/OBSERVABILITY.md`, `docs/DATA_MODEL.md`, `docs/TESTING.md`, `docs/OPERATIONS.md`
- **Verifikasi PR #1:**
  - Jalankan `gofmt`, `go mod verify`, `go vet ./...`, `go test ./... -count=1`, `go test ./... -run Integration -count=1`, `go test -race ./...`, `make slo-test`, `python deployments/grafana/generate_dashboard.py`, `docker compose config --quiet`, dan `docker build -f deployments/docker/Dockerfile .`.
  - Commit & push branch `feature/v0.7.1-observability-completion`, lalu buka & merge **PR #1** ke `main`.

---

# Bagian II — PR #2: `v0.8.0` High Availability, Kubernetes, Load, and Chaos Testing

**Branch:** `feature/v0.8-ha-kubernetes-chaos-load` (dibuat dari `main` setelah PR #1 di-*merge*)

### Task 2.1 — Healthcheck Subcommands & Multi-Replica HA Failover
- **Files:**
  - Modify: `internal/telemetry/serve.go`, `internal/telemetry/http_test.go`, `cmd/taskforge-api/main.go`, `cmd/taskforge-worker/main.go`, `cmd/taskforge-relay/main.go`
  - Modify Test: `internal/store/postgres/outbox_integration_test.go`
- **Perubahan Spesifik:**
  1. Pada `internal/telemetry/serve.go`, layani `GET /healthz` (`200 {"status":"ok"}`) di naast `GET /metrics` pada listener telemetry internal (`9101`/`9102`/`9103`).
  2. Tambahkan subcommand `healthcheck` di `cmd/taskforge-worker/main.go` dan `cmd/taskforge-relay/main.go` (memeriksa `TASKFORGE_METRICS_ADDR/healthz`), serta buat `cmd/taskforge-api/main.go` `healthcheck` menghormati `TASKFORGE_HTTP_ADDR`.
  3. Tambahkan integration test **`TestRelayStandbyTakesOverAfterActiveDisconnectIntegration`** di `internal/store/postgres/outbox_integration_test.go`: membuktikan dua relay terpisah berkompetisi melalui `pg_try_advisory_xact_lock`; saat relay aktif berhenti, relay standby otomatis mengambil alih dan mempublikasikan seluruh sisa outbox secara berurutan tanpa duplikasi/hilang.

---

### Task 2.2 — Manifest Kubernetes (`deployments/k8s/`) + Static Test + Live `kind` Cluster di CI
- **Files:**
  - Create:
    - `deployments/k8s/namespace.yaml`
    - `deployments/k8s/configmap.yaml`
    - `deployments/k8s/secret.example.yaml`
    - `deployments/k8s/migration-job.yaml`
    - `deployments/k8s/api-deployment.yaml`
    - `deployments/k8s/api-service.yaml`
    - `deployments/k8s/api-pdb.yaml`
    - `deployments/k8s/worker-statefulset.yaml`
    - `deployments/k8s/worker-pdb.yaml`
    - `deployments/k8s/relay-deployment.yaml`
    - `deployments/k8s/networkpolicy.yaml`
    - `deployments/k8s/kustomization.yaml`
  - Create Test: `internal/config/k8s_manifests_test.go`
  - Modify CI: `.github/workflows/ci.yml`
- **Desain Manifest Kubernetes HA:**
  - **`api-deployment.yaml` + `api-service.yaml` + `api-pdb.yaml`:** `replicas: 2`, `RollingUpdate` (`maxUnavailable: 0`), `livenessProbe` `/healthz`, `readinessProbe` `/readyz`, `terminationGracePeriodSeconds: 25` (> `TASKFORGE_SHUTDOWN_TIMEOUT=10s`), `PodDisruptionBudget` (`minAvailable: 1`).
  - **`worker-statefulset.yaml` + `worker-pdb.yaml`:** `StatefulSet` `replicas: 3` (`podManagementPolicy: Parallel`), `TASKFORGE_WORKER_ID` diinjeksi dari `metadata.name` via Downward API (`taskforge-worker-0..2`), `livenessProbe` exec `["/taskforge-worker", "healthcheck"]`, `terminationGracePeriodSeconds: 25`, `PodDisruptionBudget` (`minAvailable: 2`).
  - **`relay-deployment.yaml`:** `replicas: 2` (active + warm standby via PostgreSQL advisory lock), `livenessProbe` exec `["/taskforge-relay", "healthcheck"]`.
  - **`migration-job.yaml` & `networkpolicy.yaml`:** `batch/v1` Job untuk `/taskforge-migrate`, NetworkPolicy membatasi ingress hanya ke port API (`8080`) dan scrape Prometheus (`9101`–`9103`).
  - **Security Context Seragam:** `runAsNonRoot: true`, `runAsUser: 65532`, `readOnlyRootFilesystem: true`, `allowPrivilegeEscalation: false`, `capabilities.drop: ["ALL"]`, resource `requests` & `limits` eksplisit.
- **Lapis 1 — Static & Kustomize Verification (`internal/config/k8s_manifests_test.go`):**
  - `TestKubernetesManifestsStructureAndSecurityInvariants`: memvalidasi kelengkapan `kustomization.yaml`, security context non-root/read-only, `terminationGracePeriodSeconds > 10`, Downward API `TASKFORGE_WORKER_ID` pada `StatefulSet`, dan konfigurasi `PodDisruptionBudget`.
- **Lapis 2 — Live `kind` Cluster Smoke Test di CI (`.github/workflows/ci.yml`):**
  - Job CI `k8s-smoke`:
    1. Membuat cluster `kind`, membangun image `taskforge:dev`, lalu `kind load docker-image taskforge:dev`.
    2. Menjalankan PostgreSQL & NATS JetStream di dalam cluster, lalu `kubectl apply -k deployments/k8s`.
    3. Menunggu `job/taskforge-migrate` berstatus `Complete` dan rollout `api-deployment`, `worker-statefulset`, serta `relay-deployment` selesai `Ready`.
    4. Melakukan `kubectl port-forward svc/taskforge-api 8080:8080`, menjalankan smoke test `POST /v1/jobs` dan `POST /v1/workflows` (termasuk saga compensation), menghapus 1 pod worker (`kubectl delete pod taskforge-worker-0`) saat beban berjalan untuk membuktikan pemulihan pod & kelanjutan proses tanpa error, serta memverifikasi status terminal seluruh job/workflow.

---

### Task 2.3 — Automated Chaos & Fault-Injection Suite + Global Invariants (`G1–G6`)
- **Files:**
  - Create: `internal/store/postgres/invariants_integration_test.go`
  - Create: `internal/store/postgres/chaos_integration_test.go`
  - Create: `internal/worker/chaos_integration_test.go`
- **Global Database Conservation Invariants (`assertGlobalDatabaseInvariants`):**
  - **G1:** Setiap job `running` memiliki `lease_owner IS NOT NULL`, `lease_expires_at IS NOT NULL`, dan tepat 1 open attempt (`finished_at IS NULL`) dengan `fencing_token` sama.
  - **G2:** Setiap job non-`running` memiliki `lease_owner IS NULL`, `lease_expires_at IS NULL`, dan 0 open attempt.
  - **G3:** Setiap job `failed` memiliki `dead_lettered_at IS NOT NULL` dan tepat 1 baris di `dead_letters`; tidak ada job non-`failed` di `dead_letters`.
  - **G4:** Setiap `job_attempts` memenuhi `1 <= attempt <= jobs.attempt_count` dan `fencing_token <= jobs.fencing_token`.
  - **G5:** Setiap job memiliki minimal 1 baris `outbox_events` (`job.queued`), dan event terakhir untuk setiap job terminal sesuai dengan `jobs.status`.
  - **G6:** Seluruh 9 invarian workflow (`I1–I9`) terpenuhi untuk semua workflow.
  - *(Disertai self-test `TestGlobalInvariantCheckerDetectsViolationsIntegration`.)*
- **Skenario Chaos:**
  1. `TestChaosMultiWorkerCrashRecoveryAndFencingIntegration`: 6 workers + 2 recovery loops memproses campuran jobs, delayed jobs, schedules, redrive, dan workflow DAGs sementara 20% worker crash mendadak dan melakukan delayed stale write → 100% stale write ditolak `ErrOwnershipLost`, semua pekerjaan selesai, `G1–G6` + `I1–I9` bersih.
  2. `TestChaosWorkerRunnerRestartAndDrainEndToEndIntegration`: uji kill & restart `worker.Runner` nyata terhadap PostgreSQL + Registry saat job & saga berjalan.
  3. `TestChaosRelayFailoverUnderConcurrentTransitionsIntegration`: failover dua `relay.Runner` di tengah badai transisi job/workflow menuju NATS JetStream.

---

### Task 2.4 — Load & Throughput Benchmark Suite
- **Files:**
  - Create: `internal/worker/load_integration_test.go`
- **Tests & Benchmarks:**
  1. `TestHighConcurrencyLoadAndQuiescenceIntegration`: 200 job campuran lintas 4 queue + 15 workflow DAG dijalankan oleh 2 worker pool (8 executor paralel); memverifikasi quiescence sebelum deadline, `pgxpool.Stat().AcquiredConns() == 0` (nol connection leak), dan invarian `G1–G6` + `I1–I9` bersih.
  2. `BenchmarkWorkerClaimAndCompleteIntegration`: Go benchmark (`testing.B`) throughput end-to-end terhadap PostgreSQL.

---

### Task 2.5 — ADR 0014, `docs/KUBERNETES.md`, & Dokumentasi Final `v0.8.0`
- **Files:**
  - Create: `docs/adr/0014-high-availability-kubernetes-and-chaos-testing.md`, `docs/KUBERNETES.md`
  - Modify: `docs/adr/README.md`, `docs/README.md`, `docs/PRD.md`, `docs/CURRENT_STATE.md`, `docs/DESIGN.md`, `docs/OPERATIONS.md`, `docs/TESTING.md`, `README.md`, `CONTRIBUTING.md`, `SECURITY.md`, `Makefile`, `.github/workflows/ci.yml`
- **Deliverables:**
  - **ADR 0014:** Keputusan arsitektur HA multi-replica (`StatefulSet` worker + Downward API, active/standby advisory lock relay, PDB & grace period rules, chaos invariants `G1–G6`, dan verifikasi live `kind` cluster).
  - **`docs/KUBERNETES.md`:** Panduan deployment Kubernetes, urutan rollout, NetworkPolicy, PDB, failover, dan uji `kind` lokal.
  - Seluruh milestone roadmap (`v0.1` s/d `v0.8`) di `README.md` dan `docs/PRD.md` berstatus **Implemented**.

---

# Bagian III — Master Verification Gate (Wajib Lulus Sebelum Merge PR #1 & PR #2)

```powershell
# 1. Formatting, dependencies & static analysis
gofmt -w .
go mod tidy
go mod verify
go vet ./...
git diff --check

# 2. Unit tests (all packages, including K8s manifest invariants & loadgen workflows)
go test ./... -count=1

# 3. PostgreSQL & NATS integration tests (migrations v1->v8, fan-in span links, HA relay failover)
go test ./... -run Integration -count=1

# 4. Chaos, Soak & Load tests (invariants G1-G6 + I1-I9)
go test ./internal/store/postgres ./internal/worker -run "Chaos|Soak|Load" -count=1 -v

# 5. Race detector suite (run in golang:1.25 Linux container if Windows lacks cgo gcc)
go test -race ./...

# 6. Binary compilation
go build ./...

# 7. Prometheus SLO rules & unit tests (Start Latency 99%, Reliability 99.5%, API Availability 99.9%)
docker run --rm -v "${PWD}/deployments/prometheus:/p" -w /p --entrypoint promtool prom/prometheus:v3.5.0 check rules rules/slo.yml
docker run --rm -v "${PWD}/deployments/prometheus:/p" -w /p/tests --entrypoint promtool prom/prometheus:v3.5.0 test rules slo_test.yml

# 8. Grafana dashboard deterministic check
python deployments/grafana/generate_dashboard.py
git diff --exit-code deployments/grafana/dashboards

# 9. Docker Compose, Dockerfile & Kubernetes manifest check
docker compose config --quiet
docker build -f deployments/docker/Dockerfile .
kubectl kustomize deployments/k8s > $null
```
