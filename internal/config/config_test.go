package config

import (
	"strings"
	"testing"
	"time"
)

var environmentKeys = []string{
	"TASKFORGE_HTTP_ADDR", "TASKFORGE_DATABASE_URL", "TASKFORGE_DATABASE_MAX_CONNS",
	"TASKFORGE_DATABASE_MIN_CONNS", "TASKFORGE_DATABASE_CONNECT_TIMEOUT",
	"TASKFORGE_DATABASE_QUERY_TIMEOUT", "TASKFORGE_READ_TIMEOUT",
	"TASKFORGE_READ_HEADER_TIMEOUT", "TASKFORGE_WRITE_TIMEOUT", "TASKFORGE_IDLE_TIMEOUT",
	"TASKFORGE_SHUTDOWN_TIMEOUT", "TASKFORGE_WORKER_ID", "TASKFORGE_WORKER_POLL_INTERVAL",
	"TASKFORGE_JOB_EXECUTION_TIMEOUT", "TASKFORGE_JOB_PAYLOAD_MAX_BYTES",
	"TASKFORGE_JOB_RESULT_MAX_BYTES",
	"TASKFORGE_WORKER_CONCURRENCY", "TASKFORGE_JOB_LEASE_DURATION",
	"TASKFORGE_JOB_HEARTBEAT_INTERVAL", "TASKFORGE_RECOVERY_INTERVAL",
	"TASKFORGE_RECOVERY_BATCH_SIZE",
	"TASKFORGE_JOB_MAX_ATTEMPTS", "TASKFORGE_RETRY_INITIAL_BACKOFF",
	"TASKFORGE_RETRY_MAX_BACKOFF", "TASKFORGE_RETRY_JITTER_PERCENT",
	"TASKFORGE_DEAD_LETTER_PAGE_SIZE",
}

func TestLoadDefaults(t *testing.T) {
	clearEnvironment(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != "127.0.0.1:8080" || cfg.DatabaseMaxConns != 10 || cfg.DatabaseMinConns != 1 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.DatabaseConnectTimeout != 5*time.Second || cfg.DatabaseQueryTimeout != 3*time.Second ||
		cfg.ReadTimeout != 15*time.Second || cfg.ReadHeaderTimeout != 5*time.Second ||
		cfg.WriteTimeout != 15*time.Second || cfg.IdleTimeout != time.Minute ||
		cfg.ShutdownTimeout != 10*time.Second || cfg.WorkerPollInterval != 500*time.Millisecond ||
		cfg.JobExecutionTimeout != 30*time.Second {
		t.Fatalf("unexpected duration defaults: %+v", cfg)
	}
	if cfg.WorkerConcurrency != 4 || cfg.JobLeaseDuration != 30*time.Second ||
		cfg.JobHeartbeatInterval != 10*time.Second || cfg.RecoveryInterval != 5*time.Second ||
		cfg.RecoveryBatchSize != 100 {
		t.Fatalf("unexpected worker defaults: %+v", cfg)
	}
	if cfg.JobMaxAttempts != 3 || cfg.RetryInitialBackoff != time.Second || cfg.RetryMaxBackoff != time.Minute || cfg.RetryJitterPercent != 20 || cfg.DeadLetterPageSize != 50 {
		t.Fatalf("unexpected retry defaults: %+v", cfg)
	}
	if cfg.JobPayloadMaxBytes != 64*1024 || cfg.JobResultMaxBytes != 64*1024 {
		t.Fatalf("unexpected size defaults: %+v", cfg)
	}
}

func TestLoadValidOverrides(t *testing.T) {
	clearEnvironment(t)
	values := map[string]string{
		"TASKFORGE_HTTP_ADDR": "127.0.0.1:9090", "TASKFORGE_DATABASE_URL": "postgres://example",
		"TASKFORGE_DATABASE_MAX_CONNS": "20", "TASKFORGE_DATABASE_MIN_CONNS": "2",
		"TASKFORGE_DATABASE_CONNECT_TIMEOUT": "1s", "TASKFORGE_DATABASE_QUERY_TIMEOUT": "2s",
		"TASKFORGE_READ_TIMEOUT": "3s", "TASKFORGE_READ_HEADER_TIMEOUT": "4s",
		"TASKFORGE_WRITE_TIMEOUT": "5s", "TASKFORGE_IDLE_TIMEOUT": "6s",
		"TASKFORGE_SHUTDOWN_TIMEOUT": "7s", "TASKFORGE_WORKER_ID": "worker-01",
		"TASKFORGE_WORKER_POLL_INTERVAL": "250ms", "TASKFORGE_JOB_EXECUTION_TIMEOUT": "8s",
		"TASKFORGE_JOB_PAYLOAD_MAX_BYTES": "2048", "TASKFORGE_JOB_RESULT_MAX_BYTES": "4096",
		"TASKFORGE_WORKER_CONCURRENCY": "8", "TASKFORGE_JOB_LEASE_DURATION": "12s",
		"TASKFORGE_JOB_HEARTBEAT_INTERVAL": "3s", "TASKFORGE_RECOVERY_INTERVAL": "4s",
		"TASKFORGE_RECOVERY_BATCH_SIZE": "25",
		"TASKFORGE_JOB_MAX_ATTEMPTS":    "7", "TASKFORGE_RETRY_INITIAL_BACKOFF": "2s",
		"TASKFORGE_RETRY_MAX_BACKOFF": "20s", "TASKFORGE_RETRY_JITTER_PERCENT": "15",
		"TASKFORGE_DEAD_LETTER_PAGE_SIZE": "25",
	}
	for key, value := range values {
		t.Setenv(key, value)
	}
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DatabaseMaxConns != 20 || cfg.DatabaseMinConns != 2 || cfg.WorkerID != "worker-01" ||
		cfg.JobPayloadMaxBytes != 2048 || cfg.JobResultMaxBytes != 4096 ||
		cfg.DatabaseConnectTimeout != time.Second || cfg.DatabaseQueryTimeout != 2*time.Second ||
		cfg.WorkerPollInterval != 250*time.Millisecond || cfg.JobExecutionTimeout != 8*time.Second {
		t.Fatalf("unexpected overrides: %+v", cfg)
	}
	if cfg.WorkerConcurrency != 8 || cfg.JobLeaseDuration != 12*time.Second ||
		cfg.JobHeartbeatInterval != 3*time.Second || cfg.RecoveryInterval != 4*time.Second ||
		cfg.RecoveryBatchSize != 25 {
		t.Fatalf("unexpected worker overrides: %+v", cfg)
	}
	if cfg.JobMaxAttempts != 7 || cfg.RetryInitialBackoff != 2*time.Second || cfg.RetryMaxBackoff != 20*time.Second || cfg.RetryJitterPercent != 15 || cfg.DeadLetterPageSize != 25 {
		t.Fatalf("unexpected retry overrides: %+v", cfg)
	}
}

func TestLoadRejectsInvalidDurations(t *testing.T) {
	keys := []string{
		"TASKFORGE_DATABASE_CONNECT_TIMEOUT", "TASKFORGE_DATABASE_QUERY_TIMEOUT",
		"TASKFORGE_READ_TIMEOUT", "TASKFORGE_READ_HEADER_TIMEOUT", "TASKFORGE_WRITE_TIMEOUT",
		"TASKFORGE_IDLE_TIMEOUT", "TASKFORGE_SHUTDOWN_TIMEOUT",
		"TASKFORGE_WORKER_POLL_INTERVAL", "TASKFORGE_JOB_EXECUTION_TIMEOUT",
		"TASKFORGE_JOB_LEASE_DURATION", "TASKFORGE_JOB_HEARTBEAT_INTERVAL",
		"TASKFORGE_RECOVERY_INTERVAL",
		"TASKFORGE_RETRY_INITIAL_BACKOFF", "TASKFORGE_RETRY_MAX_BACKOFF",
	}
	for _, key := range keys {
		for _, value := range []string{"bad", "0s", "-1s", "31m"} {
			t.Run(key+"/"+value, func(t *testing.T) {
				clearEnvironment(t)
				t.Setenv(key, value)
				_, err := Load()
				if err == nil || !strings.Contains(err.Error(), key) {
					t.Fatalf("expected error identifying %s, got %v", key, err)
				}
			})
		}
	}
}

func TestLoadRejectsInvalidCountsAndSizes(t *testing.T) {
	tests := []struct{ key, value string }{
		{key: "TASKFORGE_DATABASE_MAX_CONNS", value: "0"},
		{key: "TASKFORGE_DATABASE_MAX_CONNS", value: "101"},
		{key: "TASKFORGE_DATABASE_MIN_CONNS", value: "-1"},
		{key: "TASKFORGE_JOB_PAYLOAD_MAX_BYTES", value: "0"},
		{key: "TASKFORGE_JOB_RESULT_MAX_BYTES", value: "1048577"},
		{key: "TASKFORGE_WORKER_CONCURRENCY", value: "0"},
		{key: "TASKFORGE_WORKER_CONCURRENCY", value: "65"},
		{key: "TASKFORGE_RECOVERY_BATCH_SIZE", value: "0"},
		{key: "TASKFORGE_RECOVERY_BATCH_SIZE", value: "1001"},
		{key: "TASKFORGE_JOB_MAX_ATTEMPTS", value: "0"}, {key: "TASKFORGE_JOB_MAX_ATTEMPTS", value: "101"},
		{key: "TASKFORGE_RETRY_JITTER_PERCENT", value: "101"}, {key: "TASKFORGE_DEAD_LETTER_PAGE_SIZE", value: "101"},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			clearEnvironment(t)
			t.Setenv(tt.key, tt.value)
			if _, err := Load(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
	clearEnvironment(t)
	t.Setenv("TASKFORGE_DATABASE_MIN_CONNS", "5")
	t.Setenv("TASKFORGE_DATABASE_MAX_CONNS", "4")
	if _, err := Load(); err == nil {
		t.Fatal("expected min/max relationship error")
	}
	clearEnvironment(t)
	t.Setenv("TASKFORGE_JOB_LEASE_DURATION", "2s")
	t.Setenv("TASKFORGE_JOB_HEARTBEAT_INTERVAL", "2s")
	if _, err := Load(); err == nil {
		t.Fatal("expected heartbeat/lease relationship error")
	}
	for _, key := range []string{"TASKFORGE_JOB_HEARTBEAT_INTERVAL", "TASKFORGE_RECOVERY_INTERVAL"} {
		clearEnvironment(t)
		t.Setenv(key, "50ms")
		if _, err := Load(); err == nil || !strings.Contains(err.Error(), key) {
			t.Fatalf("expected minimum-duration error for %s, got %v", key, err)
		}
	}
	clearEnvironment(t)
	t.Setenv("TASKFORGE_JOB_LEASE_DURATION", "999ms")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "TASKFORGE_JOB_LEASE_DURATION") {
		t.Fatalf("expected minimum lease error, got %v", err)
	}
	clearEnvironment(t)
	t.Setenv("TASKFORGE_RETRY_INITIAL_BACKOFF", "2s")
	t.Setenv("TASKFORGE_RETRY_MAX_BACKOFF", "1s")
	if _, err := Load(); err == nil {
		t.Fatal("expected retry backoff relationship error")
	}
	clearEnvironment(t)
	t.Setenv("TASKFORGE_RETRY_INITIAL_BACKOFF", "500us")
	if _, err := Load(); err == nil {
		t.Fatal("expected minimum retry backoff error")
	}
}

func TestCommandRequirements(t *testing.T) {
	clearEnvironment(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.RequireDatabase(); err == nil {
		t.Fatal("expected database URL requirement")
	}
	cfg.DatabaseURL = "postgres://example"
	if err := cfg.RequireDatabase(); err == nil {
		t.Fatal("expected incomplete database URL rejection")
	}
	cfg.DatabaseURL = "postgres://user:pass@localhost:5432/taskforge"
	if err := cfg.RequireDatabase(); err != nil {
		t.Fatal(err)
	}
	if err := cfg.RequireWorker(); err == nil {
		t.Fatal("expected worker ID requirement")
	}
	cfg.WorkerID = "worker-01"
	if err := cfg.RequireWorker(); err != nil {
		t.Fatal(err)
	}
	cfg.WorkerID = "unsafe worker"
	if err := cfg.RequireWorker(); err == nil {
		t.Fatal("expected invalid worker ID error")
	}
}

func clearEnvironment(t *testing.T) {
	t.Helper()
	for _, key := range environmentKeys {
		t.Setenv(key, "")
	}
}
