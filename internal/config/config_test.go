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
}

func TestLoadRejectsInvalidDurations(t *testing.T) {
	keys := []string{
		"TASKFORGE_DATABASE_CONNECT_TIMEOUT", "TASKFORGE_DATABASE_QUERY_TIMEOUT",
		"TASKFORGE_READ_TIMEOUT", "TASKFORGE_READ_HEADER_TIMEOUT", "TASKFORGE_WRITE_TIMEOUT",
		"TASKFORGE_IDLE_TIMEOUT", "TASKFORGE_SHUTDOWN_TIMEOUT",
		"TASKFORGE_WORKER_POLL_INTERVAL", "TASKFORGE_JOB_EXECUTION_TIMEOUT",
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
