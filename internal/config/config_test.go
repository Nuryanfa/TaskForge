package config

import (
	"strings"
	"testing"
	"time"
)

var durationEnvironmentKeys = []string{
	"TASKFORGE_READ_TIMEOUT",
	"TASKFORGE_READ_HEADER_TIMEOUT",
	"TASKFORGE_WRITE_TIMEOUT",
	"TASKFORGE_IDLE_TIMEOUT",
	"TASKFORGE_SHUTDOWN_TIMEOUT",
}

func TestLoadDefaults(t *testing.T) {
	clearEnvironment(t)

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != ":8080" ||
		cfg.ReadTimeout != 15*time.Second ||
		cfg.ReadHeaderTimeout != 5*time.Second ||
		cfg.WriteTimeout != 15*time.Second ||
		cfg.IdleTimeout != 60*time.Second ||
		cfg.ShutdownTimeout != 10*time.Second {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestLoadValidOverrides(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("TASKFORGE_HTTP_ADDR", "127.0.0.1:9090")
	t.Setenv("TASKFORGE_DATABASE_URL", "postgres://example")
	t.Setenv("TASKFORGE_READ_TIMEOUT", "1s")
	t.Setenv("TASKFORGE_READ_HEADER_TIMEOUT", "2s")
	t.Setenv("TASKFORGE_WRITE_TIMEOUT", "3s")
	t.Setenv("TASKFORGE_IDLE_TIMEOUT", "4s")
	t.Setenv("TASKFORGE_SHUTDOWN_TIMEOUT", "5s")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != "127.0.0.1:9090" || cfg.DatabaseURL != "postgres://example" {
		t.Fatalf("unexpected string overrides: %+v", cfg)
	}
	if cfg.ReadTimeout != time.Second ||
		cfg.ReadHeaderTimeout != 2*time.Second ||
		cfg.WriteTimeout != 3*time.Second ||
		cfg.IdleTimeout != 4*time.Second ||
		cfg.ShutdownTimeout != 5*time.Second {
		t.Fatalf("unexpected duration overrides: %+v", cfg)
	}
}

func TestLoadRejectsInvalidDurations(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "malformed", value: "not-a-duration"},
		{name: "zero", value: "0s"},
		{name: "negative", value: "-1s"},
	}

	for _, key := range durationEnvironmentKeys {
		for _, tt := range tests {
			t.Run(key+"/"+tt.name, func(t *testing.T) {
				clearEnvironment(t)
				t.Setenv(key, tt.value)

				_, err := Load()
				if err == nil {
					t.Fatal("expected invalid duration error")
				}
				if !strings.Contains(err.Error(), key) {
					t.Fatalf("error %q does not identify %s", err, key)
				}
			})
		}
	}
}

func clearEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("TASKFORGE_HTTP_ADDR", "")
	t.Setenv("TASKFORGE_DATABASE_URL", "")
	for _, key := range durationEnvironmentKeys {
		t.Setenv(key, "")
	}
}
