package config

import (
	"errors"
	"os"
	"time"
)

const (
	defaultHTTPAddr          = ":8080"
	defaultReadTimeout       = 15 * time.Second
	defaultReadHeaderTimeout = 5 * time.Second
	defaultWriteTimeout      = 15 * time.Second
	defaultIdleTimeout       = 60 * time.Second
	defaultShutdownTimeout   = 10 * time.Second
)

type Config struct {
	HTTPAddr          string
	DatabaseURL       string
	ReadTimeout       time.Duration
	ReadHeaderTimeout time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	ShutdownTimeout   time.Duration
}

func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:          valueOrDefault("TASKFORGE_HTTP_ADDR", defaultHTTPAddr),
		DatabaseURL:       os.Getenv("TASKFORGE_DATABASE_URL"),
		ReadTimeout:       defaultReadTimeout,
		ReadHeaderTimeout: defaultReadHeaderTimeout,
		WriteTimeout:      defaultWriteTimeout,
		IdleTimeout:       defaultIdleTimeout,
		ShutdownTimeout:   defaultShutdownTimeout,
	}

	durations := []struct {
		key    string
		target *time.Duration
	}{
		{key: "TASKFORGE_READ_TIMEOUT", target: &cfg.ReadTimeout},
		{key: "TASKFORGE_READ_HEADER_TIMEOUT", target: &cfg.ReadHeaderTimeout},
		{key: "TASKFORGE_WRITE_TIMEOUT", target: &cfg.WriteTimeout},
		{key: "TASKFORGE_IDLE_TIMEOUT", target: &cfg.IdleTimeout},
		{key: "TASKFORGE_SHUTDOWN_TIMEOUT", target: &cfg.ShutdownTimeout},
	}
	for _, duration := range durations {
		value, err := positiveDurationFromEnv(duration.key, *duration.target)
		if err != nil {
			return Config{}, err
		}
		*duration.target = value
	}

	return cfg, nil
}

func positiveDurationFromEnv(key string, fallback time.Duration) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}

	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return 0, errors.New(key + " must be a positive duration")
	}
	return value, nil
}

func valueOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
