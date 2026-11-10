package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	defaultOTelSampleRatio   = 1.0
	defaultOTelExportTimeout = 5 * time.Second
	minimumOTelExportTimeout = 100 * time.Millisecond
	maximumOTelExportTimeout = time.Minute
	defaultMetricsDBRefresh  = 15 * time.Second
	minimumMetricsDBRefresh  = time.Second
	maximumMetricsDBRefresh  = 5 * time.Minute
	maximumMetricsQueues     = 32
)

var metricsQueuePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

// loadObservability reads the v0.7 tracing and metrics settings into cfg.
func loadObservability(cfg *Config) error {
	cfg.OTelEndpoint = os.Getenv("TASKFORGE_OTEL_ENDPOINT")
	if cfg.OTelEndpoint != "" {
		parsed, err := url.Parse(cfg.OTelEndpoint)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			return fmt.Errorf("TASKFORGE_OTEL_ENDPOINT must be an http:// or https:// URL")
		}
	}

	cfg.OTelSampleRatio = defaultOTelSampleRatio
	if raw := os.Getenv("TASKFORGE_OTEL_SAMPLE_RATIO"); raw != "" {
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil || value < 0 || value > 1 {
			return fmt.Errorf("TASKFORGE_OTEL_SAMPLE_RATIO must be a number between 0 and 1")
		}
		cfg.OTelSampleRatio = value
	}

	timeout, err := durationFromEnv("TASKFORGE_OTEL_EXPORT_TIMEOUT", defaultOTelExportTimeout, maximumOTelExportTimeout)
	if err != nil {
		return err
	}
	if timeout < minimumOTelExportTimeout {
		return fmt.Errorf("TASKFORGE_OTEL_EXPORT_TIMEOUT must be at least %s", minimumOTelExportTimeout)
	}
	cfg.OTelExportTimeout = timeout

	refresh, err := durationFromEnv("TASKFORGE_METRICS_DB_REFRESH", defaultMetricsDBRefresh, maximumMetricsDBRefresh)
	if err != nil {
		return err
	}
	if refresh < minimumMetricsDBRefresh {
		return fmt.Errorf("TASKFORGE_METRICS_DB_REFRESH must be at least %s", minimumMetricsDBRefresh)
	}
	cfg.MetricsDBRefresh = refresh

	cfg.MetricsQueues = []string{"default"}
	if raw := os.Getenv("TASKFORGE_METRICS_QUEUES"); raw != "" {
		var queues []string
		for _, queue := range strings.Split(raw, ",") {
			queue = strings.TrimSpace(queue)
			if !metricsQueuePattern.MatchString(queue) {
				return fmt.Errorf("TASKFORGE_METRICS_QUEUES must be a comma list of queue identifiers")
			}
			queues = append(queues, queue)
		}
		if len(queues) > maximumMetricsQueues {
			return fmt.Errorf("TASKFORGE_METRICS_QUEUES must list at most %d queues", maximumMetricsQueues)
		}
		cfg.MetricsQueues = queues
	}

	if raw := os.Getenv("TASKFORGE_METRICS_ADDR"); raw != "" {
		if _, port, err := net.SplitHostPort(raw); err != nil || port == "" {
			return fmt.Errorf("TASKFORGE_METRICS_ADDR must be a host:port address or empty")
		}
	}
	return nil
}

// MetricsAddr returns the metrics listen address for a process: the
// TASKFORGE_METRICS_ADDR value when the variable is set (empty disables the
// listener), otherwise the process-specific default.
func (c Config) MetricsAddr(defaultAddr string) string {
	if value, ok := os.LookupEnv("TASKFORGE_METRICS_ADDR"); ok {
		return value
	}
	return defaultAddr
}
