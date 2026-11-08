package config

import (
	"errors"
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
	defaultHTTPAddr                     = "127.0.0.1:8080"
	defaultDatabaseMaxConns             = 10
	defaultDatabaseMinConns             = 1
	defaultDatabaseConnectTimeout       = 5 * time.Second
	defaultDatabaseQueryTimeout         = 3 * time.Second
	defaultReadTimeout                  = 15 * time.Second
	defaultReadHeaderTimeout            = 5 * time.Second
	defaultWriteTimeout                 = 15 * time.Second
	defaultIdleTimeout                  = 60 * time.Second
	defaultShutdownTimeout              = 10 * time.Second
	defaultWorkerPollInterval           = 500 * time.Millisecond
	defaultWorkerConcurrency            = 4
	defaultJobExecutionTimeout          = 30 * time.Second
	defaultJobLeaseDuration             = 30 * time.Second
	defaultJobHeartbeatInterval         = 10 * time.Second
	defaultRecoveryInterval             = 5 * time.Second
	defaultRecoveryBatchSize            = 100
	defaultScheduleInterval             = time.Second
	defaultScheduleBatchSize            = 50
	defaultJobPayloadMaxBytes           = 64 * 1024
	defaultJobResultMaxBytes            = 64 * 1024
	defaultJobMaxAttempts               = 3
	defaultRetryInitialBackoff          = time.Second
	defaultRetryMaxBackoff              = time.Minute
	defaultRetryJitterPercent           = 20
	defaultDeadLetterPageSize           = 50
	defaultWorkflowMaxSteps             = 32
	defaultWorkflowMaxBytes       int64 = 256 * 1024
	defaultWorkflowPageSize             = 50
	defaultNATSConnectTimeout           = 5 * time.Second
	defaultNATSPublishTimeout           = 2 * time.Second
	defaultNATSStream                   = "TASKFORGE_EVENTS"
	defaultNATSStreamMaxAge             = 7 * 24 * time.Hour
	defaultNATSDuplicateWindow          = 2 * time.Minute
	defaultOutboxPollInterval           = 200 * time.Millisecond
	defaultOutboxBatchSize              = 100
	defaultOutboxBatchTimeout           = 10 * time.Second
	defaultOutboxRetention              = 24 * time.Hour
	maximumConnectionCount              = 100
	maximumNetworkTimeout               = 5 * time.Minute
	maximumJobExecutionTimeout          = 30 * time.Minute
	maximumWorkerPollInterval           = time.Minute
	minimumLeaseDuration                = time.Second
	maximumLeaseDuration                = 10 * time.Minute
	minimumWorkerInterval               = 100 * time.Millisecond
	minimumRetryBackoff                 = time.Millisecond
	maximumWorkerConcurrency            = 64
	maximumRecoveryBatchSize            = 1000
	maximumScheduleBatchSize            = 500
	maximumJobAttempts                  = 100
	maximumRetryBackoff                 = 30 * time.Minute
	maximumDeadLetterPageSize           = 100
	maximumWorkflowSteps                = 100
	minimumWorkflowBytes          int64 = 1024
	maximumWorkflowBytes          int64 = 4 * 1024 * 1024
	maximumWorkflowPageSize             = 100
	minimumNATSStreamMaxAge             = time.Minute
	maximumNATSStreamMaxAge             = 365 * 24 * time.Hour
	minimumNATSDuplicateWindow          = time.Second
	maximumNATSDuplicateWindow          = time.Hour
	minimumOutboxPollInterval           = 10 * time.Millisecond
	maximumOutboxBatchSize              = 1000
	minimumOutboxRetention              = time.Minute
	maximumOutboxRetention              = 30 * 24 * time.Hour
	maximumJobDataBytes           int64 = 1024 * 1024
)

var (
	workerIDPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	natsStreamPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
)

type Config struct {
	HTTPAddr               string
	DatabaseURL            string
	DatabaseMaxConns       int32
	DatabaseMinConns       int32
	DatabaseConnectTimeout time.Duration
	DatabaseQueryTimeout   time.Duration
	ReadTimeout            time.Duration
	ReadHeaderTimeout      time.Duration
	WriteTimeout           time.Duration
	IdleTimeout            time.Duration
	ShutdownTimeout        time.Duration
	WorkerID               string
	WorkerPollInterval     time.Duration
	WorkerConcurrency      int
	JobExecutionTimeout    time.Duration
	JobLeaseDuration       time.Duration
	JobHeartbeatInterval   time.Duration
	RecoveryInterval       time.Duration
	RecoveryBatchSize      int
	ScheduleInterval       time.Duration
	ScheduleBatchSize      int
	JobPayloadMaxBytes     int64
	JobResultMaxBytes      int64
	JobMaxAttempts         int
	RetryInitialBackoff    time.Duration
	RetryMaxBackoff        time.Duration
	RetryJitterPercent     int
	DeadLetterPageSize     int
	WorkflowMaxSteps       int
	WorkflowMaxBytes       int64
	WorkflowPageSize       int
	NATSURL                string
	NATSConnectTimeout     time.Duration
	NATSPublishTimeout     time.Duration
	NATSStream             string
	NATSStreamMaxAge       time.Duration
	NATSDuplicateWindow    time.Duration
	OutboxPollInterval     time.Duration
	OutboxBatchSize        int
	OutboxBatchTimeout     time.Duration
	OutboxRetention        time.Duration
}

func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:               valueOrDefault("TASKFORGE_HTTP_ADDR", defaultHTTPAddr),
		DatabaseURL:            os.Getenv("TASKFORGE_DATABASE_URL"),
		DatabaseMaxConns:       defaultDatabaseMaxConns,
		DatabaseMinConns:       defaultDatabaseMinConns,
		DatabaseConnectTimeout: defaultDatabaseConnectTimeout,
		DatabaseQueryTimeout:   defaultDatabaseQueryTimeout,
		ReadTimeout:            defaultReadTimeout,
		ReadHeaderTimeout:      defaultReadHeaderTimeout,
		WriteTimeout:           defaultWriteTimeout,
		IdleTimeout:            defaultIdleTimeout,
		ShutdownTimeout:        defaultShutdownTimeout,
		WorkerID:               os.Getenv("TASKFORGE_WORKER_ID"),
		WorkerPollInterval:     defaultWorkerPollInterval,
		WorkerConcurrency:      defaultWorkerConcurrency,
		JobExecutionTimeout:    defaultJobExecutionTimeout,
		JobLeaseDuration:       defaultJobLeaseDuration,
		JobHeartbeatInterval:   defaultJobHeartbeatInterval,
		RecoveryInterval:       defaultRecoveryInterval,
		RecoveryBatchSize:      defaultRecoveryBatchSize,
		ScheduleInterval:       defaultScheduleInterval,
		ScheduleBatchSize:      defaultScheduleBatchSize,
		JobPayloadMaxBytes:     defaultJobPayloadMaxBytes,
		JobResultMaxBytes:      defaultJobResultMaxBytes,
		JobMaxAttempts:         defaultJobMaxAttempts,
		RetryInitialBackoff:    defaultRetryInitialBackoff,
		RetryMaxBackoff:        defaultRetryMaxBackoff,
		RetryJitterPercent:     defaultRetryJitterPercent,
		DeadLetterPageSize:     defaultDeadLetterPageSize,
		WorkflowMaxSteps:       defaultWorkflowMaxSteps,
		WorkflowMaxBytes:       defaultWorkflowMaxBytes,
		WorkflowPageSize:       defaultWorkflowPageSize,
		NATSURL:                os.Getenv("TASKFORGE_NATS_URL"),
		NATSConnectTimeout:     defaultNATSConnectTimeout,
		NATSPublishTimeout:     defaultNATSPublishTimeout,
		NATSStream:             valueOrDefault("TASKFORGE_NATS_STREAM", defaultNATSStream),
		NATSStreamMaxAge:       defaultNATSStreamMaxAge,
		NATSDuplicateWindow:    defaultNATSDuplicateWindow,
		OutboxPollInterval:     defaultOutboxPollInterval,
		OutboxBatchSize:        defaultOutboxBatchSize,
		OutboxBatchTimeout:     defaultOutboxBatchTimeout,
		OutboxRetention:        defaultOutboxRetention,
	}

	if err := validateHTTPAddr(cfg.HTTPAddr); err != nil {
		return Config{}, err
	}
	if cfg.NATSURL != "" {
		if err := validateNATSURL(cfg.NATSURL); err != nil {
			return Config{}, err
		}
	}
	if !natsStreamPattern.MatchString(cfg.NATSStream) {
		return Config{}, errors.New("TASKFORGE_NATS_STREAM must be 1-64 letters, digits, '_' or '-'")
	}
	durations := []struct {
		key     string
		target  *time.Duration
		maximum time.Duration
	}{
		{key: "TASKFORGE_DATABASE_CONNECT_TIMEOUT", target: &cfg.DatabaseConnectTimeout, maximum: maximumNetworkTimeout},
		{key: "TASKFORGE_DATABASE_QUERY_TIMEOUT", target: &cfg.DatabaseQueryTimeout, maximum: maximumNetworkTimeout},
		{key: "TASKFORGE_READ_TIMEOUT", target: &cfg.ReadTimeout, maximum: maximumNetworkTimeout},
		{key: "TASKFORGE_READ_HEADER_TIMEOUT", target: &cfg.ReadHeaderTimeout, maximum: maximumNetworkTimeout},
		{key: "TASKFORGE_WRITE_TIMEOUT", target: &cfg.WriteTimeout, maximum: maximumNetworkTimeout},
		{key: "TASKFORGE_IDLE_TIMEOUT", target: &cfg.IdleTimeout, maximum: maximumNetworkTimeout},
		{key: "TASKFORGE_SHUTDOWN_TIMEOUT", target: &cfg.ShutdownTimeout, maximum: maximumNetworkTimeout},
		{key: "TASKFORGE_WORKER_POLL_INTERVAL", target: &cfg.WorkerPollInterval, maximum: maximumWorkerPollInterval},
		{key: "TASKFORGE_JOB_EXECUTION_TIMEOUT", target: &cfg.JobExecutionTimeout, maximum: maximumJobExecutionTimeout},
		{key: "TASKFORGE_JOB_LEASE_DURATION", target: &cfg.JobLeaseDuration, maximum: maximumLeaseDuration},
		{key: "TASKFORGE_JOB_HEARTBEAT_INTERVAL", target: &cfg.JobHeartbeatInterval, maximum: maximumLeaseDuration},
		{key: "TASKFORGE_RECOVERY_INTERVAL", target: &cfg.RecoveryInterval, maximum: maximumNetworkTimeout},
		{key: "TASKFORGE_SCHEDULE_INTERVAL", target: &cfg.ScheduleInterval, maximum: maximumWorkerPollInterval},
		{key: "TASKFORGE_RETRY_INITIAL_BACKOFF", target: &cfg.RetryInitialBackoff, maximum: maximumRetryBackoff},
		{key: "TASKFORGE_RETRY_MAX_BACKOFF", target: &cfg.RetryMaxBackoff, maximum: maximumRetryBackoff},
		{key: "TASKFORGE_NATS_CONNECT_TIMEOUT", target: &cfg.NATSConnectTimeout, maximum: maximumNetworkTimeout},
		{key: "TASKFORGE_NATS_PUBLISH_TIMEOUT", target: &cfg.NATSPublishTimeout, maximum: maximumNetworkTimeout},
		{key: "TASKFORGE_NATS_STREAM_MAX_AGE", target: &cfg.NATSStreamMaxAge, maximum: maximumNATSStreamMaxAge},
		{key: "TASKFORGE_NATS_DUPLICATE_WINDOW", target: &cfg.NATSDuplicateWindow, maximum: maximumNATSDuplicateWindow},
		{key: "TASKFORGE_OUTBOX_POLL_INTERVAL", target: &cfg.OutboxPollInterval, maximum: maximumWorkerPollInterval},
		{key: "TASKFORGE_OUTBOX_BATCH_TIMEOUT", target: &cfg.OutboxBatchTimeout, maximum: maximumNetworkTimeout},
		{key: "TASKFORGE_OUTBOX_RETENTION", target: &cfg.OutboxRetention, maximum: maximumOutboxRetention},
	}
	for _, duration := range durations {
		value, err := durationFromEnv(duration.key, *duration.target, duration.maximum)
		if err != nil {
			return Config{}, err
		}
		*duration.target = value
	}
	if cfg.JobLeaseDuration < minimumLeaseDuration {
		return Config{}, fmt.Errorf("TASKFORGE_JOB_LEASE_DURATION must be at least %s", minimumLeaseDuration)
	}
	if cfg.JobHeartbeatInterval < minimumWorkerInterval {
		return Config{}, fmt.Errorf("TASKFORGE_JOB_HEARTBEAT_INTERVAL must be at least %s", minimumWorkerInterval)
	}
	if cfg.RecoveryInterval < minimumWorkerInterval {
		return Config{}, fmt.Errorf("TASKFORGE_RECOVERY_INTERVAL must be at least %s", minimumWorkerInterval)
	}
	if cfg.ScheduleInterval < minimumWorkerInterval {
		return Config{}, fmt.Errorf("TASKFORGE_SCHEDULE_INTERVAL must be at least %s", minimumWorkerInterval)
	}
	minimums := []struct {
		key     string
		value   time.Duration
		minimum time.Duration
	}{
		{key: "TASKFORGE_NATS_STREAM_MAX_AGE", value: cfg.NATSStreamMaxAge, minimum: minimumNATSStreamMaxAge},
		{key: "TASKFORGE_NATS_DUPLICATE_WINDOW", value: cfg.NATSDuplicateWindow, minimum: minimumNATSDuplicateWindow},
		{key: "TASKFORGE_OUTBOX_POLL_INTERVAL", value: cfg.OutboxPollInterval, minimum: minimumOutboxPollInterval},
		{key: "TASKFORGE_OUTBOX_RETENTION", value: cfg.OutboxRetention, minimum: minimumOutboxRetention},
	}
	for _, check := range minimums {
		if check.value < check.minimum {
			return Config{}, fmt.Errorf("%s must be at least %s", check.key, check.minimum)
		}
	}
	if cfg.NATSDuplicateWindow > cfg.NATSStreamMaxAge {
		return Config{}, errors.New("TASKFORGE_NATS_DUPLICATE_WINDOW must not exceed TASKFORGE_NATS_STREAM_MAX_AGE")
	}
	if cfg.JobHeartbeatInterval >= cfg.JobLeaseDuration {
		return Config{}, errors.New("TASKFORGE_JOB_HEARTBEAT_INTERVAL must be shorter than TASKFORGE_JOB_LEASE_DURATION")
	}
	if cfg.RetryInitialBackoff > cfg.RetryMaxBackoff {
		return Config{}, errors.New("TASKFORGE_RETRY_INITIAL_BACKOFF must not exceed TASKFORGE_RETRY_MAX_BACKOFF")
	}
	if cfg.RetryInitialBackoff < minimumRetryBackoff || cfg.RetryMaxBackoff < minimumRetryBackoff {
		return Config{}, fmt.Errorf("retry backoff durations must be at least %s", minimumRetryBackoff)
	}

	maxConns, err := intFromEnv("TASKFORGE_DATABASE_MAX_CONNS", int(cfg.DatabaseMaxConns), 1, maximumConnectionCount)
	if err != nil {
		return Config{}, err
	}
	minConns, err := intFromEnv("TASKFORGE_DATABASE_MIN_CONNS", int(cfg.DatabaseMinConns), 0, maximumConnectionCount)
	if err != nil {
		return Config{}, err
	}
	if minConns > maxConns {
		return Config{}, errors.New("TASKFORGE_DATABASE_MIN_CONNS must not exceed TASKFORGE_DATABASE_MAX_CONNS")
	}
	cfg.DatabaseMaxConns = int32(maxConns)
	cfg.DatabaseMinConns = int32(minConns)

	cfg.WorkerConcurrency, err = intFromEnv("TASKFORGE_WORKER_CONCURRENCY", cfg.WorkerConcurrency, 1, maximumWorkerConcurrency)
	if err != nil {
		return Config{}, err
	}
	cfg.RecoveryBatchSize, err = intFromEnv("TASKFORGE_RECOVERY_BATCH_SIZE", cfg.RecoveryBatchSize, 1, maximumRecoveryBatchSize)
	if err != nil {
		return Config{}, err
	}
	cfg.ScheduleBatchSize, err = intFromEnv("TASKFORGE_SCHEDULE_BATCH_SIZE", cfg.ScheduleBatchSize, 1, maximumScheduleBatchSize)
	if err != nil {
		return Config{}, err
	}
	cfg.JobMaxAttempts, err = intFromEnv("TASKFORGE_JOB_MAX_ATTEMPTS", cfg.JobMaxAttempts, 1, maximumJobAttempts)
	if err != nil {
		return Config{}, err
	}
	cfg.RetryJitterPercent, err = intFromEnv("TASKFORGE_RETRY_JITTER_PERCENT", cfg.RetryJitterPercent, 0, 100)
	if err != nil {
		return Config{}, err
	}
	cfg.DeadLetterPageSize, err = intFromEnv("TASKFORGE_DEAD_LETTER_PAGE_SIZE", cfg.DeadLetterPageSize, 1, maximumDeadLetterPageSize)
	if err != nil {
		return Config{}, err
	}
	cfg.WorkflowMaxSteps, err = intFromEnv("TASKFORGE_WORKFLOW_MAX_STEPS", cfg.WorkflowMaxSteps, 1, maximumWorkflowSteps)
	if err != nil {
		return Config{}, err
	}
	cfg.WorkflowMaxBytes, err = int64FromEnv("TASKFORGE_WORKFLOW_MAX_BYTES", cfg.WorkflowMaxBytes, minimumWorkflowBytes, maximumWorkflowBytes)
	if err != nil {
		return Config{}, err
	}
	cfg.WorkflowPageSize, err = intFromEnv("TASKFORGE_WORKFLOW_PAGE_SIZE", cfg.WorkflowPageSize, 1, maximumWorkflowPageSize)
	if err != nil {
		return Config{}, err
	}

	cfg.OutboxBatchSize, err = intFromEnv("TASKFORGE_OUTBOX_BATCH_SIZE", cfg.OutboxBatchSize, 1, maximumOutboxBatchSize)
	if err != nil {
		return Config{}, err
	}

	cfg.JobPayloadMaxBytes, err = int64FromEnv("TASKFORGE_JOB_PAYLOAD_MAX_BYTES", cfg.JobPayloadMaxBytes, 1, maximumJobDataBytes)
	if err != nil {
		return Config{}, err
	}
	cfg.JobResultMaxBytes, err = int64FromEnv("TASKFORGE_JOB_RESULT_MAX_BYTES", cfg.JobResultMaxBytes, 1, maximumJobDataBytes)
	if err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) RequireDatabase() error {
	if c.DatabaseURL == "" {
		return errors.New("TASKFORGE_DATABASE_URL is required")
	}
	parsed, err := url.Parse(c.DatabaseURL)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") || parsed.Hostname() == "" || strings.Trim(parsed.Path, "/") == "" {
		return errors.New("TASKFORGE_DATABASE_URL must be a valid PostgreSQL URL")
	}
	return nil
}

func (c Config) RequireWorker() error {
	if err := c.RequireDatabase(); err != nil {
		return err
	}
	if !workerIDPattern.MatchString(c.WorkerID) {
		return errors.New("TASKFORGE_WORKER_ID must be 1-64 safe identifier characters")
	}
	return nil
}

// RequireRelay validates what the outbox relay needs: PostgreSQL and NATS.
func (c Config) RequireRelay() error {
	if err := c.RequireDatabase(); err != nil {
		return err
	}
	if c.NATSURL == "" {
		return errors.New("TASKFORGE_NATS_URL is required")
	}
	return validateNATSURL(c.NATSURL)
}

// validateNATSURL never echoes the URL, which may contain credentials.
func validateNATSURL(value string) error {
	parsed, err := url.Parse(value)
	if err != nil || (parsed.Scheme != "nats" && parsed.Scheme != "tls") || parsed.Hostname() == "" {
		return errors.New("TASKFORGE_NATS_URL must be a nats:// or tls:// URL with a host")
	}
	return nil
}

func validateHTTPAddr(value string) error {
	_, port, err := net.SplitHostPort(value)
	if err != nil || port == "" {
		return errors.New("TASKFORGE_HTTP_ADDR must be a valid host:port address")
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return errors.New("TASKFORGE_HTTP_ADDR must contain a valid port")
	}
	return nil
}

func durationFromEnv(key string, fallback, maximum time.Duration) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 || value > maximum {
		return 0, fmt.Errorf("%s must be a positive duration no greater than %s", key, maximum)
	}
	return value, nil
}

func intFromEnv(key string, fallback, minimum, maximum int) (int, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("%s must be between %d and %d", key, minimum, maximum)
	}
	return value, nil
}

func int64FromEnv(key string, fallback, minimum, maximum int64) (int64, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("%s must be between %d and %d", key, minimum, maximum)
	}
	return value, nil
}

func valueOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
