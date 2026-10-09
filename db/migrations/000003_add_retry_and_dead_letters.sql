-- +goose Up
ALTER TABLE jobs
    ADD COLUMN execution_idempotency_key UUID NOT NULL DEFAULT gen_random_uuid(),
    ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts BETWEEN 1 AND 100),
    ADD COLUMN retry_initial_backoff_ms BIGINT NOT NULL DEFAULT 1000 CHECK (retry_initial_backoff_ms BETWEEN 1 AND 86400000),
    ADD COLUMN retry_max_backoff_ms BIGINT NOT NULL DEFAULT 60000 CHECK (retry_max_backoff_ms BETWEEN retry_initial_backoff_ms AND 86400000),
    ADD COLUMN retry_jitter_percent SMALLINT NOT NULL DEFAULT 20 CHECK (retry_jitter_percent BETWEEN 0 AND 100),
    ADD COLUMN dead_lettered_at TIMESTAMPTZ;

CREATE TABLE dead_letters (
    job_id UUID PRIMARY KEY REFERENCES jobs(id),
    queue TEXT NOT NULL,
    kind TEXT NOT NULL,
    final_attempt INTEGER NOT NULL CHECK (final_attempt >= 1),
    error_code TEXT NOT NULL CHECK (error_code ~ '^[A-Z][A-Z0-9_]{0,63}$'),
    dead_lettered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    redriven_job_id UUID UNIQUE REFERENCES jobs(id),
    redriven_at TIMESTAMPTZ,
    CHECK ((redriven_job_id IS NULL) = (redriven_at IS NULL))
);

CREATE INDEX jobs_retry_eligibility_index
    ON jobs (available_at, priority DESC, created_at, id)
    WHERE status = 'queued';

CREATE INDEX dead_letters_inspection_index
    ON dead_letters (dead_lettered_at DESC, job_id DESC);

-- +goose Down
DROP INDEX IF EXISTS dead_letters_inspection_index;
DROP INDEX IF EXISTS jobs_retry_eligibility_index;
DROP TABLE IF EXISTS dead_letters;
ALTER TABLE jobs
    DROP COLUMN dead_lettered_at,
    DROP COLUMN retry_jitter_percent,
    DROP COLUMN retry_max_backoff_ms,
    DROP COLUMN retry_initial_backoff_ms,
    DROP COLUMN max_attempts,
    DROP COLUMN execution_idempotency_key;
