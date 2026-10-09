-- +goose Up
CREATE TABLE jobs (
    id UUID PRIMARY KEY,
    queue TEXT NOT NULL,
    kind TEXT NOT NULL,
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    payload_hash BYTEA NOT NULL CHECK (octet_length(payload_hash) = 32),
    status TEXT NOT NULL CHECK (
        status IN (
            'queued',
            'running',
            'succeeded',
            'failed',
            'canceled'
        )
    ),
    priority SMALLINT NOT NULL DEFAULT 0 CHECK (priority BETWEEN -100 AND 100),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    idempotency_key TEXT CHECK (
        idempotency_key IS NULL OR char_length(idempotency_key) BETWEEN 1 AND 128
    ),
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    result JSONB,
    last_error_code TEXT CHECK (
        last_error_code IS NULL OR last_error_code ~ '^[A-Z][A-Z0-9_]{0,63}$'
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX jobs_idempotency_key_unique
    ON jobs (queue, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX jobs_claim_index
    ON jobs (priority DESC, created_at, id)
    WHERE status = 'queued';

CREATE TABLE job_attempts (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt INTEGER NOT NULL CHECK (attempt >= 1),
    worker_id TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    outcome TEXT CHECK (outcome IN ('succeeded', 'failed')),
    error_code TEXT CHECK (
        error_code IS NULL OR error_code ~ '^[A-Z][A-Z0-9_]{0,63}$'
    ),
    UNIQUE (job_id, attempt)
);

CREATE INDEX job_attempts_job_history_index
    ON job_attempts (job_id, attempt);

-- +goose Down
DROP TABLE IF EXISTS job_attempts;
DROP TABLE IF EXISTS jobs;
