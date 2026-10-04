CREATE TABLE jobs (
    id UUID PRIMARY KEY,
    queue TEXT NOT NULL,
    kind TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN (
            'queued',
            'running',
            'retry_scheduled',
            'succeeded',
            'failed',
            'dead_lettered',
            'canceled'
        )
    ),
    priority SMALLINT NOT NULL DEFAULT 0,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts >= 1),
    idempotency_key TEXT,
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    result JSONB,
    last_error_code TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX jobs_idempotency_key_unique
    ON jobs (queue, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX jobs_claim_index
    ON jobs (queue, available_at, priority DESC, created_at, id)
    WHERE status = 'queued';

CREATE TABLE job_attempts (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt INTEGER NOT NULL CHECK (attempt >= 1),
    worker_id TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    outcome TEXT CHECK (outcome IN ('succeeded', 'retry', 'failed', 'canceled')),
    error_code TEXT,
    UNIQUE (job_id, attempt)
);

CREATE INDEX job_attempts_job_history_index
    ON job_attempts (job_id, attempt);
