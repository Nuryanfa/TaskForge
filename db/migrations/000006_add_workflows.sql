-- +goose Up
CREATE TABLE workflows (
    id UUID PRIMARY KEY,
    label TEXT CHECK (label IS NULL OR char_length(label) BETWEEN 1 AND 128),
    status TEXT NOT NULL CHECK (status IN (
        'running',
        'compensating',
        'succeeded',
        'compensated',
        'canceled',
        'compensation_failed'
    )),
    abort_reason TEXT CHECK (abort_reason IN ('step_failed', 'canceled')),
    failed_step TEXT,
    error_code TEXT CHECK (error_code IS NULL OR error_code ~ '^[A-Z][A-Z0-9_]{0,63}$'),
    idempotency_key TEXT UNIQUE CHECK (
        idempotency_key IS NULL OR char_length(idempotency_key) BETWEEN 1 AND 128
    ),
    fingerprint BYTEA NOT NULL CHECK (octet_length(fingerprint) = 32),
    step_count SMALLINT NOT NULL CHECK (step_count BETWEEN 1 AND 100),
    -- Retry policy snapshot applied to every action and compensation job, so
    -- jobs created later by workers use the policy of the creating API.
    max_attempts INTEGER NOT NULL CHECK (max_attempts BETWEEN 1 AND 100),
    retry_initial_backoff_ms BIGINT NOT NULL CHECK (retry_initial_backoff_ms BETWEEN 1 AND 86400000),
    retry_max_backoff_ms BIGINT NOT NULL CHECK (retry_max_backoff_ms BETWEEN retry_initial_backoff_ms AND 86400000),
    retry_jitter_percent SMALLINT NOT NULL CHECK (retry_jitter_percent BETWEEN 0 AND 100),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    CHECK ((status IN ('running', 'succeeded')) = (abort_reason IS NULL)),
    CHECK ((status IN ('running', 'compensating')) = (completed_at IS NULL))
);

CREATE TABLE workflow_steps (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    workflow_id UUID NOT NULL REFERENCES workflows(id),
    position SMALLINT NOT NULL CHECK (position >= 0),
    name TEXT NOT NULL CHECK (name ~ '^[a-z0-9][a-z0-9._-]{0,63}$'),
    queue TEXT NOT NULL,
    kind TEXT NOT NULL,
    priority SMALLINT NOT NULL CHECK (priority BETWEEN -100 AND 100),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    depends_on TEXT[] NOT NULL DEFAULT '{}',
    remaining_dependencies SMALLINT NOT NULL CHECK (remaining_dependencies >= 0),
    status TEXT NOT NULL CHECK (status IN (
        'pending', 'dispatched', 'succeeded', 'failed', 'canceled', 'skipped'
    )),
    job_id UUID UNIQUE REFERENCES jobs(id),
    compensation_queue TEXT,
    compensation_kind TEXT,
    compensation_priority SMALLINT CHECK (compensation_priority BETWEEN -100 AND 100),
    compensation_payload JSONB CHECK (
        compensation_payload IS NULL OR jsonb_typeof(compensation_payload) = 'object'
    ),
    compensation_status TEXT NOT NULL DEFAULT 'none' CHECK (compensation_status IN (
        'none', 'waiting', 'dispatched', 'succeeded', 'failed', 'not_defined'
    )),
    compensation_job_id UUID UNIQUE REFERENCES jobs(id),
    UNIQUE (workflow_id, name),
    UNIQUE (workflow_id, position),
    CHECK ((status IN ('pending', 'skipped')) = (job_id IS NULL)),
    CHECK ((compensation_kind IS NULL) = (compensation_payload IS NULL)),
    CHECK ((compensation_kind IS NULL) = (compensation_queue IS NULL)),
    CHECK ((compensation_kind IS NULL) = (compensation_priority IS NULL)),
    CHECK ((compensation_status IN ('dispatched', 'succeeded', 'failed')) = (compensation_job_id IS NOT NULL))
);

ALTER TABLE jobs
    ADD COLUMN workflow_id UUID REFERENCES workflows(id),
    ADD COLUMN workflow_step_id BIGINT REFERENCES workflow_steps(id),
    ADD COLUMN workflow_role TEXT CHECK (workflow_role IN ('action', 'compensation')),
    ADD CONSTRAINT jobs_workflow_link_check CHECK (
        (workflow_id IS NULL) = (workflow_step_id IS NULL)
        AND (workflow_id IS NULL) = (workflow_role IS NULL)
    );

-- Database backstop: one action job and at most one compensation job per step.
CREATE UNIQUE INDEX jobs_workflow_step_role_unique
    ON jobs (workflow_step_id, workflow_role)
    WHERE workflow_step_id IS NOT NULL;

CREATE INDEX jobs_workflow_queued_index
    ON jobs (workflow_id, id)
    WHERE workflow_id IS NOT NULL AND status = 'queued';

CREATE INDEX workflows_list_index
    ON workflows (created_at DESC, id DESC);

CREATE INDEX workflow_steps_workflow_index
    ON workflow_steps (workflow_id, position);

-- Job events gain workflow_id (additive; event body version stays 1).
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION taskforge_record_job_event()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    event TEXT;
BEGIN
    IF TG_OP = 'INSERT' THEN
        event := 'job.queued';
    ELSIF OLD.status = 'queued' AND NEW.status = 'running' THEN
        event := 'job.started';
    ELSIF OLD.status = 'running' AND NEW.status = 'queued' THEN
        event := 'job.retry_scheduled';
    ELSIF OLD.status = 'running' AND NEW.status = 'succeeded' THEN
        event := 'job.succeeded';
    ELSIF OLD.status = 'running' AND NEW.status = 'failed' THEN
        event := 'job.failed';
    ELSIF OLD.status = 'queued' AND NEW.status = 'canceled' THEN
        event := 'job.canceled';
    ELSE
        RETURN NULL;
    END IF;
    INSERT INTO outbox_events (job_id, event_type, payload)
    VALUES (NEW.id, event, jsonb_build_object(
        'job_id', NEW.id,
        'queue', NEW.queue,
        'kind', NEW.kind,
        'status', NEW.status,
        'attempt', NEW.attempt_count,
        'max_attempts', NEW.max_attempts,
        'priority', NEW.priority,
        'error_code', NEW.last_error_code,
        'available_at', NEW.available_at,
        'schedule_id', NEW.schedule_id,
        'workflow_id', NEW.workflow_id,
        'dead_lettered', NEW.dead_lettered_at IS NOT NULL
    ));
    RETURN NULL;
END;
$$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION taskforge_record_job_event()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    event TEXT;
BEGIN
    IF TG_OP = 'INSERT' THEN
        event := 'job.queued';
    ELSIF OLD.status = 'queued' AND NEW.status = 'running' THEN
        event := 'job.started';
    ELSIF OLD.status = 'running' AND NEW.status = 'queued' THEN
        event := 'job.retry_scheduled';
    ELSIF OLD.status = 'running' AND NEW.status = 'succeeded' THEN
        event := 'job.succeeded';
    ELSIF OLD.status = 'running' AND NEW.status = 'failed' THEN
        event := 'job.failed';
    ELSIF OLD.status = 'queued' AND NEW.status = 'canceled' THEN
        event := 'job.canceled';
    ELSE
        RETURN NULL;
    END IF;
    INSERT INTO outbox_events (job_id, event_type, payload)
    VALUES (NEW.id, event, jsonb_build_object(
        'job_id', NEW.id,
        'queue', NEW.queue,
        'kind', NEW.kind,
        'status', NEW.status,
        'attempt', NEW.attempt_count,
        'max_attempts', NEW.max_attempts,
        'priority', NEW.priority,
        'error_code', NEW.last_error_code,
        'available_at', NEW.available_at,
        'schedule_id', NEW.schedule_id,
        'dead_lettered', NEW.dead_lettered_at IS NOT NULL
    ));
    RETURN NULL;
END;
$$;
-- +goose StatementEnd
DROP INDEX IF EXISTS workflow_steps_workflow_index;
DROP INDEX IF EXISTS workflows_list_index;
DROP INDEX IF EXISTS jobs_workflow_queued_index;
DROP INDEX IF EXISTS jobs_workflow_step_role_unique;
ALTER TABLE jobs
    DROP CONSTRAINT IF EXISTS jobs_workflow_link_check,
    DROP COLUMN IF EXISTS workflow_role,
    DROP COLUMN IF EXISTS workflow_step_id,
    DROP COLUMN IF EXISTS workflow_id;
DROP TABLE IF EXISTS workflow_steps;
DROP TABLE IF EXISTS workflows;
