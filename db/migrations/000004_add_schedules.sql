-- +goose Up
CREATE TABLE schedules (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL UNIQUE CHECK (name ~ '^[a-z0-9][a-z0-9._-]{0,63}$'),
    queue TEXT NOT NULL CHECK (queue ~ '^[a-z0-9][a-z0-9._-]{0,63}$'),
    kind TEXT NOT NULL CHECK (kind ~ '^[a-z0-9][a-z0-9._-]{0,63}$'),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    payload_hash BYTEA NOT NULL CHECK (octet_length(payload_hash) = 32),
    priority SMALLINT NOT NULL DEFAULT 0 CHECK (priority BETWEEN -100 AND 100),
    cron_expr TEXT NOT NULL CHECK (char_length(cron_expr) BETWEEN 9 AND 128),
    timezone TEXT NOT NULL DEFAULT 'UTC' CHECK (char_length(timezone) BETWEEN 1 AND 64),
    paused BOOLEAN NOT NULL DEFAULT FALSE,
    next_run_at TIMESTAMPTZ NOT NULL,
    last_run_at TIMESTAMPTZ,
    last_job_id UUID REFERENCES jobs(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE jobs
    ADD COLUMN schedule_id UUID REFERENCES schedules(id) ON DELETE SET NULL,
    ADD COLUMN scheduled_for TIMESTAMPTZ,
    ADD CONSTRAINT jobs_schedule_slot_check CHECK (
        schedule_id IS NULL OR scheduled_for IS NOT NULL
    );

CREATE UNIQUE INDEX jobs_schedule_slot_unique
    ON jobs (schedule_id, scheduled_for)
    WHERE schedule_id IS NOT NULL;

CREATE INDEX schedules_due_index
    ON schedules (next_run_at, id)
    WHERE NOT paused;

CREATE INDEX schedules_list_index
    ON schedules (created_at DESC, id DESC);

-- +goose Down
DROP INDEX IF EXISTS schedules_list_index;
DROP INDEX IF EXISTS schedules_due_index;
DROP INDEX IF EXISTS jobs_schedule_slot_unique;
ALTER TABLE jobs
    DROP CONSTRAINT IF EXISTS jobs_schedule_slot_check,
    DROP COLUMN IF EXISTS scheduled_for,
    DROP COLUMN IF EXISTS schedule_id;
DROP TABLE IF EXISTS schedules;

