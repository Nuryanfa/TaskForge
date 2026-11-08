-- +goose Up
CREATE TABLE outbox_events (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- No foreign key: published events may outlive any future job retention.
    job_id UUID NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN (
        'job.queued',
        'job.started',
        'job.retry_scheduled',
        'job.succeeded',
        'job.failed',
        'job.canceled'
    )),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);

CREATE INDEX outbox_events_unpublished_index
    ON outbox_events (id)
    WHERE published_at IS NULL;

CREATE INDEX outbox_events_published_index
    ON outbox_events (published_at)
    WHERE published_at IS NOT NULL;

-- Events are written by triggers so that every job insert or status change,
-- from any code path, records its event in the same transaction. The payload
-- is a privacy-safe snapshot: no payload, result, keys, or lease owner.
-- +goose StatementBegin
CREATE FUNCTION taskforge_record_job_event()
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

CREATE TRIGGER jobs_record_insert_event
    AFTER INSERT ON jobs
    FOR EACH ROW
    EXECUTE FUNCTION taskforge_record_job_event();

CREATE TRIGGER jobs_record_status_event
    AFTER UPDATE OF status ON jobs
    FOR EACH ROW
    WHEN (OLD.status IS DISTINCT FROM NEW.status)
    EXECUTE FUNCTION taskforge_record_job_event();

-- +goose Down
DROP TRIGGER IF EXISTS jobs_record_status_event ON jobs;
DROP TRIGGER IF EXISTS jobs_record_insert_event ON jobs;
DROP FUNCTION IF EXISTS taskforge_record_job_event();
DROP TABLE IF EXISTS outbox_events;
