-- +goose Up
-- W3C traceparent of the span that caused the row to exist. Not sensitive,
-- never returned by the public API, and never copied into outbox events.
ALTER TABLE jobs
    ADD COLUMN trace_parent TEXT CHECK (
        trace_parent IS NULL OR trace_parent ~ '^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$'
    ),
    -- Decided once at enqueue: TRUE when the job was not ready at creation
    -- (client run_at / delay_seconds in the future), so attempts link instead
    -- of parenting. Retry backoff later moves available_at but must not flip it.
    ADD COLUMN trace_link BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE workflows
    ADD COLUMN trace_parent TEXT CHECK (
        trace_parent IS NULL OR trace_parent ~ '^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$'
    );

-- +goose Down
ALTER TABLE workflows DROP COLUMN IF EXISTS trace_parent;
ALTER TABLE jobs
    DROP COLUMN IF EXISTS trace_link,
    DROP COLUMN IF EXISTS trace_parent;
