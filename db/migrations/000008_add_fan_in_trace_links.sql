-- +goose Up
-- Fan-in span links (ADR 0013 addendum). A succeeded step remembers its
-- attempt's traceparent; when a fan-in step is dispatched, the contexts of
-- its non-triggering dependencies are copied onto the job as links. Like
-- trace_parent, neither column is sensitive or returned by the public API.
ALTER TABLE workflow_steps
    ADD COLUMN last_attempt_trace_parent TEXT CHECK (
        last_attempt_trace_parent IS NULL
        OR last_attempt_trace_parent ~ '^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$'
    );

-- array_to_string's third argument renders NULL elements as 'NULL' so they
-- fail the pattern; without it NULLs would be skipped and '{NULL}' accepted.
ALTER TABLE jobs
    ADD COLUMN trace_links TEXT[] NOT NULL DEFAULT '{}' CHECK (
        cardinality(trace_links) <= 32
        AND (
            cardinality(trace_links) = 0
            OR array_to_string(trace_links, ',', 'NULL')
                ~ '^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}(,00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2})*$'
        )
    );

-- +goose Down
ALTER TABLE jobs DROP COLUMN IF EXISTS trace_links;
ALTER TABLE workflow_steps DROP COLUMN IF EXISTS last_attempt_trace_parent;
