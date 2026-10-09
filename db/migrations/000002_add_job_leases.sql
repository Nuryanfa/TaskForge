-- +goose Up
ALTER TABLE jobs
    ADD COLUMN lease_owner TEXT,
    ADD COLUMN lease_expires_at TIMESTAMPTZ,
    ADD COLUMN fencing_token BIGINT NOT NULL DEFAULT 0 CHECK (fencing_token >= 0);

ALTER TABLE job_attempts ADD COLUMN fencing_token BIGINT;
UPDATE job_attempts SET fencing_token = attempt;
ALTER TABLE job_attempts
    ALTER COLUMN fencing_token SET NOT NULL,
    ADD CONSTRAINT job_attempts_fencing_token_positive CHECK (fencing_token > 0);

UPDATE jobs SET fencing_token = attempt_count;

-- +goose StatementBegin
CREATE FUNCTION taskforge_prevent_fencing_token_decrease()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.fencing_token < OLD.fencing_token THEN
        RAISE EXCEPTION 'fencing_token cannot decrease';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER jobs_fencing_token_monotonic
    BEFORE UPDATE OF fencing_token ON jobs
    FOR EACH ROW
    EXECUTE FUNCTION taskforge_prevent_fencing_token_decrease();

ALTER TABLE job_attempts
    DROP CONSTRAINT job_attempts_outcome_check,
    ADD CONSTRAINT job_attempts_outcome_check
        CHECK (outcome IN ('succeeded', 'failed', 'abandoned'));

-- A v0.1 running row has no recoverable owner. Close its open attempt and
-- safely return it to the queue before enforcing v0.2 lease invariants.
UPDATE job_attempts AS attempt
SET finished_at = NOW(), outcome = 'abandoned', error_code = 'V01_MIGRATION_RECOVERY'
FROM jobs
WHERE attempt.job_id = jobs.id
  AND attempt.attempt = jobs.attempt_count
  AND attempt.finished_at IS NULL
  AND jobs.status = 'running';

UPDATE jobs
SET status = 'queued', updated_at = NOW(), completed_at = NULL
WHERE status = 'running';

ALTER TABLE jobs
    ADD CONSTRAINT jobs_lease_state_check CHECK (
        (status = 'running' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR
        (status <> 'running' AND lease_owner IS NULL AND lease_expires_at IS NULL)
    );

CREATE INDEX jobs_expired_lease_index
    ON jobs (lease_expires_at, id)
    WHERE status = 'running';

CREATE INDEX job_attempts_fencing_lookup_index
    ON job_attempts (job_id, fencing_token);

-- +goose Down
DROP INDEX IF EXISTS job_attempts_fencing_lookup_index;
DROP INDEX IF EXISTS jobs_expired_lease_index;
DROP TRIGGER IF EXISTS jobs_fencing_token_monotonic ON jobs;
DROP FUNCTION IF EXISTS taskforge_prevent_fencing_token_decrease();
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_lease_state_check;

UPDATE job_attempts
SET outcome = 'failed', error_code = COALESCE(error_code, 'LEASE_EXPIRED')
WHERE outcome = 'abandoned';

ALTER TABLE job_attempts
    DROP CONSTRAINT job_attempts_outcome_check,
    ADD CONSTRAINT job_attempts_outcome_check CHECK (outcome IN ('succeeded', 'failed')),
    DROP CONSTRAINT IF EXISTS job_attempts_fencing_token_positive,
    DROP COLUMN fencing_token;

ALTER TABLE jobs
    DROP COLUMN fencing_token,
    DROP COLUMN lease_expires_at,
    DROP COLUMN lease_owner;
