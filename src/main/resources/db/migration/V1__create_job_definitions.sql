-- ============================================================
-- V1: job_definitions — Single source of truth for scheduled tasks
-- ============================================================

CREATE TABLE job_definitions (
    id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    name                VARCHAR(255) NOT NULL UNIQUE,
    target_url          TEXT        NOT NULL,
    http_method         VARCHAR(10) NOT NULL DEFAULT 'GET'
                            CHECK (http_method IN ('GET','POST','PUT','DELETE','PATCH')),
    interval_seconds    BIGINT      NOT NULL
                            CHECK (interval_seconds BETWEEN 1 AND 86400),
    enabled             BOOLEAN     NOT NULL DEFAULT TRUE,
    -- The scheduler uses this column to decide which jobs to dispatch next
    next_execution_time TIMESTAMPTZ NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Index to speed up the poller's range scan on (enabled, next_execution_time)
CREATE INDEX idx_jd_next_execution ON job_definitions (next_execution_time)
    WHERE enabled = TRUE;

COMMENT ON TABLE  job_definitions                    IS 'Registry of all recurring HTTP cron jobs';
COMMENT ON COLUMN job_definitions.interval_seconds   IS 'Recurrence period in seconds (1 – 86400)';
COMMENT ON COLUMN job_definitions.next_execution_time IS 'Absolute UTC timestamp of the next scheduled run; updated atomically by the Scheduler after each dispatch';
