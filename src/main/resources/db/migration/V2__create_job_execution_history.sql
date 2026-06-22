-- ============================================================
-- V2: job_execution_history — Immutable audit log of every run attempt
-- ============================================================

CREATE TABLE job_execution_history (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id            UUID        NOT NULL
                          REFERENCES job_definitions(id) ON DELETE CASCADE,

    -- Timestamps
    scheduled_time    TIMESTAMPTZ NOT NULL,   -- When the job was supposed to fire
    actual_start_time TIMESTAMPTZ NOT NULL,   -- When the Worker actually began executing it

    -- Drift: computed column so application never needs to calculate it
    delay_ms          BIGINT GENERATED ALWAYS AS (
                          EXTRACT(EPOCH FROM (actual_start_time - scheduled_time)) * 1000
                      ) STORED,

    -- Outcome
    status            VARCHAR(20) NOT NULL
                          CHECK (status IN ('SUCCESS', 'FAILED', 'TIMEOUT')),
    http_status_code  INT,                    -- NULL when request never reached the server
    response_payload  TEXT,                   -- Truncated to first 10KB by the Worker

    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Primary access pattern: history for a specific job, newest-first
CREATE INDEX idx_jeh_job_id_created ON job_execution_history (job_id, created_at DESC);

-- Supports the delay-monitoring query: jobs where drift exceeded the threshold
CREATE INDEX idx_jeh_job_delay ON job_execution_history (job_id, delay_ms)
    WHERE delay_ms IS NOT NULL;

COMMENT ON TABLE  job_execution_history              IS 'Append-only execution log; one row per job invocation attempt';
COMMENT ON COLUMN job_execution_history.delay_ms     IS 'Scheduling drift in milliseconds (actual_start - scheduled). Computed by the DB.';
COMMENT ON COLUMN job_execution_history.status       IS 'SUCCESS = 2xx received; FAILED = non-2xx or network error; TIMEOUT = no response within threshold';
