-- ============================================================
-- V3: Migrate job_definitions to anchor-time scheduling model
--
-- Old model: interval_seconds (run every N seconds from last run)
-- New model: start_hour + start_minute + interval_minutes
--            → deterministic slots anchored to a fixed daily time
--            → drift-resistant: next slot always computed from anchor,
--              not from the actual (possibly delayed) execution time
-- ============================================================

-- 1. Drop the old interval constraint and column
ALTER TABLE job_definitions
    DROP CONSTRAINT IF EXISTS job_definitions_interval_seconds_check;

ALTER TABLE job_definitions
    DROP COLUMN IF EXISTS interval_seconds;

-- 2. Add new scheduling columns
ALTER TABLE job_definitions
    ADD COLUMN start_hour     SMALLINT NOT NULL DEFAULT 0
        CHECK (start_hour    BETWEEN 0 AND 23),
    ADD COLUMN start_minute   SMALLINT NOT NULL DEFAULT 0
        CHECK (start_minute  BETWEEN 0 AND 59),
    ADD COLUMN interval_minutes INT    NOT NULL DEFAULT 60
        CHECK (interval_minutes BETWEEN 1 AND 1440);

-- 3. Remove the temporary defaults (columns are now populated)
ALTER TABLE job_definitions
    ALTER COLUMN start_hour     DROP DEFAULT,
    ALTER COLUMN start_minute   DROP DEFAULT,
    ALTER COLUMN interval_minutes DROP DEFAULT;

-- 4. Comments
COMMENT ON COLUMN job_definitions.start_hour       IS 'UTC hour (0–23) of the anchor time for this job''s daily schedule';
COMMENT ON COLUMN job_definitions.start_minute     IS 'UTC minute (0–59) of the anchor time for this job''s daily schedule';
COMMENT ON COLUMN job_definitions.interval_minutes IS 'Recurrence period in minutes (1 – 1440). Slots are start_time, start_time + interval, start_time + 2×interval, …';
