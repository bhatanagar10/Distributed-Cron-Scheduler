-- V6: Replace the custom scheduling model (start_hour, start_minute, interval_minutes)
--     with a standard 5-field Linux cron expression.
--
-- The next_execution_time column is retained as a Postgres-side cache used by the
-- startup listener to rebuild the Redis ZSET after a Redis restart.

ALTER TABLE job_definitions
    DROP COLUMN IF EXISTS start_hour,
    DROP COLUMN IF EXISTS start_minute,
    DROP COLUMN IF EXISTS interval_minutes,
    ADD COLUMN  cron_expression VARCHAR(100) NOT NULL DEFAULT '* * * * *';

-- Remove the default after backfill so future inserts must supply an expression
ALTER TABLE job_definitions
    ALTER COLUMN cron_expression DROP DEFAULT;
