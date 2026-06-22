-- ============================================================
-- V4: Alter start_hour and start_minute column types to INT
--
-- The previous V3 migration created them as SMALLINT.
-- Since the JPA entity maps them to Java Integer, Hibernate's
-- schema validation fails with a type mismatch error.
-- ============================================================

ALTER TABLE job_definitions
    ALTER COLUMN start_hour TYPE INT,
    ALTER COLUMN start_minute TYPE INT;
