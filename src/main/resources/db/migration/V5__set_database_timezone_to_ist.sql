-- ============================================================
-- V5: Set database default timezone to Asia/Kolkata (IST)
--
-- This ensures that all TIMESTAMPTZ columns automatically
-- format and return values in IST (+05:30) for all sessions,
-- including direct database queries and JDBC connections.
-- ============================================================

ALTER DATABASE crondb SET timezone TO 'Asia/Kolkata';
