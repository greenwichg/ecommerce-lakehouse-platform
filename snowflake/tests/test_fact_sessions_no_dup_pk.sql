-- Test: fact_sessions has no duplicate silver_session_keys.
-- A passing test returns ZERO rows.

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE SCHEMA analytics;

SELECT
    silver_session_key,
    COUNT(*) AS dup_count
FROM fact_sessions
GROUP BY silver_session_key
HAVING COUNT(*) > 1;
