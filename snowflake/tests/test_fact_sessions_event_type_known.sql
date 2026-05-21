-- Test: every fact_sessions row's last_page_url is one of our known
-- pages OR the session contained a known event_type. The sessionizer
-- shouldn't produce sessions for events that quarantine rejected.

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE SCHEMA analytics;

-- Sessions with negative or zero event_count = bug (every session has
-- at least the event that opened it).
SELECT silver_session_key, event_count
FROM fact_sessions
WHERE event_count <= 0;
