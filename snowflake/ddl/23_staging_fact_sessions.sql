-- Staging landing table for fact_sessions. This is what Snowpipe writes
-- into (no Airflow task in the path between S3 and this table — Snowpipe
-- is S3-event-driven).
--
-- The Stream below (snowpipe_streams_tasks.sql) captures inserts here
-- and the Task MERGEs them into analytics.fact_sessions with PIT
-- customer enrichment.

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE SCHEMA staging;

CREATE TABLE IF NOT EXISTS fact_sessions_raw (
    silver_session_key VARCHAR(64) NOT NULL,
    raw_session_id VARCHAR(64) NOT NULL,
    session_start TIMESTAMP_TZ NOT NULL,
    session_end TIMESTAMP_TZ NOT NULL,
    event_count NUMBER(10, 0),
    distinct_event_types NUMBER(10, 0),
    attributed_customer_id VARCHAR(36),       -- NULL for anonymous sessions
    converted BOOLEAN,
    first_page_url VARCHAR(500),
    last_page_url VARCHAR(500),
    device VARCHAR(20),
    updated_at TIMESTAMP_TZ NOT NULL,
    _loaded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Snowpipe landing for fact_sessions. Grain: one row per silver_session_key, latest aggregates.';
