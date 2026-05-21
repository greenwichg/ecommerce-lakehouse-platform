-- analytics.fact_sessions — the BI-facing sessions table.
--
-- Schema mirrors staging.fact_sessions_raw plus a customer_sk column
-- bound via the Snowflake Streams + Tasks pipeline's PIT join against
-- dim_customer. customer_sk is NULL for anonymous sessions (no
-- attributed_customer_id) and for sessions whose attributed customer
-- doesn't resolve in the dim (orphan rate gate catches the latter).
--
-- Clustering on (session_start) — the dashboard's session funnel is
-- always windowed by time, e.g. "conversion rate by day for last 30d".

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE SCHEMA analytics;

CREATE TABLE IF NOT EXISTS fact_sessions (
    silver_session_key VARCHAR(64) NOT NULL,
    raw_session_id VARCHAR(64) NOT NULL,
    session_start TIMESTAMP_TZ NOT NULL,
    session_end TIMESTAMP_TZ NOT NULL,
    event_count NUMBER(10, 0),
    distinct_event_types NUMBER(10, 0),
    attributed_customer_id VARCHAR(36),
    customer_sk VARCHAR(64),                 -- populated by Streams + Tasks PIT join
    converted BOOLEAN,
    first_page_url VARCHAR(500),
    last_page_url VARCHAR(500),
    device VARCHAR(20),
    updated_at TIMESTAMP_TZ,
    _loaded_at TIMESTAMP_TZ,
    CONSTRAINT pk_fact_sessions PRIMARY KEY (silver_session_key),
    CONSTRAINT fk_fact_sessions_customer FOREIGN KEY (customer_sk) REFERENCES dim_customer (customer_sk)
)
CLUSTER BY (DATE_TRUNC('DAY', session_start))
COMMENT = 'Per-session fact. Grain: one row per silver_session_key. customer_sk PIT-joined post-Snowpipe.';
