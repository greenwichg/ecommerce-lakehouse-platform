-- Snowpipe + Stream + Task: the Snowflake-native CDC pipeline for the
-- clickstream gold → staging → analytics flow.
--
-- This is INTENTIONALLY parallel to the Databricks SCD2 PIT join used for
-- fact_orders (libs/gold.pit_join_scd2). Both layers do the same kind of
-- "bind fact to dim version current at the natural timestamp" enrichment;
-- having one in each platform lets reviewers compare the two flavors of
-- the same pattern (Spark DataFrame + broadcast join vs Snowflake SQL +
-- range JOIN on effective_from/effective_to).
--
-- ## Architecture
--
--   S3 gold/fact_sessions/*.parquet
--          │
--          │  (S3 event notification → SNS → Snowpipe — no Airflow task)
--          ▼
--   staging.fact_sessions_raw
--          │
--          │  CREATE STREAM stream_fact_sessions ON TABLE staging.fact_sessions_raw
--          ▼
--   [Stream captures INSERTs]
--          │
--          │  CREATE TASK enrich_and_merge_fact_sessions
--          │      SCHEDULE = '5 MINUTE'
--          │      WHEN SYSTEM$STREAM_HAS_DATA('stream_fact_sessions')
--          │  AS MERGE INTO analytics.fact_sessions ...
--          ▼
--   analytics.fact_sessions (customer_sk populated via PIT)
--
-- ## Why the WHEN clause
--
-- SCHEDULE='5 MINUTE' alone keeps the warehouse warm 24/7. The
-- SYSTEM$STREAM_HAS_DATA gate makes the task a no-op when the Stream is
-- empty (zero warehouse cost when idle). Standard Snowflake cost-awareness
-- pattern.

USE ROLE accountadmin;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};

-- ---------------------------------------------------------------------------
-- 1) Snowpipe: auto-ingest from gold_fact_sessions_stage → staging.fact_sessions_raw
-- ---------------------------------------------------------------------------
--
-- AWS_SNS_TOPIC: SNS topic ARN that S3 publishes object-created events to.
-- The Snowpipe SQS subscription is auto-created by Snowflake when AUTO_INGEST=TRUE.
-- Terraform provisions the SNS topic + S3 → SNS notification configuration
-- (Slice 5). For Slice 3 we treat the ARN as an env var placeholder.

CREATE PIPE IF NOT EXISTS staging.pipe_fact_sessions_ingest
AUTO_INGEST = TRUE
AWS_SNS_TOPIC = '{{ params.SNOWPIPE_SNS_TOPIC_ARN }}'
AS
COPY INTO staging.fact_sessions_raw (
    silver_session_key, raw_session_id, session_start, session_end,
    event_count, distinct_event_types, attributed_customer_id,
    converted, first_page_url, last_page_url, device, updated_at
)
FROM (
    SELECT
        $1:silver_session_key::VARCHAR,
        $1:raw_session_id::VARCHAR,
        $1:session_start::TIMESTAMP_TZ,
        $1:session_end::TIMESTAMP_TZ,
        $1:event_count::NUMBER,
        $1:distinct_event_types::NUMBER,
        $1:attributed_customer_id::VARCHAR,
        $1:converted::BOOLEAN,
        $1:first_page_url::VARCHAR,
        $1:last_page_url::VARCHAR,
        $1:device::VARCHAR,
        $1:updated_at::TIMESTAMP_TZ
    FROM @raw.gold_fact_sessions_stage
)
FILE_FORMAT = (FORMAT_NAME = raw.gold_parquet_format);

-- ---------------------------------------------------------------------------
-- 2) Stream: capture INSERTs into staging.fact_sessions_raw
-- ---------------------------------------------------------------------------

CREATE STREAM IF NOT EXISTS staging.stream_fact_sessions
ON TABLE staging.fact_sessions_raw
APPEND_ONLY = TRUE
COMMENT = 'Captures inserts to staging.fact_sessions_raw for the enrichment task.';

GRANT SELECT ON STREAM staging.stream_fact_sessions TO ROLE lakehouse_engineer;

-- ---------------------------------------------------------------------------
-- 3) Task: PIT-enrich + MERGE staging → analytics every 5 min IF stream has data
-- ---------------------------------------------------------------------------
--
-- The MERGE source is a SELECT from the Stream LEFT JOINed to dim_customer
-- on (attributed_customer_id, session_start in [effective_from, effective_to)).
-- LEFT JOIN preserves anonymous sessions (attributed_customer_id IS NULL)
-- and orphans (no matching dim row) so the orphan-rate gate downstream
-- can surface them. Note METADATA$ACTION='INSERT' filter — APPEND_ONLY
-- streams only emit INSERT rows anyway, but defensive.

CREATE TASK IF NOT EXISTS analytics.enrich_and_merge_fact_sessions
    WAREHOUSE = {{ params.SNOWFLAKE_WAREHOUSE }}
    SCHEDULE = '5 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA ('staging.stream_fact_sessions')
AS
    MERGE INTO analytics.fact_sessions AS t
    USING (
        SELECT
            s.silver_session_key,
            s.raw_session_id,
            s.session_start,
            s.session_end,
            s.event_count,
            s.distinct_event_types,
            s.attributed_customer_id,
            -- PIT customer surrogate: bind to the dim version current at
            -- session_start. LEFT JOIN means anonymous sessions and
            -- unresolved attributed_customer_ids stay with customer_sk = NULL.
            dc.customer_sk,
            s.converted,
            s.first_page_url,
            s.last_page_url,
            s.device,
            s.updated_at,
            s._loaded_at
        FROM staging.stream_fact_sessions AS s
        LEFT JOIN analytics.dim_customer AS dc
            ON
                s.attributed_customer_id = dc.customer_id
                AND s.session_start >= dc.effective_from
                AND s.session_start < dc.effective_to
        WHERE s.metadata$action = 'INSERT'
        -- A session re-emitted across two gold batches (late events grow
        -- the session) can appear twice in the stream window — e.g. after
        -- a backfill lands several hourly files between task runs. MERGE
        -- rejects duplicate source keys (nondeterministic merge), which
        -- would wedge the task permanently; keep only the newest version
        -- per session.
        QUALIFY
            ROW_NUMBER() OVER (
                PARTITION BY s.silver_session_key
                ORDER BY s.updated_at DESC, s._loaded_at DESC
            ) = 1
    ) AS src
        ON t.silver_session_key = src.silver_session_key
    -- Newer-wins on updated_at (= session_end): a session that grew in
    -- a later batch gets re-emitted with a later session_end.
    WHEN MATCHED AND src.updated_at > t.updated_at THEN
        UPDATE SET
            t.raw_session_id = src.raw_session_id,
            t.session_start = src.session_start,
            t.session_end = src.session_end,
            t.event_count = src.event_count,
            t.distinct_event_types = src.distinct_event_types,
            t.attributed_customer_id = src.attributed_customer_id,
            t.customer_sk = src.customer_sk,
            t.converted = src.converted,
            t.first_page_url = src.first_page_url,
            t.last_page_url = src.last_page_url,
            t.device = src.device,
            t.updated_at = src.updated_at,
            t._loaded_at = src._loaded_at
    WHEN NOT MATCHED THEN INSERT (
        silver_session_key, raw_session_id, session_start, session_end,
        event_count, distinct_event_types, attributed_customer_id, customer_sk,
        converted, first_page_url, last_page_url, device, updated_at, _loaded_at
    ) VALUES (
        src.silver_session_key, src.raw_session_id, src.session_start, src.session_end,
        src.event_count, src.distinct_event_types, src.attributed_customer_id, src.customer_sk,
        src.converted, src.first_page_url, src.last_page_url, src.device, src.updated_at, src._loaded_at
    );

-- Tasks are created suspended; resume to start the schedule.
ALTER TASK analytics.enrich_and_merge_fact_sessions RESUME;
