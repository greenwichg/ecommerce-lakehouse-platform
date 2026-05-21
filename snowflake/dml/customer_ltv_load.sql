-- Load customer_ltv mart from S3 (Databricks-built, full-refresh per run).
-- Strategy: TRUNCATE + COPY INTO, since the mart is full-refresh upstream
-- and the table is small (10k customers).

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE WAREHOUSE {{ params.SNOWFLAKE_WAREHOUSE }};

-- Stage the full snapshot first so we can swap atomically.
CREATE TABLE IF NOT EXISTS analytics.customer_ltv_staging LIKE analytics.customer_ltv;
TRUNCATE TABLE analytics.customer_ltv_staging;

COPY INTO analytics.customer_ltv_staging (
    customer_sk, total_revenue, order_count, first_order_date, last_order_date,
    avg_days_between_orders, predicted_churn_flag, updated_at, _source_batch_id
)
FROM (
    SELECT
        $1:customer_sk::VARCHAR,
        $1:total_revenue::NUMBER(18, 2),
        $1:order_count::NUMBER(10, 0),
        $1:first_order_date::DATE,
        $1:last_order_date::DATE,
        $1:avg_days_between_orders::NUMBER(12, 4),
        $1:predicted_churn_flag::BOOLEAN,
        $1:updated_at::TIMESTAMP_TZ,
        '{{ params.BATCH_ID }}'
    FROM @raw.gold_customer_ltv_stage
)
FILE_FORMAT = (FORMAT_NAME = raw.gold_parquet_format)
FORCE = FALSE
ON_ERROR = 'ABORT_STATEMENT';     -- atomic load; partial = abort

-- Atomic swap: rename old out, new in, old dropped.
ALTER TABLE analytics.customer_ltv RENAME TO customer_ltv_previous;
ALTER TABLE analytics.customer_ltv_staging RENAME TO customer_ltv;
DROP TABLE IF EXISTS analytics.customer_ltv_previous;
