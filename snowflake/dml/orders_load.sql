-- Load orders gold from S3 stages into raw, then MERGE into analytics.
-- This is the SQL the Airflow daily DAG runs via SQLExecuteQueryOperator.
--
-- Parameters (set by Airflow via session vars or templated SQL):
--   {{ params.SNOWFLAKE_DATABASE }}: target database
--   {{ params.BATCH_ID }}: lineage tag for this load
--
-- The load is idempotent: COPY INTO uses FORCE=FALSE so re-running with
-- the same S3 files doesn't double-load, and the downstream MERGE
-- preserves rows when the source updated_at <= target updated_at.

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE WAREHOUSE {{ params.SNOWFLAKE_WAREHOUSE }};

-- 1) Stage fact_orders Parquet -> raw.fact_orders_raw
COPY INTO raw.fact_orders_raw (
    order_sk, order_id, customer_id, customer_sk, product_id, product_sk,
    category, quantity, price, total_amount, status, created_at, updated_at,
    _source_batch_id
)
FROM (
    SELECT
        $1:order_sk::VARCHAR,
        $1:order_id::VARCHAR,
        $1:customer_id::VARCHAR,
        $1:customer_sk::VARCHAR,     -- Slice 2: SHA-256 hex
        $1:product_id::VARCHAR,
        $1:product_sk::VARCHAR,
        $1:category::VARCHAR,        -- Slice 4: PIT-denormalised
        $1:quantity::NUMBER,
        $1:price::NUMBER(10, 2),
        $1:total_amount::NUMBER(14, 2),
        $1:status::VARCHAR,
        $1:created_at::TIMESTAMP_TZ,
        $1:updated_at::TIMESTAMP_TZ,
        '{{ params.BATCH_ID }}'
    FROM @raw.gold_fact_orders_stage
)
FILE_FORMAT = (FORMAT_NAME = raw.gold_parquet_format)
FORCE = FALSE                    -- skip files already loaded
ON_ERROR = 'CONTINUE'
PURGE = FALSE;                   -- keep S3 files; lifecycle policy archives

-- 2) MERGE raw -> analytics.fact_orders.
--    Update only when source has strictly newer updated_at; otherwise the
--    incumbent state wins (out-of-order arrival protection mirroring the
--    silver/gold MERGE predicate upstream).
MERGE INTO analytics.fact_orders AS t
USING (
    SELECT
        order_sk, order_id, customer_id, customer_sk, product_id, product_sk,
        category, quantity, price, total_amount, status, created_at, updated_at,
        _loaded_at, _source_batch_id
    FROM raw.fact_orders_raw
    WHERE _source_batch_id = '{{ params.BATCH_ID }}'
) AS s
    ON t.order_id = s.order_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN
    UPDATE SET
        t.order_sk = s.order_sk,
        t.customer_id = s.customer_id,
        t.customer_sk = s.customer_sk,
        t.product_id = s.product_id,
        t.product_sk = s.product_sk,
        t.category = s.category,
        t.quantity = s.quantity,
        t.price = s.price,
        t.total_amount = s.total_amount,
        t.status = s.status,
        t.created_at = s.created_at,
        t.updated_at = s.updated_at,
        t._loaded_at = s._loaded_at,
        t._source_batch_id = s._source_batch_id
WHEN NOT MATCHED THEN INSERT (
    order_sk, order_id, customer_id, customer_sk, product_id, product_sk,
    category, quantity, price, total_amount, status, created_at, updated_at,
    _loaded_at, _source_batch_id
) VALUES (
    s.order_sk, s.order_id, s.customer_id, s.customer_sk, s.product_id, s.product_sk,
    s.category, s.quantity, s.price, s.total_amount, s.status, s.created_at, s.updated_at,
    s._loaded_at, s._source_batch_id
);

-- 3) Stage fact_order_lifecycle Parquet -> raw.fact_order_lifecycle_raw
COPY INTO raw.fact_order_lifecycle_raw (
    order_sk, order_id, customer_id, product_id, quantity, price, status,
    placed_at, paid_at, shipped_at, delivered_at, cancelled_at,
    days_placed_to_paid, days_paid_to_shipped, days_shipped_to_delivered,
    days_placed_to_delivered, updated_at, _source_batch_id
)
FROM (
    SELECT
        $1:order_sk::VARCHAR,
        $1:order_id::VARCHAR,
        $1:customer_id::VARCHAR,
        $1:product_id::VARCHAR,
        $1:quantity::NUMBER,
        $1:price::NUMBER(10, 2),
        $1:status::VARCHAR,
        $1:placed_at::TIMESTAMP_TZ,
        $1:paid_at::TIMESTAMP_TZ,
        $1:shipped_at::TIMESTAMP_TZ,
        $1:delivered_at::TIMESTAMP_TZ,
        $1:cancelled_at::TIMESTAMP_TZ,
        $1:days_placed_to_paid::NUMBER(12, 4),
        $1:days_paid_to_shipped::NUMBER(12, 4),
        $1:days_shipped_to_delivered::NUMBER(12, 4),
        $1:days_placed_to_delivered::NUMBER(12, 4),
        $1:updated_at::TIMESTAMP_TZ,
        '{{ params.BATCH_ID }}'
    FROM @raw.gold_fact_order_lifecycle_stage
)
FILE_FORMAT = (FORMAT_NAME = raw.gold_parquet_format)
FORCE = FALSE
ON_ERROR = 'CONTINUE'
PURGE = FALSE;

-- 4) MERGE raw -> analytics.fact_order_lifecycle. Same newer-wins predicate.
MERGE INTO analytics.fact_order_lifecycle AS t
USING (
    SELECT *
    FROM raw.fact_order_lifecycle_raw
    WHERE _source_batch_id = '{{ params.BATCH_ID }}'
) AS s
    ON t.order_id = s.order_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN
    UPDATE SET
        t.order_sk = s.order_sk,
        t.customer_id = s.customer_id,
        t.product_id = s.product_id,
        t.quantity = s.quantity,
        t.price = s.price,
        t.status = s.status,
        t.placed_at = s.placed_at,
        t.paid_at = s.paid_at,
        t.shipped_at = s.shipped_at,
        t.delivered_at = s.delivered_at,
        t.cancelled_at = s.cancelled_at,
        t.days_placed_to_paid = s.days_placed_to_paid,
        t.days_paid_to_shipped = s.days_paid_to_shipped,
        t.days_shipped_to_delivered = s.days_shipped_to_delivered,
        t.days_placed_to_delivered = s.days_placed_to_delivered,
        t.updated_at = s.updated_at,
        t._loaded_at = s._loaded_at,
        t._source_batch_id = s._source_batch_id
WHEN NOT MATCHED THEN INSERT (
    order_sk, order_id, customer_id, product_id, quantity, price, status,
    placed_at, paid_at, shipped_at, delivered_at, cancelled_at,
    days_placed_to_paid, days_paid_to_shipped, days_shipped_to_delivered,
    days_placed_to_delivered, updated_at, _loaded_at, _source_batch_id
) VALUES (
    s.order_sk, s.order_id, s.customer_id, s.product_id, s.quantity, s.price, s.status,
    s.placed_at, s.paid_at, s.shipped_at, s.delivered_at, s.cancelled_at,
    s.days_placed_to_paid, s.days_paid_to_shipped, s.days_shipped_to_delivered,
    s.days_placed_to_delivered, s.updated_at, s._loaded_at, s._source_batch_id
);
