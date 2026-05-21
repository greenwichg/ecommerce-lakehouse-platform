-- Load wishlist factless fact from S3 stage. Idempotent on
-- wishlist_event_sk (events are immutable).

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE WAREHOUSE {{ params.SNOWFLAKE_WAREHOUSE }};

COPY INTO raw.fact_customer_wishlist_product_raw (
    wishlist_event_sk, wishlist_event_id, customer_id, customer_sk,
    product_id, product_sk, added_at, added_date, source, updated_at,
    _source_batch_id
)
FROM (
    SELECT
        $1:wishlist_event_sk::VARCHAR,
        $1:wishlist_event_id::VARCHAR,
        $1:customer_id::VARCHAR,
        $1:customer_sk::VARCHAR,
        $1:product_id::VARCHAR,
        $1:product_sk::VARCHAR,
        $1:added_at::TIMESTAMP_TZ,
        $1:added_date::DATE,
        $1:source::VARCHAR,
        $1:updated_at::TIMESTAMP_TZ,
        '{{ params.BATCH_ID }}'
    FROM @raw.gold_wishlist_stage
)
FILE_FORMAT = (FORMAT_NAME = raw.gold_parquet_format)
FORCE = FALSE
ON_ERROR = 'CONTINUE'
PURGE = FALSE;

MERGE INTO analytics.fact_customer_wishlist_product AS t
USING (
    SELECT *
    FROM raw.fact_customer_wishlist_product_raw
    WHERE _source_batch_id = '{{ params.BATCH_ID }}'
) AS s
    ON t.wishlist_event_sk = s.wishlist_event_sk
WHEN NOT MATCHED THEN INSERT (
    wishlist_event_sk, wishlist_event_id, customer_id, customer_sk,
    product_id, product_sk, added_at, added_date, source, updated_at,
    _loaded_at, _source_batch_id
) VALUES (
    s.wishlist_event_sk, s.wishlist_event_id, s.customer_id, s.customer_sk,
    s.product_id, s.product_sk, s.added_at, s.added_date, s.source, s.updated_at,
    s._loaded_at, s._source_batch_id
);
-- Note: no WHEN MATCHED branch — events are immutable, replay is a no-op.
