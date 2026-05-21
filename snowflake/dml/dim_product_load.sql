-- Load dim_product SCD2 history. Same pattern as dim_customer_load.sql.

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE WAREHOUSE {{ params.SNOWFLAKE_WAREHOUSE }};

COPY INTO raw.dim_product_raw (
    product_sk, product_id, product_name, category, price, sku,
    effective_from, effective_to, is_current, _source_batch_id
)
FROM (
    SELECT
        $1:product_sk::VARCHAR,
        $1:product_id::VARCHAR,
        $1:product_name::VARCHAR,
        $1:category::VARCHAR,
        $1:price::NUMBER(10, 2),
        $1:sku::VARCHAR,
        $1:effective_from::TIMESTAMP_TZ,
        $1:effective_to::TIMESTAMP_TZ,
        $1:is_current::BOOLEAN,
        '{{ params.BATCH_ID }}'
    FROM @raw.gold_dim_product_stage
)
FILE_FORMAT = (FORMAT_NAME = raw.gold_parquet_format)
FORCE = FALSE
ON_ERROR = 'CONTINUE'
PURGE = FALSE;

MERGE INTO analytics.dim_product AS t
USING (
    SELECT *
    FROM raw.dim_product_raw
    WHERE _source_batch_id = '{{ params.BATCH_ID }}'
) AS s
    ON t.product_sk = s.product_sk
WHEN MATCHED THEN
    UPDATE SET
        t.effective_to = s.effective_to,
        t.is_current = s.is_current,
        t._loaded_at = s._loaded_at,
        t._source_batch_id = s._source_batch_id
WHEN NOT MATCHED THEN INSERT (
    product_sk, product_id, product_name, category, price, sku,
    effective_from, effective_to, is_current, _loaded_at, _source_batch_id
) VALUES (
    s.product_sk, s.product_id, s.product_name, s.category, s.price, s.sku,
    s.effective_from, s.effective_to, s.is_current, s._loaded_at, s._source_batch_id
);
