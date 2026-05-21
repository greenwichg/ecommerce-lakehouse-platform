-- Load dim_customer SCD2 history from S3 stage into raw, then MERGE
-- into analytics.dim_customer. The MERGE is keyed on the surrogate
-- (customer_sk) — each SCD2 version has a stable SHA-256-derived sk,
-- so re-loading the same version is a no-op and arrival of a new
-- version inserts cleanly.

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE WAREHOUSE {{ params.SNOWFLAKE_WAREHOUSE }};

-- 1) Stage gold Parquet -> raw.dim_customer_raw
COPY INTO raw.dim_customer_raw (
    customer_sk, customer_id, name, email, address, signup_date,
    effective_from, effective_to, is_current, _source_batch_id
)
FROM (
    SELECT
        $1:customer_sk::VARCHAR,
        $1:customer_id::VARCHAR,
        $1:name::VARCHAR,
        $1:email::VARCHAR,
        $1:address::VARCHAR,
        $1:signup_date::DATE,
        $1:effective_from::TIMESTAMP_TZ,
        $1:effective_to::TIMESTAMP_TZ,
        $1:is_current::BOOLEAN,
        '{{ params.BATCH_ID }}'
    FROM @raw.gold_dim_customer_stage
)
FILE_FORMAT = (FORMAT_NAME = raw.gold_parquet_format)
FORCE = FALSE
ON_ERROR = 'CONTINUE'
PURGE = FALSE;

-- 2) MERGE raw -> analytics.dim_customer.
--    Match on customer_sk (the SCD2 version's surrogate). On match we
--    update mutable columns — chiefly effective_to and is_current — which
--    flip on the version PRECEDING a new arrival when gold closes it.
--    On no-match we insert as the new version.
MERGE INTO analytics.dim_customer AS t
USING (
    SELECT *
    FROM raw.dim_customer_raw
    WHERE _source_batch_id = '{{ params.BATCH_ID }}'
) AS s
    ON t.customer_sk = s.customer_sk
WHEN MATCHED THEN
    UPDATE SET
        t.effective_to = s.effective_to,
        t.is_current = s.is_current,
        t._loaded_at = s._loaded_at,
        t._source_batch_id = s._source_batch_id
WHEN NOT MATCHED THEN INSERT (
    customer_sk, customer_id, name, email, address, signup_date,
    effective_from, effective_to, is_current, _loaded_at, _source_batch_id
) VALUES (
    s.customer_sk, s.customer_id, s.name, s.email, s.address, s.signup_date,
    s.effective_from, s.effective_to, s.is_current, s._loaded_at, s._source_batch_id
);
