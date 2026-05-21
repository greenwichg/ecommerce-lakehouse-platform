-- Load currency_rates from S3 stage into raw, then MERGE into analytics.
-- Idempotent: COPY FORCE=FALSE skips already-loaded files; MERGE on
-- (rate_date, target_currency) primary key.

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE WAREHOUSE {{ params.SNOWFLAKE_WAREHOUSE }};

COPY INTO raw.currency_rates_raw (
    rate_date, base_currency, target_currency, rate, _source, _fetched_at, _source_batch_id
)
FROM (
    SELECT
        $1:rate_date::DATE,
        $1:base_currency::VARCHAR,
        $1:target_currency::VARCHAR,
        $1:rate::NUMBER(18, 6),
        $1:_source::VARCHAR,
        $1:_fetched_at::TIMESTAMP_TZ,
        '{{ params.BATCH_ID }}'
    FROM @raw.gold_currency_rates_stage
)
FILE_FORMAT = (FORMAT_NAME = raw.gold_parquet_format)
FORCE = FALSE
ON_ERROR = 'CONTINUE'
PURGE = FALSE;

MERGE INTO analytics.currency_rates AS t
USING (
    SELECT *
    FROM raw.currency_rates_raw
    WHERE _source_batch_id = '{{ params.BATCH_ID }}'
) AS s
    ON t.rate_date = s.rate_date AND t.target_currency = s.target_currency
WHEN MATCHED AND s._fetched_at > t._fetched_at THEN
    UPDATE SET
        t.base_currency = s.base_currency,
        t.rate = s.rate,
        t._source = s._source,
        t._fetched_at = s._fetched_at,
        t._loaded_at = s._loaded_at,
        t._source_batch_id = s._source_batch_id
WHEN NOT MATCHED THEN INSERT (
    rate_date, base_currency, target_currency, rate, _source,
    _fetched_at, _loaded_at, _source_batch_id
) VALUES (
    s.rate_date, s.base_currency, s.target_currency, s.rate, s._source,
    s._fetched_at, s._loaded_at, s._source_batch_id
);
