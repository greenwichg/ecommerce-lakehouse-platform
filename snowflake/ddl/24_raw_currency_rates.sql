-- Raw landing for currency_rates Parquet from Databricks gold.
-- Carries _source ('api' | 'simulated') so the validate_currency_freshness
-- Airflow gate can check rate provenance.

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE SCHEMA raw;

CREATE TABLE IF NOT EXISTS currency_rates_raw (
    rate_date DATE NOT NULL,
    base_currency VARCHAR(3) NOT NULL,
    target_currency VARCHAR(3) NOT NULL,
    rate NUMBER(18, 6) NOT NULL,
    _source VARCHAR(16) NOT NULL,
    _fetched_at TIMESTAMP_TZ,
    _loaded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    _source_batch_id VARCHAR(64)
)
COMMENT = 'Raw landing for currency_rates. Grain: one row per (rate_date, target_currency).';
