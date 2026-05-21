-- RAW landing tables for the dim_customer and dim_product sources.
-- Loaded by snowflake/dml/dim_customer_load.sql and dim_product_load.sql
-- via COPY INTO from the gold-side stages.

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE SCHEMA raw;

-- dim_customer source: gold dim_customer is itself the SCD2 history table
-- (one row per version). We land it straight into raw with the SCD2 shape
-- already in place; the staging layer is a no-op MERGE for dims.
CREATE TABLE IF NOT EXISTS dim_customer_raw (
    customer_sk VARCHAR(64) NOT NULL,
    customer_id VARCHAR(36) NOT NULL,
    name VARCHAR(200),
    email VARCHAR(200),
    address VARCHAR(500),
    signup_date DATE,
    effective_from TIMESTAMP_TZ NOT NULL,
    effective_to TIMESTAMP_TZ NOT NULL,
    is_current BOOLEAN NOT NULL,
    _loaded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    _source_batch_id VARCHAR(64)
)
COMMENT = 'Raw landing for dim_customer SCD2 history from Databricks gold. Grain: one row per (customer_id, version).';

CREATE TABLE IF NOT EXISTS dim_product_raw (
    product_sk VARCHAR(64) NOT NULL,
    product_id VARCHAR(20) NOT NULL,
    product_name VARCHAR(200),
    category VARCHAR(50),
    price NUMBER(10, 2),
    sku VARCHAR(40),
    effective_from TIMESTAMP_TZ NOT NULL,
    effective_to TIMESTAMP_TZ NOT NULL,
    is_current BOOLEAN NOT NULL,
    _loaded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    _source_batch_id VARCHAR(64)
)
COMMENT = 'Raw landing for dim_product SCD2 history. Grain: one row per (product_id, version).';
