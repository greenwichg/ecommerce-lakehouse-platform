-- RAW landing tables for orders.
-- One row per row in the corresponding gold Parquet file; loaded by
-- snowflake/dml/orders_load.sql via COPY INTO.

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE SCHEMA raw;

CREATE TABLE IF NOT EXISTS fact_orders_raw (
    order_sk VARCHAR(64) NOT NULL,
    order_id VARCHAR(36) NOT NULL,
    customer_id VARCHAR(36),
    customer_sk VARCHAR(64),    -- Slice 2: SHA-256 hex from SCD2 dim
    product_id VARCHAR(20),
    product_sk VARCHAR(64),
    category VARCHAR(50),       -- Slice 4: PIT-denormalised from dim_product
    quantity NUMBER(10, 0),
    price NUMBER(10, 2),
    total_amount NUMBER(14, 2),
    status VARCHAR(20),
    created_at TIMESTAMP_TZ,
    updated_at TIMESTAMP_TZ,
    _loaded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    _source_batch_id VARCHAR(64)
)
COMMENT = 'Raw landing for fact_orders Parquet from Databricks gold. Grain: one row per order_id.';

CREATE TABLE IF NOT EXISTS fact_order_lifecycle_raw (
    order_sk VARCHAR(64) NOT NULL,
    order_id VARCHAR(36) NOT NULL,
    customer_id VARCHAR(36),
    product_id VARCHAR(20),
    quantity NUMBER(10, 0),
    price NUMBER(10, 2),
    status VARCHAR(20),
    placed_at TIMESTAMP_TZ,
    paid_at TIMESTAMP_TZ,
    shipped_at TIMESTAMP_TZ,
    delivered_at TIMESTAMP_TZ,
    cancelled_at TIMESTAMP_TZ,
    days_placed_to_paid NUMBER(12, 4),
    days_paid_to_shipped NUMBER(12, 4),
    days_shipped_to_delivered NUMBER(12, 4),
    days_placed_to_delivered NUMBER(12, 4),
    updated_at TIMESTAMP_TZ,
    _loaded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    _source_batch_id VARCHAR(64)
)
COMMENT
= 'Raw landing for the accumulating-snapshot fact. Grain: one row per order_id, milestones accumulated.';
