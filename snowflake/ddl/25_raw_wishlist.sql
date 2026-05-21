-- Raw landing for the wishlist factless fact.

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE SCHEMA raw;

CREATE TABLE IF NOT EXISTS fact_customer_wishlist_product_raw (
    wishlist_event_sk VARCHAR(64) NOT NULL,
    wishlist_event_id VARCHAR(36) NOT NULL,
    customer_id VARCHAR(36) NOT NULL,
    customer_sk VARCHAR(64),
    product_id VARCHAR(20) NOT NULL,
    product_sk VARCHAR(64),
    added_at TIMESTAMP_TZ NOT NULL,
    added_date DATE NOT NULL,
    source VARCHAR(50),
    updated_at TIMESTAMP_TZ,
    _loaded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    _source_batch_id VARCHAR(64)
)
COMMENT = 'Raw landing for the wishlist factless fact. Per-event grain.';
