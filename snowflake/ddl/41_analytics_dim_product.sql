-- analytics.dim_product — SCD Type 2.
-- Clustered on product_id (same reasoning as dim_customer; see 40_analytics_dim_customer.sql).

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE SCHEMA analytics;

CREATE TABLE IF NOT EXISTS dim_product (
    product_sk VARCHAR(64) NOT NULL,
    product_id VARCHAR(20) NOT NULL,
    product_name VARCHAR(200),
    category VARCHAR(50),
    price NUMBER(10, 2),
    sku VARCHAR(40),
    effective_from TIMESTAMP_TZ NOT NULL,
    effective_to TIMESTAMP_TZ NOT NULL,
    is_current BOOLEAN NOT NULL,
    _loaded_at TIMESTAMP_TZ,
    _source_batch_id VARCHAR(64),
    CONSTRAINT pk_dim_product PRIMARY KEY (product_sk),
    CONSTRAINT uq_dim_product_natural_version UNIQUE (product_id, effective_from)
)
CLUSTER BY (product_id)
COMMENT = 'SCD2 product dim. One row per (product_id, version). Tracked: price, category. Stable: product_name, sku.';
