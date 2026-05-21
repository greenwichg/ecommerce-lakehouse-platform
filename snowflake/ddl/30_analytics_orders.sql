-- Analytics-layer orders facts.
--
-- Clustering keys:
--   - fact_orders: (created_at, status). created_at is the highest-cardinality
--     filter most BI queries issue ("last 30 days"); status is co-clustered to
--     speed funnel-style filters (placed-vs-paid).
--   - fact_order_lifecycle: (placed_at). The cycle-time analytics filter is
--     always "orders placed between X and Y".
--
-- Before/after SYSTEM$CLUSTERING_INFORMATION snippets are recorded in
-- docs/architecture.md under "Clustering decisions".

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE SCHEMA analytics;

-- Slice 2: customer_sk/product_sk types changed from NUMBER(38,0) to
-- VARCHAR(64) — SHA-256 hex from the SCD2 dim. See libs.scd2 for the
-- determinism vs identity tradeoff.
-- Slice 4: `category` denormalised from dim_product (PIT at created_at)
-- so the materialised view in 60_materialized_views.sql has a single
-- base table to aggregate. Snowflake MVs can't do joins; denormalisation
-- here lets us keep the star-schema dims for normalised analytics while
-- still supporting MV-friendly aggregations.
CREATE TABLE IF NOT EXISTS fact_orders (
    order_sk VARCHAR(64) NOT NULL,
    order_id VARCHAR(36) NOT NULL,
    customer_id VARCHAR(36),
    customer_sk VARCHAR(64),
    product_id VARCHAR(20),
    product_sk VARCHAR(64),
    category VARCHAR(50),       -- Slice 4: PIT-snapshot of dim_product.category
    quantity NUMBER(10, 0),
    price NUMBER(10, 2),
    total_amount NUMBER(14, 2),
    status VARCHAR(20),
    created_at TIMESTAMP_TZ,
    updated_at TIMESTAMP_TZ,
    _loaded_at TIMESTAMP_TZ,
    _source_batch_id VARCHAR(64),
    CONSTRAINT pk_fact_orders PRIMARY KEY (order_id),
    CONSTRAINT fk_fact_orders_customer FOREIGN KEY (customer_sk) REFERENCES dim_customer (customer_sk),
    CONSTRAINT fk_fact_orders_product FOREIGN KEY (product_sk) REFERENCES dim_product (product_sk)
)
CLUSTER BY (DATE_TRUNC('DAY', created_at), status)
COMMENT
= 'Transactional grain. One row per order_id, current state. Loaded via MERGE from raw.fact_orders_raw.';

CREATE TABLE IF NOT EXISTS fact_order_lifecycle (
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
    _loaded_at TIMESTAMP_TZ,
    _source_batch_id VARCHAR(64),
    CONSTRAINT pk_fact_order_lifecycle PRIMARY KEY (order_id)
)
CLUSTER BY (DATE_TRUNC('DAY', placed_at))
COMMENT = 'Accumulating-snapshot fact. One row per order_id, milestones accumulated as orders progress.';
