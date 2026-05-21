-- analytics.fact_customer_wishlist_product — factless fact (Slice 4).
--
-- Grain: per-event (customer, product, added_at). No measures — only FK
-- relationships and a date. Used for "did this happen?" queries like
-- "customers who wishlisted but didn't buy".
--
-- Clustering: (customer_sk) — the most common join key in BI queries
-- pairs this fact with fact_orders via customer_sk to compute
-- conversion-from-wishlist metrics.

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE SCHEMA analytics;

CREATE TABLE IF NOT EXISTS fact_customer_wishlist_product (
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
    _loaded_at TIMESTAMP_TZ,
    _source_batch_id VARCHAR(64),
    CONSTRAINT pk_fact_wishlist PRIMARY KEY (wishlist_event_sk),
    CONSTRAINT fk_fact_wishlist_customer FOREIGN KEY (customer_sk) REFERENCES dim_customer (customer_sk),
    CONSTRAINT fk_fact_wishlist_product FOREIGN KEY (product_sk) REFERENCES dim_product (product_sk)
)
CLUSTER BY (customer_sk)
COMMENT = 'Factless wishlist fact. Per-event. Powers wishlist-to-purchase conversion queries.';
