-- analytics.customer_ltv — per-customer lifetime aggregates.
--
-- Built in Databricks Gold (uses window functions disqualified from
-- Snowflake MV); loaded full-refresh into Snowflake. Small table (10k
-- customers), refresh cost negligible.

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE SCHEMA analytics;

CREATE TABLE IF NOT EXISTS customer_ltv (
    customer_sk VARCHAR(64) NOT NULL,
    total_revenue NUMBER(18, 2),
    order_count NUMBER(10, 0),
    first_order_date DATE,
    last_order_date DATE,
    avg_days_between_orders NUMBER(12, 4),
    predicted_churn_flag BOOLEAN,
    updated_at TIMESTAMP_TZ,
    _loaded_at TIMESTAMP_TZ,
    _source_batch_id VARCHAR(64),
    CONSTRAINT pk_customer_ltv PRIMARY KEY (customer_sk),
    CONSTRAINT fk_customer_ltv_customer FOREIGN KEY (customer_sk) REFERENCES dim_customer (customer_sk)
)
CLUSTER BY (customer_sk)
COMMENT = 'Customer lifetime aggregates. Point-lookup by customer_sk is the hot path.';
