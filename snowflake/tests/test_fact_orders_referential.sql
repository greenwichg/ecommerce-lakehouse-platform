-- Test: every fact_orders row's order_id appears in fact_order_lifecycle.
-- Both facts are sourced from the same silver upstream, so divergence
-- means a load lag or partial-load. (Once dim_customer/dim_product land in
-- Slice 2, this file gains FK checks against those dims.)

USE ROLE lakehouse_engineer;
USE DATABASE &{SNOWFLAKE_DATABASE};
USE SCHEMA analytics;

SELECT
    f.order_id
FROM fact_orders AS f
LEFT JOIN fact_order_lifecycle AS l
    ON f.order_id = l.order_id
WHERE l.order_id IS NULL;
