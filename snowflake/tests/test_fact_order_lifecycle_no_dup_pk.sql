-- Test: fact_order_lifecycle has no duplicate primary keys (one row per order).

USE ROLE lakehouse_engineer;
USE DATABASE &{SNOWFLAKE_DATABASE};
USE SCHEMA analytics;

SELECT
    order_id,
    COUNT(*) AS dup_count
FROM fact_order_lifecycle
GROUP BY order_id
HAVING COUNT(*) > 1;
