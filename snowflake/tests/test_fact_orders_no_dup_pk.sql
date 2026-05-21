-- Test: fact_orders has no duplicate primary keys.
-- A passing test returns ZERO rows; any returned row is a violation.

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE SCHEMA analytics;

SELECT
    order_id,
    COUNT(*) AS dup_count
FROM fact_orders
GROUP BY order_id
HAVING COUNT(*) > 1;
