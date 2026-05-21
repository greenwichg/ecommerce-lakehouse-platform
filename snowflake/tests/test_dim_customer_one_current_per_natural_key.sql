-- SCD2 invariant: exactly one is_current=TRUE row per customer_id.
-- A non-zero result means the SCD2 MERGE upstream either failed to
-- close the prior current version OR inserted a duplicate current.

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE SCHEMA analytics;

SELECT
    customer_id,
    COUNT(*) AS current_rows
FROM dim_customer
WHERE is_current
GROUP BY customer_id
HAVING COUNT(*) > 1;
