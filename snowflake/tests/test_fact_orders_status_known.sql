-- Test: every fact_orders row has a known status value.
-- The status enum is mirrored from the silver DQ rule "status_known".
-- A row here means quarantine logic upstream let a bad status through.

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE SCHEMA analytics;

SELECT
    order_id,
    status
FROM fact_orders
WHERE
    status NOT IN ('placed', 'paid', 'shipped', 'delivered', 'cancelled')
    OR status IS NULL;
