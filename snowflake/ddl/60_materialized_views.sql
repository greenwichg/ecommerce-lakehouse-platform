-- Materialized view: daily revenue by category (Slice 4).
--
-- See snowflake/ddl/mv_evidence.md for the design walkthrough including
-- the rejected alternatives and the Snowflake-MV-restriction workaround
-- (`category` denormalised into fact_orders at gold-build time).
--
-- Snowflake MVs auto-refresh on base-table changes. Refresh cost is
-- proportional to the changed micro-partition set — tiny for the
-- "insert N today's orders" pattern we have.

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE SCHEMA analytics;

CREATE OR REPLACE MATERIALIZED VIEW mv_daily_revenue_by_category
COMMENT = 'Daily revenue + order count + AOV per category. Single-table MV over fact_orders.'
AS
SELECT
    DATE_TRUNC('DAY', created_at) AS revenue_date,
    category,
    COUNT(*) AS order_count,
    SUM(total_amount) AS revenue_usd,
    AVG(total_amount) AS avg_order_value_usd,
    SUM(CASE WHEN status = 'cancelled' THEN total_amount ELSE 0 END) AS cancelled_revenue_usd,
    SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_count
FROM fact_orders
GROUP BY 1, 2;
