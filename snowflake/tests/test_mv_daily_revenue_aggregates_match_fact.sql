-- Test: the MV's totals reconcile with a fresh aggregation of fact_orders.
-- A row here means the MV is stale or has drifted from its base table.

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE SCHEMA analytics;

WITH mv_total AS (
    SELECT SUM(revenue_usd) AS total
    FROM mv_daily_revenue_by_category
),

fact_total AS (
    SELECT SUM(total_amount) AS total
    FROM fact_orders
)

SELECT
    'mv_daily_revenue_by_category' AS view_name,
    mv_total.total AS mv_revenue,
    fact_total.total AS fact_revenue
FROM mv_total, fact_total
WHERE ABS(COALESCE(mv_total.total, 0) - COALESCE(fact_total.total, 0)) > 0.01;
