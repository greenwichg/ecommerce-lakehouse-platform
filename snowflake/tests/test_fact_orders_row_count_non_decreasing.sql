-- Test: today's fact_orders row count is at least as many as yesterday's.
-- A drop means a load failure or a stale source — investigate before
-- accepting the next pipeline run.
--
-- Uses INFORMATION_SCHEMA.TABLE_HISTORY-style approach via Snowflake's
-- TABLE_STORAGE_METRICS. For dev where data hasn't been loaded twice yet,
-- the test returns zero rows and passes vacuously.

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE SCHEMA analytics;

WITH today_count AS (
    SELECT COUNT(*) AS n FROM fact_orders
),

yesterday_count AS (
    SELECT COUNT(*) AS n
    FROM
        fact_orders
        AT (OFFSET => -86400)            -- Snowflake Time Travel: 24h ago
)

SELECT
    'fact_orders' AS table_name,
    today_count.n AS today,
    yesterday_count.n AS yesterday
FROM today_count, yesterday_count
WHERE today_count.n < yesterday_count.n;
