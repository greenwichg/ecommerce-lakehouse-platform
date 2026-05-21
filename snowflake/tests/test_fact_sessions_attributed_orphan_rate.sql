-- Test: of sessions WITH an attributed_customer_id, the % whose
-- customer_sk failed to bind in dim_customer must be < 1%.
--
-- Critical scoping: anonymous sessions (attributed_customer_id IS NULL)
-- are EXPECTED to have customer_sk = NULL and must NOT be counted as
-- orphans — they're the model. Only attributed-but-unresolved sessions
-- are orphans (and indicate either dim load lag or upstream FK break).
--
-- Returns zero rows on pass; one row if the threshold is breached.

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE SCHEMA analytics;

WITH attributed AS (
    SELECT *
    FROM fact_sessions
    WHERE attributed_customer_id IS NOT NULL
),

orphans AS (
    SELECT
        COUNT(*) AS total,
        SUM(CASE WHEN customer_sk IS NULL THEN 1 ELSE 0 END) AS orphan
    FROM attributed
)

SELECT
    'fact_sessions' AS table_name,
    total,
    orphan,
    ROUND(100.0 * orphan / NULLIF(total, 0), 4) AS orphan_pct
FROM orphans
WHERE 100.0 * orphan / NULLIF(total, 0) > 1.0;
