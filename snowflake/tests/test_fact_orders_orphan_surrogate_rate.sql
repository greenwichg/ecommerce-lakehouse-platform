-- Snowflake-side mirror of the Databricks orphan_surrogate_rate check.
-- Returns one row of (table_name, total, orphan, orphan_pct) IF the
-- rate exceeds 1% — otherwise returns no rows. Zero rows = pass.
--
-- The HAVING clause hard-codes 1.0 because schemachange / sqlfluff don't
-- have a clean way to inject runtime variables into HAVING. The Airflow
-- DAG's dq_gates.check_orphan_surrogate_rate uses the configurable
-- threshold for the per-run gate; this test is a static guardrail.

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE SCHEMA analytics;

SELECT
    'fact_orders' AS table_name,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN customer_sk IS NULL OR product_sk IS NULL THEN 1 ELSE 0 END) AS orphan_rows,
    ROUND(
        100.0 * SUM(CASE WHEN customer_sk IS NULL OR product_sk IS NULL THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0),
        4
    ) AS orphan_pct
FROM fact_orders
HAVING orphan_pct > 1.0;
