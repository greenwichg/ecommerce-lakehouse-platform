-- Test: currency rates exist for the current date AND the simulated-rate
-- share is below 50% (warn band). Returns rows on FAILURE only.
--
-- Mirrors the validate_currency_freshness Airflow gate as a static SQL
-- guardrail. The Airflow task uses the configurable threshold from
-- config; this test hardcodes 50% as a tripwire.

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE SCHEMA analytics;

WITH today_rates AS (
    SELECT
        COUNT(*) AS total,
        SUM(CASE WHEN _source = 'simulated' THEN 1 ELSE 0 END) AS simulated
    FROM currency_rates
    WHERE rate_date = CURRENT_DATE
)

SELECT
    'currency_rates' AS table_name,
    total,
    simulated,
    CASE
        WHEN total = 0 THEN 'NO_RATES_FOR_TODAY'
        WHEN 100.0 * simulated / total > 50 THEN 'TOO_MANY_SIMULATED'
        ELSE 'OK'
    END AS status
FROM today_rates
WHERE
    total = 0
    OR 100.0 * simulated / NULLIF(total, 0) > 50;
