-- Test: currency_rates has no duplicate (rate_date, target_currency) pairs.
-- Zero rows = pass.

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE SCHEMA analytics;

SELECT
    rate_date,
    target_currency,
    COUNT(*) AS dup_count
FROM currency_rates
GROUP BY rate_date, target_currency
HAVING COUNT(*) > 1;
