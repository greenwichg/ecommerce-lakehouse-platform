-- Test: every customer_ltv row references a customer that exists in
-- dim_customer. customer_ltv is built from fact_orders so the FK should
-- always resolve; any orphan here indicates a load ordering issue.

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE SCHEMA analytics;

SELECT l.customer_sk
FROM customer_ltv AS l
LEFT JOIN dim_customer AS d
    ON l.customer_sk = d.customer_sk
WHERE d.customer_sk IS NULL;
