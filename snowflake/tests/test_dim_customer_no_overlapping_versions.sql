-- SCD2 invariant: no two versions of the same customer have overlapping
-- effective_from/effective_to intervals. A row here means version chaining
-- is broken upstream.

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE SCHEMA analytics;

SELECT
    a.customer_id,
    a.customer_sk AS sk_a,
    b.customer_sk AS sk_b,
    a.effective_from AS a_from,
    a.effective_to AS a_to,
    b.effective_from AS b_from,
    b.effective_to AS b_to
FROM dim_customer AS a
INNER JOIN dim_customer AS b
    ON
        a.customer_id = b.customer_id
        AND a.customer_sk < b.customer_sk      -- avoid self-join + dedupe pairs
        AND a.effective_from < b.effective_to
        AND a.effective_to > b.effective_from;
