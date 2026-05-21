-- Test: every wishlist event has a non-null customer_id (wishlists are
-- authenticated-only in this model). Any row here means the silver DQ
-- rule customer_id_not_null was bypassed.

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE SCHEMA analytics;

SELECT wishlist_event_id
FROM fact_customer_wishlist_product
WHERE customer_id IS NULL;
