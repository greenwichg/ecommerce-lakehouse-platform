-- Storage integration + external stages pointing at the Databricks Gold
-- Delta tables on S3. Snowflake reads the underlying Parquet files (not the
-- Delta log) — there's a planned "Delta as native source" feature but for
-- now we rely on the fact that gold writes are full Parquet rewrites
-- after each MERGE.
--
-- The storage integration is created here as DDL so it's source-controlled;
-- the IAM role/trust policy on the AWS side is managed by Terraform
-- (infrastructure/terraform/snowflake_integration.tf in Slice 5).

USE ROLE accountadmin;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};

CREATE STORAGE INTEGRATION IF NOT EXISTS lakehouse_s3_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
STORAGE_AWS_ROLE_ARN = '{{ params.S3_INTEGRATION_ROLE_ARN }}'
STORAGE_ALLOWED_LOCATIONS = ('s3://{{ params.LAKEHOUSE_BUCKET }}/gold/')
ENABLED = TRUE
COMMENT = 'Storage integration for reading Databricks Gold Parquet from S3.';

GRANT USAGE ON INTEGRATION lakehouse_s3_integration TO ROLE lakehouse_engineer;

USE ROLE lakehouse_engineer;
USE SCHEMA raw;

CREATE FILE FORMAT IF NOT EXISTS gold_parquet_format
TYPE = PARQUET
COMPRESSION = SNAPPY
COMMENT = 'Snappy-compressed Parquet, matching what Databricks gold writes.';

CREATE STAGE IF NOT EXISTS gold_fact_orders_stage
    URL = 's3://{{ params.LAKEHOUSE_BUCKET }}/gold/fact_orders/'
    STORAGE_INTEGRATION = lakehouse_s3_integration
    FILE_FORMAT = gold_parquet_format
    COMMENT = 'External stage for fact_orders Parquet files written by Databricks.';

CREATE STAGE IF NOT EXISTS gold_fact_order_lifecycle_stage
    URL = 's3://{{ params.LAKEHOUSE_BUCKET }}/gold/fact_order_lifecycle/'
    STORAGE_INTEGRATION = lakehouse_s3_integration
    FILE_FORMAT = gold_parquet_format
    COMMENT = 'External stage for the accumulating-snapshot fact.';

-- Slice 2: stages for the SCD2 dim Parquet exports
CREATE STAGE IF NOT EXISTS gold_dim_customer_stage
    URL = 's3://{{ params.LAKEHOUSE_BUCKET }}/gold/dim_customer/'
    STORAGE_INTEGRATION = lakehouse_s3_integration
    FILE_FORMAT = gold_parquet_format
    COMMENT = 'External stage for dim_customer SCD2 history Parquet files.';

CREATE STAGE IF NOT EXISTS gold_dim_product_stage
    URL = 's3://{{ params.LAKEHOUSE_BUCKET }}/gold/dim_product/'
    STORAGE_INTEGRATION = lakehouse_s3_integration
    FILE_FORMAT = gold_parquet_format
    COMMENT = 'External stage for dim_product SCD2 history Parquet files.';

-- Slice 3: gold/fact_sessions stage feeds the Snowpipe auto-ingest below.
CREATE STAGE IF NOT EXISTS gold_fact_sessions_stage
    URL = 's3://{{ params.LAKEHOUSE_BUCKET }}/gold/fact_sessions/'
    STORAGE_INTEGRATION = lakehouse_s3_integration
    FILE_FORMAT = gold_parquet_format
    COMMENT = 'External stage for fact_sessions Parquet files. Drives the Snowpipe.';

-- Slice 4: stages for the new sources + customer_ltv mart.
CREATE STAGE IF NOT EXISTS gold_currency_rates_stage
    URL = 's3://{{ params.LAKEHOUSE_BUCKET }}/gold/currency_rates/'
    STORAGE_INTEGRATION = lakehouse_s3_integration
    FILE_FORMAT = gold_parquet_format
    COMMENT = 'External stage for currency_rates Parquet (daily reference table).';

CREATE STAGE IF NOT EXISTS gold_wishlist_stage
    URL = 's3://{{ params.LAKEHOUSE_BUCKET }}/gold/fact_customer_wishlist_product/'
    STORAGE_INTEGRATION = lakehouse_s3_integration
    FILE_FORMAT = gold_parquet_format
    COMMENT = 'External stage for the wishlist factless fact.';

CREATE STAGE IF NOT EXISTS gold_customer_ltv_stage
    URL = 's3://{{ params.LAKEHOUSE_BUCKET }}/gold/customer_ltv/'
    STORAGE_INTEGRATION = lakehouse_s3_integration
    FILE_FORMAT = gold_parquet_format
    COMMENT = 'External stage for the customer_ltv mart (full-refresh).';
