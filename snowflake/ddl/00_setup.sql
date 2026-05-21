-- Database, schemas, warehouse, and roles for the lakehouse.
-- Run by a role with CREATE privileges (ACCOUNTADMIN or delegated).
-- Uses ${SNOWFLAKE_DATABASE} placeholder so the same DDL deploys to dev
-- and prod by setting the var via schemachange or psql parameter passing.

USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS &{SNOWFLAKE_DATABASE}
    COMMENT = 'Ecommerce lakehouse analytics database.';

USE DATABASE &{SNOWFLAKE_DATABASE};

CREATE SCHEMA IF NOT EXISTS raw
    COMMENT = 'Raw landing zone for Parquet files loaded from Databricks gold on S3.';

CREATE SCHEMA IF NOT EXISTS staging
    COMMENT = 'Staging zone for type validation and pre-fact cleansing.';

CREATE SCHEMA IF NOT EXISTS analytics
    COMMENT = 'Star schema for BI: facts, dimensions, materialized views.';

CREATE WAREHOUSE IF NOT EXISTS &{SNOWFLAKE_WAREHOUSE}
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'ETL warehouse. XS + 60s auto-suspend caps idle cost; size up via ALTER for backfills.';

USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS lakehouse_engineer
    COMMENT = 'Read/write across raw, staging, analytics.';

CREATE ROLE IF NOT EXISTS lakehouse_analyst
    COMMENT = 'Read-only on analytics (dashboards, ad-hoc).';

GRANT USAGE ON DATABASE &{SNOWFLAKE_DATABASE} TO ROLE lakehouse_engineer;
GRANT USAGE ON ALL SCHEMAS IN DATABASE &{SNOWFLAKE_DATABASE} TO ROLE lakehouse_engineer;
GRANT ALL ON SCHEMA &{SNOWFLAKE_DATABASE}.raw TO ROLE lakehouse_engineer;
GRANT ALL ON SCHEMA &{SNOWFLAKE_DATABASE}.staging TO ROLE lakehouse_engineer;
GRANT ALL ON SCHEMA &{SNOWFLAKE_DATABASE}.analytics TO ROLE lakehouse_engineer;
GRANT USAGE ON WAREHOUSE &{SNOWFLAKE_WAREHOUSE} TO ROLE lakehouse_engineer;
GRANT OPERATE ON WAREHOUSE &{SNOWFLAKE_WAREHOUSE} TO ROLE lakehouse_engineer;

GRANT USAGE ON DATABASE &{SNOWFLAKE_DATABASE} TO ROLE lakehouse_analyst;
GRANT USAGE ON SCHEMA &{SNOWFLAKE_DATABASE}.analytics TO ROLE lakehouse_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA &{SNOWFLAKE_DATABASE}.analytics TO ROLE lakehouse_analyst;
GRANT SELECT ON FUTURE TABLES IN SCHEMA &{SNOWFLAKE_DATABASE}.analytics TO ROLE lakehouse_analyst;
GRANT SELECT ON ALL VIEWS IN SCHEMA &{SNOWFLAKE_DATABASE}.analytics TO ROLE lakehouse_analyst;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA &{SNOWFLAKE_DATABASE}.analytics TO ROLE lakehouse_analyst;
GRANT USAGE ON WAREHOUSE &{SNOWFLAKE_WAREHOUSE} TO ROLE lakehouse_analyst;
