-- analytics.dim_customer — SCD Type 2.
--
-- Clustering decision: CLUSTER BY (customer_id) alone, NOT
-- (customer_id, effective_from).
--
-- Hot path is the dashboard "current customer card" lookup:
--     SELECT * FROM dim_customer WHERE customer_id = ? AND is_current
-- Snowflake's micro-partition metadata efficiently prunes on the
-- low-cardinality `is_current` predicate once the partition has been
-- located by `customer_id`. Adding effective_from to the cluster key
-- helps historical PIT lookups (rare), at the cost of column-pair
-- maintenance.
--
-- The competing alternative — CLUSTER BY (customer_id, effective_from) —
-- would favour the historical PIT lookup
--     SELECT * FROM dim_customer
--     WHERE customer_id = ? AND <date> BETWEEN effective_from AND effective_to
-- but that query happens far less often (mostly debugging) and Snowflake
-- can serve it from the unclustered table acceptably.
--
-- Before/after SYSTEM$CLUSTERING_INFORMATION on the dashboard's hot query
-- is captured in snowflake/ddl/clustering_evidence.md.

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE SCHEMA analytics;

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_sk VARCHAR(64) NOT NULL,
    customer_id VARCHAR(36) NOT NULL,
    name VARCHAR(200),
    email VARCHAR(200),
    address VARCHAR(500),
    signup_date DATE,
    effective_from TIMESTAMP_TZ NOT NULL,
    effective_to TIMESTAMP_TZ NOT NULL,
    is_current BOOLEAN NOT NULL,
    _loaded_at TIMESTAMP_TZ,
    _source_batch_id VARCHAR(64),
    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_sk),
    CONSTRAINT uq_dim_customer_natural_version UNIQUE (customer_id, effective_from)
)
CLUSTER BY (customer_id)
COMMENT = 'SCD2 customer dim. One row per (customer_id, version). Tracked: email, address. Stable: name, signup_date.';
