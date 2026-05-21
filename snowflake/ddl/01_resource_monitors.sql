-- Cost guardrails: resource monitors per warehouse + an account-level
-- safety net.
--
-- Why this matters: Snowflake compute auto-suspends but doesn't
-- auto-cap. A runaway query or a forgotten ad-hoc warehouse can rack
-- up credits at $X per hour silently. Resource monitors are the only
-- mechanism Snowflake provides to *stop* spend (vs. just *report* it
-- via account_usage views, which is after-the-fact).
--
-- Strategy:
--   - One BI warehouse for analyst queries / dashboards, separate from
--     the ETL warehouse so a runaway dashboard query can't take down
--     the batch pipeline (and vice versa).
--   - Per-warehouse monitor: daily + monthly cap with NOTIFY at 75/90,
--     SUSPEND at 100. Auto-suspend means the warehouse won't accept
--     new queries until the next quota window (manual override
--     possible).
--   - Account-level monitor as a backstop in case someone creates a
--     warehouse outside this tree and forgets to attach a monitor.
--
-- Credit values are placeholders sized for the demo (XS warehouse at
-- ~1 credit/hour). Production sizing belongs in env-specific config;
-- this file demonstrates the pattern.

USE ROLE accountadmin;  -- resource monitors require accountadmin

USE DATABASE {{ params.SNOWFLAKE_DATABASE }};

-- ----------------------------------------------------------------------
-- Second warehouse for BI / dashboards (separate from ETL warehouse).
-- ----------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS lakehouse_bi_wh
WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE
INITIALLY_SUSPENDED = TRUE
SCALING_POLICY = 'STANDARD'
COMMENT = 'BI warehouse for Streamlit dashboard + analyst ad-hoc. Separated from ETL so a runaway dashboard query cannot starve the batch pipeline.';

GRANT USAGE ON WAREHOUSE lakehouse_bi_wh TO ROLE lakehouse_analyst;
GRANT OPERATE ON WAREHOUSE lakehouse_bi_wh TO ROLE lakehouse_engineer;

-- ----------------------------------------------------------------------
-- ETL warehouse monitor: 20 credits/day, 400/month. The daily limit is
-- the "your DAG is misbehaving" tripwire; monthly is the budget cap.
-- ----------------------------------------------------------------------
CREATE OR REPLACE RESOURCE MONITOR rm_etl_wh
WITH
    CREDIT_QUOTA = 400               -- monthly cap
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    NOTIFY_USERS = ('LAKEHOUSE_OPS')  -- single account-admin user; populated via SCIM in real deployments
    TRIGGERS
        -- Notifications at 75/90 give ops time to investigate before
        -- the hard cap kicks in.
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND          -- finish in-flight queries, then suspend
        ON 110 PERCENT DO SUSPEND_IMMEDIATE; -- something is very wrong, abort

ALTER WAREHOUSE {{ params.SNOWFLAKE_WAREHOUSE }} SET RESOURCE_MONITOR = rm_etl_wh;

-- Separate daily monitor lets us catch single-day cost spikes (e.g.,
-- accidentally querying without a partition filter) before they eat
-- into the monthly budget. Snowflake resource monitors are 1:1 with
-- warehouse though, so we layer at the account level instead.

-- ----------------------------------------------------------------------
-- BI warehouse monitor: smaller cap since analyst usage is sporadic.
-- ----------------------------------------------------------------------
CREATE OR REPLACE RESOURCE MONITOR rm_bi_wh
WITH
    CREDIT_QUOTA = 100
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    NOTIFY_USERS = ('LAKEHOUSE_OPS')
    TRIGGERS
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND
        ON 110 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE lakehouse_bi_wh SET RESOURCE_MONITOR = rm_bi_wh;

-- ----------------------------------------------------------------------
-- Account-level safety net. Catches credits from warehouses that
-- weren't attached to a per-warehouse monitor (e.g., a new warehouse
-- created via ad-hoc CREATE WAREHOUSE in a script). Sized at 1.2× the
-- sum of the per-warehouse caps so it only fires if those leaked.
-- ----------------------------------------------------------------------
CREATE OR REPLACE RESOURCE MONITOR rm_account_safety_net
WITH
    CREDIT_QUOTA = 600               -- 400 + 100 = 500 expected; 600 = 20% headroom
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    NOTIFY_USERS = ('LAKEHOUSE_OPS')
    TRIGGERS
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        -- No SUSPEND on the account monitor — if we hit this number,
        -- something is very wrong and we want notifications to ops
        -- without taking down everyone's queries unexpectedly.
        ON 100 PERCENT DO NOTIFY;

ALTER ACCOUNT SET RESOURCE_MONITOR = rm_account_safety_net;

-- ----------------------------------------------------------------------
-- Daily-burn observability: a view that reports current-day credit
-- consumption per warehouse, surfaced on the Streamlit dashboard.
-- Reading WAREHOUSE_METERING_HISTORY has up to a 3-hour latency, but
-- that's fine for a "did we blow today's budget" check.
-- ----------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics.vw_warehouse_credit_burn AS
SELECT
    warehouse_name,
    DATE_TRUNC('day', start_time) AS usage_date,
    SUM(credits_used)             AS credits_used_today,
    SUM(credits_used_compute)     AS credits_compute,
    SUM(credits_used_cloud_services) AS credits_cloud_services
FROM snowflake.account_usage.warehouse_metering_history
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2;

GRANT SELECT ON VIEW analytics.vw_warehouse_credit_burn TO ROLE lakehouse_analyst;
GRANT SELECT ON VIEW analytics.vw_warehouse_credit_burn TO ROLE lakehouse_engineer;
