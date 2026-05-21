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
COMMENT = 'BI warehouse for Streamlit + analyst ad-hoc; separate from ETL so a runaway query cannot starve batch.';

GRANT USAGE ON WAREHOUSE lakehouse_bi_wh TO ROLE lakehouse_analyst;
GRANT OPERATE ON WAREHOUSE lakehouse_bi_wh TO ROLE lakehouse_engineer;

-- ----------------------------------------------------------------------
-- ETL warehouse monitor: 400 credits/month. NOTIFY at 75/90 give ops
-- time to investigate before the hard SUSPEND at 100% (in-flight
-- queries finish) and SUSPEND_IMMEDIATE at 110% (something is very
-- wrong, abort everything).
-- ----------------------------------------------------------------------
-- noqa: disable=all
CREATE OR REPLACE RESOURCE MONITOR rm_etl_wh
WITH
    CREDIT_QUOTA = 400
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    NOTIFY_USERS = ('LAKEHOUSE_OPS')
    TRIGGERS
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND
        ON 110 PERCENT DO SUSPEND_IMMEDIATE;
-- noqa: enable=all

ALTER WAREHOUSE {{ params.SNOWFLAKE_WAREHOUSE }} SET RESOURCE_MONITOR = rm_etl_wh;

-- Snowflake resource monitors are 1:1 with warehouses, so we can't
-- attach BOTH a daily and a monthly monitor to the same warehouse.
-- The account-level monitor (below) gives us a coarser daily safety
-- net that covers all warehouses at once.

-- ----------------------------------------------------------------------
-- BI warehouse monitor: smaller cap since analyst usage is sporadic.
-- ----------------------------------------------------------------------
-- noqa: disable=all
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
-- noqa: enable=all

ALTER WAREHOUSE lakehouse_bi_wh SET RESOURCE_MONITOR = rm_bi_wh;

-- ----------------------------------------------------------------------
-- Account-level safety net. Catches credits from warehouses that
-- weren't attached to a per-warehouse monitor (e.g., a new warehouse
-- created via ad-hoc CREATE WAREHOUSE in a script). Sized at 1.2× the
-- sum of the per-warehouse caps so it only fires if those leaked.
-- ----------------------------------------------------------------------
-- noqa: disable=all
-- Note: 600 credits = 400 + 100 = 500 expected + 20% headroom. No
-- SUSPEND action on the account monitor — if we hit this we want
-- notifications, not for ops queries to be cut off mid-investigation.
CREATE OR REPLACE RESOURCE MONITOR rm_account_safety_net
WITH
    CREDIT_QUOTA = 600
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    NOTIFY_USERS = ('LAKEHOUSE_OPS')
    TRIGGERS
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO NOTIFY;
-- noqa: enable=all

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
