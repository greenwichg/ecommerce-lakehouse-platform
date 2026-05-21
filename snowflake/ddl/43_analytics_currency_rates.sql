-- Currency rates reference table.
-- Clustering: (rate_date) — the dashboard's hot path is "show me last 30d
-- of EUR rates" which filters by date range. target_currency is also a
-- common filter but its cardinality (10) makes Snowflake's auto-pruning
-- on the column statistics sufficient without explicit clustering.

USE ROLE lakehouse_engineer;
USE DATABASE {{ params.SNOWFLAKE_DATABASE }};
USE SCHEMA analytics;

CREATE TABLE IF NOT EXISTS currency_rates (
    rate_date DATE NOT NULL,
    base_currency VARCHAR(3) NOT NULL,
    target_currency VARCHAR(3) NOT NULL,
    rate NUMBER(18, 6) NOT NULL,
    -- _source = 'api' or 'simulated'. Lets analysts filter out fallback
    -- rates for "strict provenance" reports, and lets the freshness gate
    -- compute %-simulated metrics.
    _source VARCHAR(16) NOT NULL,
    _fetched_at TIMESTAMP_TZ,
    _loaded_at TIMESTAMP_TZ,
    _source_batch_id VARCHAR(64),
    CONSTRAINT pk_currency_rates PRIMARY KEY (rate_date, target_currency)
)
CLUSTER BY (rate_date)
COMMENT = 'Daily exchange rates from USD. _source distinguishes API-fetched from simulated.';
