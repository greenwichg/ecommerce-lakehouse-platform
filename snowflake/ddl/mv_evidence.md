# Materialized view rationale: `mv_daily_revenue_by_category`

## Choice: MV over plain table + clustering

The hot query is the BI dashboard's "revenue trendline by category for
last 30 days":

```sql
SELECT revenue_date, category, revenue_usd, order_count
FROM analytics.mv_daily_revenue_by_category
WHERE revenue_date >= CURRENT_DATE - 30
ORDER BY revenue_date, category;
```

| | MV (chosen) | Plain table + clustering |
|---|---|---|
| Hot query: "30d × 10 categories" | Scan ~300 rows, no join | Scan ~30k order rows + GROUP BY |
| Refresh cost | Incremental on micro-partition changes | Full Airflow task on schedule |
| Freshness lag from new order insert | Seconds | Whatever Airflow schedules |
| Maintenance burden | None (Snowflake-managed) | One more Airflow task to monitor |
| Trade-off | Adds `category` column to `fact_orders` (denormalisation) | None at the schema level |

At our volume (1000 orders/day → 365k rows/year) the scan-and-group path
takes 100-500ms cold. With the MV it's <100ms warm. Either is fine for a
human dashboard, but the MV wins on the freshness story — analyst
opening the dashboard sees the orders that landed 30 seconds ago, not
30 minutes ago.

## Why the denormalisation is justified

Snowflake materialised views have hard restrictions:

- **Single base table only — no joins**
- No HAVING, subqueries, window functions
- No GROUPING SETS / CUBE / ROLLUP
- Limited aggregate functions
- No `current_date()` / `current_timestamp()` in the body

This kills the naive design (`MV ON fact_orders JOIN dim_product`). The
workaround: copy `category` into `fact_orders` during the Databricks
gold build, then MV becomes a single-table GROUP BY. The
denormalisation cost is one VARCHAR(50) column on every order — about
50 bytes uncompressed per row, dictionary-encoded down to a few bytes
in practice.

The PIT-correctness argument actually *strengthens* the denormalisation:
`category` snapshotted at order placement time is more correct than a
current-category join would be for historical revenue analytics. If a
product moves from "electronics" to "automotive" in May, May's revenue
should still aggregate under "electronics" — which the PIT snapshot
gives us automatically.

## Rejected alternatives

1. **Wide `fact_orders_summary` table populated by a separate Snowflake task**.
   Would let the MV stay strict-star-schema, but adds another loadable
   table to maintain and another freshness lag. Not worth it for one
   denormalised column.

2. **Regular view + result cache**. Snowflake's result cache holds query
   results for 24h and is very effective for repeated queries. But it
   invalidates on every base-table write, so the dashboard would see
   miss-rates throughout the day. MV's incremental refresh is better.

3. **MV on `customer_ltv`-style aggregate**. `customer_ltv` uses LAG()
   and window-function-derived metrics that Snowflake MVs don't support.
   Documented in `libs.marts` why it stays a regular table in Databricks
   Gold.

## Refresh expectations

Snowflake auto-refreshes MVs in the background. Visible lag is "seconds
behind the latest base-table insert" in practice. The
`SYSTEM$ESTIMATE_MV_REFRESH_BENEFIT` function can quantify the speedup
for any candidate query; documented in `docs/runbook.md` under
"MV operations".

## What invalidates the choice

If the dashboard's query pattern shifted to needing joins across multiple
facts (e.g., "category revenue minus refunds" where refunds live in a
separate fact), we'd need to either:

- Add the refund signal to `fact_orders` (deeper denormalisation)
- Drop the MV and accept the per-query join cost
- Build a wide gold table in Databricks

We pick the first option only as long as the denormalisation column
count stays in the single digits.
