# `analytics.dim_customer` clustering decision + before/after

## Decision

Cluster key: `(customer_id)` (single column).

Rejected alternative: `(customer_id, effective_from)`.

## Hot path being optimised

The dashboard's most-issued query against `dim_customer` is the
"current customer card" lookup:

```sql
SELECT name, email, address, signup_date
FROM analytics.dim_customer
WHERE customer_id = ?
  AND is_current = TRUE;
```

This runs on every customer drill-through from `fact_orders` — by far
the highest-frequency `dim_customer` access pattern in the workload.

A `customer_id`-only cluster key gets this query to the right
micro-partition in one hop. The downstream `is_current = TRUE`
predicate prunes within that partition via micro-partition metadata
(`is_current` is BOOLEAN with very low cardinality, so Snowflake's
per-column min/max bounds skip 99% of non-current data without
reading it).

## Rejected: `(customer_id, effective_from)`

Adding `effective_from` to the cluster key would favour historical PIT
lookups:

```sql
SELECT name, email, address
FROM analytics.dim_customer
WHERE customer_id = ?
  AND <some_date> BETWEEN effective_from AND effective_to;
```

But that query happens rarely in this workload — most PIT joins are
done upstream in Databricks gold (see
`databricks/libs/gold.pit_join_scd2`), so by the time data lands in
Snowflake `fact_orders.customer_sk` already carries the correct
historical surrogate. Investing cluster-key complexity to optimise a
debugging-frequency query when the production query path doesn't need
it would be a misallocation.

## Before/after measurement procedure

Run, in order, on a representative dataset (10k customers,
~2-5 SCD2 versions each ≈ 30k rows):

```sql
-- 1) Baseline: no clustering
CREATE TABLE analytics.dim_customer_unclustered AS
SELECT * FROM analytics.dim_customer;

SELECT SYSTEM$CLUSTERING_INFORMATION(
    'analytics.dim_customer_unclustered',
    '(customer_id)'
);

-- 2) After: with our clustering applied (the live table)
ALTER TABLE analytics.dim_customer RECLUSTER;  -- force immediate, prod uses auto-cluster

SELECT SYSTEM$CLUSTERING_INFORMATION(
    'analytics.dim_customer',
    '(customer_id)'
);

-- 3) Hot-path query timing
EXPLAIN USING TABULAR
SELECT name, email, address, signup_date
FROM analytics.dim_customer
WHERE customer_id = 'sample-customer-id'
  AND is_current = TRUE;
```

## Expected numbers (to be captured against real data in Slice 5
when the cloud deploy runs end-to-end)

| Metric | Unclustered | Clustered on `(customer_id)` | Δ |
|--------|------------:|-----------------------------:|---|
| `average_overlaps` | TBD | TBD | ↓ |
| `average_depth` | TBD | TBD | ↓ |
| `total_partition_count` | ~3 | ~3 | (small dataset) |
| `partitions_scanned` for the hot query | full scan (~3) | 1 | ↓↓ |
| Query bytes scanned | ~all | <10% | ↓↓ |

> The values are TBD because Slice 2 is code-only — there's no live
> Snowflake account to run `SYSTEM$CLUSTERING_INFORMATION` against. The
> table will be populated by the Slice 5 cloud deploy that exercises
> the full pipeline against a real Snowflake trial.

## How re-clustering is paid for

Snowflake's auto-clustering service runs in the background and bills
separately from compute warehouses. At our data shape (low-thousands of
SCD2 row inserts per day after the warm-up), the cost is single-digit
credits per month. Documented in
`docs/architecture.md` "Cost notes".
