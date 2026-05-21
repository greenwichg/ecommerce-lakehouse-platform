# Architecture decisions

This document covers the "why" behind the platform's shape. The "what"
lives in [`README.md`](../README.md) and the runbook in
[`runbook.md`](runbook.md).

Decisions are presented as "X was chosen over Y because Z". When two
options were roughly even, the trade-off is named explicitly so a future
reader knows the choice was deliberate, not accidental.

## Why Databricks for transformation, Snowflake for serving

**Databricks (medallion) owns transformation.** It's the right tool for
the heavy lifting:

- Schema evolution via Auto Loader's `cloudFiles` source absorbs new
  columns automatically. Same trick in Snowflake requires bespoke
  scripts.
- MERGE on Delta with predicate-guarded updates is concise and ACID; the
  same in Snowflake works but Snowflake credits are 3-5x the cost per
  CPU-second for batch transform.
- Spark's window functions are the natural fit for sessionization and
  deduplication.
- We keep raw + bronze + silver + gold in S3-backed Delta, so storage is
  cheap and the lake is the source of truth.

**Snowflake owns serving.** It's the right tool for BI:

- BI tools (Streamlit, Tableau, Looker) have first-class Snowflake
  connectors; Delta-direct serving from BI is improving but still
  awkward.
- Snowflake's micro-partitioning + clustering gives sub-second
  point-lookups on the dashboard's hot queries.
- RBAC on schemas + masked views is well-trodden; equivalent in
  Databricks via Unity Catalog works but is one more system to learn.
- Resource monitors with auto-suspend keep idle cost near zero.

**Considered and rejected**: single-platform variants.

- *Databricks-only with SQL Warehouses for serving*: works, but the
  dashboard latency story (3-5s cold start) is worse than Snowflake's
  warm XS warehouse. Snowflake also wins on per-user concurrency.
- *Snowflake-only (Snowpipe + Streams + Tasks for transformation)*:
  works for simpler shapes; the medallion-with-quarantine flow we want
  becomes awkward, and Snowpark Python's debugging story is much weaker
  than chispa + local Spark.

## Why a medallion architecture

We could have collapsed Bronze and Silver, or Silver and Gold. We didn't:

- **Bronze is append-only and minimally transformed.** That makes it a
  *literal* record of what arrived from the source — every row keeps
  `_source_file` and `_batch_id`, and we can always re-derive Silver if
  the dedup rules change. Without Bronze, a bug in Silver loses raw
  data forever.
- **Silver is where DQ rules live.** Quarantine flows out, dedup'd
  current-state flows in. This is the layer dashboards must not query
  directly (no surrogate keys, no business logic) but it's the layer
  data engineers do most of their work on.
- **Gold is shaped for BI.** Star schema, surrogate keys, pre-aggregated
  marts. Separating it from Silver lets us redesign one without breaking
  the other.

**Trade-off accepted**: three layers means 3x the storage. With Delta's
file-level deduplication and S3's tiered storage (Glacier after 90 days,
configured in Slice 5 Terraform), the cost is single-digit dollars per
TB-month at our volume — well worth the operational clarity.

## Why Airflow (Slice 5 — included here for completeness)

The orchestration choice between Airflow and Step Functions deserves a
brief note even though the Step Functions piece lands in Slice 5:

- **Airflow** for the daily/hourly/weekly batch DAGs. The deferrable
  operator pattern lets a sensor wait for hours without holding a worker
  slot, and the dynamic task mapping (Slice 2) is exactly the right
  shape for "one task per source table". Python-defined DAGs version
  alongside the transformation code.
- **Step Functions** for the *disaster recovery / replay* workflow.
  Replays are infrequent, run from a fresh AWS Lambda, and have no
  scheduling needs — SFN's pay-per-transition model + visual workflow
  beats Airflow's "always-on scheduler" cost shape for this.

The point of the split is to show we know *when* to use each tool, not
that one is universally better.

## Slice 1 design decisions

### Generator semantics: "1000/day" applies to *new* placed records

The spec says "orders: ~1000/day" with "2% late-arriving records" and
"1% updates to existing orders". A literal reading — 1000 records total,
of which 10 are status updates — leaves the accumulating snapshot 99%
empty (orders never progress past placed). That makes the entire
"accumulating snapshot fact" exercise pointless.

Decision: interpret 1000/day as *new placed records per day*. Natural
status transitions for orders from prior days are emitted *in addition*,
based on each order's deterministic trajectory. Per-day file volume
ramps from 1000 (day 1) to ~2000-3000 once enough orders are in flight.
The 2% late-arrival and 1% same-day-update knobs still apply against
the 1000 new-placed baseline.

Documented in `generators/orders.py` module docstring and
[runbook §"Generator semantics"](runbook.md#generator-semantics).

### Cancellation snapshots preserve earlier milestone timestamps

A bug in the first cut of `_snapshot()` nulled `paid_at`/`shipped_at` on
cancellation snapshots even when the order had progressed past those
stages. The silver MERGE (which replaces target columns from source)
would have erased those timestamps the moment a cancellation arrived,
breaking the accumulating snapshot.

Fix: `_snapshot(spec, status="cancelled", ...)` carries through
`spec.paid_at` / `spec.shipped_at`, which the spec only populates when
the order actually reached those stages. Caught while designing the
silver MERGE, fixed in commit `d622fcc` with a forced-cancellation test
that proves the preservation works end-to-end through gold.

### MERGE predicate `s.updated_at > t.updated_at` everywhere

Silver, Gold, and Snowflake all merge with the same predicate: only
update target when source has a strictly newer `updated_at`. This means
late-arriving "placed" records can't erase later "paid" milestones — a
real failure mode for any system with multiple producers or retries.

The predicate has one consequence worth flagging: an idempotent re-run
of the *same* batch produces zero updates (source and target have equal
timestamps). That's deliberate — re-running yesterday's batch should be
a no-op, not a regression. Re-runs that *should* take effect (because
upstream changed) come with a new batch_id and naturally newer
`_loaded_at` / `updated_at`.

### Bronze writes are streaming + `availableNow=True`

Auto Loader semantics with a `trigger(availableNow=True)` setting means
each Airflow-triggered run processes all currently-available raw files
and stops, but reuses the streaming checkpoint machinery to track which
files have been processed. This gives us "batch with exactly-once
file processing" without writing our own watermark file.

### Surrogate keys via SHA-256

`order_sk = sha256(order_id)` rather than an auto-incrementing INT
because:

- Deterministic across machines / time / clusters — a re-run produces
  the same surrogate, so caching and incremental loads stay stable.
- No central sequence to coordinate — important once Silver + Gold can
  run on different clusters.
- 256 bits is overkill for collision resistance, but cheap on Delta's
  string-encoded format.

Trade-off: SHA-256 hex strings are 64 bytes vs an INT8's 8 bytes.
Acceptable: Delta's columnar storage compresses them efficiently, and
joins are still O(1) with dictionary encoding.

`customer_sk` / `product_sk` are reserved as nullable BIGINT placeholders
in Slice 1, filled in by Slice 2 dim lookups. The schema is locked in
now so downstream consumers don't break when those columns populate.

### Clustering decisions (Snowflake)

| Table | Cluster key | Why |
|-------|-------------|-----|
| `analytics.fact_orders` | `(DATE_TRUNC('DAY', created_at), status)` | Dashboard's hottest query is "last N days by status"; status co-clustered for funnel filters. |
| `analytics.fact_order_lifecycle` | `(DATE_TRUNC('DAY', placed_at))` | Cycle-time analytics filter is always "orders placed between X and Y". |

`SYSTEM$CLUSTERING_INFORMATION` before/after numbers will be added once
real data is loaded (out of scope for this code-only slice — see Slice 5
for the real-cloud deployment path).

### Why `importmode=importlib` and a root `conftest.py`

Pytest's default import mode walks up looking for `__init__.py` files,
which creates module-name collisions when we have parallel test trees
(`tests/`, `databricks/tests/`, `orchestration/tests/`). Switching to
`importlib` mode avoids the collision but leaves a different problem:
in some test-ordering scenarios involving Airflow imports, sys.path
entries get dropped between conftest load and test collection.

The fix in the root `conftest.py` eagerly imports `generators` and
`libs` so they're cached in `sys.modules` before any test file's
imports run. Documented in the conftest itself.

## Cost notes

Per-component, at our slice-1 scale (~1000 orders/day → ~2000-3000
silver writes/day):

| Component | Estimated monthly cost (USD) | Notes |
|-----------|-----------------------------|-------|
| S3 (raw + bronze + silver + gold + checkpoints) | $2-5 | Standard for 90d, then Glacier (Slice 5 lifecycle). |
| Databricks (job clusters, ~10 min/day) | $10-30 | XS cluster, spot pricing. |
| Snowflake (XS warehouse, ~5 min/day) | $5-15 | 60s auto-suspend keeps idle cost near zero. |
| Airflow (managed MWAA `mw1.small`) | $400-450 | The big-ticket item. Self-hosting on EC2 is ~$30/mo if cost is a hard constraint. |
| SNS / CloudWatch / Lambda | <$1 | |

Recommended path for portfolio / demo: self-hosted Airflow on a single
EC2 `t3.small` brings the monthly total to <$60.
