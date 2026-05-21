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

---

## Slice 3 additions

### Sessionization: gap-and-island on `raw_session_id`

The clickstream generator emits a `session_id` field that represents
the upstream tracker's cookie identity. One cookie can host many
sessions over time, separated by >30 minutes of inactivity. Silver's
sessionizer recomputes the *session* identity:

```
silver_session_key = sha256(raw_session_id || '|' || session_seq)
```

`session_seq` is the cumulative count of "new session" boundaries
within a cookie (gap > 30 min OR first event), computed via window-
function lag + cumulative sum. The textbook gap-and-island pattern.

Why not Spark Structured Streaming `session_window`: our Silver runs
batch (`availableNow=True`), not streaming. Mixing modes adds confusion
without gain. Gap-and-island ports cleanly to dbt / Snowflake Tasks if
we ever want to push the work down later.

Customer attribution uses `max_by(customer_id, IF(customer_id IS NOT
NULL, event_ts))` — the rank-by-null trick (Spark `max_by` ignores NULL
keys, not NULL values, so a trailing anonymous event after a sign-in
would blank attribution under naive `max_by(customer_id, event_ts)`).
Caught during design; documented inline in `libs/sessionize.py`.

### Watermark in batch mode: coordination boundary, not filter

The "10-minute watermark" for clickstream is **not** a current-batch
filter that drops late events. In batch mode (`availableNow=True`),
every event in the current bronze batch must be processed.

The 10-minute window is a *next-batch coordination boundary*: any
session whose end is within 10 minutes of `max(event_ts)` may still
grow in the next batch (events from this batch's tail-end could land in
the next batch and extend the same session). The Silver MERGE handles
that naturally via newer-`session_end`-wins.

This semantic mismatch with streaming watermarks bit us during design.
Documented in `silver_clickstream.py`'s top-of-file docstring so the
next maintainer doesn't accidentally add an `apply_watermark()` call
that would drop legitimate events.

### Snowpipe + Streams + Tasks: parallel to the Databricks PIT join

The Snowflake-side flow for `fact_sessions` intentionally mirrors the
Databricks-side PIT join used for `fact_orders` (`libs/gold.pit_join_scd2`).
Both layers do "fact → dim PIT binding" enrichment; having one in each
platform lets reviewers compare the two flavors:

| | Databricks (`fact_orders`) | Snowflake (`fact_sessions`) |
|---|---|---|
| Engine | Spark | Snowflake |
| API | DataFrame + `broadcast(dim)` | SQL + range JOIN on `effective_from`/`effective_to` |
| Trigger | Airflow Databricks job (scheduled) | Snowflake Task (5-min schedule) |
| Cost control | Job cluster spot price | `WHEN SYSTEM$STREAM_HAS_DATA(...)` makes the Task a no-op when idle |

The cost-aware `WHEN` clause is the key Snowflake cost pattern — without
it, the task would warm the warehouse every 5 minutes regardless of
whether new data arrived. Standard Snowflake idiom; signals cost
awareness in interviews.

### Dataset triggering: separate consumer DAG, not hybrid scheduling

`hourly_clickstream_pipeline` publishes `DATASET_FACT_SESSIONS` on
success. A separate `dashboard_refresh` DAG consumes it via
`schedule=[DATASET_FACT_SESSIONS]`.

We **deliberately did not** put the Dataset on the daily DAG's schedule
(via `DatasetOrTimeSchedule`). That would have fired the daily 24× per
day (once per hourly publish) plus its 02:00 cron run — wasted
Snowflake credits for full SCD2 MERGEs that don't need hourly cadence.

**Pattern**:
- **Cron for source-of-truth refreshes** (the daily DAG).
- **Dataset triggering for downstream consumers** that only need to
  react to fresh upstream signal (the dashboard refresh).

Each DAG has one job; failures are isolated; SLAs are meaningful.

---

## Slice 4 additions

### Snowflake MV restrictions + denormalisation workaround

Snowflake materialised views are stricter than the docs make obvious:
**single base table only, no joins, no window functions, limited
aggregates**. The natural target (`mv ON fact_orders × dim_product`)
isn't possible. The chosen workaround: copy `category` into
`fact_orders` during the Databricks gold build (via
`pit_join_scd2(extra_cols=["category"])`), making the MV a single-table
GROUP BY.

PIT-correctness of the denorm is actually a *feature*: `category` is
snapshotted at order-placement time, so a later product re-categorisation
doesn't retroactively rewrite historical revenue. Matches the same
PIT-on-`created_at` semantic we use for customer attribution.

Full walkthrough in `snowflake/ddl/mv_evidence.md`.

### `customer_ltv` lives in Databricks Gold (not MV)

`avg_days_between_orders` uses `LAG()` over a per-customer order
timeline. Snowflake MVs disqualify window functions; so the mart lives
in Databricks Gold and Snowflake loads it via TRUNCATE+COPY swap.

Tradeoff documented: Snowflake MV auto-refreshes incrementally on base
table change; the Databricks mart refreshes once per daily DAG run.
For "customer 360" point-lookups by `customer_sk` (the dashboard's
actual access pattern), the freshness lag is acceptable — we don't
need < 30-second LTV updates.

### Currency `_source` provenance + freshness gate

The currency_rates generator emits a per-row `_source` column with one
of `'api'` (live exchangerate.host fetch) or `'simulated'` (deterministic
fallback when the API was unavailable). It propagates through
Bronze → Silver → Gold → Snowflake unchanged.

The Airflow `dq_currency_freshness` gate (in `dq_gates.py`) hard-fails
the daily DAG when:

1. Today's rates table is empty (revenue conversion would yield NULL)
2. More than 50% of today's rates are simulated (API has been down for
   most of the fetch window — revenue numbers shouldn't be quoted as
   authoritative until investigated)

Critical observability detail: without `_source` we couldn't
distinguish "no rates loaded" from "rates loaded but all fallback"; both
look identical from a row-count perspective. The provenance column
lets the gate fire on the second case before the dashboard reports it.

### Factless fact at per-event grain

`fact_customer_wishlist_product` keeps the per-event grain
`(customer, product, added_at)` rather than per-relationship. Re-adds
(same customer adding the same product after removing it) carry signal
worth preserving. The trade-off (DISTINCT needed for "is this
relationship currently active?" queries) is the canonical factless-fact
ergonomic and matches Kimball's textbook treatment of events.
