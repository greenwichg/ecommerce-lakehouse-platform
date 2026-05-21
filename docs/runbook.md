# Operational runbook

Practical procedures for running the platform: failure modes, backfill,
replay, escalation. Pair with [`architecture.md`](architecture.md) for
the "why".

## SLA / SLO targets

| Pipeline | Schedule | SLA | SLO (rolling 7d) |
|----------|----------|-----|-------------------|
| `daily_batch_pipeline` | 02:00 UTC daily | 4h end-to-end | 99% on-time, 99.5% data correctness |
| `hourly_clickstream_pipeline` | hourly (Slice 3) | 15min end-to-end | 99% on-time |
| `weekly_maintenance` | Sunday 04:00 UTC (Slice 5) | 2h | 100% completion |

Data correctness = (rows passing all DQ rules) / (rows ingested). Tracked
via the `_quarantine_reason` column on the quarantine Delta tables.

## Generator semantics

Read this once before debugging row counts. The generators document an
intentional design choice that's easy to misinterpret.

- The spec headline "~1000/day" applies to **new placed records per
  day**. A given day's raw/orders file also contains:
  - Natural status transitions for orders placed in prior days
    (paid/shipped/delivered/cancelled), based on each order's
    deterministic trajectory.
  - ~2% late-arriving placed records (placed_at < partition date).
  - ~1% same-day updates (orders placed and paid on the same day).
- After ~14 days of steady-state generation, expect 2000–3000 records
  per day total. Sliced by status, roughly:
  - ~1000 placed
  - ~700 paid (from D-1)
  - ~500 shipped (from D-3)
  - ~300 delivered (from D-7)
  - ~50 cancelled (across stages)
- This isn't a bug. Trying to enforce "exactly 1000 total" by sampling
  transitions strips the accumulating-snapshot fact of useful signal —
  most orders would never show paid/shipped/delivered timestamps.

See `generators/orders.py` module docstring for the formal description
and `docs/architecture.md` §"Generator semantics" for the rationale.

## Running end-to-end locally

Without any AWS / Databricks / Snowflake access:

```bash
# 1) Install
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev,spark]"
export PROJECT_ROOT="$(pwd)"
export LAKEHOUSE_ENV=dev
export JAVA_HOME=/path/to/java17    # PySpark 3.5 needs Java 17

# 2) Generate 7 days of orders → local file://data/raw/orders/
python -m generators.orders --start-date 2025-05-01 --end-date 2025-05-07

# 3) Run the Spark transformations locally (the notebook files are
#    Databricks-notebook-style Python; the libs/ functions they use are
#    plain-vanilla PySpark and run anywhere).
python -m pytest databricks/tests -q     # confirms transforms work
```

The Databricks notebooks themselves require a Databricks workspace to
run (they use `dbutils.widgets` and the cloudFiles source). For local
end-to-end execution, port the notebook bodies into a Python script that
calls the same `libs.bronze` / `libs.silver` / `libs.gold` functions on
a static Delta read.

## Running end-to-end in cloud

1. Apply Terraform from `infrastructure/terraform/` (Slice 5). This
   provisions: S3 buckets, IAM roles, Secrets Manager entries, SNS
   topics, CloudWatch log groups, Lambda validator.
2. Upload `databricks/notebooks/` to Databricks Repos + create jobs
   pointing at each notebook. Capture the job IDs.
3. Run `snowflake/ddl/*.sql` against the Snowflake account (via
   schemachange or psql). Set `&{SNOWFLAKE_DATABASE}` /
   `&{SNOWFLAKE_WAREHOUSE}` / `&{LAKEHOUSE_BUCKET}` /
   `&{S3_INTEGRATION_ROLE_ARN}` per environment.
4. Configure Airflow Variables:
   - `lakehouse_env` = `dev` or `prod`
   - `lakehouse_s3_bucket` = the bucket from Terraform
   - `databricks_job_bronze_orders`, `..._silver_orders`, ...,
     `..._gold_fact_order_lifecycle` = job IDs from step 2
   - `snowflake_database`, `snowflake_warehouse` = matching the Snowflake DDL
   - `sns_alert_topic_arn` = topic ARN from Terraform
5. Configure Airflow Connections (via Secrets Manager backend in prod):
   - `aws_default` (auto-discovered if running in EKS/MWAA with IAM role)
   - `databricks_default` (host + PAT)
   - `snowflake_default` (account/user/warehouse/role/private key)
6. Unpause `daily_batch_pipeline` in the Airflow UI. First run is
   triggered manually to verify end-to-end; subsequent runs go on
   schedule.

## Backfill procedure

To re-process a date range (e.g., recover from a Bronze schema
mismatch fixed via redeploy):

1. **Re-generate raw data** for the date range. The generators are
   idempotent (same args → same output), so this is safe:

   ```bash
   python -m generators.orders --start-date 2025-05-01 --end-date 2025-05-07
   ```

2. **Trigger Airflow backfill** for the DAG over the same date range:

   ```bash
   airflow dags backfill -s 2025-05-01 -e 2025-05-07 \
       --reset-dagruns daily_batch_pipeline
   ```

3. The Silver/Gold/Snowflake MERGEs are idempotent: re-runs with newer
   `updated_at` overwrite; re-runs with equal-or-older `updated_at` no-op.
   Total data should be byte-equivalent after the backfill completes.

4. **Verify** with the SQL tests in `snowflake/tests/`:

   ```bash
   for f in snowflake/tests/*.sql; do
       snowsql -o output_format=csv -f "$f" -v SNOWFLAKE_DATABASE=$SNOWFLAKE_DATABASE
   done
   # Any non-zero row output is a violation.
   ```

## Replaying a failed run

Three modes depending on where the failure landed:

### A. Bronze failed (Auto Loader rejected file)

Symptom: `bronze.bronze_orders` task fails with a
`PERMISSION_DENIED` / `MALFORMED_FILE` error.

1. Check the file in S3:
   ```bash
   aws s3 ls s3://<bucket>/raw/orders/year=YYYY/month=MM/day=DD/
   ```
2. If the file is corrupted upstream, route it to quarantine and
   regenerate:
   ```bash
   aws s3 mv s3://<bucket>/raw/orders/year=YYYY/month=MM/day=DD/orders.parquet \
             s3://<bucket>/quarantine/raw/orders/...
   python -m generators.orders --start-date YYYY-MM-DD --end-date YYYY-MM-DD
   ```
3. Clear the Airflow task state and retry from the failed task:
   ```bash
   airflow tasks clear daily_batch_pipeline -t 'bronze.bronze_orders' \
       --start-date YYYY-MM-DD --end-date YYYY-MM-DD
   ```

### B. Silver failed (DQ gate quarantined too much)

Symptom: `dq_gate_fact_orders` raises `AirflowFailException` with
`Row-count drop X% exceeds threshold 20%`.

1. Inspect the quarantine table:
   ```sql
   SELECT _quarantine_reason, COUNT(*) FROM quarantine.orders
   WHERE _ingestion_timestamp >= CURRENT_DATE - 1
   GROUP BY 1 ORDER BY 2 DESC;
   ```
2. If the reasons are legitimate (e.g., upstream produced bad data), the
   gate is doing its job. Resolve upstream then re-run.
3. If the reasons are a DQ rule bug (false positives), fix the rule in
   `databricks/libs/quality.py` → ORDERS_RULES, deploy, and clear the
   failed task.

### C. Snowflake load failed

Symptom: `snowflake_load_orders` task fails with
`STORAGE_INTEGRATION` / `STAGE` / network error.

1. Test the stage:
   ```sql
   LIST @raw.gold_fact_orders_stage;
   ```
2. If the stage URL is wrong, check the storage integration
   (`DESC INTEGRATION lakehouse_s3_integration`) and recreate the
   Terraform IAM role if needed.
3. The MERGE is idempotent (newer-wins predicate), so simply retrying
   the task after fixing the issue is safe.

## Demo failure scenario: bad file → quarantine → alert → replay

This is the end-to-end "operational drill" we exercise in CI:

1. **Inject a bad file.** Write a Parquet file with `quantity = -1` to
   today's raw/orders partition (violates `quantity_positive` rule).
2. **Pipeline runs.** Bronze ingests cleanly (append-only, no rules).
   Silver's `split_quarantine` routes the bad row to
   `quarantine.orders` with `_quarantine_reason = "quantity_positive"`.
   Gold sees one fewer row.
3. **DQ gate may fire.** If the drop exceeds 20% (small batches do),
   `dq_gate_fact_orders` raises `AirflowFailException` →
   `on_failure_callback` publishes to SNS → email/Slack alert.
4. **Operator inspects.** `SELECT * FROM quarantine.orders WHERE
   _quarantine_reason = 'quantity_positive'` shows the offending row.
5. **Replay.** Fix upstream (regenerate the offending day's file), clear
   the failed Airflow tasks from `bronze` onwards, let the DAG retry.

Reproducible smoke test (Slice 5 will codify this in a CI job):

```bash
# 1) Inject
python scripts/inject_bad_quantity.py --date 2025-05-07

# 2) Run pipeline
airflow dags trigger daily_batch_pipeline --conf '{"date":"2025-05-07"}'

# 3) Assert quarantine grew
snowsql -q "SELECT COUNT(*) FROM quarantine.orders WHERE _quarantine_reason='quantity_positive'"
# Expect: 1 (or more)

# 4) Assert DQ gate behavior
# Slice 1 with small batches: gate fires. Slice 2+ at full volume: gate passes
# but the quarantine row count is still surfaced in monitoring.
```

(`scripts/inject_bad_quantity.py` lands in Slice 5 alongside the CI
job that exercises it.)

## Escalation paths

| Symptom | First responder | Escalate to | After |
|---------|-----------------|-------------|-------|
| DAG fails, on-call alert in Slack | data-platform on-call | platform lead | 30 min unresolved |
| DQ gate fires repeatedly (>2 days) | data-platform | data-product owner | same day |
| Snowflake bill spike (>20% week-over-week) | data-platform | finance + platform lead | within 24h |
| Databricks job stuck >2 SLA hours | data-platform on-call | Databricks support + platform lead | immediately |
| Quarantine table grows >1% of bronze | data-platform | source-system owner | within 4h |

On-call rotation lives in the team Confluence page; alerts land in
`#data-platform-oncall` Slack channel.

## Databricks Community Edition limitations

If running this against Databricks Community Edition (free tier) instead
of a full workspace:

- **No Unity Catalog.** The 3-part naming in our gold DDL
  (`catalog.schema.table`) doesn't apply; tables live under the
  workspace's default Hive metastore. Update notebooks to use
  `schema.table`.
- **`OPTIMIZE ... ZORDER BY` not supported on CE.** The
  `optimize_zorder()` helper in `libs/gold.py` is a no-op on CE — it
  succeeds silently because the OPTIMIZE statement parses but
  doesn't execute the ZORDER on CE.
- **No streaming auto-loader.** CE doesn't support the cloudFiles
  source. Notebooks that use `spark.readStream.format("cloudFiles")`
  need to fall back to `spark.read.parquet(source_path)` for CE
  testing.
- **No job scheduler.** CE can run notebooks ad-hoc but doesn't
  support the `DatabricksRunNowOperator` flow. Use CE for unit testing
  the libs only.

For full feature exercise, the project assumes a real Databricks
workspace (Standard or Premium tier).

## Clickstream specifics (Slice 3)

### Shared-device limitation

Cookie-based sessionization collapses two physical humans on the same
device + browser into one session. This is the same limitation every
cookie-based sessionizer has (Google Analytics, Adobe, Snowplow). Worth
flagging because:

- Visible in `fact_sessions` as sessions with mid-session
  `attributed_customer_id` changes that aren't sign-in events.
- Not actionable — accept and document.

A future enhancement (Slice 6+) could add a `device_fingerprint` derived
column that splits sessions when fingerprint shifts.

### Snowpipe pipeline status

In production, check Snowpipe + Streams + Tasks health with:

```sql
-- Pipe status (latency, file backlog)
SELECT SYSTEM$PIPE_STATUS('staging.pipe_fact_sessions_ingest');

-- Stream has unprocessed data?
SELECT SYSTEM$STREAM_HAS_DATA('staging.stream_fact_sessions');

-- Task run history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('HOUR', -24, CURRENT_TIMESTAMP()),
    TASK_NAME => 'analytics.enrich_and_merge_fact_sessions'
))
ORDER BY scheduled_time DESC;
```

### Sessionization regression diagnosis

If `fact_sessions.event_count = 1` becomes the modal value, the
sessionizer is treating each event as its own session — likely a bug in
the window function ordering or the gap predicate. Check:

1. `silver_session_key` distribution: should have many keys per cookie
   for active users, not 1:1 with events.
2. Generator's intra-visit gap (test_intra_visit_events_within_30min):
   if events within a visit are spaced > 30 min, every event becomes
   its own session.

### Watermark misconception trap

If you're tempted to add `apply_watermark()` to `silver_clickstream.py`,
read the docstring at the top of that notebook first. Dropping events
from the current batch based on a 10-min watermark would silently
discard data — the watermark is a *next-batch coordination* concept,
not a current-batch filter. Documented because we hit this during
design and the next maintainer probably will too.
