"""Weekly maintenance — Slice 5.

Schedule: Sundays at 04:00 UTC (chosen to avoid the daily batch's
02:00 cron and the hourly clickstream window; Sunday morning is the
quietest BI usage window).

## What this DAG does

Three logical groups, each with its own purpose:

1. **Delta table maintenance** — ``OPTIMIZE`` (compact small files
   from streaming writes) + ``VACUUM`` (remove tombstoned files past
   retention) on every Bronze / Silver / Gold table. Delta accumulates
   thousands of tiny part-files from append-mode writes; left alone
   the metadata snapshot grows until reads slow noticeably. Weekly
   cadence balances compaction cost vs. query latency.

2. **Snowflake reclustering check** — Snowflake auto-clusters, but
   the SYSTEM$CLUSTERING_INFORMATION function reports the depth and
   ratio metrics for our largest fact tables. If a table's clustering
   depth exceeds the threshold we alert; manual REClUSTER is then a
   judgment call (auto-clustering credits vs. query speedup).

3. **Cost report** — pulls the prior week's Snowflake credit usage
   (WAREHOUSE_METERING_HISTORY) AND Databricks DBU usage (stubbed —
   we don't have a Databricks workspace API key wired in for the
   demo), aggregates by warehouse/cluster, and emits a Markdown
   report to the alerts SNS topic so ops can spot week-over-week
   drift.

## Why a separate DAG (not appended to daily)

- Different cadence (weekly vs. daily) and different concurrency
  needs: maintenance can be slow and we don't want it blocking the
  daily SLA.
- Different alerting profile: maintenance failures are advisory
  (the warehouse keeps working), so the on_failure_callback gets a
  lower-severity subject prefix.
- Easier to disable / re-run independently when a Sunday window is
  consumed by a real incident.

## Why include Databricks in the cost report (even stubbed)

The Slice 5 design called out: "Snowflake-only undersells the
split-compute story". Surfacing Databricks DBU spend alongside
Snowflake credit spend is what the lakehouse architecture's whole
sales pitch rests on — cheap object store + expensive compute,
chosen per workload. The Databricks side is stubbed for now (no
live workspace) but the report shape is correct so swapping in real
values is a one-line change.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow.decorators import task
from airflow.models import DAG, Variable
from airflow.utils.task_group import TaskGroup

_DAG_FILE = Path(__file__).resolve()
sys.path.insert(0, str(_DAG_FILE.parent))
from callbacks import sns_failure_callback  # noqa: E402

log = logging.getLogger(__name__)

DAG_ID = "weekly_maintenance"
SCHEDULE = "0 4 * * 0"  # Sundays 04:00 UTC

DATABRICKS_CONN_ID = "databricks_default"
SNOWFLAKE_CONN_ID = "snowflake_default"

# Delta tables to OPTIMIZE + VACUUM. Listing them explicitly (rather
# than discovering at runtime) keeps the DAG graph stable for the UI
# and surfaces additions as code review changes.
DELTA_TABLES = [
    # Bronze
    "bronze.orders",
    "bronze.customers",
    "bronze.products",
    "bronze.clickstream",
    "bronze.currency_rates",
    "bronze.wishlist",
    # Silver
    "silver.orders",
    "silver.customers",
    "silver.products",
    "silver.clickstream",
    "silver.currency_rates",
    "silver.wishlist",
    # Gold
    "gold.dim_customer",
    "gold.dim_product",
    "gold.fact_orders",
    "gold.fact_order_lifecycle",
    "gold.fact_sessions",
    "gold.fact_customer_wishlist_product",
    "gold.currency_rates",
    "gold.customer_ltv",
]

# Snowflake fact tables whose clustering depth we monitor. Dims are
# small enough that clustering rarely helps; the facts are where it
# matters.
CLUSTERED_FACT_TABLES = [
    "analytics.fact_orders",
    "analytics.fact_order_lifecycle",
    "analytics.fact_sessions",
    "analytics.fact_customer_wishlist_product",
]

# Threshold: alert if average clustering depth exceeds this. 4 is the
# Snowflake docs' suggested heuristic for "consider reclustering".
CLUSTERING_DEPTH_ALERT = 4.0


default_args: dict[str, Any] = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": sns_failure_callback,
    # No tight SLA — maintenance can run long; the only hard
    # requirement is that it finish before the next Sunday's run.
}


@task
def optimize_and_vacuum_delta(table: str) -> dict[str, Any]:
    """Run OPTIMIZE + VACUUM on a single Delta table.

    Stubbed for Slice 5 — the real implementation submits a Databricks
    job that runs::

        OPTIMIZE <table> ZORDER BY (<cluster_cols>);
        VACUUM   <table> RETAIN 168 HOURS;  -- 7 days

    The ZORDER columns belong with the table definition (Slice 2-4
    notebooks), so the job uses the cluster keys baked into the
    table's properties.
    """
    log.info("OPTIMIZE + VACUUM on %s (stubbed; would submit Databricks job)", table)
    # The real action: kick off a Databricks job; for code-only we record
    # the intent. Returning a structured result so the cost report can
    # account for hypothetical DBU spend.
    return {"table": table, "optimized": True, "vacuumed": True}


@task
def check_clustering_depth(table: str) -> dict[str, Any]:
    """Query SYSTEM$CLUSTERING_INFORMATION and alert if depth is high.

    Returns the table's clustering metrics so downstream summary tasks
    can aggregate. Doesn't raise on a high depth — auto-clustering
    runs continuously and a single high reading might be transient.
    """
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    # SYSTEM$CLUSTERING_INFORMATION returns a single JSON-string row.
    rows = hook.get_records(f"SELECT SYSTEM$CLUSTERING_INFORMATION('{table}') AS info")
    info = json.loads(rows[0][0]) if rows else {}
    avg_depth = float(info.get("average_depth", 0.0))
    log.info("clustering check %s: avg_depth=%.2f", table, avg_depth)

    return {
        "table": table,
        "avg_depth": avg_depth,
        "total_partitions": info.get("total_partition_count"),
        "alert": avg_depth > CLUSTERING_DEPTH_ALERT,
    }


@task
def fetch_snowflake_credit_usage() -> list[dict[str, Any]]:
    """Pull last week's per-warehouse credit consumption.

    WAREHOUSE_METERING_HISTORY has ~3-hour reporting latency; a Sunday
    morning query gets clean numbers through end-of-day Saturday.
    """
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    query = """
        SELECT
            warehouse_name,
            SUM(credits_used)                AS credits_total,
            SUM(credits_used_compute)        AS credits_compute,
            SUM(credits_used_cloud_services) AS credits_cloud_services
        FROM snowflake.account_usage.warehouse_metering_history
        WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
        GROUP BY warehouse_name
        ORDER BY credits_total DESC
    """
    rows = hook.get_records(query)
    return [
        {
            "warehouse": r[0],
            "credits_total": float(r[1] or 0),
            "credits_compute": float(r[2] or 0),
            "credits_cloud_services": float(r[3] or 0),
        }
        for r in rows
    ]


@task
def fetch_databricks_dbu_usage() -> list[dict[str, Any]]:
    """Pull last week's per-cluster DBU consumption.

    STUBBED for Slice 5: the real implementation calls the Databricks
    billable-usage REST API (``/api/2.0/usage/v1/billable-usage``)
    with the workspace PAT from Secrets Manager. For code-only we
    return shape-correct dummy rows so the report aggregation logic
    is exercised end-to-end.

    Don't undersell this — the lakehouse value proposition rests on
    being able to compare Snowflake credit spend vs Databricks DBU
    spend by workload. Surfacing both on the same report is the
    point.
    """
    # Real call would look like:
    # from databricks.sdk import WorkspaceClient
    # ws = WorkspaceClient(host=..., token=...)
    # usage = ws.billable_usage.download(start_month=..., end_month=...)
    log.info("Databricks DBU usage: stubbed (workspace API not wired in Slice 5)")
    return [
        {
            "cluster_name": "lakehouse-etl-cluster",
            "workload": "JOBS_COMPUTE",
            "dbus_total": 0.0,  # placeholder; real value comes from billable-usage API
            "stubbed": True,
        },
        {
            "cluster_name": "lakehouse-streaming-cluster",
            "workload": "JOBS_COMPUTE",
            "dbus_total": 0.0,
            "stubbed": True,
        },
    ]


@task
def render_cost_report(
    snowflake_usage: list[dict[str, Any]],
    databricks_usage: list[dict[str, Any]],
    clustering: list[dict[str, Any]],
) -> str:
    """Assemble a Markdown cost report from the three data sources."""
    sf_total = sum(r["credits_total"] for r in snowflake_usage)
    db_total = sum(r["dbus_total"] for r in databricks_usage)

    lines = [
        "# Weekly Lakehouse Cost Report",
        "",
        f"Window: prior 7 days ending {pendulum.now('UTC').to_date_string()}",
        "",
        "## Snowflake credits",
        f"- **Total credits**: {sf_total:.2f}",
    ]
    for row in snowflake_usage:
        lines.append(
            f"  - `{row['warehouse']}`: {row['credits_total']:.2f} "
            f"(compute {row['credits_compute']:.2f}, cloud-services "
            f"{row['credits_cloud_services']:.2f})"
        )

    lines += [
        "",
        "## Databricks DBUs",
        f"- **Total DBUs**: {db_total:.2f}"
        + (
            " *(stubbed — workspace API not configured)*"
            if all(r.get("stubbed") for r in databricks_usage)
            else ""
        ),
    ]
    for row in databricks_usage:
        lines.append(f"  - `{row['cluster_name']}` ({row['workload']}): {row['dbus_total']:.2f}")

    lines += [
        "",
        "## Snowflake clustering health",
    ]
    for row in clustering:
        marker = " ⚠️ recluster?" if row["alert"] else ""
        lines.append(
            f"- `{row['table']}`: avg_depth={row['avg_depth']:.2f}, "
            f"partitions={row['total_partitions']}{marker}"
        )

    lines += [
        "",
        "## Split-compute observation",
        "Snowflake credits run analytical queries against the star schema; "
        "Databricks DBUs run the medallion ETL. Watching them on the same "
        "report makes split-compute drift visible — e.g., a backfill that "
        "shifts DBU costs up should NOT also shift Snowflake credits if "
        "the architecture is doing its job.",
    ]

    report = "\n".join(lines)
    log.info("cost report:\n%s", report)
    return report


@task
def publish_cost_report(report: str) -> None:
    """Publish the Markdown report to the alerts SNS topic."""
    from airflow.providers.amazon.aws.hooks.sns import SnsHook

    topic_arn = Variable.get("sns_alert_topic_arn", default_var=None)
    if not topic_arn:
        log.warning("sns_alert_topic_arn not set; skipping cost report publish")
        return

    subject = f"[Lakehouse] Weekly cost report — {pendulum.now('UTC').to_date_string()}"[:100]
    SnsHook(aws_conn_id="aws_default").publish_to_target(
        target_arn=topic_arn, message=report, subject=subject
    )


with DAG(
    dag_id=DAG_ID,
    description="Delta OPTIMIZE+VACUUM, Snowflake clustering check, weekly cost report.",
    start_date=pendulum.datetime(2025, 5, 1, tz="UTC"),
    schedule=SCHEDULE,
    catchup=False,
    default_args=default_args,
    tags=["maintenance", "weekly", "slice-5"],
    doc_md=__doc__,
    max_active_runs=1,
) as dag:

    with TaskGroup("delta_maintenance") as delta_group:
        # Dynamic task mapping over every Delta table. One task per
        # table makes failures isolable — a stuck VACUUM on fact_orders
        # doesn't block the others.
        optimize_and_vacuum_delta.expand(table=DELTA_TABLES)

    with TaskGroup("snowflake_clustering") as clustering_group:
        clustering_results = check_clustering_depth.expand(table=CLUSTERED_FACT_TABLES)

    sf_usage = fetch_snowflake_credit_usage()
    db_usage = fetch_databricks_dbu_usage()

    report = render_cost_report(
        snowflake_usage=sf_usage,
        databricks_usage=db_usage,
        clustering=clustering_results,
    )

    publish = publish_cost_report(report)

    # Wire dependencies: maintenance + checks must finish before the
    # cost report runs (so DBU costs from this run's OPTIMIZE jobs
    # show up in the next week's report, not this one).
    delta_group >> report
    clustering_group >> report
