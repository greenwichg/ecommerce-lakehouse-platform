"""Pre-aggregated marts (Slice 4).

Two builders here:

- ``build_customer_ltv``: per-customer lifetime aggregates. Uses window
  functions (``LAG`` over per-customer order timeline) which disqualify
  it from Snowflake MV — so it lives in Databricks Gold as a regular
  table, refreshed daily by the orders DAG.

- (Future: ``build_daily_revenue_by_category`` lives in Snowflake as
  ``mv_daily_revenue_by_category``. The denormalization of ``category``
  into ``fact_orders`` happens in ``libs.gold.build_fact_orders``; the
  MV does the GROUP BY. Documented here for traceability.)

## `predicted_churn_flag` heuristic

Simple rule, intentionally not ML: ``True`` when

    last_order_date < (today - 90 days)
    AND order_count > 1

i.e., a customer who used to order but hasn't in 90+ days. Single-order
customers ("trial buyer never came back") get ``False`` because we can't
distinguish them from a brand-new buyer.

This is a sketch, not a model. Real churn prediction is a Slice 6+
endeavour with proper features and a held-out test set. The flag exists
so the Streamlit dashboard has *something* to display in the "at-risk
customers" widget and so the data shape is correct for that future model.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

# Default churn threshold: 90 days since last order.
_DEFAULT_CHURN_DAYS = 90


def build_customer_ltv(
    fact_orders: DataFrame,
    today: dt.date | None = None,
    churn_days: int = _DEFAULT_CHURN_DAYS,
) -> DataFrame:
    """Build the customer_ltv mart from fact_orders.

    Args:
        fact_orders: gold fact_orders DataFrame with at least
            ``customer_sk``, ``created_at``, ``total_amount``, ``status``.
        today: the "as of" date for churn calculation. Defaults to the
            max created_at in the input — so the mart's churn flag is
            stable for re-runs (re-running yesterday produces yesterday's
            churn flag, not today's).
        churn_days: threshold for predicted_churn_flag (default 90).

    Output: one row per ``customer_sk`` with:
        - total_revenue: SUM(total_amount) over non-cancelled orders
        - order_count: COUNT(*) over non-cancelled orders
        - first_order_date / last_order_date
        - avg_days_between_orders: NULL for single-order customers
        - predicted_churn_flag
        - updated_at: max(created_at) per customer (for the MERGE)
    """
    # Filter out cancelled — they don't count toward LTV
    non_cancelled = fact_orders.filter(F.col("status") != "cancelled")

    # Window: each customer's orders sorted by created_at, with the
    # previous order's date attached via LAG
    cust_window = Window.partitionBy("customer_sk").orderBy("created_at")
    with_gap = non_cancelled.withColumn(
        "_prev_order_at", F.lag("created_at").over(cust_window)
    ).withColumn(
        "_days_since_prev",
        F.when(
            F.col("_prev_order_at").isNotNull(),
            (F.col("created_at").cast("long") - F.col("_prev_order_at").cast("long")) / 86400.0,
        ),
    )

    # Determine the "as of" date — use the input's max created_at unless
    # explicitly overridden, so re-running for a historical state is
    # deterministic.
    if today is None:
        max_row = non_cancelled.agg(F.max("created_at").cast("date").alias("d")).first()
        today_lit = F.lit(max_row["d"]) if max_row and max_row["d"] else F.current_date()
    else:
        today_lit = F.lit(today)

    return (
        with_gap.groupBy("customer_sk")
        .agg(
            F.sum("total_amount").alias("total_revenue"),
            F.count("*").alias("order_count"),
            F.min("created_at").cast("date").alias("first_order_date"),
            F.max("created_at").cast("date").alias("last_order_date"),
            F.avg("_days_since_prev").alias("avg_days_between_orders"),
            F.max("created_at").alias("updated_at"),
        )
        .withColumn(
            "predicted_churn_flag",
            (F.col("order_count") > 1)
            & (F.datediff(today_lit, F.col("last_order_date")) > churn_days),
        )
    )


def ensure_mart_table(spark: SparkSession, path: str, schema_df: DataFrame) -> None:
    """Idempotent create of an empty mart Delta table.

    Marts are full-refresh per run (small enough that incremental MERGE is
    overkill); this helper just ensures the path exists with the right
    schema before the writer goes in.
    """
    from delta.tables import DeltaTable

    if DeltaTable.isDeltaTable(spark, path):
        return
    spark.createDataFrame([], schema_df.schema).write.format("delta").save(path)


def write_mart_overwrite(df: DataFrame, path: str) -> None:
    """Full-refresh write to a mart Delta table.

    Marts are small (10k customers for customer_ltv) and computed from
    full history each run, so MERGE adds no value over overwrite. The
    ``overwriteSchema`` option lets a schema evolution in the builder
    propagate without manual DDL.
    """
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path))
