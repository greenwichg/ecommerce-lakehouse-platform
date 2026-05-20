"""Gold layer transformations.

Two Gold models in Slice 1:

- ``fact_orders``: transactional grain. One row per order, latest state from
  silver. Surrogate-key columns (``customer_sk``, ``product_sk``) are
  reserved as NULL placeholders until Slice 2 lands ``dim_customer`` and
  ``dim_product``; the structure stays stable across slices so downstream
  consumers don't break.
- ``fact_order_lifecycle``: accumulating snapshot. One row per order
  carrying every milestone the order has reached (placed/paid/shipped/
  delivered/cancelled) plus computed durations between milestones. When
  a new transition record arrives at silver, the gold MERGE fills in the
  next milestone without erasing earlier ones.

Both builders are pure transforms. ``merge_into_gold`` wraps Delta MERGE
with the same "newer wins" predicate as silver, so out-of-order arrival
at gold doesn't corrupt state either.
"""

from __future__ import annotations

from collections.abc import Sequence

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F


def _duration_days(start_col: str, end_col: str) -> Column:
    """Decimal days between two timestamps; NULL if either is NULL.

    Implemented as ``(end_unix - start_unix) / 86400`` rather than
    ``datediff`` because the latter truncates to whole days, and a 12-hour
    order-to-pay would round to 0.
    """
    return F.when(
        F.col(start_col).isNotNull() & F.col(end_col).isNotNull(),
        (F.col(end_col).cast("long") - F.col(start_col).cast("long")) / F.lit(86400.0),
    )


def build_fact_orders(silver_orders: DataFrame) -> DataFrame:
    """Build the transactional-grain fact_orders DataFrame.

    Adds:
      - ``order_sk``: deterministic surrogate via sha2(order_id, 256)
      - ``customer_sk`` / ``product_sk``: NULL placeholders (Slice 2)
      - ``total_amount``: ``quantity * price``
    Carries the silver natural keys, status, and the canonical timestamps.
    """
    return silver_orders.select(
        F.sha2(F.col("order_id"), 256).alias("order_sk"),
        F.col("order_id"),
        F.col("customer_id"),
        F.lit(None).cast("long").alias("customer_sk"),
        F.col("product_id"),
        F.lit(None).cast("long").alias("product_sk"),
        F.col("quantity"),
        F.col("price"),
        (F.col("quantity") * F.col("price")).alias("total_amount"),
        F.col("status"),
        F.col("created_at"),
        F.col("updated_at"),
    )


def build_fact_order_lifecycle(silver_orders: DataFrame) -> DataFrame:
    """Build the accumulating-snapshot fact_order_lifecycle DataFrame.

    Each row carries the full set of milestone timestamps (those reached
    so far) and computed milestone-to-milestone durations.
    """
    return silver_orders.select(
        F.sha2(F.col("order_id"), 256).alias("order_sk"),
        F.col("order_id"),
        F.col("customer_id"),
        F.col("product_id"),
        F.col("quantity"),
        F.col("price"),
        F.col("status"),
        F.col("created_at").alias("placed_at"),
        F.col("paid_at"),
        F.col("shipped_at"),
        F.col("delivered_at"),
        F.col("cancelled_at"),
        _duration_days("created_at", "paid_at").alias("days_placed_to_paid"),
        _duration_days("paid_at", "shipped_at").alias("days_paid_to_shipped"),
        _duration_days("shipped_at", "delivered_at").alias("days_shipped_to_delivered"),
        _duration_days("created_at", "delivered_at").alias("days_placed_to_delivered"),
        F.col("updated_at"),
    )


def merge_into_gold(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    merge_keys: Sequence[str],
    timestamp_col: str = "updated_at",
) -> None:
    """MERGE source_df into the gold Delta table.

    Same predicate as silver: WHEN MATCHED only if ``s.{timestamp_col} >
    t.{timestamp_col}``. This means a late-arriving "placed" record can't
    erase a later "paid" milestone in the accumulating snapshot.
    """
    target = DeltaTable.forPath(spark, target_path)
    on = " AND ".join(f"t.{k} = s.{k}" for k in merge_keys)
    update_cond = f"s.{timestamp_col} > t.{timestamp_col}"
    (
        target.alias("t")
        .merge(source_df.alias("s"), on)
        .whenMatchedUpdateAll(condition=update_cond)
        .whenNotMatchedInsertAll()
        .execute()
    )


def ensure_gold_table(spark: SparkSession, path: str, schema_df: DataFrame) -> None:
    """Idempotent create of an empty gold Delta table."""
    if DeltaTable.isDeltaTable(spark, path):
        return
    spark.createDataFrame([], schema_df.schema).write.format("delta").save(path)


def optimize_zorder(spark: SparkSession, path: str, zorder_cols: Sequence[str]) -> None:
    """Run ``OPTIMIZE ... ZORDER BY`` against a Delta path.

    Skipped silently if the table doesn't exist yet (e.g. very first run).
    Tests don't exercise this — OPTIMIZE is slow and benefits from production
    cluster sizing.
    """
    if not DeltaTable.isDeltaTable(spark, path):
        return
    cols = ", ".join(zorder_cols)
    spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({cols})")
