"""Gold layer transformations.

Two Gold models:

- ``fact_orders``: transactional grain. One row per order, latest state
  from silver. As of Slice 2, ``customer_sk`` and ``product_sk`` are
  populated via point-in-time correct SCD2 joins (see ``pit_join_scd2``).
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


def pit_join_scd2(
    fact_df: DataFrame,
    dim_df: DataFrame,
    natural_key: str,
    sk_col: str,
    fact_timestamp_col: str = "created_at",
    extra_cols: Sequence[str] | None = None,
) -> DataFrame:
    """Point-in-time correct SCD2 join.

    Binds each fact row to the dim version that was *valid at the order's
    placement time*. Returns the fact DataFrame with the surrogate
    column populated (NULL on no match — LEFT join, orphans surfaced for
    downstream DQ rather than silently dropped), plus any columns listed
    in ``extra_cols`` carried over from the dim version.

    Why ``created_at`` (the order's placed_at), not ``updated_at`` or load-
    time:

      - ``created_at``: binds to the dim version current WHEN THE ORDER
        WAS PLACED. This is the standard dimensional-modelling semantic —
        "which customer placed this order? — the one at the address they
        had at placement". A late status update that arrives months later
        still resolves to the customer-at-order-time.
      - ``updated_at``: binds to the dim version current when the order's
        STATUS last changed. Loses meaning when an order pays 3 days after
        placement but the customer moved in between — the fact would show
        the new address as if they ordered from there.
      - ``current_timestamp() / load_time``: ignores history. Fine for
        slowly-changing references where history doesn't matter (e.g.
        currency conversion); wrong for entities where address-at-order
        is the correct semantic.

    Slice 4 ``extra_cols``: lets a caller pull additional dim columns
    along with the surrogate — e.g., ``category`` denormalised into
    ``fact_orders`` so the Snowflake MV (which can't join) has a single
    base table to aggregate.

    Broadcast hint: ``broadcast(dim_df)`` — dim_customer at ~10k natural
    keys × ~10 SCD2 versions across a few years = O(100k) rows,
    comfortably under Spark's default 10MB broadcast threshold once
    encoded.
    """
    extras = tuple(extra_cols or ())
    dim_select_args = [
        F.col(natural_key).alias("__dim_nk"),
        F.col(sk_col).alias("__dim_sk"),
        F.col("effective_from").alias("__dim_from"),
        F.col("effective_to").alias("__dim_to"),
    ]
    for col in extras:
        dim_select_args.append(F.col(col).alias(f"__dim_extra_{col}"))
    sel_dim = dim_df.select(*dim_select_args)

    joined = (
        fact_df.alias("f")
        .join(
            F.broadcast(sel_dim),
            (F.col(f"f.{natural_key}") == F.col("__dim_nk"))
            & (F.col(f"f.{fact_timestamp_col}") >= F.col("__dim_from"))
            & (F.col(f"f.{fact_timestamp_col}") < F.col("__dim_to")),
            "left",
        )
        .withColumn(sk_col, F.col("__dim_sk"))
    )
    for col in extras:
        joined = joined.withColumn(col, F.col(f"__dim_extra_{col}"))
    drop_cols = ["__dim_nk", "__dim_sk", "__dim_from", "__dim_to"] + [
        f"__dim_extra_{c}" for c in extras
    ]
    return joined.drop(*drop_cols)


def build_fact_orders(
    silver_orders: DataFrame,
    dim_customer: DataFrame,
    dim_product: DataFrame,
) -> DataFrame:
    """Build the transactional-grain fact_orders DataFrame.

    Args:
        silver_orders: deduplicated silver orders (one row per order_id).
        dim_customer: SCD2 customer dim with ``customer_id``,
            ``customer_sk``, ``effective_from``, ``effective_to``.
        dim_product: SCD2 product dim with the analogous columns.

    Adds:
      - ``order_sk``: deterministic surrogate via sha2(order_id, 256)
      - ``customer_sk`` / ``product_sk``: bound via point-in-time SCD2
        join on ``created_at`` (the order's placement timestamp).
        VARCHAR(64) (SHA-256 hex) — matches dim surrogate type.
      - ``total_amount``: ``quantity * price``

    Slice 1 → Slice 2 schema change: ``customer_sk`` / ``product_sk`` types
    flip from BIGINT NULL placeholder to VARCHAR(64). Tests construct dims
    via the ``_single_version_dim`` helper to keep Slice 1 assertions
    intact while exercising the new join path.
    """
    base = silver_orders.select(
        F.sha2(F.col("order_id"), 256).alias("order_sk"),
        F.col("order_id"),
        F.col("customer_id"),
        F.col("product_id"),
        F.col("quantity"),
        F.col("price"),
        (F.col("quantity") * F.col("price")).alias("total_amount"),
        F.col("status"),
        F.col("created_at"),
        F.col("updated_at"),
    )
    with_customer = pit_join_scd2(base, dim_customer, "customer_id", "customer_sk", "created_at")
    # PIT-join product: pull surrogate AND category (Slice 4 denormalisation
    # so the Snowflake MV — which can't do joins — has a single-table
    # GROUP BY surface). category is snapshotted at order-placement time;
    # a later product re-categorisation does NOT retroactively rewrite
    # historical orders' category. This is PIT-correct for revenue-by-
    # category analytics.
    with_both = pit_join_scd2(
        with_customer,
        dim_product,
        "product_id",
        "product_sk",
        "created_at",
        extra_cols=["category"],
    )

    # Re-order so SK + denormalised cols sit next to the natural key they shadow.
    return with_both.select(
        "order_sk",
        "order_id",
        "customer_id",
        "customer_sk",
        "product_id",
        "product_sk",
        "category",
        "quantity",
        "price",
        "total_amount",
        "status",
        "created_at",
        "updated_at",
    )


def orphan_surrogate_rate(fact_df: DataFrame, sk_cols: Sequence[str]) -> float:
    """Compute the % of fact rows where any ``sk_cols`` is NULL.

    Returns 0.0 on an empty DataFrame (vacuous pass — matches the
    Snowflake-side gate behavior in dq_gates).
    """
    total = fact_df.count()
    if total == 0:
        return 0.0
    null_pred = " OR ".join(f"{c} IS NULL" for c in sk_cols)
    orphans = fact_df.filter(null_pred).count()
    return orphans / total * 100.0


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
