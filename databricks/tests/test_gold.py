"""Tests for libs.gold.

The accumulating-snapshot merge behavior is the most consequential test —
that's the whole point of the lifecycle table. Direct round-trips through
local Delta tables exercise the real MERGE semantics.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

from libs.gold import (
    build_fact_order_lifecycle,
    build_fact_orders,
    ensure_gold_table,
    merge_into_gold,
)
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _silver_schema() -> StructType:
    """The silver.orders schema, as fact_* would see it."""
    return StructType(
        [
            StructField("order_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("product_id", StringType()),
            StructField("quantity", IntegerType()),
            StructField("price", DoubleType()),
            StructField("status", StringType()),
            StructField("created_at", TimestampType()),
            StructField("paid_at", TimestampType(), nullable=True),
            StructField("shipped_at", TimestampType(), nullable=True),
            StructField("delivered_at", TimestampType(), nullable=True),
            StructField("cancelled_at", TimestampType(), nullable=True),
            StructField("updated_at", TimestampType()),
        ]
    )


def test_fact_orders_columns_and_total_amount(spark: SparkSession) -> None:
    silver = spark.createDataFrame(
        [
            (
                "o1", "c1", "p1", 2, 10.0, "placed",
                dt.datetime(2025, 5, 1, 10), None, None, None, None,
                dt.datetime(2025, 5, 1, 10),
            )
        ],
        schema=_silver_schema(),
    )
    fact = build_fact_orders(silver)
    expected = {
        "order_sk", "order_id", "customer_id", "customer_sk",
        "product_id", "product_sk", "quantity", "price", "total_amount",
        "status", "created_at", "updated_at",
    }
    assert set(fact.columns) == expected
    row = fact.first()
    assert row["total_amount"] == 20.0
    assert row["customer_sk"] is None  # placeholder until Slice 2
    assert row["product_sk"] is None
    assert row["order_sk"] is not None
    assert len(row["order_sk"]) == 64  # sha256 hex


def test_order_sk_is_deterministic(spark: SparkSession) -> None:
    """The surrogate key must be stable across runs for the same order_id."""
    rows = [
        ("o1", "c1", "p1", 1, 1.0, "placed",
         dt.datetime(2025, 5, 1), None, None, None, None, dt.datetime(2025, 5, 1))
    ]
    df = spark.createDataFrame(rows, schema=_silver_schema())
    a = build_fact_orders(df).first()["order_sk"]
    b = build_fact_orders(df).first()["order_sk"]
    assert a == b


def test_lifecycle_durations_decimal_days(spark: SparkSession) -> None:
    silver = spark.createDataFrame(
        [
            (
                "o1", "c1", "p1", 1, 9.99, "delivered",
                dt.datetime(2025, 5, 1, 10, 0, 0),     # placed
                dt.datetime(2025, 5, 1, 22, 0, 0),     # paid 12h later = 0.5d
                dt.datetime(2025, 5, 3, 4, 0, 0),      # shipped 1.25d later
                dt.datetime(2025, 5, 6, 16, 0, 0),     # delivered 3.5d later
                None,
                dt.datetime(2025, 5, 6, 16, 0, 0),
            )
        ],
        schema=_silver_schema(),
    )
    snap = build_fact_order_lifecycle(silver).first()
    assert abs(snap["days_placed_to_paid"] - 0.5) < 1e-6
    assert abs(snap["days_paid_to_shipped"] - 1.25) < 1e-6
    assert abs(snap["days_shipped_to_delivered"] - 3.5) < 1e-6
    # End-to-end: May 1 10:00 -> May 6 16:00 = 5d 6h = 5.25d
    assert abs(snap["days_placed_to_delivered"] - 5.25) < 1e-6


def test_lifecycle_null_milestones_yield_null_durations(spark: SparkSession) -> None:
    silver = spark.createDataFrame(
        [
            (
                "o1", "c1", "p1", 1, 9.99, "placed",
                dt.datetime(2025, 5, 1, 10),
                None, None, None, None,
                dt.datetime(2025, 5, 1, 10),
            )
        ],
        schema=_silver_schema(),
    )
    row = build_fact_order_lifecycle(silver).first()
    assert row["paid_at"] is None
    assert row["shipped_at"] is None
    assert row["days_placed_to_paid"] is None
    assert row["days_paid_to_shipped"] is None
    assert row["days_shipped_to_delivered"] is None
    assert row["days_placed_to_delivered"] is None


def test_lifecycle_cancelled_carries_prior_milestones(spark: SparkSession) -> None:
    """A cancellation after paid keeps paid_at populated. This is the silver-
    layer fix verified end-to-end through gold."""
    silver = spark.createDataFrame(
        [
            (
                "o1", "c1", "p1", 1, 9.99, "cancelled",
                dt.datetime(2025, 5, 1, 10),       # placed
                dt.datetime(2025, 5, 1, 22),       # paid
                None,                              # never shipped
                None,                              # never delivered
                dt.datetime(2025, 5, 2, 9),        # cancelled
                dt.datetime(2025, 5, 2, 9),
            )
        ],
        schema=_silver_schema(),
    )
    row = build_fact_order_lifecycle(silver).first()
    assert row["status"] == "cancelled"
    assert row["paid_at"] is not None  # preserved through gold
    assert row["cancelled_at"] is not None
    assert row["delivered_at"] is None
    # Duration to pay is computed; duration to ship/deliver is NULL.
    assert row["days_placed_to_paid"] is not None
    assert row["days_paid_to_shipped"] is None
    assert row["days_placed_to_delivered"] is None


def test_merge_lifecycle_fills_in_milestone(spark: SparkSession, tmp_path: Path) -> None:
    """The headline test for the accumulating snapshot: a paid record landing
    after a placed record should leave the gold row with BOTH placed_at and
    paid_at populated."""
    target = str(tmp_path / "gold" / "fact_order_lifecycle")

    seed = spark.createDataFrame(
        [
            (
                "o1", "c1", "p1", 1, 9.99, "placed",
                dt.datetime(2025, 5, 1, 10), None, None, None, None,
                dt.datetime(2025, 5, 1, 10),
            )
        ],
        schema=_silver_schema(),
    )
    fact_seed = build_fact_order_lifecycle(seed)
    fact_seed.write.format("delta").save(target)

    # A "paid" transition arrives for the same order
    paid_silver = spark.createDataFrame(
        [
            (
                "o1", "c1", "p1", 1, 9.99, "paid",
                dt.datetime(2025, 5, 1, 10),
                dt.datetime(2025, 5, 1, 22),  # newly populated
                None, None, None,
                dt.datetime(2025, 5, 1, 22),
            )
        ],
        schema=_silver_schema(),
    )
    fact_paid = build_fact_order_lifecycle(paid_silver)
    merge_into_gold(spark, fact_paid, target, ["order_id"])

    back = spark.read.format("delta").load(target).first()
    assert back["status"] == "paid"
    assert back["placed_at"] is not None
    assert back["paid_at"] is not None  # newly filled by the merge
    assert abs(back["days_placed_to_paid"] - 0.5) < 1e-6


def test_merge_fact_orders_inserts_then_updates(spark: SparkSession, tmp_path: Path) -> None:
    target = str(tmp_path / "gold" / "fact_orders")
    seed = spark.createDataFrame(
        [
            (
                "o1", "c1", "p1", 1, 9.99, "placed",
                dt.datetime(2025, 5, 1), None, None, None, None,
                dt.datetime(2025, 5, 1),
            )
        ],
        schema=_silver_schema(),
    )
    ensure_gold_table(spark, target, build_fact_orders(seed))
    merge_into_gold(spark, build_fact_orders(seed), target, ["order_id"])

    # Update to paid
    paid = spark.createDataFrame(
        [
            (
                "o1", "c1", "p1", 1, 9.99, "paid",
                dt.datetime(2025, 5, 1), dt.datetime(2025, 5, 2), None, None, None,
                dt.datetime(2025, 5, 2),
            )
        ],
        schema=_silver_schema(),
    )
    merge_into_gold(spark, build_fact_orders(paid), target, ["order_id"])

    back = spark.read.format("delta").load(target).collect()
    assert len(back) == 1
    assert back[0]["status"] == "paid"


def test_ensure_gold_table_idempotent(spark: SparkSession, tmp_path: Path) -> None:
    target = str(tmp_path / "gold" / "fact_orders")
    seed = spark.createDataFrame(
        [
            (
                "o1", "c1", "p1", 1, 9.99, "placed",
                dt.datetime(2025, 5, 1), None, None, None, None,
                dt.datetime(2025, 5, 1),
            )
        ],
        schema=_silver_schema(),
    )
    fact = build_fact_orders(seed)
    ensure_gold_table(spark, target, fact)
    ensure_gold_table(spark, target, fact)  # second call is a no-op
    # Table exists and is still empty
    assert spark.read.format("delta").load(target).count() == 0
