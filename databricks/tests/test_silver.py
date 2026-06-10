"""Tests for libs.silver.

Covers dedup, watermark filtering, and the MERGE semantics (insert,
update-with-newer, ignore-with-older). MERGE tests round-trip through
local Delta tables to exercise the real Delta runtime, not just transform
shape.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

from libs.silver import (
    append_quarantine,
    apply_watermark,
    dedup_by_key,
    ensure_silver_table,
    merge_into_silver,
)
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def test_dedup_keeps_latest_per_key(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [
            ("o1", "placed", dt.datetime(2025, 5, 1, 10)),
            ("o1", "paid", dt.datetime(2025, 5, 1, 12)),
            ("o1", "shipped", dt.datetime(2025, 5, 2, 8)),
            ("o2", "placed", dt.datetime(2025, 5, 1, 11)),
        ],
        ["order_id", "status", "updated_at"],
    )
    result = dedup_by_key(df, ["order_id"], "updated_at").collect()
    by_id = {r["order_id"]: r["status"] for r in result}
    assert by_id == {"o1": "shipped", "o2": "placed"}


def test_dedup_unique_keys_returns_all(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("o1", "x"), ("o2", "y"), ("o3", "z")],
        ["order_id", "ts"],
    )
    assert dedup_by_key(df, ["order_id"], "ts").count() == 3


def test_dedup_composite_key(spark: SparkSession) -> None:
    """When two columns make up the key, dedup operates on the combination."""
    df = spark.createDataFrame(
        [
            ("o1", "v1", 1),
            ("o1", "v1", 2),  # later → keep
            ("o1", "v2", 1),  # different variant → keep
        ],
        ["order_id", "variant", "ts"],
    )
    assert dedup_by_key(df, ["order_id", "variant"], "ts").count() == 2


def test_watermark_drops_old_rows(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [
            ("o1", dt.datetime(2025, 5, 1)),
            ("o2", dt.datetime(2025, 5, 10)),
            ("o3", dt.datetime(2025, 5, 20)),  # max
        ],
        ["order_id", "updated_at"],
    )
    result = apply_watermark(df, "updated_at", window_days=5)
    # cutoff = May 20 - 5 days = May 15. Only o3 stays.
    ids = {row["order_id"] for row in result.collect()}
    assert ids == {"o3"}


def test_watermark_window_zero_disables(spark: SparkSession) -> None:
    df = spark.createDataFrame([("o1", dt.datetime(2025, 5, 1))], ["order_id", "updated_at"])
    assert apply_watermark(df, "updated_at", window_days=0).count() == 1


def test_watermark_empty_df(spark: SparkSession) -> None:
    schema = StructType(
        [
            StructField("order_id", StringType()),
            StructField("updated_at", TimestampType()),
        ]
    )
    df = spark.createDataFrame([], schema)
    assert apply_watermark(df, "updated_at", window_days=5).count() == 0


def test_ensure_silver_table_creates_when_missing(spark: SparkSession, tmp_path: Path) -> None:
    target = str(tmp_path / "silver" / "orders")
    schema_df = spark.createDataFrame(
        [("placeholder", dt.datetime(2024, 1, 1))], ["order_id", "updated_at"]
    )
    ensure_silver_table(spark, target, schema_df)
    # Table exists and is empty
    assert spark.read.format("delta").load(target).count() == 0


def test_merge_inserts_new_rows(spark: SparkSession, tmp_path: Path) -> None:
    target = str(tmp_path / "silver" / "orders")
    schema_df = spark.createDataFrame(
        [("placeholder", "placed", dt.datetime(2024, 1, 1))],
        ["order_id", "status", "updated_at"],
    )
    ensure_silver_table(spark, target, schema_df)

    src = spark.createDataFrame(
        [
            ("o1", "placed", dt.datetime(2025, 5, 1)),
            ("o2", "placed", dt.datetime(2025, 5, 1)),
        ],
        ["order_id", "status", "updated_at"],
    )
    merge_into_silver(spark, src, target, ["order_id"])

    back = spark.read.format("delta").load(target).collect()
    ids = {r["order_id"] for r in back}
    assert ids == {"o1", "o2"}


def test_merge_updates_with_newer_data(spark: SparkSession, tmp_path: Path) -> None:
    target = str(tmp_path / "silver" / "orders")
    seed = spark.createDataFrame(
        [("o1", "placed", dt.datetime(2025, 5, 1))],
        ["order_id", "status", "updated_at"],
    )
    seed.write.format("delta").save(target)

    src = spark.createDataFrame(
        [("o1", "paid", dt.datetime(2025, 5, 2))],
        ["order_id", "status", "updated_at"],
    )
    merge_into_silver(spark, src, target, ["order_id"])

    back = spark.read.format("delta").load(target).collect()
    assert len(back) == 1
    assert back[0]["status"] == "paid"


def test_merge_ignores_older_data(spark: SparkSession, tmp_path: Path) -> None:
    """Out-of-order arrival: source carries an OLDER updated_at than target.
    The merge predicate must prevent the overwrite."""
    target = str(tmp_path / "silver" / "orders")
    seed = spark.createDataFrame(
        [("o1", "paid", dt.datetime(2025, 5, 5))],
        ["order_id", "status", "updated_at"],
    )
    seed.write.format("delta").save(target)

    src = spark.createDataFrame(
        [("o1", "placed", dt.datetime(2025, 5, 1))],  # older
        ["order_id", "status", "updated_at"],
    )
    merge_into_silver(spark, src, target, ["order_id"])

    back = spark.read.format("delta").load(target).collect()
    assert len(back) == 1
    assert back[0]["status"] == "paid"  # original wins


def test_quarantine_append(spark: SparkSession, tmp_path: Path) -> None:
    qpath = str(tmp_path / "quarantine" / "orders")
    schema = StructType(
        [
            StructField("order_id", StringType(), nullable=True),
            StructField("status", StringType(), nullable=True),
            StructField("_quarantine_reason", StringType(), nullable=True),
        ]
    )
    bad = spark.createDataFrame(
        [(None, "WAT", "qty_positive")],
        schema,
    )
    append_quarantine(bad, qpath)
    bad2 = spark.createDataFrame(
        [("o9", "WAT", "status_known")],
        schema,
    )
    append_quarantine(bad2, qpath)
    back = spark.read.format("delta").load(qpath)
    assert back.count() == 2
