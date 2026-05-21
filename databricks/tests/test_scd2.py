"""Tests for libs.scd2 — the SCD2 MERGE primitive.

Coverage:
- First load inserts every source row as is_current=true.
- Re-applying with unchanged source is a no-op (no new versions).
- Tracked-column change closes old and inserts new with the right effective_to.
- Untracked-column change is absorbed silently (no new version).
- Multiple changes chain correctly: v[i].effective_to == v[i+1].effective_from.
- Exactly one is_current=true per natural_key at all times.
- New customer arriving in v2 inserts alongside unchanged v1 customers.
- Surrogate is deterministic from (natural_key, ts).
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

from libs.scd2 import apply_scd2_merge, compute_surrogate
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _silver_customers_schema() -> StructType:
    return StructType(
        [
            StructField("customer_id", StringType()),
            StructField("name", StringType()),
            StructField("email", StringType()),
            StructField("address", StringType()),
            StructField("updated_at", TimestampType()),
        ]
    )


def _make_silver(spark: SparkSession, rows: list[tuple]) -> object:
    return spark.createDataFrame(rows, _silver_customers_schema())


def test_first_load_inserts_all_as_current(spark: SparkSession, tmp_path: Path) -> None:
    target = str(tmp_path / "dim_customer")
    src = _make_silver(
        spark,
        [
            ("c1", "alice", "alice@a.com", "1 Main St", dt.datetime(2025, 1, 1)),
            ("c2", "bob", "bob@b.com", "2 Oak Ave", dt.datetime(2025, 1, 1)),
        ],
    )
    apply_scd2_merge(
        spark, src, target,
        natural_key="customer_id",
        attribute_cols=["name", "email", "address"],
        tracked_cols=["email", "address"],
        sk_col="customer_sk",
    )
    rows = spark.read.format("delta").load(target).collect()
    assert len(rows) == 2
    assert all(r["is_current"] for r in rows)
    by_id = {r["customer_id"]: r for r in rows}
    assert by_id["c1"]["email"] == "alice@a.com"
    assert by_id["c2"]["email"] == "bob@b.com"


def test_reapplying_identical_source_no_op(spark: SparkSession, tmp_path: Path) -> None:
    target = str(tmp_path / "dim_customer")
    src = _make_silver(
        spark,
        [("c1", "alice", "alice@a.com", "1 Main St", dt.datetime(2025, 1, 1))],
    )
    apply_scd2_merge(
        spark, src, target,
        natural_key="customer_id",
        attribute_cols=["name", "email", "address"],
        tracked_cols=["email", "address"],
        sk_col="customer_sk",
    )
    apply_scd2_merge(
        spark, src, target,
        natural_key="customer_id",
        attribute_cols=["name", "email", "address"],
        tracked_cols=["email", "address"],
        sk_col="customer_sk",
    )
    rows = spark.read.format("delta").load(target).collect()
    assert len(rows) == 1, "identical re-apply should not create a new version"


def test_tracked_change_closes_old_and_inserts_new(
    spark: SparkSession, tmp_path: Path
) -> None:
    target = str(tmp_path / "dim_customer")
    src_v1 = _make_silver(
        spark,
        [("c1", "alice", "alice@old.com", "1 Main St", dt.datetime(2025, 1, 1))],
    )
    apply_scd2_merge(
        spark, src_v1, target,
        natural_key="customer_id",
        attribute_cols=["name", "email", "address"],
        tracked_cols=["email", "address"],
        sk_col="customer_sk",
    )
    src_v2 = _make_silver(
        spark,
        [("c1", "alice", "alice@new.com", "1 Main St", dt.datetime(2025, 5, 1))],
    )
    apply_scd2_merge(
        spark, src_v2, target,
        natural_key="customer_id",
        attribute_cols=["name", "email", "address"],
        tracked_cols=["email", "address"],
        sk_col="customer_sk",
    )

    rows = sorted(
        spark.read.format("delta").load(target).collect(),
        key=lambda r: r["effective_from"],
    )
    assert len(rows) == 2
    # v1: closed at v2's effective_from
    assert rows[0]["email"] == "alice@old.com"
    assert rows[0]["is_current"] is False
    assert rows[0]["effective_to"] == dt.datetime(2025, 5, 1)
    # v2: current, effective_from = source.updated_at
    assert rows[1]["email"] == "alice@new.com"
    assert rows[1]["is_current"] is True
    assert rows[1]["effective_from"] == dt.datetime(2025, 5, 1)


def test_untracked_column_change_absorbed_silently(
    spark: SparkSession, tmp_path: Path
) -> None:
    """If we only track email but address changes, no new version emitted.

    Note this means the new address value is not persisted either —
    SCD2 absorbs changes to untracked attributes by ignoring them. This
    is the expected semantic when tracked_cols != attribute_cols.
    """
    target = str(tmp_path / "dim_customer")
    src_v1 = _make_silver(
        spark,
        [("c1", "alice", "alice@a.com", "1 Main St", dt.datetime(2025, 1, 1))],
    )
    apply_scd2_merge(
        spark, src_v1, target,
        natural_key="customer_id",
        attribute_cols=["name", "email", "address"],
        tracked_cols=["email"],  # tracking email ONLY
        sk_col="customer_sk",
    )
    src_v2 = _make_silver(
        spark,
        [("c1", "alice", "alice@a.com", "2 Oak Ave", dt.datetime(2025, 5, 1))],
    )
    apply_scd2_merge(
        spark, src_v2, target,
        natural_key="customer_id",
        attribute_cols=["name", "email", "address"],
        tracked_cols=["email"],
        sk_col="customer_sk",
    )
    rows = spark.read.format("delta").load(target).collect()
    assert len(rows) == 1


def test_multiple_versions_chain_contiguously(
    spark: SparkSession, tmp_path: Path
) -> None:
    target = str(tmp_path / "dim_customer")
    timestamps = [
        dt.datetime(2025, 1, 1),
        dt.datetime(2025, 2, 1),
        dt.datetime(2025, 3, 1),
        dt.datetime(2025, 4, 1),
    ]
    emails = ["v0@x.com", "v1@x.com", "v2@x.com", "v3@x.com"]
    for ts, email in zip(timestamps, emails, strict=True):
        src = _make_silver(spark, [("c1", "alice", email, "1 Main St", ts)])
        apply_scd2_merge(
            spark, src, target,
            natural_key="customer_id",
            attribute_cols=["name", "email", "address"],
            tracked_cols=["email"],
            sk_col="customer_sk",
        )

    rows = sorted(
        spark.read.format("delta").load(target).collect(),
        key=lambda r: r["effective_from"],
    )
    assert len(rows) == 4
    for prev, nxt in zip(rows, rows[1:], strict=False):
        # v[i].effective_to == v[i+1].effective_from (no gaps, no overlaps)
        assert prev["effective_to"] == nxt["effective_from"]
        assert prev["is_current"] is False
    assert rows[-1]["is_current"] is True
    assert rows[-1]["email"] == "v3@x.com"


def test_exactly_one_current_per_natural_key(
    spark: SparkSession, tmp_path: Path
) -> None:
    target = str(tmp_path / "dim_customer")
    timestamps = [dt.datetime(2025, i, 1) for i in range(1, 6)]
    for ts in timestamps:
        src = _make_silver(spark, [("c1", "alice", f"v{ts.month}@x.com", "1 Main St", ts)])
        apply_scd2_merge(
            spark, src, target,
            natural_key="customer_id",
            attribute_cols=["name", "email", "address"],
            tracked_cols=["email"],
            sk_col="customer_sk",
        )
    df = spark.read.format("delta").load(target)
    current_count = df.filter("is_current = true").count()
    assert current_count == 1


def test_new_customer_in_later_batch(spark: SparkSession, tmp_path: Path) -> None:
    target = str(tmp_path / "dim_customer")
    # v1: just c1
    apply_scd2_merge(
        spark,
        _make_silver(spark, [("c1", "alice", "a@x.com", "1 Main", dt.datetime(2025, 1, 1))]),
        target,
        natural_key="customer_id",
        attribute_cols=["name", "email", "address"],
        tracked_cols=["email", "address"],
        sk_col="customer_sk",
    )
    # v2: c1 (unchanged) + c2 (new)
    apply_scd2_merge(
        spark,
        _make_silver(
            spark,
            [
                ("c1", "alice", "a@x.com", "1 Main", dt.datetime(2025, 1, 1)),
                ("c2", "bob", "b@x.com", "2 Oak", dt.datetime(2025, 2, 1)),
            ],
        ),
        target,
        natural_key="customer_id",
        attribute_cols=["name", "email", "address"],
        tracked_cols=["email", "address"],
        sk_col="customer_sk",
    )
    rows = spark.read.format("delta").load(target).collect()
    assert len(rows) == 2
    by_id = {r["customer_id"]: r for r in rows}
    assert by_id["c1"]["is_current"] is True
    assert by_id["c2"]["is_current"] is True


def test_surrogate_is_deterministic_from_natural_key_and_ts(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("c1", dt.datetime(2025, 1, 1))],
        ["customer_id", "updated_at"],
    )
    sk_a = compute_surrogate(df, "customer_id", "updated_at", "customer_sk").first()["customer_sk"]
    sk_b = compute_surrogate(df, "customer_id", "updated_at", "customer_sk").first()["customer_sk"]
    assert sk_a == sk_b
    assert len(sk_a) == 64  # SHA-256 hex


def test_surrogate_differs_for_different_ts(spark: SparkSession) -> None:
    """Same customer, different version timestamp -> different SK."""
    df = spark.createDataFrame(
        [
            ("c1", dt.datetime(2025, 1, 1)),
            ("c1", dt.datetime(2025, 5, 1)),
        ],
        ["customer_id", "updated_at"],
    )
    sks = [r["customer_sk"] for r in compute_surrogate(df, "customer_id", "updated_at", "customer_sk").collect()]
    assert sks[0] != sks[1]


def test_null_safe_equality_does_not_trigger_change(
    spark: SparkSession, tmp_path: Path
) -> None:
    """Two consecutive snapshots where email is NULL in both -> no new version."""
    target = str(tmp_path / "dim_customer")
    src = _make_silver(
        spark,
        [("c1", "alice", None, "1 Main St", dt.datetime(2025, 1, 1))],
    )
    apply_scd2_merge(
        spark, src, target,
        natural_key="customer_id",
        attribute_cols=["name", "email", "address"],
        tracked_cols=["email", "address"],
        sk_col="customer_sk",
    )
    src_v2 = _make_silver(
        spark,
        [("c1", "alice", None, "1 Main St", dt.datetime(2025, 2, 1))],
    )
    apply_scd2_merge(
        spark, src_v2, target,
        natural_key="customer_id",
        attribute_cols=["name", "email", "address"],
        tracked_cols=["email", "address"],
        sk_col="customer_sk",
    )
    assert spark.read.format("delta").load(target).count() == 1
