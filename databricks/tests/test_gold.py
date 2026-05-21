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
    orphan_surrogate_rate,
    pit_join_scd2,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# --- SCD2 test helpers ---------------------------------------------------
# build_fact_orders now requires dim_customer and dim_product. Tests that
# don't care about SCD2 history use _single_version_dim to construct a
# minimal dim covering [epoch, year=3000) so the PIT join always matches.


_EPOCH = dt.datetime(1970, 1, 1)
_FAR_FUTURE = dt.datetime(3000, 1, 1)


def _single_version_dim(
    spark: SparkSession,
    natural_keys: list[str],
    natural_key_col: str,
    sk_col: str,
    extra_cols: dict[str, list] | None = None,
) -> DataFrame:
    """Build a one-version-per-key dim DataFrame spanning [epoch, far-future).

    Returns the SCD2-shaped DF (sk_col, natural_key_col, effective_from,
    effective_to, is_current). Used by tests that need the SCD2 join to
    "always match" — i.e. exercise the post-Slice-1 contract without
    needing real version history.
    """
    rows = []
    extras = extra_cols or {}
    for i, nk in enumerate(natural_keys):
        row = {
            sk_col: f"sk-{i:04d}",
            natural_key_col: nk,
            "effective_from": _EPOCH,
            "effective_to": _FAR_FUTURE,
            "is_current": True,
        }
        for col, vals in extras.items():
            row[col] = vals[i]
        rows.append(row)
    fields = [
        StructField(sk_col, StringType(), nullable=False),
        StructField(natural_key_col, StringType(), nullable=False),
    ]
    for col in extras:
        fields.append(StructField(col, StringType(), nullable=True))
    fields.extend(
        [
            StructField("effective_from", TimestampType(), nullable=False),
            StructField("effective_to", TimestampType(), nullable=False),
            StructField("is_current", BooleanType(), nullable=False),
        ]
    )
    return spark.createDataFrame(rows, StructType(fields))


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
    dim_c = _single_version_dim(spark, ["c1"], "customer_id", "customer_sk")
    dim_p = _single_version_dim(spark, ["p1"], "product_id", "product_sk", extra_cols={"category": ["electronics"]})
    fact = build_fact_orders(silver, dim_c, dim_p)
    expected = {
        "order_sk", "order_id", "customer_id", "customer_sk",
        "product_id", "product_sk", "category",            # Slice 4: denormalised
        "quantity", "price", "total_amount",
        "status", "created_at", "updated_at",
    }
    assert set(fact.columns) == expected
    row = fact.first()
    assert row["total_amount"] == 20.0
    # SK populated via PIT join — no longer NULL post-Slice 2.
    assert row["customer_sk"] == "sk-0000"
    assert row["product_sk"] == "sk-0000"
    # category denormalised from dim_product PIT (Slice 4 — for the MV)
    assert row["category"] == "electronics"
    assert row["order_sk"] is not None
    assert len(row["order_sk"]) == 64  # sha256 hex


def test_order_sk_is_deterministic(spark: SparkSession) -> None:
    """The surrogate key must be stable across runs for the same order_id."""
    rows = [
        ("o1", "c1", "p1", 1, 1.0, "placed",
         dt.datetime(2025, 5, 1), None, None, None, None, dt.datetime(2025, 5, 1))
    ]
    df = spark.createDataFrame(rows, schema=_silver_schema())
    dim_c = _single_version_dim(spark, ["c1"], "customer_id", "customer_sk")
    dim_p = _single_version_dim(spark, ["p1"], "product_id", "product_sk", extra_cols={"category": ["electronics"]})
    a = build_fact_orders(df, dim_c, dim_p).first()["order_sk"]
    b = build_fact_orders(df, dim_c, dim_p).first()["order_sk"]
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
    dim_c = _single_version_dim(spark, ["c1"], "customer_id", "customer_sk")
    dim_p = _single_version_dim(spark, ["p1"], "product_id", "product_sk", extra_cols={"category": ["electronics"]})
    ensure_gold_table(spark, target, build_fact_orders(seed, dim_c, dim_p))
    merge_into_gold(spark, build_fact_orders(seed, dim_c, dim_p), target, ["order_id"])

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
    merge_into_gold(spark, build_fact_orders(paid, dim_c, dim_p), target, ["order_id"])

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
    dim_c = _single_version_dim(spark, ["c1"], "customer_id", "customer_sk")
    dim_p = _single_version_dim(spark, ["p1"], "product_id", "product_sk", extra_cols={"category": ["electronics"]})
    fact = build_fact_orders(seed, dim_c, dim_p)
    ensure_gold_table(spark, target, fact)
    ensure_gold_table(spark, target, fact)  # second call is a no-op
    # Table exists and is still empty
    assert spark.read.format("delta").load(target).count() == 0


# --- PIT join + orphan rate tests (new in Slice 2) -------------------------


def _scd2_dim(
    spark: SparkSession,
    rows: list[tuple],
) -> DataFrame:
    """Construct an SCD2-shaped dim DF from explicit (sk, nk, ef, et, is_current) tuples."""
    schema = StructType(
        [
            StructField("customer_sk", StringType(), nullable=False),
            StructField("customer_id", StringType(), nullable=False),
            StructField("effective_from", TimestampType(), nullable=False),
            StructField("effective_to", TimestampType(), nullable=False),
            StructField("is_current", BooleanType(), nullable=False),
        ]
    )
    return spark.createDataFrame(rows, schema)


def test_pit_join_binds_to_version_valid_at_fact_timestamp(spark: SparkSession) -> None:
    """Two versions of a customer: order at T1 binds to v1, order at T3 binds to v2."""
    dim = _scd2_dim(
        spark,
        [
            ("sk-v1", "c1", dt.datetime(2025, 1, 1), dt.datetime(2025, 6, 1), False),
            ("sk-v2", "c1", dt.datetime(2025, 6, 1), dt.datetime(3000, 1, 1), True),
        ],
    )
    fact = spark.createDataFrame(
        [
            ("o1", "c1", dt.datetime(2025, 3, 15)),  # before v2 starts -> v1
            ("o2", "c1", dt.datetime(2025, 9, 15)),  # after v2 starts -> v2
        ],
        ["order_id", "customer_id", "created_at"],
    )
    joined = pit_join_scd2(fact, dim, "customer_id", "customer_sk", "created_at").collect()
    by_order = {r["order_id"]: r["customer_sk"] for r in joined}
    assert by_order["o1"] == "sk-v1"
    assert by_order["o2"] == "sk-v2"


def test_pit_join_customer_changes_between_placement_and_silver_merge(
    spark: SparkSession,
) -> None:
    """User-flagged edge case: order placed at T1, customer update at T2,
    silver MERGE happens at T3, with T1 < T2 < T3. The fact's customer_sk
    must bind to the T1 customer version (the one valid at placement),
    NOT to the T2 version (which only became current after placement)."""
    t1 = dt.datetime(2025, 5, 1, 10)   # order placed
    t2 = dt.datetime(2025, 5, 1, 14)   # customer changes address
    t3 = dt.datetime(2025, 5, 2, 8)    # silver MERGE runs

    dim = _scd2_dim(
        spark,
        [
            ("sk-v1", "c1", dt.datetime(2025, 1, 1), t2, False),  # v1 valid at T1
            ("sk-v2", "c1", t2, dt.datetime(3000, 1, 1), True),    # v2 starts at T2
        ],
    )
    # Fact carries created_at = T1 (placement time) and updated_at = T3
    # (last silver MERGE time). The PIT join must use created_at, not
    # updated_at, to bind correctly.
    fact = spark.createDataFrame(
        [("o1", "c1", t1, t3)],
        ["order_id", "customer_id", "created_at", "updated_at"],
    )
    joined = pit_join_scd2(fact, dim, "customer_id", "customer_sk", "created_at").first()
    assert joined["customer_sk"] == "sk-v1", (
        "must bind to v1 (current at placement), not v2 (current at MERGE)"
    )


def test_pit_join_orphan_yields_null_sk(spark: SparkSession) -> None:
    """A fact row for a customer absent from the dim retains NULL sk
    (LEFT join semantic) rather than being dropped."""
    dim = _scd2_dim(
        spark,
        [("sk-v1", "c1", dt.datetime(2025, 1, 1), dt.datetime(3000, 1, 1), True)],
    )
    fact = spark.createDataFrame(
        [
            ("o1", "c1", dt.datetime(2025, 5, 1)),
            ("o2", "c_missing", dt.datetime(2025, 5, 1)),
        ],
        ["order_id", "customer_id", "created_at"],
    )
    joined = pit_join_scd2(fact, dim, "customer_id", "customer_sk", "created_at").collect()
    by_order = {r["order_id"]: r["customer_sk"] for r in joined}
    assert by_order["o1"] == "sk-v1"
    assert by_order["o2"] is None
    assert len(joined) == 2  # NOT dropped


def test_orphan_rate_zero_when_all_resolved(spark: SparkSession) -> None:
    fact = spark.createDataFrame(
        [("sk1", "sk2"), ("sk3", "sk4")],
        ["customer_sk", "product_sk"],
    )
    assert orphan_surrogate_rate(fact, ["customer_sk", "product_sk"]) == 0.0


def test_orphan_rate_counts_any_null_column(spark: SparkSession) -> None:
    """A row with EITHER customer_sk OR product_sk NULL counts as one orphan."""
    fact = spark.createDataFrame(
        [
            ("sk1", "sk2"),  # ok
            (None, "sk4"),   # orphan
            ("sk3", None),   # orphan
            (None, None),    # orphan (counts once, not twice)
        ],
        ["customer_sk", "product_sk"],
    )
    rate = orphan_surrogate_rate(fact, ["customer_sk", "product_sk"])
    assert rate == 75.0  # 3 of 4


def test_orphan_rate_empty_df_is_vacuous_zero(spark: SparkSession) -> None:
    fact = spark.createDataFrame(
        [],
        StructType(
            [
                StructField("customer_sk", StringType()),
                StructField("product_sk", StringType()),
            ]
        ),
    )
    assert orphan_surrogate_rate(fact, ["customer_sk", "product_sk"]) == 0.0


def test_build_fact_orders_orphan_customer_yields_null_sk(spark: SparkSession) -> None:
    """End-to-end: a silver order referencing a customer absent from dim_customer
    flows through as fact row with customer_sk = NULL (caught later by
    check_orphan_surrogate_rate in the daily DAG)."""
    silver = spark.createDataFrame(
        [
            (
                "o1", "c_missing", "p1", 1, 9.99, "placed",
                dt.datetime(2025, 5, 1, 10), None, None, None, None,
                dt.datetime(2025, 5, 1, 10),
            )
        ],
        schema=_silver_schema(),
    )
    dim_c = _single_version_dim(spark, ["c_present"], "customer_id", "customer_sk")
    dim_p = _single_version_dim(spark, ["p1"], "product_id", "product_sk", extra_cols={"category": ["electronics"]})
    fact = build_fact_orders(silver, dim_c, dim_p).first()
    assert fact["customer_sk"] is None
    assert fact["product_sk"] == "sk-0000"  # product still resolved
