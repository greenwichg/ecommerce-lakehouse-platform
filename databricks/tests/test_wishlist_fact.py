"""Tests for libs.wishlist.build_fact_customer_wishlist_product.

The factless fact's main correctness properties:
- One row per input event (no collapsing).
- wishlist_event_sk is deterministic (SHA-256 of natural key).
- Customer and product surrogates come from PIT join at added_at.
- Orphans (customer/product missing from dim) get NULL sk; row not dropped.
- added_date column matches DATE(added_at).
"""

from __future__ import annotations

import datetime as dt

from libs.wishlist import build_fact_customer_wishlist_product
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

_EPOCH = dt.datetime(1970, 1, 1)
_FAR_FUTURE = dt.datetime(3000, 1, 1)


def _silver_wishlist_schema() -> StructType:
    return StructType(
        [
            StructField("wishlist_event_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("product_id", StringType()),
            StructField("added_at", TimestampType()),
            StructField("source", StringType()),
        ]
    )


def _single_version_dim(
    spark: SparkSession,
    natural_keys: list[str],
    natural_key_col: str,
    sk_col: str,
) -> DataFrame:
    """One-row-per-key dim spanning [epoch, year=3000). Same helper as the
    gold test suite — kept local so this file is independent."""
    rows = [
        {
            sk_col: f"sk-{i:04d}",
            natural_key_col: nk,
            "effective_from": _EPOCH,
            "effective_to": _FAR_FUTURE,
            "is_current": True,
        }
        for i, nk in enumerate(natural_keys)
    ]
    schema = StructType(
        [
            StructField(sk_col, StringType(), nullable=False),
            StructField(natural_key_col, StringType(), nullable=False),
            StructField("effective_from", TimestampType(), nullable=False),
            StructField("effective_to", TimestampType(), nullable=False),
            StructField("is_current", BooleanType(), nullable=False),
        ]
    )
    return spark.createDataFrame(rows, schema)


def test_one_row_per_input_event(spark: SparkSession) -> None:
    rows = [
        ("e1", "c1", "p1", dt.datetime(2025, 5, 1, 10), "/products"),
        ("e2", "c1", "p1", dt.datetime(2025, 5, 2, 11), "/products"),  # same (c,p), different ts
        ("e3", "c2", "p1", dt.datetime(2025, 5, 1, 12), "/search"),
    ]
    df = spark.createDataFrame(rows, _silver_wishlist_schema())
    dim_c = _single_version_dim(spark, ["c1", "c2"], "customer_id", "customer_sk")
    dim_p = _single_version_dim(spark, ["p1"], "product_id", "product_sk")
    fact = build_fact_customer_wishlist_product(df, dim_c, dim_p).collect()
    assert len(fact) == 3
    # Per-event grain preserves the re-add: e1 and e2 are both kept.
    assert {f["wishlist_event_id"] for f in fact} == {"e1", "e2", "e3"}


def test_event_sk_is_deterministic_sha256(spark: SparkSession) -> None:
    rows = [("e1", "c1", "p1", dt.datetime(2025, 5, 1), "/products")]
    df = spark.createDataFrame(rows, _silver_wishlist_schema())
    dim_c = _single_version_dim(spark, ["c1"], "customer_id", "customer_sk")
    dim_p = _single_version_dim(spark, ["p1"], "product_id", "product_sk")
    a = build_fact_customer_wishlist_product(df, dim_c, dim_p).first()["wishlist_event_sk"]
    b = build_fact_customer_wishlist_product(df, dim_c, dim_p).first()["wishlist_event_sk"]
    assert a == b
    assert len(a) == 64  # SHA-256 hex


def test_orphan_customer_yields_null_sk(spark: SparkSession) -> None:
    """Event for a customer not in dim_customer → customer_sk = NULL,
    row preserved (LEFT join), product_sk still resolved."""
    rows = [("e1", "c_missing", "p1", dt.datetime(2025, 5, 1), "/products")]
    df = spark.createDataFrame(rows, _silver_wishlist_schema())
    dim_c = _single_version_dim(spark, ["c_present"], "customer_id", "customer_sk")
    dim_p = _single_version_dim(spark, ["p1"], "product_id", "product_sk")
    fact = build_fact_customer_wishlist_product(df, dim_c, dim_p).first()
    assert fact["customer_sk"] is None
    assert fact["product_sk"] == "sk-0000"


def test_added_date_matches_added_at(spark: SparkSession) -> None:
    rows = [
        ("e1", "c1", "p1", dt.datetime(2025, 5, 1, 23, 59), "/products"),
        ("e2", "c1", "p1", dt.datetime(2025, 5, 2, 0, 0, 1), "/products"),
    ]
    df = spark.createDataFrame(rows, _silver_wishlist_schema())
    dim_c = _single_version_dim(spark, ["c1"], "customer_id", "customer_sk")
    dim_p = _single_version_dim(spark, ["p1"], "product_id", "product_sk")
    by_id = {f["wishlist_event_id"]: f for f in
             build_fact_customer_wishlist_product(df, dim_c, dim_p).collect()}
    assert by_id["e1"]["added_date"] == dt.date(2025, 5, 1)
    assert by_id["e2"]["added_date"] == dt.date(2025, 5, 2)


def test_updated_at_equals_added_at(spark: SparkSession) -> None:
    """Events are immutable — updated_at = added_at by design so MERGEs
    on wishlist_event_sk no-op on replay."""
    rows = [("e1", "c1", "p1", dt.datetime(2025, 5, 1, 10), "/products")]
    df = spark.createDataFrame(rows, _silver_wishlist_schema())
    dim_c = _single_version_dim(spark, ["c1"], "customer_id", "customer_sk")
    dim_p = _single_version_dim(spark, ["p1"], "product_id", "product_sk")
    fact = build_fact_customer_wishlist_product(df, dim_c, dim_p).first()
    assert fact["added_at"] == fact["updated_at"]


def test_output_column_set(spark: SparkSession) -> None:
    rows = [("e1", "c1", "p1", dt.datetime(2025, 5, 1, 10), "/products")]
    df = spark.createDataFrame(rows, _silver_wishlist_schema())
    dim_c = _single_version_dim(spark, ["c1"], "customer_id", "customer_sk")
    dim_p = _single_version_dim(spark, ["p1"], "product_id", "product_sk")
    fact = build_fact_customer_wishlist_product(df, dim_c, dim_p)
    expected = {
        "wishlist_event_sk", "wishlist_event_id",
        "customer_id", "customer_sk",
        "product_id", "product_sk",
        "added_at", "added_date", "source", "updated_at",
    }
    assert set(fact.columns) == expected
