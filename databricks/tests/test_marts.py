"""Tests for libs.marts.

Customer LTV uses window functions (LAG, AVG over a partition) so the
test data has to span a meaningful customer order history — multiple
orders per customer at staggered timestamps.
"""

from __future__ import annotations

import datetime as dt

from libs.marts import build_customer_ltv
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _fact_orders_schema() -> StructType:
    """Minimal fact_orders shape that build_customer_ltv actually reads."""
    return StructType(
        [
            StructField("customer_sk", StringType()),
            StructField("created_at", TimestampType()),
            StructField("total_amount", DoubleType()),
            StructField("status", StringType()),
        ]
    )


def _orders(spark: SparkSession, rows: list[tuple]) -> object:
    return spark.createDataFrame(rows, _fact_orders_schema())


def test_single_customer_single_order(spark: SparkSession) -> None:
    rows = [("c1", dt.datetime(2025, 5, 1), 100.0, "delivered")]
    ltv = build_customer_ltv(_orders(spark, rows)).first()
    assert ltv["customer_sk"] == "c1"
    assert ltv["total_revenue"] == 100.0
    assert ltv["order_count"] == 1
    assert ltv["first_order_date"] == dt.date(2025, 5, 1)
    assert ltv["last_order_date"] == dt.date(2025, 5, 1)
    assert ltv["avg_days_between_orders"] is None  # only 1 order, no prior


def test_multi_order_customer_avg_days(spark: SparkSession) -> None:
    """3 orders on day 1, 11, 21 → gaps of 10, 10 → avg = 10."""
    rows = [
        ("c1", dt.datetime(2025, 5, 1), 50.0, "delivered"),
        ("c1", dt.datetime(2025, 5, 11), 75.0, "delivered"),
        ("c1", dt.datetime(2025, 5, 21), 100.0, "delivered"),
    ]
    ltv = build_customer_ltv(_orders(spark, rows)).first()
    assert ltv["order_count"] == 3
    assert ltv["total_revenue"] == 225.0
    assert abs(ltv["avg_days_between_orders"] - 10.0) < 0.01


def test_cancelled_orders_excluded_from_ltv(spark: SparkSession) -> None:
    rows = [
        ("c1", dt.datetime(2025, 5, 1), 100.0, "delivered"),
        ("c1", dt.datetime(2025, 5, 5), 999.0, "cancelled"),  # excluded
        ("c1", dt.datetime(2025, 5, 10), 50.0, "delivered"),
    ]
    ltv = build_customer_ltv(_orders(spark, rows)).first()
    assert ltv["total_revenue"] == 150.0
    assert ltv["order_count"] == 2


def test_predicted_churn_flag_true_for_lapsed_customer(spark: SparkSession) -> None:
    """Customer with multiple orders, last one 120 days ago → churned."""
    today = dt.date(2025, 9, 1)
    rows = [
        ("c1", dt.datetime(2025, 1, 1), 100.0, "delivered"),
        ("c1", dt.datetime(2025, 1, 15), 200.0, "delivered"),
        ("c1", dt.datetime(2025, 5, 3), 75.0, "delivered"),  # 120 days before today
    ]
    ltv = build_customer_ltv(_orders(spark, rows), today=today).first()
    assert ltv["predicted_churn_flag"] is True


def test_predicted_churn_flag_false_for_recent_customer(spark: SparkSession) -> None:
    """Customer who ordered in the last 90 days → not churned."""
    today = dt.date(2025, 9, 1)
    rows = [
        ("c1", dt.datetime(2025, 6, 1), 100.0, "delivered"),
        ("c1", dt.datetime(2025, 8, 25), 200.0, "delivered"),  # 7 days before today
    ]
    ltv = build_customer_ltv(_orders(spark, rows), today=today).first()
    assert ltv["predicted_churn_flag"] is False


def test_predicted_churn_flag_false_for_single_order_customer(spark: SparkSession) -> None:
    """Single-order customers ('trial never returned') don't trigger churn —
    we can't distinguish them from brand-new customers."""
    today = dt.date(2025, 9, 1)
    rows = [
        ("c1", dt.datetime(2024, 5, 1), 100.0, "delivered"),  # 480 days ago
    ]
    ltv = build_customer_ltv(_orders(spark, rows), today=today).first()
    assert ltv["predicted_churn_flag"] is False


def test_today_defaults_to_max_created_at(spark: SparkSession) -> None:
    """If today isn't passed, the function infers it from the input. This
    makes historical re-runs deterministic — running the mart for last
    week's state produces last week's churn flags."""
    rows = [
        ("c1", dt.datetime(2024, 1, 1), 100.0, "delivered"),
        ("c1", dt.datetime(2024, 1, 15), 200.0, "delivered"),
        ("c1", dt.datetime(2024, 5, 1), 100.0, "delivered"),  # max_ts → "today"
    ]
    ltv = build_customer_ltv(_orders(spark, rows)).first()
    # Last order is at the inferred "today", so days-since-last = 0, not churned
    assert ltv["predicted_churn_flag"] is False


def test_multiple_customers_separate_aggregates(spark: SparkSession) -> None:
    rows = [
        ("c1", dt.datetime(2025, 5, 1), 100.0, "delivered"),
        ("c1", dt.datetime(2025, 5, 5), 200.0, "delivered"),
        ("c2", dt.datetime(2025, 5, 2), 50.0, "delivered"),
    ]
    result = {r["customer_sk"]: r for r in build_customer_ltv(_orders(spark, rows)).collect()}
    assert result["c1"]["total_revenue"] == 300.0
    assert result["c1"]["order_count"] == 2
    assert result["c2"]["total_revenue"] == 50.0
    assert result["c2"]["order_count"] == 1


def test_only_cancelled_customer_excluded_entirely(spark: SparkSession) -> None:
    """A customer whose ONLY orders were cancelled doesn't appear in the LTV
    table at all (the filter happens before aggregation)."""
    rows = [
        ("c1", dt.datetime(2025, 5, 1), 50.0, "delivered"),
        ("c2", dt.datetime(2025, 5, 1), 999.0, "cancelled"),
    ]
    customer_sks = {r["customer_sk"] for r in build_customer_ltv(_orders(spark, rows)).collect()}
    assert customer_sks == {"c1"}


def test_avg_days_between_orders_handles_microsecond_precision(spark: SparkSession) -> None:
    """Computed as decimal days, not whole datediff. 12-hour gap → 0.5."""
    rows = [
        ("c1", dt.datetime(2025, 5, 1, 10, 0), 100.0, "delivered"),
        ("c1", dt.datetime(2025, 5, 1, 22, 0), 100.0, "delivered"),  # +12h
    ]
    ltv = build_customer_ltv(_orders(spark, rows)).first()
    assert abs(ltv["avg_days_between_orders"] - 0.5) < 0.01
