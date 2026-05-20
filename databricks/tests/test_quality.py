"""Tests for libs.quality."""

from __future__ import annotations

import datetime as dt

from libs.quality import ORDERS_RULES, Rule, split_quarantine
from pyspark.sql import SparkSession


def test_empty_rules_all_good(spark: SparkSession) -> None:
    df = spark.createDataFrame([("o1",), ("o2",)], ["order_id"])
    good, bad = split_quarantine(df, [])
    assert good.count() == 2
    assert bad.count() == 0
    assert "_quarantine_reason" in bad.columns  # added for schema compat


def test_all_pass_when_rules_satisfied(spark: SparkSession) -> None:
    rules = [Rule("not_null", "order_id IS NOT NULL")]
    df = spark.createDataFrame([("o1",), ("o2",)], ["order_id"])
    good, bad = split_quarantine(df, rules)
    assert good.count() == 2
    assert bad.count() == 0


def test_one_rule_fails(spark: SparkSession) -> None:
    rules = [Rule("not_null", "order_id IS NOT NULL")]
    df = spark.createDataFrame([("o1",), (None,)], ["order_id"])
    good, bad = split_quarantine(df, rules)
    assert good.count() == 1
    assert bad.count() == 1
    assert bad.first()["_quarantine_reason"] == "not_null"


def test_multiple_rules_all_failing_listed_in_reason(spark: SparkSession) -> None:
    rules = [
        Rule("qty_positive", "qty > 0"),
        Rule("price_positive", "price > 0"),
    ]
    df = spark.createDataFrame([(-1, -1.0)], ["qty", "price"])
    good, bad = split_quarantine(df, rules)
    assert good.count() == 0
    assert bad.count() == 1
    reason = bad.first()["_quarantine_reason"]
    assert "qty_positive" in reason
    assert "price_positive" in reason


def test_quarantine_preserves_other_columns(spark: SparkSession) -> None:
    rules = [Rule("not_null", "order_id IS NOT NULL")]
    df = spark.createDataFrame([("o1", "extra1"), (None, "extra2")], ["order_id", "extra"])
    good, bad = split_quarantine(df, rules)
    assert "extra" in good.columns
    assert "extra" in bad.columns
    assert bad.first()["extra"] == "extra2"


def test_orders_rules_smoke(spark: SparkSession) -> None:
    """End-to-end ORDERS_RULES applied to representative valid/invalid rows."""
    now = dt.datetime(2025, 5, 20, 12, 0, 0)
    rows = [
        {
            "order_id": "o1", "customer_id": "c1", "product_id": "p1",
            "quantity": 1, "price": 10.0, "status": "placed",
            "created_at": now - dt.timedelta(hours=2),
            "updated_at": now - dt.timedelta(hours=1),
        },
        {  # bad: status unknown
            "order_id": "o2", "customer_id": "c1", "product_id": "p1",
            "quantity": 1, "price": 10.0, "status": "WAT",
            "created_at": now - dt.timedelta(hours=1), "updated_at": now,
        },
        {  # bad: quantity <= 0
            "order_id": "o3", "customer_id": "c1", "product_id": "p1",
            "quantity": 0, "price": 10.0, "status": "placed",
            "created_at": now - dt.timedelta(hours=1), "updated_at": now,
        },
        {  # bad: negative price + null customer (two reasons)
            "order_id": "o4", "customer_id": None, "product_id": "p1",
            "quantity": 1, "price": -5.0, "status": "placed",
            "created_at": now - dt.timedelta(hours=1), "updated_at": now,
        },
    ]
    df = spark.createDataFrame(rows)
    good, bad = split_quarantine(df, ORDERS_RULES)
    assert good.count() == 1
    assert bad.count() == 3
    reasons = {row["order_id"]: row["_quarantine_reason"] for row in bad.collect()}
    assert "status_known" in reasons["o2"]
    assert "quantity_positive" in reasons["o3"]
    assert "customer_id_not_null" in reasons["o4"]
    assert "price_positive" in reasons["o4"]


def test_orders_rules_updated_at_before_created_at(spark: SparkSession) -> None:
    """Pathological clock skew where updated_at < created_at should be flagged."""
    now = dt.datetime(2025, 5, 20, 12, 0, 0)
    rows = [
        {
            "order_id": "o1", "customer_id": "c1", "product_id": "p1",
            "quantity": 1, "price": 10.0, "status": "placed",
            "created_at": now, "updated_at": now - dt.timedelta(hours=1),
        }
    ]
    df = spark.createDataFrame(rows)
    _, bad = split_quarantine(df, ORDERS_RULES)
    assert bad.count() == 1
    assert "updated_at_not_before_created" in bad.first()["_quarantine_reason"]
