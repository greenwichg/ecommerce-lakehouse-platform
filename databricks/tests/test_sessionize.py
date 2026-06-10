"""Tests for libs.sessionize.

Covers the gap-and-island sessionization + customer attribution.
The key correctness properties:
- Single-visit cookie gets exactly one session_seq (= 1).
- Multi-visit cookie (events > 30 min apart) gets ascending session_seqs.
- silver_session_key differs across visits of the same cookie.
- Anonymous events don't shadow non-NULL customer_id attribution.
- "Browse anonymous, sign in, purchase" flow attributes to the customer.
- Pure anonymous session: attributed_customer_id = NULL.
- Conversion flag: True iff any event has event_type='purchase'.
"""

from __future__ import annotations

import datetime as dt

from libs.sessionize import build_fact_sessions, sessionize_events
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _events_schema() -> StructType:
    return StructType(
        [
            StructField("event_id", StringType()),
            StructField("session_id", StringType()),
            StructField("customer_id", StringType(), nullable=True),
            StructField("event_type", StringType()),
            StructField("page_url", StringType()),
            StructField("event_ts", TimestampType()),
            StructField("device", StringType()),
            StructField("user_agent", StringType()),
        ]
    )


def _events(spark: SparkSession, rows: list[tuple]) -> object:
    return spark.createDataFrame(rows, _events_schema())


def test_single_visit_yields_session_seq_1(spark: SparkSession) -> None:
    """One cookie, 3 events all within 5 min → one session, seq=1."""
    rows = [
        ("e1", "cookie-A", "c1", "page_view", "/", dt.datetime(2025, 5, 1, 10, 0), "desktop", "ua"),
        (
            "e2",
            "cookie-A",
            "c1",
            "page_view",
            "/p",
            dt.datetime(2025, 5, 1, 10, 2),
            "desktop",
            "ua",
        ),
        (
            "e3",
            "cookie-A",
            "c1",
            "page_view",
            "/p2",
            dt.datetime(2025, 5, 1, 10, 5),
            "desktop",
            "ua",
        ),
    ]
    df = _events(spark, rows)
    out = sessionize_events(df).collect()
    seqs = {r["session_seq"] for r in out}
    keys = {r["silver_session_key"] for r in out}
    assert seqs == {1}
    assert len(keys) == 1, "single visit should yield single session_key"


def test_two_visits_with_gap_yields_two_sessions(spark: SparkSession) -> None:
    """Cookie revisits after 2h → two distinct session_seqs and keys."""
    rows = [
        # Visit 1
        ("e1", "cookie-A", "c1", "page_view", "/", dt.datetime(2025, 5, 1, 10, 0), "desktop", "ua"),
        (
            "e2",
            "cookie-A",
            "c1",
            "page_view",
            "/p",
            dt.datetime(2025, 5, 1, 10, 5),
            "desktop",
            "ua",
        ),
        # 2-hour gap
        # Visit 2
        ("e3", "cookie-A", "c1", "page_view", "/", dt.datetime(2025, 5, 1, 12, 5), "desktop", "ua"),
        (
            "e4",
            "cookie-A",
            "c1",
            "add_to_cart",
            "/cart",
            dt.datetime(2025, 5, 1, 12, 10),
            "desktop",
            "ua",
        ),
    ]
    df = _events(spark, rows)
    out = sessionize_events(df).collect()
    by_id = {r["event_id"]: r for r in out}
    assert by_id["e1"]["session_seq"] == 1
    assert by_id["e2"]["session_seq"] == 1
    assert by_id["e3"]["session_seq"] == 2
    assert by_id["e4"]["session_seq"] == 2
    # Distinct silver_session_keys for the two visits
    assert by_id["e1"]["silver_session_key"] != by_id["e3"]["silver_session_key"]


def test_gap_exactly_at_threshold_does_not_break_session(spark: SparkSession) -> None:
    """30-min gap = boundary case. We use strict > 30, so events exactly
    30 minutes apart stay in the same session. Documents the boundary."""
    rows = [
        ("e1", "cookie-A", None, "page_view", "/", dt.datetime(2025, 5, 1, 10, 0), "desktop", "ua"),
        (
            "e2",
            "cookie-A",
            None,
            "page_view",
            "/",
            dt.datetime(2025, 5, 1, 10, 30),
            "desktop",
            "ua",
        ),
    ]
    df = _events(spark, rows)
    out = sessionize_events(df).collect()
    seqs = {r["session_seq"] for r in out}
    assert seqs == {1}, f"30-min gap should stay in same session, got seqs={seqs}"


def test_session_key_deterministic_across_runs(spark: SparkSession) -> None:
    """silver_session_key = sha256(raw_session_id || '|' || session_seq).
    Two sessionizations on the same input must yield the same keys."""
    rows = [
        ("e1", "cookie-A", "c1", "page_view", "/", dt.datetime(2025, 5, 1, 10, 0), "desktop", "ua"),
        ("e2", "cookie-A", "c1", "page_view", "/", dt.datetime(2025, 5, 1, 12, 0), "desktop", "ua"),
    ]
    df = _events(spark, rows)
    keys_a = sorted(r["silver_session_key"] for r in sessionize_events(df).collect())
    keys_b = sorted(r["silver_session_key"] for r in sessionize_events(df).collect())
    assert keys_a == keys_b


def test_two_cookies_have_disjoint_session_keys(spark: SparkSession) -> None:
    rows = [
        ("e1", "cookie-A", "c1", "page_view", "/", dt.datetime(2025, 5, 1, 10, 0), "desktop", "ua"),
        ("e2", "cookie-B", "c2", "page_view", "/", dt.datetime(2025, 5, 1, 10, 0), "mobile", "ua2"),
    ]
    df = _events(spark, rows)
    out = sessionize_events(df).collect()
    assert out[0]["silver_session_key"] != out[1]["silver_session_key"]


def test_build_fact_sessions_aggregates_counts_and_timestamps(spark: SparkSession) -> None:
    rows = [
        ("e1", "cookie-A", "c1", "page_view", "/", dt.datetime(2025, 5, 1, 10, 0), "desktop", "ua"),
        (
            "e2",
            "cookie-A",
            "c1",
            "page_view",
            "/p",
            dt.datetime(2025, 5, 1, 10, 5),
            "desktop",
            "ua",
        ),
        (
            "e3",
            "cookie-A",
            "c1",
            "add_to_cart",
            "/cart",
            dt.datetime(2025, 5, 1, 10, 10),
            "desktop",
            "ua",
        ),
    ]
    df = _events(spark, rows)
    sessionized = sessionize_events(df)
    fact = build_fact_sessions(sessionized).first()
    assert fact["event_count"] == 3
    assert fact["distinct_event_types"] == 2
    assert fact["session_start"] == dt.datetime(2025, 5, 1, 10, 0)
    assert fact["session_end"] == dt.datetime(2025, 5, 1, 10, 10)
    assert fact["first_page_url"] == "/"
    assert fact["last_page_url"] == "/cart"
    assert fact["converted"] is False
    assert fact["attributed_customer_id"] == "c1"


def test_purchase_marks_converted_true(spark: SparkSession) -> None:
    rows = [
        ("e1", "cookie-A", "c1", "page_view", "/", dt.datetime(2025, 5, 1, 10, 0), "desktop", "ua"),
        (
            "e2",
            "cookie-A",
            "c1",
            "purchase",
            "/orders",
            dt.datetime(2025, 5, 1, 10, 15),
            "desktop",
            "ua",
        ),
    ]
    df = _events(spark, rows)
    fact = build_fact_sessions(sessionize_events(df)).first()
    assert fact["converted"] is True


def test_anonymous_session_yields_null_attributed_customer(spark: SparkSession) -> None:
    rows = [
        ("e1", "cookie-A", None, "page_view", "/", dt.datetime(2025, 5, 1, 10, 0), "mobile", "ua"),
        ("e2", "cookie-A", None, "page_view", "/p", dt.datetime(2025, 5, 1, 10, 5), "mobile", "ua"),
    ]
    df = _events(spark, rows)
    fact = build_fact_sessions(sessionize_events(df)).first()
    assert fact["attributed_customer_id"] is None


def test_anonymous_then_signin_attributes_to_customer(spark: SparkSession) -> None:
    """The headline attribution case: user browses anonymously, signs in,
    purchases. The session should attribute to the customer."""
    rows = [
        ("e1", "cookie-A", None, "page_view", "/", dt.datetime(2025, 5, 1, 10, 0), "desktop", "ua"),
        (
            "e2",
            "cookie-A",
            None,
            "page_view",
            "/p",
            dt.datetime(2025, 5, 1, 10, 3),
            "desktop",
            "ua",
        ),
        (
            "e3",
            "cookie-A",
            "c-signed-in",
            "add_to_cart",
            "/cart",
            dt.datetime(2025, 5, 1, 10, 8),
            "desktop",
            "ua",
        ),
        (
            "e4",
            "cookie-A",
            "c-signed-in",
            "purchase",
            "/orders",
            dt.datetime(2025, 5, 1, 10, 12),
            "desktop",
            "ua",
        ),
    ]
    df = _events(spark, rows)
    fact = build_fact_sessions(sessionize_events(df)).first()
    assert fact["attributed_customer_id"] == "c-signed-in"
    assert fact["converted"] is True


def test_two_visits_yield_two_fact_rows_with_distinct_attribution(spark: SparkSession) -> None:
    """One cookie, two visits, different sign-ins → two fact rows."""
    rows = [
        # Visit 1: anonymous browse
        ("e1", "cookie-A", None, "page_view", "/", dt.datetime(2025, 5, 1, 10, 0), "desktop", "ua"),
        # 2h gap
        # Visit 2: signed in as c1 then purchase
        ("e2", "cookie-A", "c1", "page_view", "/", dt.datetime(2025, 5, 1, 12, 0), "desktop", "ua"),
        (
            "e3",
            "cookie-A",
            "c1",
            "purchase",
            "/orders",
            dt.datetime(2025, 5, 1, 12, 10),
            "desktop",
            "ua",
        ),
    ]
    df = _events(spark, rows)
    facts = build_fact_sessions(sessionize_events(df)).collect()
    assert len(facts) == 2
    by_count = {f["event_count"]: f for f in facts}
    # Anonymous visit: 1 event, no customer, not converted
    assert by_count[1]["attributed_customer_id"] is None
    assert by_count[1]["converted"] is False
    # Signed-in visit: 2 events, c1, converted
    assert by_count[2]["attributed_customer_id"] == "c1"
    assert by_count[2]["converted"] is True


def test_max_by_ignores_null_customer_in_middle(spark: SparkSession) -> None:
    """A session with [non-null, null, non-null, null] customer_ids should
    attribute to the LAST non-null, not be confused by the trailing null."""
    rows = [
        ("e1", "cookie-A", "c1", "page_view", "/", dt.datetime(2025, 5, 1, 10, 0), "desktop", "ua"),
        (
            "e2",
            "cookie-A",
            None,
            "page_view",
            "/p",
            dt.datetime(2025, 5, 1, 10, 3),
            "desktop",
            "ua",
        ),
        (
            "e3",
            "cookie-A",
            "c2",
            "page_view",
            "/p2",
            dt.datetime(2025, 5, 1, 10, 6),
            "desktop",
            "ua",
        ),
        (
            "e4",
            "cookie-A",
            None,
            "page_view",
            "/p3",
            dt.datetime(2025, 5, 1, 10, 9),
            "desktop",
            "ua",
        ),
    ]
    df = _events(spark, rows)
    fact = build_fact_sessions(sessionize_events(df)).first()
    assert fact["attributed_customer_id"] == "c2"
