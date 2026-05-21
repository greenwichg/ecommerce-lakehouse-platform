"""Tests for lambda/file_validator/handler.py.

Uses ``moto`` to mock S3 / SNS / CloudWatch. Each test sets up the
required AWS resources via boto3, populates the bucket with a known
file, invokes ``handler.validate``, and asserts the outcome / side
effects.
"""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

_LAMBDA_DIR = Path(__file__).resolve().parent.parent.parent / "lambda" / "file_validator"

_TEST_BUCKET = "lakehouse-test-bucket"
_TEST_TOPIC = "arn:aws:sns:us-east-1:123456789012:lakehouse-pipeline-alerts"


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub the AWS env vars before handler module's globals are read."""
    # Both file_validator and quarantine_helper have a top-level
    # ``handler.py`` module. Prepending the validator's lambda dir per-test
    # ensures we get the right one regardless of collection order; sys.path
    # is restored when the test ends.
    monkeypatch.syspath_prepend(str(_LAMBDA_DIR))
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("LAKEHOUSE_BUCKET", _TEST_BUCKET)
    monkeypatch.setenv("ALERTS_TOPIC_ARN", _TEST_TOPIC)
    # The handler module reads env at import time — force a fresh import.
    sys.modules.pop("handler", None)


@pytest.fixture()
def aws() -> object:
    """Mock AWS context with bucket + SNS topic pre-created."""
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=_TEST_BUCKET)
        sns = boto3.client("sns", region_name="us-east-1")
        sns.create_topic(Name="lakehouse-pipeline-alerts")
        yield {"s3": s3, "sns": sns}


def _orders_parquet(rows: int = 3) -> bytes:
    """Build a tiny Parquet payload that matches the orders schema."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    cols = {
        "order_id": [f"o{i}" for i in range(rows)],
        "customer_id": [f"c{i}" for i in range(rows)],
        "product_id": [f"p{i}" for i in range(rows)],
        "quantity": list(range(1, rows + 1)),
        "price": [9.99] * rows,
        "status": ["placed"] * rows,
        "created_at": [None] * rows,
        "updated_at": [None] * rows,
    }
    table = pa.table(cols)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _put(aws: dict, key: str, body: bytes) -> None:
    aws["s3"].put_object(Bucket=_TEST_BUCKET, Key=key, Body=body)


def test_source_from_key_matches_known_sources(aws: dict) -> None:
    from handler import source_from_key

    assert source_from_key("raw/orders/year=2025/month=05/day=01/orders.parquet") == "orders"
    assert source_from_key("raw/wishlist/year=2025/month=05/day=01/wishlist.json") == "wishlist"


def test_source_from_key_returns_none_for_unknown(aws: dict) -> None:
    from handler import source_from_key

    assert source_from_key("raw/unknown_source/...") is None
    assert source_from_key("bronze/orders/...") is None  # bronze isn't raw
    assert source_from_key("garbage") is None


def test_validate_orders_parquet_happy_path(aws: dict) -> None:
    from handler import validate

    key = "raw/orders/year=2025/month=05/day=01/orders.parquet"
    _put(aws, key, _orders_parquet(rows=5))
    outcome = validate(_TEST_BUCKET, key)
    assert outcome.severity == "validated"
    assert outcome.rows == 5
    assert outcome.source == "orders"


def test_validate_zero_byte_file_quarantines(aws: dict) -> None:
    from handler import validate

    key = "raw/orders/year=2025/month=05/day=01/orders.parquet"
    _put(aws, key, b"")
    outcome = validate(_TEST_BUCKET, key)
    assert outcome.severity == "quarantine"
    assert outcome.reason == "empty_file"


def test_validate_unparseable_parquet_quarantines(aws: dict) -> None:
    from handler import validate

    key = "raw/orders/year=2025/month=05/day=01/orders.parquet"
    _put(aws, key, b"this is not parquet")
    outcome = validate(_TEST_BUCKET, key)
    assert outcome.severity == "quarantine"
    assert outcome.reason.startswith("unreadable_parquet")


def test_validate_missing_required_column_quarantines(aws: dict) -> None:
    """Build a Parquet without ``customer_id`` — orders requires it."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from handler import validate

    table = pa.table({"order_id": ["o1"], "quantity": [1]})
    buf = io.BytesIO()
    pq.write_table(table, buf)

    key = "raw/orders/year=2025/month=05/day=01/orders.parquet"
    _put(aws, key, buf.getvalue())
    outcome = validate(_TEST_BUCKET, key)
    assert outcome.severity == "quarantine"
    assert "missing_columns" in outcome.reason
    assert "customer_id" in outcome.reason


def test_validate_zero_rows_quarantines(aws: dict) -> None:
    """Empty Parquet (header but no data rows)."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from handler import validate

    schema = pa.schema(
        [
            ("order_id", pa.string()),
            ("customer_id", pa.string()),
            ("product_id", pa.string()),
            ("quantity", pa.int64()),
            ("price", pa.float64()),
            ("status", pa.string()),
            ("created_at", pa.timestamp("us")),
            ("updated_at", pa.timestamp("us")),
        ]
    )
    empty = pa.table(
        {n: [] for n, _ in zip(schema.names, schema.types, strict=False)}, schema=schema
    )
    buf = io.BytesIO()
    pq.write_table(empty, buf)
    key = "raw/orders/year=2025/month=05/day=01/orders.parquet"
    _put(aws, key, buf.getvalue())
    outcome = validate(_TEST_BUCKET, key)
    assert outcome.severity == "quarantine"
    assert outcome.reason == "zero_rows"


def test_validate_csv_products_happy_path(aws: dict) -> None:
    from handler import validate

    body = (
        b"product_id,product_name,category,price,sku,updated_at\n"
        b"PRD-0042-00001,Widget,electronics,9.99,SKU-1234567,2025-05-01T00:00:00+00:00\n"
    )
    key = "raw/products/year=2025/month=05/day=01/products.csv"
    _put(aws, key, body)
    outcome = validate(_TEST_BUCKET, key)
    assert outcome.severity == "validated"
    assert outcome.rows == 1


def test_validate_jsonl_clickstream_happy_path(aws: dict) -> None:
    from handler import validate

    lines = [
        json.dumps(
            {
                "event_id": "e1",
                "session_id": "s1",
                "customer_id": "c1",
                "event_type": "page_view",
                "page_url": "/",
                "event_ts": "2025-05-01T10:00:00+00:00",
                "device": "desktop",
                "user_agent": "ua",
            }
        ),
        json.dumps(
            {
                "event_id": "e2",
                "session_id": "s1",
                "customer_id": "c1",
                "event_type": "page_view",
                "page_url": "/p",
                "event_ts": "2025-05-01T10:05:00+00:00",
                "device": "desktop",
                "user_agent": "ua",
            }
        ),
    ]
    body = ("\n".join(lines) + "\n").encode()
    key = "raw/clickstream/year=2025/month=05/day=01/hour=10/events.json"
    _put(aws, key, body)
    outcome = validate(_TEST_BUCKET, key)
    assert outcome.severity == "validated"
    assert outcome.rows == 2


def test_validate_unexpected_column_emits_warning(aws: dict) -> None:
    """Extra columns beyond required → warn (schema evolution).
    Bronze handles new columns via addNewColumns; we log but don't reject."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from handler import validate

    cols = {
        "order_id": ["o1"],
        "customer_id": ["c1"],
        "product_id": ["p1"],
        "quantity": [1],
        "price": [9.99],
        "status": ["placed"],
        "created_at": [None],
        "updated_at": [None],
        "experiment_variant_id": ["A"],  # NEW unexpected column
    }
    buf = io.BytesIO()
    pq.write_table(pa.table(cols), buf)
    key = "raw/orders/year=2025/month=05/day=01/orders.parquet"
    _put(aws, key, buf.getvalue())
    outcome = validate(_TEST_BUCKET, key)
    assert outcome.severity == "warn"
    assert "experiment_variant_id" in outcome.warnings[0]


def test_unknown_source_quarantines(aws: dict) -> None:
    from handler import validate

    key = "raw/totally_made_up/foo.txt"
    _put(aws, key, b"hello")
    outcome = validate(_TEST_BUCKET, key)
    assert outcome.severity == "quarantine"
    assert outcome.reason == "unknown_source"


def test_quarantine_moves_file_to_quarantine_prefix(aws: dict) -> None:
    from handler import quarantine_file

    src = "raw/orders/year=2025/month=05/day=01/orders.parquet"
    _put(aws, src, b"contents")
    q_key = quarantine_file(_TEST_BUCKET, src)
    assert q_key == "quarantine/orders/year=2025/month=05/day=01/orders.parquet"
    # Original deleted
    with pytest.raises(aws["s3"].exceptions.ClientError):
        aws["s3"].head_object(Bucket=_TEST_BUCKET, Key=src)
    # Copy at quarantine path
    assert aws["s3"].get_object(Bucket=_TEST_BUCKET, Key=q_key)["Body"].read() == b"contents"


def test_manifest_appended_per_call(aws: dict) -> None:
    from handler import Outcome, write_manifest

    key1 = "raw/orders/year=2025/month=05/day=01/orders.parquet"
    key2 = "raw/orders/year=2025/month=05/day=02/orders.parquet"
    write_manifest(
        _TEST_BUCKET, key1, Outcome(severity="validated", reason="ok", source="orders", rows=10)
    )
    write_manifest(
        _TEST_BUCKET, key2, Outcome(severity="quarantine", reason="zero_rows", source="orders")
    )

    # Find the manifest object (path is keyed by today's date)
    response = aws["s3"].list_objects_v2(Bucket=_TEST_BUCKET, Prefix="processed/_manifests/")
    keys = [obj["Key"] for obj in response.get("Contents", [])]
    assert len(keys) == 1, "should produce a single manifest for today"
    body = aws["s3"].get_object(Bucket=_TEST_BUCKET, Key=keys[0])["Body"].read()
    lines = body.decode().strip().split("\n")
    assert len(lines) == 2
    payloads = [json.loads(line) for line in lines]
    assert {p["status"] for p in payloads} == {"validated", "quarantine"}


def test_handler_processes_sqs_batch(aws: dict) -> None:
    from handler import handler

    _put(aws, "raw/orders/year=2025/month=05/day=01/orders.parquet", _orders_parquet())
    _put(aws, "raw/orders/year=2025/month=05/day=02/orders.parquet", b"")

    event = {
        "Records": [
            {
                "body": json.dumps(
                    {
                        "Records": [
                            {
                                "s3": {
                                    "bucket": {"name": _TEST_BUCKET},
                                    "object": {
                                        "key": "raw/orders/year=2025/month=05/day=01/orders.parquet"
                                    },
                                }
                            },
                            {
                                "s3": {
                                    "bucket": {"name": _TEST_BUCKET},
                                    "object": {
                                        "key": "raw/orders/year=2025/month=05/day=02/orders.parquet"
                                    },
                                }
                            },
                        ]
                    }
                )
            }
        ]
    }
    result = handler(event, None)
    assert result["count"] == 2
    outcomes = {r["key"]: r["outcome"] for r in result["processed"]}
    assert outcomes["raw/orders/year=2025/month=05/day=01/orders.parquet"] == "validated"
    assert outcomes["raw/orders/year=2025/month=05/day=02/orders.parquet"] == "quarantine"


def test_handler_skips_invalid_sqs_body(aws: dict) -> None:
    """Malformed SQS body shouldn't crash the whole batch."""
    from handler import handler

    event = {"Records": [{"body": "not json"}]}
    result = handler(event, None)
    assert result["count"] == 0


def test_publish_alert_includes_severity_in_subject(aws: dict) -> None:
    """Subject capped at 100 chars (SNS limit) and includes severity."""
    from handler import Outcome, publish_alert

    # We can't easily peek at SNS message content via moto without a
    # subscribed endpoint; assert the call succeeds and the subject is
    # well-formed via mock.
    with patch("handler.sns.publish") as publish_mock:
        publish_alert(
            Outcome(severity="quarantine", reason="x", source="orders"),
            "raw/orders/year=2025/month=05/day=01/orders.parquet",
        )
        publish_mock.assert_called_once()
        call_kwargs = publish_mock.call_args.kwargs
        assert "quarantine" in call_kwargs["Subject"]
        assert "orders" in call_kwargs["Subject"]
        assert len(call_kwargs["Subject"]) <= 100
