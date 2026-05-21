"""Tests for lambda/quarantine_helper/handler.py.

Mocks S3 + Secrets Manager via moto. Airflow REST API calls are
mocked via urllib monkeypatching since moto doesn't proxy external
HTTPS.
"""

from __future__ import annotations

import json
import sys
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

_LAMBDA_DIR = Path(__file__).resolve().parent.parent.parent / "lambda" / "quarantine_helper"
sys.path.insert(0, str(_LAMBDA_DIR))

_TEST_BUCKET = "lakehouse-test-bucket"
_AIRFLOW_SECRET = "lakehouse-dev/airflow-api-creds"


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("LAKEHOUSE_BUCKET", _TEST_BUCKET)
    monkeypatch.setenv("AIRFLOW_SECRET_NAME", _AIRFLOW_SECRET)
    sys.modules.pop("handler", None)


@pytest.fixture()
def aws() -> object:
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=_TEST_BUCKET)
        sm = boto3.client("secretsmanager", region_name="us-east-1")
        sm.create_secret(
            Name=_AIRFLOW_SECRET,
            SecretString=json.dumps({
                "base_url": "https://airflow.example.com",
                "username": "lakehouse",
                "password": "secret",
            }),
        )
        yield {"s3": s3, "sm": sm}


def test_discard_deletes_file_and_writes_audit(aws: dict) -> None:
    from handler import discard

    key = "quarantine/orders/year=2025/month=05/day=01/orders.parquet"
    aws["s3"].put_object(Bucket=_TEST_BUCKET, Key=key, Body=b"contents")

    result = discard(key, operator="alice", reason="upstream dead")
    assert result == {"discarded": True, "key": key}

    # File is gone
    with pytest.raises(aws["s3"].exceptions.ClientError):
        aws["s3"].head_object(Bucket=_TEST_BUCKET, Key=key)

    # Audit log written today
    objects = aws["s3"].list_objects_v2(
        Bucket=_TEST_BUCKET, Prefix="processed/_quarantine_audit/"
    )["Contents"]
    assert len(objects) == 1
    body = aws["s3"].get_object(Bucket=_TEST_BUCKET, Key=objects[0]["Key"])["Body"].read()
    entry = json.loads(body.decode().strip())
    assert entry["action"] == "discard"
    assert entry["operator"] == "alice"
    assert entry["reason"] == "upstream dead"
    assert entry["key"] == key


def test_move_to_raw_copies_and_deletes_quarantine(aws: dict) -> None:
    from handler import move_to_raw

    q_key = "quarantine/orders/year=2025/month=05/day=01/orders.parquet"
    aws["s3"].put_object(Bucket=_TEST_BUCKET, Key=q_key, Body=b"corrected contents")
    result = move_to_raw(q_key, operator="alice")
    assert result["from"] == q_key
    assert result["to"] == "raw/orders/year=2025/month=05/day=01/orders.parquet"

    # Copy at raw
    assert aws["s3"].get_object(Bucket=_TEST_BUCKET, Key=result["to"])["Body"].read() == b"corrected contents"
    # Quarantine deleted
    with pytest.raises(aws["s3"].exceptions.ClientError):
        aws["s3"].head_object(Bucket=_TEST_BUCKET, Key=q_key)


def test_move_to_raw_rejects_non_quarantine_key(aws: dict) -> None:
    from handler import move_to_raw

    with pytest.raises(ValueError, match="expected key under quarantine/"):
        move_to_raw("raw/orders/foo.parquet", operator="alice")


def test_poll_for_fix_returns_present_when_file_exists(aws: dict) -> None:
    from handler import poll_for_fix

    q_key = "quarantine/orders/year=2025/month=05/day=01/orders.parquet"
    fix_key = "quarantine/orders/year=2025/month=05/day=01/_fixed/orders.parquet"
    aws["s3"].put_object(Bucket=_TEST_BUCKET, Key=fix_key, Body=b"fixed!")
    result = poll_for_fix(q_key)
    assert result == {"present": True, "key": fix_key}


def test_poll_for_fix_returns_absent_when_missing(aws: dict) -> None:
    from handler import poll_for_fix

    q_key = "quarantine/orders/year=2025/month=05/day=01/orders.parquet"
    result = poll_for_fix(q_key)
    assert result == {
        "present": False,
        "key": "quarantine/orders/year=2025/month=05/day=01/_fixed/orders.parquet",
    }


def test_trigger_airflow_posts_to_airflow_rest_api(aws: dict) -> None:
    from handler import trigger_airflow

    class FakeResp:
        def __init__(self, payload: dict) -> None:
            self._payload = json.dumps(payload).encode()
        def read(self) -> bytes:
            return self._payload
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass

    fake_response = FakeResp({"dag_run_id": "manual__abc123", "state": "queued"})

    with patch("handler.urllib.request.urlopen", return_value=fake_response) as urlopen_mock:
        result = trigger_airflow(dag_id="daily_batch_pipeline", logical_date="2025-05-01T00:00:00+00:00")

    assert result["triggered"] is True
    assert result["response"]["dag_run_id"] == "manual__abc123"
    # Confirm the URL + auth header
    req = urlopen_mock.call_args[0][0]
    assert req.full_url.endswith("/api/v1/dags/daily_batch_pipeline/dagRuns")
    assert req.headers["Authorization"].startswith("Basic ")


def test_handler_routes_on_action(aws: dict) -> None:
    from handler import handler

    q_key = "quarantine/orders/year=2025/month=05/day=01/orders.parquet"
    aws["s3"].put_object(Bucket=_TEST_BUCKET, Key=q_key, Body=b"contents")

    result = handler(
        {"action": "discard", "quarantine_key": q_key, "operator": "alice", "reason": "test"},
        None,
    )
    assert result == {"discarded": True, "key": q_key}


def test_handler_rejects_unknown_action(aws: dict) -> None:
    from handler import handler

    with pytest.raises(ValueError, match="unknown action"):
        handler({"action": "nuke_everything"}, None)


def test_audit_log_appends_across_calls(aws: dict) -> None:
    """Two discard calls produce two JSONL lines in the same audit file."""
    from handler import discard

    key1 = "quarantine/orders/year=2025/month=05/day=01/a.parquet"
    key2 = "quarantine/orders/year=2025/month=05/day=01/b.parquet"
    aws["s3"].put_object(Bucket=_TEST_BUCKET, Key=key1, Body=b"x")
    aws["s3"].put_object(Bucket=_TEST_BUCKET, Key=key2, Body=b"y")
    discard(key1, operator="a", reason="r1")
    discard(key2, operator="a", reason="r2")

    objects = aws["s3"].list_objects_v2(
        Bucket=_TEST_BUCKET, Prefix="processed/_quarantine_audit/"
    )["Contents"]
    assert len(objects) == 1
    body = aws["s3"].get_object(Bucket=_TEST_BUCKET, Key=objects[0]["Key"])["Body"].read()
    lines = body.decode().strip().split("\n")
    assert len(lines) == 2
