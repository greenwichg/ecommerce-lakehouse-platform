"""End-to-end demo: inject a malformed file, watch it get quarantined,
walk through the operator-decision flow, replay it.

Designed to be runnable two ways:

- ``python demo/inject_bad_file.py`` (mock mode, default)
    Spins up an in-process moto AWS, creates the bucket + SQS queue,
    drops a malformed Parquet file into raw/orders/, calls the
    validator handler directly on the resulting SQS event,
    asserts the file moved to quarantine/, simulates the
    SNS publish, and prints the suggested ``send-task-success``
    commands for each of the three decision branches. With
    ``--auto-replay`` it also drives the replay path end-to-end
    (helper Lambda's ``move_to_raw`` + ``trigger_airflow`` invocations).
    Runs anywhere — no AWS account, no network.

- ``python demo/inject_bad_file.py --live`` (real AWS)
    Uses your default boto3 credentials. Requires ``LAKEHOUSE_BUCKET``,
    ``LAKEHOUSE_ENV``, and the resources from ``terraform apply``.
    Drops a bad file into the real raw/ prefix and lets the Lambda
    chain fire. The script polls SQS / SFN to surface what happened.

Why both modes: the mock path is the demo a recruiter can run from a
laptop in 30 seconds. The live path is the one we'd actually use in
ops when reproducing a real quarantine incident.
"""

from __future__ import annotations

import argparse
import importlib.util
import io
import json
import sys
import time
from datetime import date
from pathlib import Path
from types import ModuleType

import boto3

_REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_handler(lambda_name: str) -> ModuleType:
    """Load lambda/<lambda_name>/handler.py as a uniquely-named module.

    Avoids the sys.path-shadowing problem when both lambda dirs expose
    a top-level ``handler.py``: we never put either dir on sys.path —
    we import each handler under a distinct module name via importlib.
    """
    path = _REPO_ROOT / "lambda" / lambda_name / "handler.py"
    mod_name = f"_demo_{lambda_name}_handler"
    spec = importlib.util.spec_from_file_location(mod_name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    # Register in sys.modules BEFORE exec — @dataclass looks up
    # cls.__module__ via sys.modules and crashes if it's not there.
    sys.modules[mod_name] = module
    spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------
# Bad-file generator
# ---------------------------------------------------------------------------


def _make_bad_parquet() -> bytes:
    """Build a Parquet file missing the required ``customer_id`` column.

    The validator's REQUIRED_COLUMNS["orders"] includes customer_id, so
    this file will be quarantined with reason ``missing_columns:
    customer_id``.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({
        "order_id": ["bad-1", "bad-2", "bad-3"],
        "product_id": ["p1", "p2", "p3"],
        "quantity": [1, 2, 3],
        "price": [9.99, 19.99, 29.99],
        # customer_id deliberately omitted
    })
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _bad_key() -> str:
    today = date.today()
    return f"raw/orders/year={today.year}/month={today.month:02d}/day={today.day:02d}/orders.parquet"


# ---------------------------------------------------------------------------
# Mock-mode walkthrough
# ---------------------------------------------------------------------------


def run_mock(*, auto_replay: bool) -> int:
    """In-process simulation of the full quarantine flow.

    Returns the suggested next-action exit code (0 for success, 1 for
    any unexpected outcome).
    """
    from moto import mock_aws

    print("=" * 70)
    print("DEMO — Inject malformed orders file → quarantine → operator → replay")
    print("=" * 70)
    print()
    print("Mode: MOCK (in-process moto AWS; no network, no credentials)")
    print()

    # Reset env so the validator handler reads our test values
    import os as _os
    _os.environ["LAKEHOUSE_BUCKET"] = "demo-lakehouse-bucket"
    _os.environ["ALERTS_TOPIC_ARN"] = "arn:aws:sns:us-east-1:123456789012:demo-alerts"
    _os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    _os.environ["AIRFLOW_SECRET_NAME"] = "demo/airflow-api-creds"

    # Force re-import after env is set so the handler picks up our values
    sys.modules.pop("handler", None)

    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        sns = boto3.client("sns", region_name="us-east-1")
        sm = boto3.client("secretsmanager", region_name="us-east-1")

        s3.create_bucket(Bucket="demo-lakehouse-bucket")
        sns_resp = sns.create_topic(Name="demo-alerts")
        _os.environ["ALERTS_TOPIC_ARN"] = sns_resp["TopicArn"]
        sm.create_secret(
            Name="demo/airflow-api-creds",
            SecretString=json.dumps({
                "base_url": "https://demo.airflow.example.com",
                "username": "demo",
                "password": "demo",
            }),
        )

        # ---- Step 1: drop a malformed file in raw/ ----
        key = _bad_key()
        body = _make_bad_parquet()
        s3.put_object(Bucket="demo-lakehouse-bucket", Key=key, Body=body)

        print(f"[1] Dropped malformed file: s3://demo-lakehouse-bucket/{key}")
        print(f"    Size: {len(body)} bytes, missing 'customer_id' column")
        print()

        # ---- Step 2: validator processes it ----
        # The real flow is S3 → SQS → Lambda; in mock mode we invoke
        # the handler directly with the SQS-shaped event.
        validator_mod = _load_handler("file_validator")
        validator_handler = validator_mod.handler
        sqs_event = {
            "Records": [{
                "body": json.dumps({
                    "Records": [{
                        "s3": {
                            "bucket": {"name": "demo-lakehouse-bucket"},
                            "object": {"key": key},
                        }
                    }]
                })
            }]
        }
        validator_result = validator_handler(sqs_event, None)
        print(f"[2] Validator processed: {validator_result['count']} record(s)")
        for outcome in validator_result["processed"]:
            print(f"    - {outcome['key']}: outcome={outcome['outcome']}, reason={outcome.get('reason')}")
        print()

        # ---- Step 3: confirm file is in quarantine ----
        # The validator moves bad files: deletes raw/, writes quarantine/.
        quarantine_key = key.replace("raw/", "quarantine/", 1)
        try:
            s3.head_object(Bucket="demo-lakehouse-bucket", Key=quarantine_key)
            print(f"[3] File moved to quarantine: s3://demo-lakehouse-bucket/{quarantine_key}")
        except s3.exceptions.ClientError:
            print(f"[3] ✗ Expected file at quarantine/{quarantine_key}, not found")
            return 1

        # Original location should be gone
        try:
            s3.head_object(Bucket="demo-lakehouse-bucket", Key=key)
            print(f"    ✗ Original raw/ file still present (should have been deleted)")
            return 1
        except s3.exceptions.ClientError:
            pass
        print()

        # ---- Step 4: operator-decision branches ----
        # In real AWS, SFN's WaitForOperatorDecision state holds a task
        # token that the operator references via send-task-success. In
        # mock mode we print the three suggested commands so a viewer
        # sees the operator UX.
        fake_token = "AAAAKgAAAAIAAAAAAAAAAS9...mock-demo-token..."
        print("[4] Step Functions execution started (mocked).")
        print("    Operator now picks one of:")
        print()
        for decision, blurb in [
            ("replay", "The file is actually fine — re-run pipeline"),
            ("discard", "Upstream is broken; abandon this day's data"),
            ("fix-and-replay", "We'll upload a corrected file to _fixed/"),
        ]:
            print(f"    # {decision}: {blurb}")
            print(f"    aws stepfunctions send-task-success \\")
            print(f"        --task-token '{fake_token}' \\")
            print(f"        --task-output '{json.dumps({'decision': decision, 'operator': 'demo@example.com'})}'")
            print()

        if not auto_replay:
            print("Run again with --auto-replay to drive the replay path end-to-end.")
            return 0

        # ---- Step 5: auto-replay path ----
        # The replay path: helper Lambda's move_to_raw → trigger_airflow.
        # We call the helper handler functions directly.
        print("[5] AUTO-REPLAY: operator chose 'replay'.")
        helper_mod = _load_handler("quarantine_helper")

        move_result = helper_mod.move_to_raw(quarantine_key, operator="demo@example.com")
        print(f"    helper.move_to_raw: from={move_result['from']}")
        print(f"                        to={move_result['to']}")

        # trigger_airflow does an HTTP POST — patch its urllib import to
        # avoid hitting a real Airflow during the demo.
        from unittest.mock import patch

        class FakeResp:
            def read(self): return b'{"dag_run_id": "manual__demo", "state": "queued"}'
            def __enter__(self): return self
            def __exit__(self, *a): pass

        with patch.object(helper_mod.urllib.request, "urlopen", return_value=FakeResp()):
            trigger_result = helper_mod.trigger_airflow(
                dag_id="daily_batch_pipeline",
                logical_date=f"{date.today().isoformat()}T00:00:00+00:00",
            )
        print(f"    helper.trigger_airflow: triggered={trigger_result['triggered']}, "
              f"dag_run_id={trigger_result['response']['dag_run_id']}")
        print()

        # ---- Step 6: validate the corrected file would land cleanly ----
        # Drop a WELL-FORMED orders file in raw/ now and re-run the
        # validator. The replay path expects this file to land via the
        # same S3 → SQS → Lambda chain, so re-running the validator
        # against it proves the loop closes.
        good_body = _make_good_parquet()
        s3.put_object(Bucket="demo-lakehouse-bucket", Key=key, Body=good_body)
        # Re-load the validator module so its module-level S3 client picks
        # up the moto session that was reset by put_object (it wasn't — but
        # importing fresh keeps the script defensive).
        validator_mod_again = _load_handler("file_validator")
        result2 = validator_mod_again.handler(sqs_event, None)
        outcome = result2["processed"][0]
        print(f"[6] Replay validation: outcome={outcome['outcome']}, reason={outcome.get('reason')}")
        if outcome["outcome"] != "validated":
            print(f"    ✗ Expected 'validated', got '{outcome['outcome']}'")
            return 1
        print()

        print("=" * 70)
        print("DEMO COMPLETE — quarantine → operator → replay loop closed successfully")
        print("=" * 70)
        return 0


def _make_good_parquet() -> bytes:
    """Well-formed orders parquet — counterpart to _make_bad_parquet."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({
        "order_id": ["good-1", "good-2", "good-3"],
        "customer_id": ["c1", "c2", "c3"],
        "product_id": ["p1", "p2", "p3"],
        "quantity": [1, 2, 3],
        "price": [9.99, 19.99, 29.99],
        "status": ["placed", "placed", "placed"],
        "created_at": [None, None, None],
        "updated_at": [None, None, None],
    })
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Live-mode walkthrough
# ---------------------------------------------------------------------------


def run_live() -> int:
    """Real AWS variant. Drops a bad file into the configured bucket
    and polls for the quarantine + SFN execution."""
    import os

    bucket = os.environ.get("LAKEHOUSE_BUCKET")
    if not bucket:
        print("✗ LAKEHOUSE_BUCKET env var not set")
        return 1

    s3 = boto3.client("s3")
    sfn = boto3.client("stepfunctions")

    print("=" * 70)
    print("DEMO — Live AWS mode")
    print("=" * 70)
    print(f"Bucket: {bucket}")
    print()

    key = _bad_key()
    body = _make_bad_parquet()

    print(f"[1] Dropping malformed file: s3://{bucket}/{key}")
    s3.put_object(Bucket=bucket, Key=key, Body=body)
    print()

    # Poll for the file landing in quarantine/ — up to 90 seconds.
    quarantine_key = key.replace("raw/", "quarantine/", 1)
    print(f"[2] Polling for quarantine landing: s3://{bucket}/{quarantine_key}")
    deadline = time.time() + 90
    found = False
    while time.time() < deadline:
        try:
            s3.head_object(Bucket=bucket, Key=quarantine_key)
            found = True
            break
        except s3.exceptions.ClientError:
            time.sleep(3)
    if not found:
        print("    ✗ Quarantine file never appeared. Check Lambda logs.")
        return 1
    print("    ✓ File quarantined.")
    print()

    # Find the SFN execution
    state_machine_arn = os.environ.get("SFN_STATE_MACHINE_ARN")
    if state_machine_arn:
        print("[3] Looking for SFN execution waiting on operator decision...")
        execs = sfn.list_executions(stateMachineArn=state_machine_arn, statusFilter="RUNNING")
        if execs["executions"]:
            ex = execs["executions"][0]
            detail = sfn.describe_execution(executionArn=ex["executionArn"])
            print(f"    Execution: {ex['name']}")
            print(f"    Started: {ex['startDate']}")
            print(f"    Input: {detail['input']}")
            print()
            print("    Operator decision options:")
            print("    aws stepfunctions send-task-success --task-token <TOKEN>"
                  " --task-output '{\"decision\": \"discard\", ...}'")
        else:
            print("    No RUNNING executions found. Check the state machine.")

    print()
    print("=" * 70)
    print("DEMO STAGED — operator now decides via SFN or the Streamlit dashboard")
    print("=" * 70)
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--live", action="store_true", help="Use real AWS instead of moto.")
    parser.add_argument("--auto-replay", action="store_true",
                        help="In mock mode, drive the replay path end-to-end (default: stop after quarantine).")
    args = parser.parse_args(argv)

    if args.live:
        return run_live()
    return run_mock(auto_replay=args.auto_replay)


if __name__ == "__main__":
    sys.exit(main())
