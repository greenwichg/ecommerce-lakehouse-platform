"""Quarantine helper Lambda — small set of operations driven by Step Functions.

Step Functions can't move S3 objects directly, can't call the Airflow
REST API directly, and can't write a structured audit-log entry to S3
without templating gymnastics. This helper exposes those as discrete
``action``-tagged invocations.

Actions:
- ``discard``: delete the quarantined file + append an audit-log JSONL
  entry to ``processed/_quarantine_audit/...`` for forensic trail.
- ``move_to_raw``: copy quarantined object back into the raw/ prefix
  (re-validation runs separately, by S3 event firing on the new raw
  object).
- ``poll_for_fix``: check whether ``<fix_prefix>/<original_filename>``
  exists in S3. Returns ``{"present": bool, "key": str | None}``.
  Step Functions polls this on a Wait → Choice loop.
- ``trigger_airflow``: POST to the Airflow REST API to manually
  trigger ``daily_batch_pipeline`` for the affected logical_date.
  Looks up the Airflow connection details from Secrets Manager.

The state machine in ``modules/step_functions/definition.asl.json``
calls this with an ``action`` field plus action-specific args.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request
from datetime import UTC, datetime
from typing import Any

import boto3

log = logging.getLogger()
log.setLevel(logging.INFO)

BUCKET = os.environ.get("LAKEHOUSE_BUCKET", "")
AIRFLOW_SECRET_NAME = os.environ.get("AIRFLOW_SECRET_NAME", "")

s3 = boto3.client("s3")
secretsmanager = boto3.client("secretsmanager")


# ---------------------------------------------------------------------------
# discard
# ---------------------------------------------------------------------------


def _audit_log_entry(bucket: str, entry: dict) -> None:
    """Append one JSONL line to processed/_quarantine_audit/.

    Same append-via-read-modify-write pattern as the validator's manifest.
    Operationally tiny — quarantine actions are infrequent.
    """
    today = datetime.now(UTC).date()
    audit_key = (
        f"processed/_quarantine_audit/year={today:%Y}/month={today:%m}/day={today:%d}/audit.jsonl"
    )
    try:
        existing = s3.get_object(Bucket=bucket, Key=audit_key)["Body"].read()
    except s3.exceptions.NoSuchKey:
        existing = b""
    s3.put_object(
        Bucket=bucket,
        Key=audit_key,
        Body=existing + (json.dumps(entry) + "\n").encode(),
        ContentType="application/x-ndjson",
    )


def discard(quarantine_key: str, operator: str, reason: str) -> dict:
    """Delete the quarantined file + write an audit entry."""
    s3.delete_object(Bucket=BUCKET, Key=quarantine_key)
    entry = {
        "action": "discard",
        "discarded_at": datetime.now(UTC).isoformat(),
        "key": quarantine_key,
        "operator": operator,
        "reason": reason,
    }
    _audit_log_entry(BUCKET, entry)
    return {"discarded": True, "key": quarantine_key}


# ---------------------------------------------------------------------------
# move_to_raw
# ---------------------------------------------------------------------------


def _raw_key(quarantine_key: str) -> str:
    """quarantine/<source>/... → raw/<source>/..."""
    if not quarantine_key.startswith("quarantine/"):
        raise ValueError(f"expected key under quarantine/, got {quarantine_key!r}")
    return "raw/" + quarantine_key[len("quarantine/"):]


def move_to_raw(quarantine_key: str, operator: str) -> dict:
    """Copy quarantined object → raw/, delete the quarantine copy.

    The new raw/ object triggers the S3→SQS→validator chain so the file
    gets re-validated automatically. The state machine doesn't need to
    invoke the validator directly.
    """
    raw_key = _raw_key(quarantine_key)
    s3.copy_object(
        Bucket=BUCKET, Key=raw_key,
        CopySource={"Bucket": BUCKET, "Key": quarantine_key},
    )
    s3.delete_object(Bucket=BUCKET, Key=quarantine_key)
    _audit_log_entry(BUCKET, {
        "action": "move_to_raw",
        "moved_at": datetime.now(UTC).isoformat(),
        "from": quarantine_key,
        "to": raw_key,
        "operator": operator,
    })
    return {"moved": True, "from": quarantine_key, "to": raw_key}


# ---------------------------------------------------------------------------
# poll_for_fix
# ---------------------------------------------------------------------------


def _fix_key(quarantine_key: str) -> str:
    """Expected location of a fixed file: quarantine/<source>/.../_fixed/<filename>.

    Operators upload the corrected file under the same key path but with
    a ``_fixed`` directory inserted before the filename. State machine
    documentation tells them where to upload.
    """
    if "/" not in quarantine_key:
        raise ValueError(f"malformed quarantine_key: {quarantine_key!r}")
    parent, filename = quarantine_key.rsplit("/", 1)
    return f"{parent}/_fixed/{filename}"


def poll_for_fix(quarantine_key: str) -> dict:
    """Return whether the expected fixed file exists in S3."""
    fix_key = _fix_key(quarantine_key)
    try:
        s3.head_object(Bucket=BUCKET, Key=fix_key)
        return {"present": True, "key": fix_key}
    except s3.exceptions.ClientError as e:
        # head_object 404 surfaces as ClientError with NoSuchKey or 404.
        if e.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
            return {"present": False, "key": fix_key}
        raise


# ---------------------------------------------------------------------------
# trigger_airflow
# ---------------------------------------------------------------------------


def trigger_airflow(dag_id: str, logical_date: str) -> dict:
    """POST to the Airflow REST API to trigger a DAG run.

    Looks up the Airflow connection from Secrets Manager
    (``airflow-api-creds``). Secret JSON shape:
        {"base_url": "https://...", "username": "...", "password": "..."}

    Returns the API response payload on success; raises on failure (SFN
    routes the failure to the catch handler).
    """
    if not AIRFLOW_SECRET_NAME:
        raise RuntimeError("AIRFLOW_SECRET_NAME env var not configured")

    secret_string = secretsmanager.get_secret_value(SecretId=AIRFLOW_SECRET_NAME)["SecretString"]
    creds = json.loads(secret_string)

    url = f"{creds['base_url'].rstrip('/')}/api/v1/dags/{dag_id}/dagRuns"
    body = json.dumps({
        "logical_date": logical_date,
        "conf": {"triggered_by": "quarantine-replay-sfn"},
    }).encode()

    # Basic auth header; Airflow 2.x API supports this when
    # api.auth_backends includes airflow.api.auth.backend.basic_auth.
    import base64
    auth = base64.b64encode(f"{creds['username']}:{creds['password']}".encode()).decode()

    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Basic {auth}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:  # noqa: S310 — known host from secret
            response_body = resp.read().decode()
        return {"triggered": True, "response": json.loads(response_body)}
    except urllib.error.HTTPError as e:
        log.exception("Airflow API HTTP error")
        raise RuntimeError(f"Airflow API returned {e.code}: {e.read().decode()}") from e


# ---------------------------------------------------------------------------
# Handler dispatch
# ---------------------------------------------------------------------------


_ACTIONS = {
    "discard": lambda event: discard(
        quarantine_key=event["quarantine_key"],
        operator=event.get("operator", "unknown"),
        reason=event.get("reason", "not provided"),
    ),
    "move_to_raw": lambda event: move_to_raw(
        quarantine_key=event["quarantine_key"],
        operator=event.get("operator", "unknown"),
    ),
    "poll_for_fix": lambda event: poll_for_fix(
        quarantine_key=event["quarantine_key"],
    ),
    "trigger_airflow": lambda event: trigger_airflow(
        dag_id=event["dag_id"],
        logical_date=event["logical_date"],
    ),
}


def handler(event: dict, context: Any) -> dict:
    """Lambda entry point. Dispatches on ``event['action']``."""
    action = event.get("action")
    if action not in _ACTIONS:
        raise ValueError(f"unknown action: {action!r}")
    log.info("quarantine_helper action=%s", action)
    return _ACTIONS[action](event)
