"""File validator Lambda — single-function source dispatcher.

Triggered by SQS messages carrying S3 ObjectCreated events. For each
event:

1. Parse the S3 key to determine the source (orders / customers /
   products / clickstream / currency_rates / wishlist).
2. Run that source's validator against the file's metadata + content.
3. Classify the outcome as ``validated`` / ``warn`` / ``quarantine``.
4. Write a manifest line to ``processed/_manifests/year=/month=/day=/manifest.jsonl``.
5. Emit a CloudWatch metric to ``Lakehouse/Validator``.
6. If quarantine: move the file from ``raw/`` to ``quarantine/``,
   publish SNS.
7. If warn: leave the file in place, publish SNS for ops-awareness.
8. If validated: silent (manifest only).

Heavy dependencies (pyarrow for Parquet schema inspection) make this a
Container Image deploy rather than zipfile.
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import unquote_plus

import boto3

log = logging.getLogger()
log.setLevel(logging.INFO)

# Configuration via env vars set by Terraform.
BUCKET = os.environ.get("LAKEHOUSE_BUCKET", "")
ALERTS_TOPIC_ARN = os.environ.get("ALERTS_TOPIC_ARN", "")
METRICS_NAMESPACE = "Lakehouse/Validator"
MAX_FILE_SIZE_BYTES = 1024 * 1024 * 1024  # 1 GB
MIN_FILE_SIZE_BYTES = 1  # zero-byte files always quarantine

s3 = boto3.client("s3")
sns = boto3.client("sns")
cloudwatch = boto3.client("cloudwatch")

# Required column sets per source. The validator confirms these all
# exist in the file's column/key listing. Extra columns are fine (Bronze
# handles schema evolution); missing ones are a quarantine.
REQUIRED_COLUMNS: dict[str, set[str]] = {
    "orders": {
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "price",
        "status",
        "created_at",
        "updated_at",
    },
    "customers": {
        "customer_id",
        "name",
        "email",
        "address",
        "signup_date",
        "updated_at",
    },
    "products": {
        "product_id",
        "product_name",
        "category",
        "price",
        "sku",
        "updated_at",
    },
    "clickstream": {
        "event_id",
        "session_id",
        "customer_id",
        "event_type",
        "page_url",
        "event_ts",
        "device",
        "user_agent",
    },
    "currency_rates": {
        "rate_date",
        "base_currency",
        "target_currency",
        "rate",
        "_source",
    },
    "wishlist": {
        "wishlist_event_id",
        "customer_id",
        "product_id",
        "added_at",
        "source",
    },
}

# Source → file format. Drives which reader the validator uses.
SOURCE_FORMATS: dict[str, str] = {
    "orders": "parquet",
    "customers": "parquet",
    "products": "csv",
    "clickstream": "jsonl",
    "currency_rates": "csv",
    "wishlist": "jsonl",
}


# ---------------------------------------------------------------------------
# Outcome dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Outcome:
    """The result of validating one file."""

    severity: str  # "validated" | "warn" | "quarantine"
    reason: str  # short reason code (matches the manifest)
    source: str  # orders / customers / etc.
    rows: int | None = None  # row count if successfully parsed
    size_bytes: int | None = None
    warnings: tuple[str, ...] = ()


# ---------------------------------------------------------------------------
# Source detection
# ---------------------------------------------------------------------------


_SOURCE_RE = re.compile(r"^raw/(?P<source>[^/]+)/")


def source_from_key(key: str) -> str | None:
    """Extract the source name from an S3 key.

    Expects keys of the form ``raw/<source>/year=YYYY/...``. Returns the
    source name if matched, None if the key is outside raw/ or doesn't
    name a known source.
    """
    match = _SOURCE_RE.match(key)
    if not match:
        return None
    source = match.group("source")
    return source if source in REQUIRED_COLUMNS else None


# ---------------------------------------------------------------------------
# Per-format readers
# ---------------------------------------------------------------------------


def _read_parquet_columns(body: bytes) -> tuple[set[str], int]:
    """Return (column names, row count) for a Parquet payload.

    Imports pyarrow lazily so the import cost is paid once per cold start.
    """
    import pyarrow.parquet as pq  # noqa: PLC0415

    table = pq.read_table(io.BytesIO(body))
    return set(table.column_names), table.num_rows


def _read_csv_columns(body: bytes) -> tuple[set[str], int]:
    """Return (column names from header, row count) for a CSV payload."""
    text = body.decode("utf-8", errors="replace")
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        return set(), 0
    header = lines[0].split(",")
    columns = {col.strip().strip('"') for col in header}
    # Row count excludes the header
    return columns, max(0, len(lines) - 1)


def _read_jsonl_columns(body: bytes) -> tuple[set[str], int]:
    """Return (keys from the first JSON object, row count) for JSON Lines."""
    text = body.decode("utf-8", errors="replace")
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        return set(), 0
    try:
        first = json.loads(lines[0])
    except json.JSONDecodeError:
        return set(), 0
    columns = set(first.keys()) if isinstance(first, dict) else set()
    return columns, len(lines)


_READERS = {
    "parquet": _read_parquet_columns,
    "csv": _read_csv_columns,
    "jsonl": _read_jsonl_columns,
}


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------


def validate(bucket: str, key: str) -> Outcome:
    """Validate one file. Pure function aside from S3 reads."""
    source = source_from_key(key)
    if source is None:
        return Outcome(severity="quarantine", reason="unknown_source", source="unknown")

    # Size check via HeadObject (cheap; avoids downloading huge files
    # only to reject them).
    head = s3.head_object(Bucket=bucket, Key=key)
    size = head["ContentLength"]
    if size < MIN_FILE_SIZE_BYTES:
        return Outcome(severity="quarantine", reason="empty_file", source=source, size_bytes=size)
    if size > MAX_FILE_SIZE_BYTES:
        return Outcome(
            severity="quarantine", reason="file_too_large", source=source, size_bytes=size
        )

    fmt = SOURCE_FORMATS[source]
    reader = _READERS[fmt]

    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    try:
        columns, rows = reader(body)
    except Exception as e:  # noqa: BLE001 — readers raise diverse types
        log.exception("Reader failed for %s", key)
        return Outcome(
            severity="quarantine",
            reason=f"unreadable_{fmt}: {type(e).__name__}",
            source=source,
            size_bytes=size,
        )

    if rows == 0:
        return Outcome(severity="quarantine", reason="zero_rows", source=source, size_bytes=size)

    required = REQUIRED_COLUMNS[source]
    missing = required - columns
    if missing:
        return Outcome(
            severity="quarantine",
            reason=f"missing_columns: {','.join(sorted(missing))}",
            source=source,
            rows=rows,
            size_bytes=size,
        )

    # Warnings (file passes but worth noting):
    warnings: list[str] = []
    unexpected = columns - required
    if unexpected:
        warnings.append(f"unexpected_columns: {','.join(sorted(unexpected))}")

    if warnings:
        return Outcome(
            severity="warn",
            reason="passed_with_warnings",
            source=source,
            rows=rows,
            size_bytes=size,
            warnings=tuple(warnings),
        )

    return Outcome(severity="validated", reason="ok", source=source, rows=rows, size_bytes=size)


# ---------------------------------------------------------------------------
# Actions on outcome
# ---------------------------------------------------------------------------


def _quarantine_key(key: str) -> str:
    """Map raw/<source>/year=.../foo to quarantine/<source>/year=.../foo."""
    return key.replace("raw/", "quarantine/", 1)


def quarantine_file(bucket: str, key: str) -> str:
    """Copy raw → quarantine, delete raw. Returns the quarantine key."""
    quarantine_key = _quarantine_key(key)
    s3.copy_object(
        Bucket=bucket,
        Key=quarantine_key,
        CopySource={"Bucket": bucket, "Key": key},
    )
    s3.delete_object(Bucket=bucket, Key=key)
    return quarantine_key


def write_manifest(bucket: str, key: str, outcome: Outcome) -> None:
    """Append a manifest line for this file's outcome.

    Manifest path is keyed by *today's* date (when the validator ran),
    not the file's logical date — operational records of validator runs
    are most useful aligned to wall-clock time.
    """
    today = datetime.now(UTC).date()
    manifest_key = (
        f"processed/_manifests/year={today:%Y}/month={today:%m}/day={today:%d}/manifest.jsonl"
    )
    entry = {
        "validated_at": datetime.now(UTC).isoformat(),
        "source": outcome.source,
        "key": key,
        "status": outcome.severity,
        "reason": outcome.reason,
        "rows": outcome.rows,
        "size_bytes": outcome.size_bytes,
        "warnings": list(outcome.warnings),
    }
    # Append-mode S3 requires read-modify-write. For our manifest write
    # rate (handful per minute), one PutObject per file is fine — keeps
    # the code simple. If volume climbs we'd batch.
    try:
        existing = s3.get_object(Bucket=bucket, Key=manifest_key)["Body"].read()
    except s3.exceptions.NoSuchKey:
        existing = b""
    s3.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=existing + (json.dumps(entry) + "\n").encode(),
        ContentType="application/x-ndjson",
    )


def publish_alert(outcome: Outcome, original_key: str) -> None:
    """Publish to SNS for warn-tier and quarantine outcomes."""
    if not ALERTS_TOPIC_ARN:
        log.warning("ALERTS_TOPIC_ARN not configured; skipping SNS publish")
        return
    subject = f"[lakehouse-validator] {outcome.severity}: {outcome.source}"[:100]
    message = json.dumps(
        {
            "severity": outcome.severity,
            "source": outcome.source,
            "key": original_key,
            "reason": outcome.reason,
            "rows": outcome.rows,
            "warnings": list(outcome.warnings),
        },
        indent=2,
    )
    sns.publish(TopicArn=ALERTS_TOPIC_ARN, Subject=subject, Message=message)


def emit_metrics(outcome: Outcome) -> None:
    """Emit one metric data point per outcome."""
    metric_name = {
        "validated": "ValidationSuccess",
        "warn": "ValidationWarning",
        "quarantine": "QuarantineCount",
    }[outcome.severity]
    cloudwatch.put_metric_data(
        Namespace=METRICS_NAMESPACE,
        MetricData=[
            {
                "MetricName": metric_name,
                "Dimensions": [{"Name": "Source", "Value": outcome.source}],
                "Value": 1,
                "Unit": "Count",
            }
        ],
    )


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------


def _pairs_from_body(body: dict) -> list[tuple[str, str]]:
    """Extract (bucket, key) tuples from one S3 notification payload.

    One SQS message may bundle multiple S3 records.
    """
    pairs: list[tuple[str, str]] = []
    for s3_record in body.get("Records", []):
        bucket = s3_record["s3"]["bucket"]["name"]
        # S3 keys are URL-encoded in the notification
        key = unquote_plus(s3_record["s3"]["object"]["key"])
        pairs.append((bucket, key))
    return pairs


def _process_file(bucket: str, key: str) -> dict:
    """Validate one file and act on the outcome. Returns the summary entry."""
    log.info("Validating s3://%s/%s", bucket, key)
    outcome = validate(bucket, key)

    write_manifest(bucket, key, outcome)
    emit_metrics(outcome)

    if outcome.severity == "quarantine":
        quarantined_to = quarantine_file(bucket, key)
        log.warning("Quarantined to s3://%s/%s — %s", bucket, quarantined_to, outcome.reason)
        publish_alert(outcome, key)
    elif outcome.severity == "warn":
        log.info("Warn for s3://%s/%s — %s", bucket, key, outcome.warnings)
        publish_alert(outcome, key)
    else:
        log.info("Validated s3://%s/%s (%d rows)", bucket, key, outcome.rows or 0)

    return {"key": key, "outcome": outcome.severity, "reason": outcome.reason}


def handler(event: dict, context: Any) -> dict:
    """Lambda entry point.

    Implements SQS partial-batch responses — the event source mapping is
    configured with ``ReportBatchItemFailures``, so a message whose
    processing throws marks only itself for redelivery via
    ``batchItemFailures`` instead of failing (and re-driving) the whole
    batch. Without this, one transient S3/KMS error would re-deliver
    already-quarantined siblings whose raw/ key no longer exists, poisoning
    every retry until the batch lands in the DLQ.
    """
    processed = []
    batch_item_failures: list[dict[str, str]] = []
    for sqs_record in event.get("Records", []):
        message_id = sqs_record.get("messageId")
        try:
            body = json.loads(sqs_record.get("body", "{}"))
        except json.JSONDecodeError:
            # Malformed JSON can never succeed on redelivery — drop it
            # rather than retry-loop into the DLQ.
            log.exception("SQS body not valid JSON; skipping")
            continue
        try:
            for bucket, key in _pairs_from_body(body):
                processed.append(_process_file(bucket, key))
        except Exception:
            if message_id is None:
                raise  # direct invocation (no SQS envelope): fail loudly
            log.exception("Message %s failed; marking for SQS redelivery", message_id)
            batch_item_failures.append({"itemIdentifier": message_id})

    return {
        "processed": processed,
        "count": len(processed),
        "batchItemFailures": batch_item_failures,
    }
