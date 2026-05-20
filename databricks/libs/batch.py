"""Batch ID generation and parsing.

A batch ID identifies one logical run of the pipeline. It travels through
every layer (Bronze → Silver → Gold → Snowflake) attached to each row as
``_batch_id`` so lineage queries can trace a row back to the run that
produced it, and Airflow XCom uses it to coordinate downstream tasks.

Format::

    YYYYMMDDTHHMMSSZ-<hex8>     e.g.  20250520T143022Z-a3f2c5b1

The timestamp anchors lineage to wall-clock UTC; the hex suffix
disambiguates runs that start within the same second.
"""

from __future__ import annotations

import datetime as dt
import re
import uuid

_BATCH_ID_RE = re.compile(r"^(\d{8}T\d{6}Z)-([0-9a-f]{8})$")
_TS_FORMAT = "%Y%m%dT%H%M%SZ"


def make_batch_id(now: dt.datetime | None = None) -> str:
    """Generate a new batch ID. ``now`` is injectable for tests.

    A naive ``now`` is treated as UTC; aware datetimes are converted to UTC
    so two callers in different zones produce comparable IDs.
    """
    if now is None:
        now = dt.datetime.now(dt.UTC)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=dt.UTC)
    else:
        now = now.astimezone(dt.UTC)
    ts = now.strftime(_TS_FORMAT)
    suffix = uuid.uuid4().hex[:8]
    return f"{ts}-{suffix}"


def parse_batch_id(batch_id: str) -> tuple[dt.datetime, str]:
    """Parse a batch ID into its UTC timestamp and hex suffix components.

    Raises ``ValueError`` for malformed inputs so callers can fail loudly
    rather than propagating garbage through XCom.
    """
    match = _BATCH_ID_RE.match(batch_id)
    if not match:
        raise ValueError(f"Not a valid batch_id: {batch_id!r}")
    ts = dt.datetime.strptime(match.group(1), _TS_FORMAT).replace(tzinfo=dt.UTC)
    return ts, match.group(2)
