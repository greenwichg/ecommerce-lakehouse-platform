"""Shared helpers across source generators.

Anything reused by more than one generator lives here: deterministic seeding,
UUID derivation, date iteration, partitioned Parquet writes. Keeping these
in one place means later generators (customers, products, clickstream) plug
into the same idempotency contract without re-inventing it.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import random
import uuid
from collections.abc import Iterator
from pathlib import Path
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

# A fixed namespace UUID for the whole project. All deterministic UUIDs are
# derived from this so they're stable across runs and across machines.
LAKEHOUSE_NAMESPACE = uuid.UUID("c0ffee00-cafe-babe-dead-c0ffee000001")


def daily_dates(start: dt.date, end: dt.date) -> Iterator[dt.date]:
    """Yield every date in the inclusive range [start, end].

    Raises:
        ValueError: if end < start.
    """
    if end < start:
        raise ValueError(f"end ({end}) must be >= start ({start})")
    current = start
    while current <= end:
        yield current
        current += dt.timedelta(days=1)


def seeded_rng(*parts: object) -> random.Random:
    """A deterministic ``random.Random`` seeded from the concatenation of parts.

    Uses blake2b rather than Python's built-in ``hash()`` because ``hash()``
    randomises per interpreter session (PYTHONHASHSEED), which would break
    idempotency.
    """
    payload = ":".join(str(p) for p in parts).encode()
    seed_bytes = hashlib.blake2b(payload, digest_size=16).digest()
    return random.Random(int.from_bytes(seed_bytes, "big"))


def deterministic_uuid(*parts: object) -> uuid.UUID:
    """A deterministic UUIDv5 derived from the project namespace + parts."""
    name = ":".join(str(p) for p in parts)
    return uuid.uuid5(LAKEHOUSE_NAMESPACE, name)


def write_parquet(table: pa.Table, target_url: str, compression: str = "snappy") -> None:
    """Write a pyarrow Table to a URL.

    Handles ``file://`` URLs and bare local paths by creating parent
    directories first. Remote schemes (``s3://``, ``gs://``, ...) are
    dispatched through ``pyarrow.fs.FileSystem.from_uri``.
    """
    parsed = urlparse(target_url)
    if parsed.scheme in {"", "file"}:
        local = parsed.path if parsed.scheme == "file" else target_url
        path = Path(local)
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, str(path), compression=compression)
        return
    fs, fs_path = pafs.FileSystem.from_uri(target_url)
    pq.write_table(table, fs_path, filesystem=fs, compression=compression)
