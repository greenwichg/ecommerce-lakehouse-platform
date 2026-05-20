"""Helpers for constructing partitioned data paths.

All lakehouse paths follow this layout::

    <bucket>/<layer>/<source>/year=YYYY/month=MM/day=DD/

Layers are ``raw``, ``bronze``, ``silver``, ``gold``, ``quarantine``. The
partition template comes from config (``paths.partitioning``) so the format
can be tweaked without code edits.
"""

from __future__ import annotations

import datetime as dt
from urllib.parse import urlparse

DEFAULT_PARTITION_TEMPLATE = "year={year:04d}/month={month:02d}/day={day:02d}"


def _build_partition(template: str, date: dt.date) -> str:
    return template.format(year=date.year, month=date.month, day=date.day)


def partition_path(
    bucket: str,
    layer_prefix: str,
    source: str,
    date: dt.date,
    partition_template: str = DEFAULT_PARTITION_TEMPLATE,
) -> str:
    """Build a fully-qualified partition path.

    >>> partition_path("s3://b", "raw", "orders", dt.date(2025, 5, 1))
    's3://b/raw/orders/year=2025/month=05/day=01'
    """
    base = bucket.rstrip("/")
    partition = _build_partition(partition_template, date)
    return f"{base}/{layer_prefix}/{source}/{partition}"


def layer_root(bucket: str, layer_prefix: str, source: str) -> str:
    """Path to the source root within a layer (no date partition).

    >>> layer_root("s3://b", "bronze", "orders")
    's3://b/bronze/orders'
    """
    return f"{bucket.rstrip('/')}/{layer_prefix}/{source}"


def checkpoint_path(bucket: str, checkpoint_prefix: str, source: str, layer: str) -> str:
    """Auto Loader / Structured Streaming checkpoint location.

    Checkpoints live under a separate prefix so cleanup policies for raw data
    do not accidentally invalidate stream state.
    """
    return f"{bucket.rstrip('/')}/{checkpoint_prefix}/{layer}/{source}"


def is_local(url: str) -> bool:
    """True for ``file://`` URLs and bare absolute paths."""
    parsed = urlparse(url)
    return parsed.scheme in {"", "file"}


def to_filesystem_path(url: str) -> str:
    """Convert a ``file://`` URL to a plain filesystem path. No-op otherwise."""
    parsed = urlparse(url)
    if parsed.scheme == "file":
        return parsed.path
    if parsed.scheme == "":
        return url
    return url
