"""Tests for libs.paths."""

from __future__ import annotations

import datetime as dt

from libs.paths import (
    checkpoint_path,
    is_local,
    layer_root,
    partition_path,
    to_filesystem_path,
)


def test_partition_path_s3() -> None:
    assert (
        partition_path("s3://bucket", "raw", "orders", dt.date(2025, 5, 1))
        == "s3://bucket/raw/orders/year=2025/month=05/day=01"
    )


def test_partition_path_strips_trailing_slash() -> None:
    assert (
        partition_path("s3://bucket/", "raw", "orders", dt.date(2025, 1, 9))
        == "s3://bucket/raw/orders/year=2025/month=01/day=09"
    )


def test_partition_path_zero_pads_month_and_day() -> None:
    path = partition_path("file:///tmp/d", "raw", "orders", dt.date(2025, 1, 9))
    assert "year=2025" in path
    assert "month=01" in path
    assert "day=09" in path


def test_partition_path_custom_template() -> None:
    path = partition_path(
        "s3://b",
        "raw",
        "orders",
        dt.date(2025, 5, 1),
        partition_template="dt={year}-{month:02d}-{day:02d}",
    )
    assert path == "s3://b/raw/orders/dt=2025-05-01"


def test_layer_root() -> None:
    assert layer_root("s3://b", "bronze", "orders") == "s3://b/bronze/orders"
    assert layer_root("s3://b/", "silver", "orders") == "s3://b/silver/orders"


def test_checkpoint_path() -> None:
    assert (
        checkpoint_path("s3://b", "_checkpoints", "orders", "bronze")
        == "s3://b/_checkpoints/bronze/orders"
    )


def test_is_local() -> None:
    assert is_local("file:///tmp/data")
    assert is_local("/tmp/data")
    assert not is_local("s3://bucket")
    assert not is_local("https://example.com")


def test_to_filesystem_path_strips_file_scheme() -> None:
    assert to_filesystem_path("file:///tmp/data") == "/tmp/data"
    assert to_filesystem_path("/tmp/data") == "/tmp/data"
    assert to_filesystem_path("s3://bucket") == "s3://bucket"
