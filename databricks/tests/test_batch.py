"""Tests for libs.batch."""

from __future__ import annotations

import datetime as dt

import pytest
from libs.batch import make_batch_id, parse_batch_id


def test_make_batch_id_format() -> None:
    bid = make_batch_id(dt.datetime(2025, 5, 20, 14, 30, 22, tzinfo=dt.UTC))
    assert bid.startswith("20250520T143022Z-")
    assert len(bid) == len("20250520T143022Z-") + 8


def test_round_trip_preserves_utc_timestamp() -> None:
    now = dt.datetime(2025, 5, 20, 14, 30, 22, tzinfo=dt.UTC)
    bid = make_batch_id(now)
    ts, suffix = parse_batch_id(bid)
    assert ts == now
    assert len(suffix) == 8


def test_naive_datetime_treated_as_utc() -> None:
    naive = dt.datetime(2025, 5, 20, 14, 30, 22)
    assert make_batch_id(naive).startswith("20250520T143022Z-")


def test_aware_datetime_converted_to_utc() -> None:
    # 10:30 EDT == 14:30 UTC
    eastern = dt.timezone(dt.timedelta(hours=-4))
    aware = dt.datetime(2025, 5, 20, 10, 30, 22, tzinfo=eastern)
    assert make_batch_id(aware).startswith("20250520T143022Z-")


def test_unique_suffix_at_same_timestamp() -> None:
    now = dt.datetime(2025, 5, 20, 14, 30, 22, tzinfo=dt.UTC)
    assert make_batch_id(now) != make_batch_id(now)


@pytest.mark.parametrize(
    "bad",
    [
        "not-a-batch-id",
        "20250520T143022Z",          # missing suffix
        "20250520T143022Z-deadbeefX",  # suffix too long
        "20250520T143022Z-zzzzzzzz",   # non-hex suffix
        "",
    ],
)
def test_parse_rejects_bad_format(bad: str) -> None:
    with pytest.raises(ValueError, match="Not a valid batch_id"):
        parse_batch_id(bad)
