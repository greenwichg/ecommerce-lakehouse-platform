"""Tests for the currency_rates generator.

Network-touching paths are tested with ``responses`` (HTTP mocking)
rather than hitting the real API — keeps the suite fast and
deterministic. The simulated-fallback path is tested directly without
any mock.
"""

from __future__ import annotations

import csv
import datetime as dt
from pathlib import Path
from unittest.mock import patch

import pytest
import responses

from generators.currency_rates import (
    _API_URL,
    _CSV_COLUMNS,
    DEFAULT_BASE,
    DEFAULT_TARGETS,
    _simulate_rates,
    fetch_for_date,
    run,
)


@pytest.fixture()
def currency_test_cfg() -> dict:
    return {
        "generators": {
            "currency_rates": {
                "base_currency": "USD",
                "target_currencies": list(DEFAULT_TARGETS),
            },
            "random_seed": 42,
        },
        "paths": {
            "raw_prefix": "raw",
            "partitioning": "year={year:04d}/month={month:02d}/day={day:02d}",
        },
    }


def _api_payload(date: dt.date, rates: dict[str, float]) -> dict:
    return {"success": True, "base": "USD", "date": date.isoformat(), "rates": rates}


# ---------------------------------------------------------------------------
# Simulated-only path
# ---------------------------------------------------------------------------


def test_simulated_rates_deterministic() -> None:
    """Same (date, seed) -> same simulated rates."""
    a = _simulate_rates(dt.date(2025, 5, 1), seed=42, base="USD", targets=DEFAULT_TARGETS)
    b = _simulate_rates(dt.date(2025, 5, 1), seed=42, base="USD", targets=DEFAULT_TARGETS)
    assert a == b


def test_simulated_rates_within_jitter_band() -> None:
    """±2% multiplicative jitter; rates stay within 2% of the base rate."""
    from generators.currency_rates import _BASE_RATES

    sim = _simulate_rates(dt.date(2025, 5, 1), seed=42, base="USD", targets=DEFAULT_TARGETS)
    for currency, rate in sim.items():
        base_rate = _BASE_RATES[currency]
        assert (
            0.98 * base_rate <= rate <= 1.02 * base_rate
        ), f"{currency} rate {rate} outside ±2% of {base_rate}"


def test_simulated_rates_rejects_non_usd_base() -> None:
    with pytest.raises(ValueError, match="base=USD"):
        _simulate_rates(dt.date(2025, 5, 1), seed=42, base="EUR", targets=("USD",))


def test_simulated_rates_rejects_unknown_currency() -> None:
    with pytest.raises(ValueError, match="no simulated base rate"):
        _simulate_rates(dt.date(2025, 5, 1), seed=42, base="USD", targets=("XYZ",))


def test_fetch_with_use_api_false_yields_simulated() -> None:
    rates = fetch_for_date(dt.date(2025, 5, 1), seed=42, use_api=False)
    assert {r.source for r in rates} == {"simulated"}
    assert len(rates) == len(DEFAULT_TARGETS)


# ---------------------------------------------------------------------------
# API path (with retry + fallback)
# ---------------------------------------------------------------------------


@responses.activate
def test_fetch_api_success_returns_api_source() -> None:
    date = dt.date(2025, 5, 1)
    responses.add(
        responses.GET,
        _API_URL.format(date=date.isoformat()),
        json=_api_payload(date, {c: 1.0 + i * 0.1 for i, c in enumerate(DEFAULT_TARGETS)}),
        status=200,
    )
    rates = fetch_for_date(date, seed=42, use_api=True)
    assert {r.source for r in rates} == {"api"}


@responses.activate
def test_fetch_api_failure_falls_back_to_simulated() -> None:
    date = dt.date(2025, 5, 1)
    responses.add(
        responses.GET,
        _API_URL.format(date=date.isoformat()),
        status=500,
    )
    # Skip the retry delays — patching time.sleep makes the test fast.
    with patch("generators.currency_rates.time.sleep"):
        rates = fetch_for_date(date, seed=42, use_api=True)
    assert {r.source for r in rates} == {"simulated"}


@responses.activate
def test_fetch_api_partial_failure_then_recovery() -> None:
    """First two attempts fail, third succeeds → all rows are 'api'."""
    date = dt.date(2025, 5, 1)
    url = _API_URL.format(date=date.isoformat())
    responses.add(responses.GET, url, status=500)
    responses.add(responses.GET, url, status=500)
    responses.add(
        responses.GET,
        url,
        json=_api_payload(date, dict.fromkeys(DEFAULT_TARGETS, 1.5)),
        status=200,
    )
    with patch("generators.currency_rates.time.sleep"):
        rates = fetch_for_date(date, seed=42, use_api=True)
    assert {r.source for r in rates} == {"api"}


@responses.activate
def test_api_missing_a_target_currency_triggers_fallback() -> None:
    """If API response omits any requested currency, treat as failure."""
    date = dt.date(2025, 5, 1)
    incomplete = dict.fromkeys(DEFAULT_TARGETS[:-1], 1.0)  # missing the last
    responses.add(
        responses.GET,
        _API_URL.format(date=date.isoformat()),
        json=_api_payload(date, incomplete),
        status=200,
    )
    with patch("generators.currency_rates.time.sleep"):
        rates = fetch_for_date(date, seed=42, use_api=True)
    assert {r.source for r in rates} == {"simulated"}


@responses.activate
def test_api_invalid_json_triggers_fallback() -> None:
    date = dt.date(2025, 5, 1)
    responses.add(
        responses.GET,
        _API_URL.format(date=date.isoformat()),
        body="not json",
        status=200,
    )
    with patch("generators.currency_rates.time.sleep"):
        rates = fetch_for_date(date, seed=42, use_api=True)
    assert {r.source for r in rates} == {"simulated"}


# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------


def test_run_writes_csv_with_canonical_columns(currency_test_cfg: dict, tmp_path: Path) -> None:
    bucket = f"file://{tmp_path}"
    run(
        start_date=dt.date(2025, 5, 1),
        end_date=dt.date(2025, 5, 1),
        bucket=bucket,
        cfg=currency_test_cfg,
        seed=42,
        use_api=False,
    )
    path = tmp_path / "raw" / "currency_rates" / "year=2025" / "month=05" / "day=01" / "rates.csv"
    assert path.is_file()
    with path.open() as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames == list(_CSV_COLUMNS)
        rows = list(reader)
    assert len(rows) == len(DEFAULT_TARGETS)
    # All rows have a _source column populated
    sources = {row["_source"] for row in rows}
    assert sources == {"simulated"}  # since use_api=False


def test_run_idempotent_bytes_equivalent(currency_test_cfg: dict, tmp_path: Path) -> None:
    """Two simulated-mode runs produce byte-identical files (modulo _fetched_at
    which uses wall-clock — verify all non-time columns match)."""
    bucket_a = f"file://{tmp_path / 'a'}"
    bucket_b = f"file://{tmp_path / 'b'}"
    for bucket in (bucket_a, bucket_b):
        run(
            start_date=dt.date(2025, 5, 1),
            end_date=dt.date(2025, 5, 1),
            bucket=bucket,
            cfg=currency_test_cfg,
            seed=42,
            use_api=False,
        )

    def _rate_cols_only(path: Path) -> list[dict]:
        with path.open() as f:
            reader = csv.DictReader(f)
            return [{k: v for k, v in row.items() if k != "_fetched_at"} for row in reader]

    a = _rate_cols_only(
        tmp_path
        / "a"
        / "raw"
        / "currency_rates"
        / "year=2025"
        / "month=05"
        / "day=01"
        / "rates.csv"
    )
    b = _rate_cols_only(
        tmp_path
        / "b"
        / "raw"
        / "currency_rates"
        / "year=2025"
        / "month=05"
        / "day=01"
        / "rates.csv"
    )
    assert a == b


def test_run_writes_one_file_per_day(currency_test_cfg: dict, tmp_path: Path) -> None:
    bucket = f"file://{tmp_path}"
    written = run(
        start_date=dt.date(2025, 5, 1),
        end_date=dt.date(2025, 5, 3),
        bucket=bucket,
        cfg=currency_test_cfg,
        seed=42,
        use_api=False,
    )
    assert len(written) == 3


def test_default_targets_size_is_ten() -> None:
    """Per spec: ~10 currencies."""
    assert len(DEFAULT_TARGETS) == 10
    assert DEFAULT_BASE == "USD"


def test_source_values_are_normalised_strings() -> None:
    """The _source column is a free-form string in our schema; constrain to
    the two values downstream consumers expect ('api' | 'simulated')."""
    rates = fetch_for_date(dt.date(2025, 5, 1), seed=42, use_api=False)
    assert {r.source for r in rates} == {"simulated"}
    assert all(isinstance(r.source, str) for r in rates)
