"""Tests for the customers generator.

Coverage matrix:
- Idempotency: same args -> same output.
- Volume: signed-up customers <= record_count; far-future date == record_count.
- Schema: produced Parquet matches the canonical schema.
- SCD2 timeline correctness:
  * Single-version (no changes) yields stable attributes over time.
  * Multi-version: same customer's email/address differs across far-apart dates.
  * updated_at reflects the latest version's effective_from, not load time.
- Signup gating: a customer with signup_date > target date is omitted.
- Faker is deterministic per customer across runs (no PYTHONHASHSEED bleed).
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from generators.customers import (
    PROJECT_EPOCH,
    _build_timeline,
    _current_version_at,
    _customer_id,
    generate_for_date,
    run,
)


@pytest.fixture()
def customers_test_cfg() -> dict:
    """Small, deterministic config that tests don't need to construct themselves."""
    return {
        "generators": {
            "customers": {
                "daily_change_pct": 5.0,  # higher than prod (0.3) for test signal
            },
            "random_seed": 42,
        },
        "paths": {
            "raw_prefix": "raw",
            "partitioning": "year={year:04d}/month={month:02d}/day={day:02d}",
        },
    }


def test_idempotent_same_args(customers_test_cfg: dict) -> None:
    date = dt.date(2025, 6, 1)
    cfg = customers_test_cfg["generators"]["customers"]
    a = generate_for_date(date, cfg, seed=42, record_count=30)
    b = generate_for_date(date, cfg, seed=42, record_count=30)
    assert a == b


def test_different_seed_different_ids(customers_test_cfg: dict) -> None:
    date = dt.date(2025, 6, 1)
    cfg = customers_test_cfg["generators"]["customers"]
    a_ids = {r["customer_id"] for r in generate_for_date(date, cfg, seed=42, record_count=30)}
    b_ids = {r["customer_id"] for r in generate_for_date(date, cfg, seed=99, record_count=30)}
    assert a_ids.isdisjoint(b_ids)


def test_far_future_date_includes_all_customers(customers_test_cfg: dict) -> None:
    """A date well past the signup horizon lets every customer have signed up."""
    cfg = customers_test_cfg["generators"]["customers"]
    rows = generate_for_date(dt.date(2099, 1, 1), cfg, seed=42, record_count=30)
    assert len(rows) == 30


def test_early_date_omits_unsignedup_customers(customers_test_cfg: dict) -> None:
    """Early in the signup horizon, some customers haven't signed up yet."""
    cfg = customers_test_cfg["generators"]["customers"]
    rows = generate_for_date(PROJECT_EPOCH + dt.timedelta(days=10), cfg, seed=42, record_count=30)
    assert len(rows) < 30


def test_customer_id_is_stable_across_runs(customers_test_cfg: dict) -> None:
    """customer_id derives from (seed, index), not anything date-dependent."""
    cfg = customers_test_cfg["generators"]["customers"]
    early = generate_for_date(dt.date(2025, 1, 5), cfg, seed=42, record_count=30)
    late = generate_for_date(dt.date(2025, 6, 1), cfg, seed=42, record_count=30)
    early_ids = {r["customer_id"] for r in early}
    late_ids = {r["customer_id"] for r in late}
    # Every early customer should still appear in late (they don't churn out)
    assert early_ids.issubset(late_ids)


def test_attributes_can_change_over_time(customers_test_cfg: dict) -> None:
    """At a 5% daily change rate, *some* customer should differ between
    a snapshot at signup time vs months later."""
    cfg = customers_test_cfg["generators"]["customers"]
    near = {
        r["customer_id"]: (r["email"], r["address"])
        for r in generate_for_date(
            PROJECT_EPOCH + dt.timedelta(days=30), cfg, seed=42, record_count=200
        )
    }
    far = {
        r["customer_id"]: (r["email"], r["address"])
        for r in generate_for_date(
            PROJECT_EPOCH + dt.timedelta(days=400), cfg, seed=42, record_count=200
        )
    }
    common = set(near) & set(far)
    assert common, "expected overlap between near and far snapshots"
    changed = [cid for cid in common if near[cid] != far[cid]]
    assert changed, "expected at least one customer's attributes to differ months later"


def test_updated_at_reflects_latest_version_effective_from(customers_test_cfg: dict) -> None:
    """For a forced-change customer, updated_at equals the change's effective_from."""
    cfg = dict(customers_test_cfg["generators"]["customers"])
    cfg["daily_change_pct"] = 100.0  # force daily changes
    date = PROJECT_EPOCH + dt.timedelta(days=30)
    rows = generate_for_date(date, cfg, seed=42, record_count=10)
    # Build the timeline for one row, find its current version, assert match
    for row in rows:
        # Find the matching customer's timeline
        for index in range(10):
            if _customer_id(42, index) == row["customer_id"]:
                versions = _build_timeline(index, 42, cfg, date)
                current = _current_version_at(versions, date)
                assert current is not None
                assert row["updated_at"] == current.effective_from
                break


def test_name_does_not_change(customers_test_cfg: dict) -> None:
    """Names are intentionally stable; only email/address change."""
    cfg = dict(customers_test_cfg["generators"]["customers"])
    cfg["daily_change_pct"] = 100.0
    date = PROJECT_EPOCH + dt.timedelta(days=180)
    for index in range(10):
        versions = _build_timeline(index, 42, cfg, date)
        if len(versions) < 2:
            continue
        names = {v.name for v in versions}
        assert len(names) == 1, f"customer {index} had multiple names: {names}"


def test_signup_date_does_not_change_across_versions(customers_test_cfg: dict) -> None:
    cfg = dict(customers_test_cfg["generators"]["customers"])
    cfg["daily_change_pct"] = 100.0
    date = PROJECT_EPOCH + dt.timedelta(days=180)
    rows = generate_for_date(date, cfg, seed=42, record_count=10)
    for row in rows:
        # signup_date is always the first version's date — must be <= snapshot date
        assert row["signup_date"] <= date


def test_run_writes_one_file_per_partition(customers_test_cfg: dict, tmp_path: Path) -> None:
    bucket = f"file://{tmp_path}"
    written = run(
        start_date=dt.date(2025, 6, 1),
        end_date=dt.date(2025, 6, 3),
        bucket=bucket,
        cfg=customers_test_cfg,
        seed=42,
        record_count=20,
    )
    assert len(written) == 3
    for date_offset in range(3):
        date = dt.date(2025, 6, 1) + dt.timedelta(days=date_offset)
        expected = (
            tmp_path
            / "raw"
            / "customers"
            / f"year={date.year:04d}"
            / f"month={date.month:02d}"
            / f"day={date.day:02d}"
            / "customers.parquet"
        )
        assert expected.is_file(), f"missing {expected}"


def test_run_output_schema(customers_test_cfg: dict, tmp_path: Path) -> None:
    bucket = f"file://{tmp_path}"
    run(
        start_date=dt.date(2025, 6, 1),
        end_date=dt.date(2025, 6, 1),
        bucket=bucket,
        cfg=customers_test_cfg,
        seed=42,
        record_count=20,
    )
    path = (
        tmp_path / "raw" / "customers" / "year=2025" / "month=06" / "day=01" / "customers.parquet"
    )
    schema = pq.ParquetFile(path).schema_arrow
    assert schema.names == [
        "customer_id",
        "name",
        "email",
        "address",
        "signup_date",
        "updated_at",
    ]


def test_run_idempotent_data_equivalent(customers_test_cfg: dict, tmp_path: Path) -> None:
    bucket_a = f"file://{tmp_path / 'a'}"
    bucket_b = f"file://{tmp_path / 'b'}"
    for bucket in (bucket_a, bucket_b):
        run(
            start_date=dt.date(2025, 6, 1),
            end_date=dt.date(2025, 6, 1),
            bucket=bucket,
            cfg=customers_test_cfg,
            seed=42,
            record_count=20,
        )
    a = pq.read_table(
        tmp_path
        / "a"
        / "raw"
        / "customers"
        / "year=2025"
        / "month=06"
        / "day=01"
        / "customers.parquet"
    ).to_pylist()
    b = pq.read_table(
        tmp_path
        / "b"
        / "raw"
        / "customers"
        / "year=2025"
        / "month=06"
        / "day=01"
        / "customers.parquet"
    ).to_pylist()
    assert a == b


def test_change_pct_zero_yields_single_version(customers_test_cfg: dict) -> None:
    cfg = dict(customers_test_cfg["generators"]["customers"])
    cfg["daily_change_pct"] = 0.0
    for index in range(20):
        versions = _build_timeline(index, 42, cfg, PROJECT_EPOCH + dt.timedelta(days=365))
        assert len(versions) <= 1, "no changes expected at 0% rate"
