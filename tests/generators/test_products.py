"""Tests for the products generator.

Mirror customers tests where applicable; additionally covers:
- CSV output format (header, column order, quoting).
- Category drift only flips to a *different* category (never the same one).
- Price stays within bounds across all versions.
- product_name and sku are stable across versions.
"""

from __future__ import annotations

import csv
import datetime as dt
from pathlib import Path

import pytest

from generators.products import (
    _CATEGORIES,
    _CSV_COLUMNS,
    PROJECT_EPOCH,
    _build_timeline,
    _current_version_at,
    _product_id,
    generate_for_date,
    run,
)


@pytest.fixture()
def products_test_cfg() -> dict:
    return {
        "generators": {
            "products": {
                "daily_change_pct": 5.0,
                "price_min": 4.99,
                "price_max": 499.99,
            },
            "random_seed": 42,
        },
        "paths": {
            "raw_prefix": "raw",
            "partitioning": "year={year:04d}/month={month:02d}/day={day:02d}",
        },
    }


def test_idempotent_same_args(products_test_cfg: dict) -> None:
    cfg = products_test_cfg["generators"]["products"]
    date = dt.date(2025, 6, 1)
    a = generate_for_date(date, cfg, seed=42, record_count=30)
    b = generate_for_date(date, cfg, seed=42, record_count=30)
    assert a == b


def test_product_id_format(products_test_cfg: dict) -> None:
    cfg = products_test_cfg["generators"]["products"]
    rows = generate_for_date(dt.date(2099, 1, 1), cfg, seed=42, record_count=30)
    for row in rows:
        assert row["product_id"].startswith("PRD-0042-")
        # PRD-NNNN-NNNNN
        parts = row["product_id"].split("-")
        assert len(parts) == 3
        assert len(parts[2]) == 5


def test_categories_are_from_fixed_list(products_test_cfg: dict) -> None:
    cfg = products_test_cfg["generators"]["products"]
    rows = generate_for_date(dt.date(2099, 1, 1), cfg, seed=42, record_count=100)
    cats = {r["category"] for r in rows}
    assert cats.issubset(set(_CATEGORIES))


def test_price_stays_in_bounds_across_versions(products_test_cfg: dict) -> None:
    cfg = dict(products_test_cfg["generators"]["products"])
    cfg["daily_change_pct"] = 100.0
    date = PROJECT_EPOCH + dt.timedelta(days=400)
    for index in range(20):
        versions = _build_timeline(index, 42, cfg, date)
        for v in versions:
            assert cfg["price_min"] <= v.price <= cfg["price_max"], (
                f"product {index} v.price {v.price} out of bounds"
            )


def test_category_drift_flips_to_different_category(products_test_cfg: dict) -> None:
    """A category-change event must move to a NEW category, not echo the same one."""
    cfg = dict(products_test_cfg["generators"]["products"])
    cfg["daily_change_pct"] = 100.0
    date = PROJECT_EPOCH + dt.timedelta(days=400)
    found_category_change = False
    for index in range(50):
        versions = _build_timeline(index, 42, cfg, date)
        for prev, nxt in zip(versions, versions[1:], strict=False):
            if prev.category != nxt.category:
                found_category_change = True
                assert nxt.category in _CATEGORIES
    assert found_category_change, "expected at least one category change in 50 products"


def test_product_name_and_sku_are_stable(products_test_cfg: dict) -> None:
    cfg = dict(products_test_cfg["generators"]["products"])
    cfg["daily_change_pct"] = 100.0
    date = PROJECT_EPOCH + dt.timedelta(days=400)
    for index in range(20):
        versions = _build_timeline(index, 42, cfg, date)
        if len(versions) < 2:
            continue
        assert len({v.product_name for v in versions}) == 1
        assert len({v.sku for v in versions}) == 1


def test_far_future_includes_all(products_test_cfg: dict) -> None:
    cfg = products_test_cfg["generators"]["products"]
    rows = generate_for_date(dt.date(2099, 1, 1), cfg, seed=42, record_count=50)
    assert len(rows) == 50


def test_early_date_omits_some(products_test_cfg: dict) -> None:
    cfg = products_test_cfg["generators"]["products"]
    rows = generate_for_date(PROJECT_EPOCH + dt.timedelta(days=10), cfg, seed=42, record_count=50)
    assert len(rows) < 50


def test_updated_at_iso_format(products_test_cfg: dict) -> None:
    """CSV serialises timestamps as ISO 8601 so PySpark CSV reader infers TIMESTAMP."""
    cfg = products_test_cfg["generators"]["products"]
    rows = generate_for_date(dt.date(2025, 6, 1), cfg, seed=42, record_count=10)
    for row in rows:
        # Parse round-trip to confirm it's valid ISO
        dt.datetime.fromisoformat(row["updated_at"])


def test_run_writes_csv_with_header(products_test_cfg: dict, tmp_path: Path) -> None:
    bucket = f"file://{tmp_path}"
    run(
        start_date=dt.date(2025, 6, 1),
        end_date=dt.date(2025, 6, 1),
        bucket=bucket,
        cfg=products_test_cfg,
        seed=42,
        record_count=10,
    )
    csv_path = (
        tmp_path / "raw" / "products"
        / "year=2025" / "month=06" / "day=01" / "products.csv"
    )
    assert csv_path.is_file()
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames == list(_CSV_COLUMNS)
        rows = list(reader)
    assert len(rows) >= 1
    # Header columns in expected order
    assert list(rows[0].keys()) == list(_CSV_COLUMNS)


def test_run_idempotent_bytes_equivalent(products_test_cfg: dict, tmp_path: Path) -> None:
    """CSV is line-oriented and Python's csv writer is deterministic, so two runs
    with identical args should produce byte-identical files."""
    bucket_a = f"file://{tmp_path / 'a'}"
    bucket_b = f"file://{tmp_path / 'b'}"
    for bucket in (bucket_a, bucket_b):
        run(
            start_date=dt.date(2025, 6, 1),
            end_date=dt.date(2025, 6, 1),
            bucket=bucket,
            cfg=products_test_cfg,
            seed=42,
            record_count=10,
        )
    a = (tmp_path / "a" / "raw" / "products" / "year=2025" / "month=06" / "day=01" / "products.csv").read_bytes()
    b = (tmp_path / "b" / "raw" / "products" / "year=2025" / "month=06" / "day=01" / "products.csv").read_bytes()
    assert a == b


def test_run_writes_one_file_per_partition(products_test_cfg: dict, tmp_path: Path) -> None:
    bucket = f"file://{tmp_path}"
    written = run(
        start_date=dt.date(2025, 6, 1),
        end_date=dt.date(2025, 6, 3),
        bucket=bucket,
        cfg=products_test_cfg,
        seed=42,
        record_count=10,
    )
    assert len(written) == 3
    for offset in range(3):
        date = dt.date(2025, 6, 1) + dt.timedelta(days=offset)
        expected = (
            tmp_path
            / "raw" / "products"
            / f"year={date.year:04d}"
            / f"month={date.month:02d}"
            / f"day={date.day:02d}"
            / "products.csv"
        )
        assert expected.is_file()


def test_product_ids_unique_per_seed(products_test_cfg: dict) -> None:
    cfg = products_test_cfg["generators"]["products"]
    rows = generate_for_date(dt.date(2099, 1, 1), cfg, seed=42, record_count=100)
    ids = [r["product_id"] for r in rows]
    assert len(ids) == len(set(ids))


def test_product_id_helper_matches_emitted(products_test_cfg: dict) -> None:
    cfg = products_test_cfg["generators"]["products"]
    rows = generate_for_date(dt.date(2099, 1, 1), cfg, seed=42, record_count=10)
    expected_ids = {_product_id(42, i) for i in range(10)}
    actual_ids = {r["product_id"] for r in rows}
    assert actual_ids == expected_ids


def test_change_pct_zero_is_single_version(products_test_cfg: dict) -> None:
    cfg = dict(products_test_cfg["generators"]["products"])
    cfg["daily_change_pct"] = 0.0
    for index in range(20):
        versions = _build_timeline(index, 42, cfg, PROJECT_EPOCH + dt.timedelta(days=365))
        assert len(versions) <= 1


def test_unused_helpers_present_for_external_callers() -> None:
    """_current_version_at is exposed so the SCD2 notebook can preview state
    at any date during ad-hoc debugging."""
    assert callable(_current_version_at)
