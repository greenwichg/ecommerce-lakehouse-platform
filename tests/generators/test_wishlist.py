"""Tests for the wishlist generator."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pytest

from generators.wishlist import _SOURCES, generate_for_date, run


@pytest.fixture()
def wishlist_test_cfg() -> dict:
    return {
        "paths": {
            "raw_prefix": "raw",
            "partitioning": "year={year:04d}/month={month:02d}/day={day:02d}",
        },
    }


def test_idempotent_same_args() -> None:
    a = generate_for_date(
        dt.date(2025, 5, 1),
        seed=42,
        events_per_day=100,
        customer_pool_size=200,
        product_pool_size=100,
    )
    b = generate_for_date(
        dt.date(2025, 5, 1),
        seed=42,
        events_per_day=100,
        customer_pool_size=200,
        product_pool_size=100,
    )
    assert a == b


def test_exact_event_count() -> None:
    rows = generate_for_date(
        dt.date(2025, 5, 1),
        seed=42,
        events_per_day=37,
        customer_pool_size=100,
        product_pool_size=100,
    )
    assert len(rows) == 37


def test_all_events_have_required_schema() -> None:
    rows = generate_for_date(
        dt.date(2025, 5, 1),
        seed=42,
        events_per_day=50,
        customer_pool_size=100,
        product_pool_size=50,
    )
    required = {"wishlist_event_id", "customer_id", "product_id", "added_at", "source"}
    for r in rows:
        assert set(r.keys()) == required


def test_customer_id_never_null() -> None:
    """Wishlists are authenticated-only — no anonymous customers."""
    rows = generate_for_date(
        dt.date(2025, 5, 1),
        seed=42,
        events_per_day=100,
        customer_pool_size=100,
        product_pool_size=100,
    )
    assert all(r["customer_id"] is not None for r in rows)


def test_added_at_falls_within_target_day() -> None:
    target = dt.date(2025, 5, 1)
    rows = generate_for_date(
        target,
        seed=42,
        events_per_day=100,
        customer_pool_size=100,
        product_pool_size=100,
    )
    for r in rows:
        ts = dt.datetime.fromisoformat(r["added_at"])
        assert ts.date() == target


def test_sources_from_known_enum() -> None:
    rows = generate_for_date(
        dt.date(2025, 5, 1),
        seed=42,
        events_per_day=200,
        customer_pool_size=100,
        product_pool_size=100,
    )
    for r in rows:
        assert r["source"] in _SOURCES


def test_customer_ids_match_customers_pool() -> None:
    """FK alignment: wishlist.customer_id must be derivable from
    customers.py's pool so dim_customer PIT joins resolve."""
    from generators.customers import _customer_id as customers_id

    pool = {customers_id(seed=42, index=i) for i in range(100)}
    rows = generate_for_date(
        dt.date(2025, 5, 1),
        seed=42,
        events_per_day=200,
        customer_pool_size=100,
        product_pool_size=100,
    )
    used = {r["customer_id"] for r in rows}
    assert used.issubset(pool)


def test_events_only_reference_entities_alive_at_event_time() -> None:
    """Wishlist events must reference customers signed up — and products
    introduced — by the event date, or the factless fact's PIT joins
    orphan (same regression as generators.orders)."""
    from generators.customers import _customer_id, signup_date_for
    from generators.products import introduction_date_for

    date = dt.date(2025, 5, 1)
    n_cust, n_prod = 200, 100
    signup_by_id = {_customer_id(42, i): signup_date_for(42, i) for i in range(n_cust)}
    intro_by_id = {f"PRD-{42:04d}-{i:05d}": introduction_date_for(42, i) for i in range(n_prod)}

    rows = generate_for_date(
        date, seed=42, events_per_day=300, customer_pool_size=n_cust, product_pool_size=n_prod
    )
    assert rows
    for r in rows:
        assert signup_by_id[r["customer_id"]] <= date
        assert intro_by_id[r["product_id"]] <= date


def test_product_ids_match_products_pool() -> None:
    from generators.products import _product_id as products_id

    pool = {products_id(seed=42, index=i) for i in range(100)}
    rows = generate_for_date(
        dt.date(2025, 5, 1),
        seed=42,
        events_per_day=200,
        customer_pool_size=100,
        product_pool_size=100,
    )
    used = {r["product_id"] for r in rows}
    assert used.issubset(pool)


def test_wishlist_event_ids_unique_within_day() -> None:
    rows = generate_for_date(
        dt.date(2025, 5, 1),
        seed=42,
        events_per_day=500,
        customer_pool_size=100,
        product_pool_size=100,
    )
    ids = [r["wishlist_event_id"] for r in rows]
    assert len(ids) == len(set(ids))


def test_event_ids_disjoint_across_days() -> None:
    """Different dates yield disjoint wishlist_event_id sets (seeded per
    (date, index) so collisions would indicate a bug)."""
    a = generate_for_date(
        dt.date(2025, 5, 1),
        seed=42,
        events_per_day=50,
        customer_pool_size=50,
        product_pool_size=50,
    )
    b = generate_for_date(
        dt.date(2025, 5, 2),
        seed=42,
        events_per_day=50,
        customer_pool_size=50,
        product_pool_size=50,
    )
    a_ids = {r["wishlist_event_id"] for r in a}
    b_ids = {r["wishlist_event_id"] for r in b}
    assert a_ids.isdisjoint(b_ids)


def test_events_sorted_by_added_at() -> None:
    """Output is sorted so the file reads as an event stream."""
    rows = generate_for_date(
        dt.date(2025, 5, 1),
        seed=42,
        events_per_day=100,
        customer_pool_size=100,
        product_pool_size=100,
    )
    timestamps = [r["added_at"] for r in rows]
    assert timestamps == sorted(timestamps)


def test_run_writes_one_file_per_day(wishlist_test_cfg: dict, tmp_path: Path) -> None:
    bucket = f"file://{tmp_path}"
    written = run(
        start_date=dt.date(2025, 5, 1),
        end_date=dt.date(2025, 5, 3),
        bucket=bucket,
        cfg=wishlist_test_cfg,
        seed=42,
        events_per_day=20,
        customer_pool_size=50,
        product_pool_size=50,
    )
    assert len(written) == 3


def test_run_output_is_valid_jsonl(wishlist_test_cfg: dict, tmp_path: Path) -> None:
    bucket = f"file://{tmp_path}"
    run(
        start_date=dt.date(2025, 5, 1),
        end_date=dt.date(2025, 5, 1),
        bucket=bucket,
        cfg=wishlist_test_cfg,
        seed=42,
        events_per_day=20,
        customer_pool_size=50,
        product_pool_size=50,
    )
    path = tmp_path / "raw" / "wishlist" / "year=2025" / "month=05" / "day=01" / "wishlist.json"
    assert path.is_file()
    with path.open() as f:
        lines = [line.strip() for line in f if line.strip()]
    assert len(lines) == 20
    for line in lines:
        record = json.loads(line)
        assert "wishlist_event_id" in record


def test_run_idempotent_bytes_equivalent(wishlist_test_cfg: dict, tmp_path: Path) -> None:
    bucket_a = f"file://{tmp_path / 'a'}"
    bucket_b = f"file://{tmp_path / 'b'}"
    for bucket in (bucket_a, bucket_b):
        run(
            start_date=dt.date(2025, 5, 1),
            end_date=dt.date(2025, 5, 1),
            bucket=bucket,
            cfg=wishlist_test_cfg,
            seed=42,
            events_per_day=50,
            customer_pool_size=50,
            product_pool_size=50,
        )
    a = (
        tmp_path / "a" / "raw" / "wishlist" / "year=2025" / "month=05" / "day=01" / "wishlist.json"
    ).read_bytes()
    b = (
        tmp_path / "b" / "raw" / "wishlist" / "year=2025" / "month=05" / "day=01" / "wishlist.json"
    ).read_bytes()
    assert a == b
