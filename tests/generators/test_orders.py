"""Tests for the orders generator.

Coverage:
- Deterministic / idempotent: same args -> same output.
- Volume contract: new + late + same-day-update counts match config.
- Late arrivals: have ``created_at < partition_date``.
- Same-day updates: same ``order_id`` appears with status=placed and
  status=paid in the same day's records.
- Natural transitions: orders from prior days appear with later statuses.
- Schema: produced Parquet has the canonical column set and types.
- CLI: invoking ``run()`` writes one file per partition at the right path.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from generators.orders import (
    STATUS_PAID,
    STATUS_PLACED,
    _build_order_spec,
    generate_for_date,
    run,
)


def test_idempotent_same_seed_same_output(orders_test_cfg: dict) -> None:
    date = dt.date(2025, 5, 1)
    a = generate_for_date(date, orders_test_cfg["generators"]["orders"], seed=42)
    b = generate_for_date(date, orders_test_cfg["generators"]["orders"], seed=42)
    assert a == b


def test_different_seed_different_output(orders_test_cfg: dict) -> None:
    date = dt.date(2025, 5, 1)
    a = generate_for_date(date, orders_test_cfg["generators"]["orders"], seed=42)
    b = generate_for_date(date, orders_test_cfg["generators"]["orders"], seed=43)
    # Same length is fine; content must differ on the new-placed indices.
    a_placed_ids = {r["order_id"] for r in a if r["status"] == STATUS_PLACED}
    b_placed_ids = {r["order_id"] for r in b if r["status"] == STATUS_PLACED}
    assert a_placed_ids != b_placed_ids


def test_new_placed_count_matches_config(orders_test_cfg: dict) -> None:
    cfg = orders_test_cfg["generators"]["orders"]
    date = dt.date(2025, 5, 1)
    records = generate_for_date(date, cfg, seed=42)
    new_placed = [
        r for r in records if r["status"] == STATUS_PLACED and r["created_at"].date() == date
    ]
    # 50 new placed + (up to) ~1 late record dated on D from a previous day's
    # spec coincidence. Filter strictly by index by checking created_at on D.
    assert len(new_placed) == cfg["records_per_day"]


def test_late_arrivals_have_old_created_at(orders_test_cfg: dict) -> None:
    cfg = orders_test_cfg["generators"]["orders"]
    date = dt.date(2025, 5, 1)
    records = generate_for_date(date, cfg, seed=42)
    late = [r for r in records if r["status"] == STATUS_PLACED and r["created_at"].date() < date]
    expected_late = int(cfg["records_per_day"] * cfg["late_arrival_pct"] / 100)
    assert len(late) == expected_late
    for record in late:
        assert record["created_at"].date() < date


def test_same_day_update_pairs_share_order_id(orders_test_cfg: dict) -> None:
    cfg = orders_test_cfg["generators"]["orders"]
    date = dt.date(2025, 5, 1)
    records = generate_for_date(date, cfg, seed=42)
    placed_today = {
        r["order_id"]
        for r in records
        if r["status"] == STATUS_PLACED and r["created_at"].date() == date
    }
    paid_today_with_placed_today = {
        r["order_id"]
        for r in records
        if r["status"] == STATUS_PAID
        and r["created_at"].date() == date
        and r["updated_at"].date() == date
    }
    # Every same-day paid record should correspond to a placed record dated D.
    assert paid_today_with_placed_today.issubset(placed_today)
    # And we should have generated *some* — same_day_update_pct=4% of 50 = 2.
    assert len(paid_today_with_placed_today) >= 1


def test_natural_transitions_emit_with_old_created_at(orders_test_cfg: dict) -> None:
    """Orders placed days ago should appear today with status != placed."""
    cfg = orders_test_cfg["generators"]["orders"]
    # Generate D and observe records with created_at < D and status != placed.
    date = dt.date(2025, 5, 20)
    records = generate_for_date(date, cfg, seed=42)
    transitions_from_past = [
        r for r in records if r["status"] != STATUS_PLACED and r["created_at"].date() < date
    ]
    assert len(transitions_from_past) > 0, "expected some natural transitions on D"
    for record in transitions_from_past:
        assert record["updated_at"].date() == date


def test_updated_at_consistency(orders_test_cfg: dict) -> None:
    """For each record, updated_at should equal the status-specific timestamp."""
    cfg = orders_test_cfg["generators"]["orders"]
    records = generate_for_date(dt.date(2025, 5, 5), cfg, seed=42)
    for record in records:
        status = record["status"]
        if status == STATUS_PLACED:
            assert record["updated_at"] == record["created_at"]
        elif status == STATUS_PAID:
            assert record["updated_at"] == record["paid_at"]
        elif status == "shipped":
            assert record["updated_at"] == record["shipped_at"]
        elif status == "delivered":
            assert record["updated_at"] == record["delivered_at"]
        elif status == "cancelled":
            assert record["updated_at"] == record["cancelled_at"]


def test_order_spec_idempotent(orders_test_cfg: dict) -> None:
    cfg = orders_test_cfg["generators"]["orders"]
    a = _build_order_spec(
        dt.date(2025, 5, 1), 7, cfg, seed=42, customer_count=100, product_count=50
    )
    b = _build_order_spec(
        dt.date(2025, 5, 1), 7, cfg, seed=42, customer_count=100, product_count=50
    )
    assert a == b


def test_cancellation_after_paid_preserves_paid_at(orders_test_cfg: dict) -> None:
    """A cancellation snapshot must carry paid_at/shipped_at if the order
    progressed past those stages before being cancelled."""
    from generators.orders import _build_order_spec, _snapshot

    cfg = dict(orders_test_cfg["generators"]["orders"])
    cfg["cancellation_pct"] = 100.0  # force cancellation
    # Find a spec that cancelled at step 2 (after paid) or step 3 (after shipped)
    for index in range(500):
        spec = _build_order_spec(
            dt.date(2025, 5, 1), index, cfg, seed=42, customer_count=100, product_count=50
        )
        if spec.cancelled_at is not None and spec.paid_at is not None:
            snap = _snapshot(spec, "cancelled", spec.cancelled_at)
            assert snap["paid_at"] == spec.paid_at, "cancellation must preserve paid_at"
            assert snap["cancelled_at"] == spec.cancelled_at
            assert snap["delivered_at"] is None  # never delivered if cancelled
            if spec.shipped_at is not None:
                assert snap["shipped_at"] == spec.shipped_at
            return
    raise AssertionError("expected at least one cancel-after-paid in 500 trials")


def test_order_spec_cancellation_clears_later_timestamps(orders_test_cfg: dict) -> None:
    """An order that cancels at step 1 must have paid/shipped/delivered = None."""
    cfg = dict(orders_test_cfg["generators"]["orders"])
    cfg["cancellation_pct"] = 100.0  # force cancellation
    # Find one that cancels at the first step (placed). Iterate until we hit it.
    found_step1 = False
    for index in range(200):
        spec = _build_order_spec(
            dt.date(2025, 5, 1),
            index,
            cfg,
            seed=42,
            customer_count=100,
            product_count=50,
        )
        if spec.cancelled_at is not None and spec.paid_at is None:
            found_step1 = True
            assert spec.shipped_at is None
            assert spec.delivered_at is None
            break
    assert found_step1, "expected at least one early-cancellation spec in 200 trials"


def test_orders_only_reference_entities_alive_at_placement(orders_test_cfg: dict) -> None:
    """Every order's customer must be signed up — and its product
    introduced — by the order's placement date.

    Regression: FKs used to be drawn uniformly from the whole pool, so
    ~30%+ of orders referenced not-yet-existing entities; their PIT joins
    NULLed the surrogates and the 1% orphan-rate gates failed every run.
    """
    from generators.customers import _customer_id, signup_date_for
    from generators.products import introduction_date_for

    cfg = orders_test_cfg["generators"]["orders"]
    date = dt.date(2025, 5, 1)  # mid-horizon: ~1/3 of entities don't exist yet
    n_cust, n_prod = 200, 100
    signup_by_id = {_customer_id(42, i): signup_date_for(42, i) for i in range(n_cust)}
    intro_by_id = {f"PRD-{42:04d}-{i:05d}": introduction_date_for(42, i) for i in range(n_prod)}

    records = generate_for_date(date, cfg, seed=42, customer_count=n_cust, product_count=n_prod)
    assert records
    for rec in records:
        placed = rec["created_at"].date()
        assert signup_by_id[rec["customer_id"]] <= placed, (
            f"order {rec['order_id']} placed {placed} references customer "
            f"signed up {signup_by_id[rec['customer_id']]}"
        )
        assert intro_by_id[rec["product_id"]] <= placed, (
            f"order {rec['order_id']} placed {placed} references product "
            f"introduced {intro_by_id[rec['product_id']]}"
        )


def test_run_writes_one_file_per_partition(orders_test_cfg: dict, tmp_path: Path) -> None:
    bucket = f"file://{tmp_path}"
    written = run(
        start_date=dt.date(2025, 5, 1),
        end_date=dt.date(2025, 5, 3),
        bucket=bucket,
        cfg=orders_test_cfg,
        seed=42,
        customer_count=100,
        product_count=50,
    )
    assert len(written) == 3
    for date_offset, _ in enumerate(written):
        date = dt.date(2025, 5, 1) + dt.timedelta(days=date_offset)
        expected_dir = (
            tmp_path
            / "raw"
            / "orders"
            / f"year={date.year:04d}"
            / f"month={date.month:02d}"
            / f"day={date.day:02d}"
        )
        f = expected_dir / "orders.parquet"
        assert f.is_file(), f"missing {f}"


def test_run_output_has_canonical_schema(orders_test_cfg: dict, tmp_path: Path) -> None:
    import pyarrow as pa

    bucket = f"file://{tmp_path}"
    run(
        start_date=dt.date(2025, 5, 1),
        end_date=dt.date(2025, 5, 1),
        bucket=bucket,
        cfg=orders_test_cfg,
        seed=42,
        customer_count=100,
        product_count=50,
    )
    path = tmp_path / "raw" / "orders" / "year=2025" / "month=05" / "day=01" / "orders.parquet"
    # Read the file's own schema (without partition inference) so we assert
    # what the generator actually wrote, not what pyarrow synthesises by
    # parsing the Hive-style directory names.
    file_schema = pq.ParquetFile(path).schema_arrow
    assert file_schema.names == [
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "price",
        "status",
        "created_at",
        "paid_at",
        "shipped_at",
        "delivered_at",
        "cancelled_at",
        "updated_at",
    ]
    assert file_schema.field("order_id").type.equals(pa.string())
    assert file_schema.field("price").type.equals(pa.float64())
    assert file_schema.field("quantity").type.equals(pa.int32())


def test_run_idempotent_byte_equivalent_data(orders_test_cfg: dict, tmp_path: Path) -> None:
    """Two runs with identical args write equivalent data (row-level)."""
    bucket_a = f"file://{tmp_path / 'a'}"
    bucket_b = f"file://{tmp_path / 'b'}"
    run(
        start_date=dt.date(2025, 5, 1),
        end_date=dt.date(2025, 5, 1),
        bucket=bucket_a,
        cfg=orders_test_cfg,
        seed=42,
        customer_count=100,
        product_count=50,
    )
    run(
        start_date=dt.date(2025, 5, 1),
        end_date=dt.date(2025, 5, 1),
        bucket=bucket_b,
        cfg=orders_test_cfg,
        seed=42,
        customer_count=100,
        product_count=50,
    )
    a = pq.read_table(
        tmp_path / "a" / "raw" / "orders" / "year=2025" / "month=05" / "day=01" / "orders.parquet"
    ).to_pylist()
    b = pq.read_table(
        tmp_path / "b" / "raw" / "orders" / "year=2025" / "month=05" / "day=01" / "orders.parquet"
    ).to_pylist()
    assert a == b


@pytest.mark.parametrize(
    "date",
    [
        dt.date(2025, 1, 1),
        dt.date(2025, 5, 15),
        dt.date(2025, 12, 31),
    ],
)
def test_partition_path_zero_pads(orders_test_cfg: dict, tmp_path: Path, date: dt.date) -> None:
    bucket = f"file://{tmp_path}"
    run(
        start_date=date,
        end_date=date,
        bucket=bucket,
        cfg=orders_test_cfg,
        seed=42,
        customer_count=100,
        product_count=50,
    )
    expected = (
        tmp_path
        / "raw"
        / "orders"
        / f"year={date.year:04d}"
        / f"month={date.month:02d}"
        / f"day={date.day:02d}"
        / "orders.parquet"
    )
    assert expected.is_file()
