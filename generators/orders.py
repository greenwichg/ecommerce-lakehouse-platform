"""Synthetic orders generator.

Produces a deterministic stream of orders for a date range. Each order's
trajectory (placed -> paid -> shipped -> delivered, or cancelled along the
way) is derived from its order_id hash, so re-runs with identical arguments
produce byte-identical output.

For each date D, one Parquet file is written at:

    <bucket>/raw/orders/year=YYYY/month=MM/day=DD/orders.parquet

The file contains four kinds of records:

1. **New placed orders** — the headline ~1000/day. Status=placed,
   ``created_at`` on D.
2. **Natural status transitions** — for orders from prior days whose
   trajectory says they move to paid/shipped/delivered/cancelled on D.
   This is what makes the accumulating snapshot meaningful: an order
   placed on D-2 shows up again on D-1 with status=paid, again on D+1
   with status=shipped, etc.
3. **Late-arriving records (~2%)** — placed records whose ``created_at`` is
   on a prior date but which land in D's partition. Exercises the Silver
   watermark.
4. **Same-day status updates (~1%)** — orders placed on D that *also*
   have a paid record in D's file. Exercises Silver MERGE deduplication
   within a single batch.

Interpretation note (documented in docs/runbook.md "Generator semantics"):
the 1000/day, 2%, 1% knobs in config apply to *new placed records*. Natural
transition records are emitted *in addition* and grow per-day volume to
~2000-3000 over a 30-day warm-up. This is the only realistic way to drive
the accumulating snapshot — emitting only 10 status updates per 1000 orders
would leave 99% of orders stuck on "placed" forever.
"""

from __future__ import annotations

import datetime as dt
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import click
import pyarrow as pa

# Make databricks/libs importable when this module is run as a script from
# the repo root.
_REPO_ROOT = Path(__file__).resolve().parent.parent
_LIBS_DIR = _REPO_ROOT / "databricks"
if str(_LIBS_DIR) not in sys.path:
    sys.path.insert(0, str(_LIBS_DIR))

from libs.config import get_path, load_config  # noqa: E402
from libs.paths import partition_path  # noqa: E402

from generators._common import (  # noqa: E402
    daily_dates,
    deterministic_uuid,
    seeded_rng,
    write_parquet,
)

STATUS_PLACED = "placed"
STATUS_PAID = "paid"
STATUS_SHIPPED = "shipped"
STATUS_DELIVERED = "delivered"
STATUS_CANCELLED = "cancelled"

# How far back to look for orders whose natural transition might land today.
# Beyond ~14 days the trajectory durations (means in single-digit days) make
# further transitions vanishingly rare.
_TRANSITION_LOOKBACK_DAYS = 14
# How far back a "late-arriving" placed record can be dated.
_LATE_ARRIVAL_LOOKBACK_DAYS = 5

# Default pool sizes — line up with Slice 2's customers (10k) and products (5k)
# generators so order foreign keys resolve once those land.
_DEFAULT_CUSTOMER_COUNT = 10000
_DEFAULT_PRODUCT_COUNT = 5000


@dataclass(frozen=True)
class _OrderSpec:
    """Full lifecycle of one order, pre-computed deterministically.

    Once we have the spec we can render any point-in-time snapshot from it
    without re-running the RNG.
    """

    order_id: str
    customer_id: str
    product_id: str
    quantity: int
    price: float
    placed_at: dt.datetime
    paid_at: dt.datetime | None
    shipped_at: dt.datetime | None
    delivered_at: dt.datetime | None
    cancelled_at: dt.datetime | None


def _customer_pool_id(seed: int, index: int) -> str:
    return str(deterministic_uuid(f"customers:{seed}", index))


def _product_pool_id(seed: int, index: int) -> str:
    # SKU-style. Seed-prefixed so different seeds get distinct product pools
    # and we don't accidentally compare orders generated under different seeds.
    return f"PRD-{seed:04d}-{index:05d}"


def _build_order_spec(
    placed_date: dt.date,
    placement_index: int,
    cfg: dict,
    seed: int,
    customer_count: int,
    product_count: int,
) -> _OrderSpec:
    """Compute the full lifecycle of one order from (placed_date, index)."""
    order_id = str(deterministic_uuid(f"orders:{seed}", placed_date.isoformat(), placement_index))
    rng = seeded_rng("trajectory", order_id)

    customer_id = _customer_pool_id(seed, rng.randint(0, customer_count - 1))
    product_id = _product_pool_id(seed, rng.randint(0, product_count - 1))
    quantity = max(1, int(rng.gammavariate(2.0, cfg["avg_items_per_order"] / 2.0)))
    price = round(rng.uniform(cfg["price_min"], cfg["price_max"]), 2)

    # placed_at: a random second within placement_date (UTC).
    seconds_into_day = rng.randint(0, 24 * 3600 - 1)
    placed_at = (
        dt.datetime.combine(placed_date, dt.time(0, 0, 0)) + dt.timedelta(seconds=seconds_into_day)
    ).replace(tzinfo=dt.UTC)

    # Compute all forward transition timestamps. We may null some of them out
    # if the order cancels.
    means = cfg["transition_days_mean"]
    paid_at: dt.datetime | None = placed_at + dt.timedelta(
        days=rng.expovariate(1.0 / means["placed_to_paid"])
    )
    shipped_at: dt.datetime | None = paid_at + dt.timedelta(
        days=rng.expovariate(1.0 / means["paid_to_shipped"])
    )
    delivered_at: dt.datetime | None = shipped_at + dt.timedelta(
        days=rng.expovariate(1.0 / means["shipped_to_delivered"])
    )

    # Cancellation? If so, decide at which step.
    will_cancel = rng.random() * 100 < cfg["cancellation_pct"]
    cancelled_at: dt.datetime | None = None
    if will_cancel:
        # Cancel after step k in {1,2,3} — i.e. after placed/paid/shipped.
        cancel_after = rng.randint(1, 3)
        if cancel_after == 1:
            cancelled_at = placed_at + dt.timedelta(hours=rng.uniform(1, 24))
            paid_at = shipped_at = delivered_at = None
        elif cancel_after == 2:
            cancelled_at = paid_at + dt.timedelta(hours=rng.uniform(1, 48))
            shipped_at = delivered_at = None
        else:
            cancelled_at = shipped_at + dt.timedelta(hours=rng.uniform(12, 72))
            delivered_at = None

    return _OrderSpec(
        order_id=order_id,
        customer_id=customer_id,
        product_id=product_id,
        quantity=quantity,
        price=price,
        placed_at=placed_at,
        paid_at=paid_at,
        shipped_at=shipped_at,
        delivered_at=delivered_at,
        cancelled_at=cancelled_at,
    )


def _transition_events(spec: _OrderSpec) -> list[tuple[str, dt.datetime]]:
    """Return the list of (status, timestamp) transitions *after* placed."""
    events: list[tuple[str, dt.datetime]] = []
    if spec.paid_at is not None:
        events.append((STATUS_PAID, spec.paid_at))
    if spec.shipped_at is not None:
        events.append((STATUS_SHIPPED, spec.shipped_at))
    if spec.delivered_at is not None:
        events.append((STATUS_DELIVERED, spec.delivered_at))
    if spec.cancelled_at is not None:
        events.append((STATUS_CANCELLED, spec.cancelled_at))
    return events


def _snapshot(spec: _OrderSpec, status: str, at: dt.datetime) -> dict:
    """Render the order's record at the given status with ``updated_at=at``.

    Earlier transition timestamps remain populated; later ones (relative to
    ``status``) are nulled. This is the wire format the bronze layer ingests.
    """
    paid = spec.paid_at if status in {STATUS_PAID, STATUS_SHIPPED, STATUS_DELIVERED} else None
    shipped = spec.shipped_at if status in {STATUS_SHIPPED, STATUS_DELIVERED} else None
    delivered = spec.delivered_at if status == STATUS_DELIVERED else None
    cancelled = spec.cancelled_at if status == STATUS_CANCELLED else None
    return {
        "order_id": spec.order_id,
        "customer_id": spec.customer_id,
        "product_id": spec.product_id,
        "quantity": spec.quantity,
        "price": spec.price,
        "status": status,
        "created_at": spec.placed_at,
        "paid_at": paid,
        "shipped_at": shipped,
        "delivered_at": delivered,
        "cancelled_at": cancelled,
        "updated_at": at,
    }


_PARQUET_SCHEMA = pa.schema(
    [
        ("order_id", pa.string()),
        ("customer_id", pa.string()),
        ("product_id", pa.string()),
        ("quantity", pa.int32()),
        ("price", pa.float64()),
        ("status", pa.string()),
        ("created_at", pa.timestamp("us", tz="UTC")),
        ("paid_at", pa.timestamp("us", tz="UTC")),
        ("shipped_at", pa.timestamp("us", tz="UTC")),
        ("delivered_at", pa.timestamp("us", tz="UTC")),
        ("cancelled_at", pa.timestamp("us", tz="UTC")),
        ("updated_at", pa.timestamp("us", tz="UTC")),
    ]
)


def generate_for_date(
    date: dt.date,
    cfg: dict,
    seed: int,
    customer_count: int = _DEFAULT_CUSTOMER_COUNT,
    product_count: int = _DEFAULT_PRODUCT_COUNT,
) -> list[dict]:
    """Build all records that belong in ``date``'s partition.

    Deterministic given (date, cfg, seed, pool sizes).
    """
    rpd = int(cfg["records_per_day"])
    late_count = int(rpd * cfg["late_arrival_pct"] / 100)
    same_day_update_count = int(rpd * cfg["same_day_update_pct"] / 100)

    records: list[dict] = []

    # 1. New placed orders dated D.
    for i in range(rpd):
        spec = _build_order_spec(date, i, cfg, seed, customer_count, product_count)
        records.append(_snapshot(spec, STATUS_PLACED, spec.placed_at))

    # 2. Natural transitions from prior days landing on D.
    for days_back in range(1, _TRANSITION_LOOKBACK_DAYS + 1):
        prior_date = date - dt.timedelta(days=days_back)
        for i in range(rpd):
            spec = _build_order_spec(prior_date, i, cfg, seed, customer_count, product_count)
            for status, when in _transition_events(spec):
                if when.date() == date:
                    records.append(_snapshot(spec, status, when))

    # 3. Late-arriving placed records: orders dated D-k showing up in D's
    #    partition. Use a separate index range so their order_ids don't
    #    collide with regular orders.
    rng = seeded_rng("late", seed, date.isoformat())
    for j in range(late_count):
        days_back = rng.randint(1, _LATE_ARRIVAL_LOOKBACK_DAYS)
        late_date = date - dt.timedelta(days=days_back)
        # Index offset chosen well above rpd to keep the late-arrival pool
        # disjoint from the on-time pool.
        spec = _build_order_spec(
            late_date, rpd + 10_000 + j, cfg, seed, customer_count, product_count
        )
        records.append(_snapshot(spec, STATUS_PLACED, spec.placed_at))

    # 4. Same-day updates: orders placed on D that also pay on D.
    #    Sample candidates from today's pool and keep those whose trajectory
    #    actually pays before midnight.
    rng2 = seeded_rng("same_day", seed, date.isoformat())
    next_day = date + dt.timedelta(days=1)
    emitted = 0
    # Iterate today's indices in shuffled order; emit until we've hit the
    # target count or run out. Use a fixed shuffle for determinism.
    indices = list(range(rpd))
    rng2.shuffle(indices)
    for i in indices:
        if emitted >= same_day_update_count:
            break
        spec = _build_order_spec(date, i, cfg, seed, customer_count, product_count)
        if spec.paid_at is not None and spec.paid_at.date() < next_day:
            records.append(_snapshot(spec, STATUS_PAID, spec.paid_at))
            emitted += 1

    return records


def _records_to_table(records: Iterable[dict]) -> pa.Table:
    """Build a pyarrow Table with the canonical schema."""
    return pa.Table.from_pylist(list(records), schema=_PARQUET_SCHEMA)


def run(
    start_date: dt.date,
    end_date: dt.date,
    bucket: str,
    cfg: dict,
    seed: int,
    customer_count: int = _DEFAULT_CUSTOMER_COUNT,
    product_count: int = _DEFAULT_PRODUCT_COUNT,
    partition_template: str | None = None,
) -> list[str]:
    """Generate and write per-day Parquet files. Returns the list of paths."""
    orders_cfg = cfg["generators"]["orders"]
    raw_prefix = cfg["paths"]["raw_prefix"]
    template = partition_template or cfg["paths"]["partitioning"]

    written: list[str] = []
    for date in daily_dates(start_date, end_date):
        records = generate_for_date(date, orders_cfg, seed, customer_count, product_count)
        target_dir = partition_path(bucket, raw_prefix, "orders", date, template)
        target_url = f"{target_dir}/orders.parquet"
        write_parquet(_records_to_table(records), target_url)
        written.append(target_url)
    return written


@click.command()
@click.option(
    "--start-date",
    required=True,
    type=click.DateTime(["%Y-%m-%d"]),
    help="Inclusive start date (UTC).",
)
@click.option(
    "--end-date", required=True, type=click.DateTime(["%Y-%m-%d"]), help="Inclusive end date (UTC)."
)
@click.option("--env", default=None, help="Env name (defaults to $LAKEHOUSE_ENV, then 'dev').")
@click.option("--seed", default=None, type=int, help="Master seed; overrides config.")
@click.option(
    "--records-per-day", default=None, type=int, help="Override generators.orders.records_per_day."
)
@click.option(
    "--bucket", default=None, help="Override storage.bucket (e.g. file:///tmp/data or s3://bkt)."
)
def main(
    start_date: dt.datetime,
    end_date: dt.datetime,
    env: str | None,
    seed: int | None,
    records_per_day: int | None,
    bucket: str | None,
) -> None:
    """Generate orders for a date range and write partitioned Parquet."""
    cfg = load_config(env=env)
    if records_per_day is not None:
        cfg["generators"]["orders"]["records_per_day"] = records_per_day
    if seed is None:
        seed = int(get_path(cfg, "generators.random_seed", default=42))
    if bucket is None:
        bucket = get_path(cfg, "storage.bucket")

    written = run(
        start_date=start_date.date(),
        end_date=end_date.date(),
        bucket=bucket,
        cfg=cfg,
        seed=seed,
    )
    click.echo(f"Wrote {len(written)} partition file(s) to {bucket}/raw/orders/")
    for path in written:
        click.echo(f"  {path}")


if __name__ == "__main__":
    main()
