"""Synthetic products generator.

Daily CSV snapshot at:

    <bucket>/raw/products/year=YYYY/month=MM/day=DD/products.csv

Each product has a deterministic SCD2 timeline derived from
``(master_seed, product_index)``. SCD2-tracked attributes are ``price``
and ``category``; ``product_name`` and ``sku`` are stable for a given
``product_id``.

CSV (per spec) means no native nullable/typed columns, so the schema is
written with a header row and PySpark infers types on read. The
order_id-style ``PRD-NNNNN`` SKUs are stable across runs at the same seed.
"""

from __future__ import annotations

import csv
import datetime as dt
import io
import sys
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlparse

import click
import fsspec
from faker import Faker

_REPO_ROOT = Path(__file__).resolve().parent.parent
_LIBS_DIR = _REPO_ROOT / "databricks"
if str(_LIBS_DIR) not in sys.path:
    sys.path.insert(0, str(_LIBS_DIR))

from libs.config import get_path, load_config  # noqa: E402
from libs.paths import partition_path  # noqa: E402

from generators._common import daily_dates, seeded_rng  # noqa: E402

PROJECT_EPOCH = dt.date(2024, 1, 1)
_INTRODUCTION_HORIZON_DAYS = 730

_DEFAULT_RECORD_COUNT = 5_000

# Fixed category list — Snowflake clustering / aggregations expect a known
# enum. Adding a category later is a deliberate schema action, not a generator
# implicit drift.
_CATEGORIES = (
    "electronics",
    "books",
    "clothing",
    "home_kitchen",
    "sports_outdoors",
    "toys_games",
    "beauty",
    "grocery",
    "automotive",
    "office",
)

_CSV_COLUMNS = ("product_id", "product_name", "category", "price", "sku", "updated_at")


@dataclass(frozen=True)
class _ProductVersion:
    effective_from: dt.datetime
    product_name: str
    category: str
    price: float
    sku: str


def _product_id(seed: int, index: int) -> str:
    return f"PRD-{seed:04d}-{index:05d}"


@lru_cache(maxsize=65_536)
def introduction_date_for(seed: int, product_index: int) -> dt.date:
    """The deterministic catalogue-introduction date for one product.

    Counterpart of ``customers.signup_date_for`` — event generators use
    it to only reference products that exist at event time (PIT-join
    orphan prevention). Must consume the RNG stream in the same order as
    ``_build_timeline``; ``test_introduction_date_for_matches_timeline``
    guards the alignment.
    """
    rng = seeded_rng("product", seed, product_index)
    rng.randint(0, 1_000_000_000)  # the faker-seed draw in _build_timeline
    return PROJECT_EPOCH + dt.timedelta(days=rng.randint(0, _INTRODUCTION_HORIZON_DAYS))


def _build_timeline(
    product_index: int,
    seed: int,
    cfg: dict,
    max_date: dt.date,
    project_epoch: dt.date = PROJECT_EPOCH,
) -> list[_ProductVersion]:
    """Compute one product's full SCD2 timeline over the fixed horizon.

    ``max_date`` only gates existence (introduction date); the version
    chain spans (introduction, project_epoch + horizon] regardless of the
    generation date so consecutive daily snapshots agree about a
    product's history (see the matching note in customers.py).
    """
    rng = seeded_rng("product", seed, product_index)
    fake = Faker()
    fake.seed_instance(rng.randint(0, 1_000_000_000))

    intro_offset = rng.randint(0, _INTRODUCTION_HORIZON_DAYS)
    intro_date = project_epoch + dt.timedelta(days=intro_offset)
    if intro_date > max_date:
        return []

    base = _ProductVersion(
        effective_from=dt.datetime.combine(intro_date, dt.time(0, 0, 0), dt.UTC),
        product_name=fake.catch_phrase()[:80],
        category=rng.choice(_CATEGORIES),
        price=round(rng.uniform(cfg["price_min"], cfg["price_max"]), 2),
        sku=f"SKU-{rng.randint(1_000_000, 9_999_999)}",
    )
    versions: list[_ProductVersion] = [base]

    if cfg["daily_change_pct"] <= 0:
        return versions
    horizon_end = project_epoch + dt.timedelta(days=_INTRODUCTION_HORIZON_DAYS)
    chain_days = max(1, (horizon_end - intro_date).days)
    avg_changes = chain_days * cfg["daily_change_pct"] / 100.0
    n_changes = int(rng.gammavariate(max(avg_changes, 0.1), 1.0))
    if n_changes == 0:
        return versions

    candidate_offsets = sorted({rng.randint(1, chain_days) for _ in range(n_changes)})

    current_price = base.price
    current_category = base.category
    for offset in candidate_offsets:
        change_date = intro_date + dt.timedelta(days=offset)
        # Change price OR category, one per event. Price changes are
        # bounded multiplicative jitter (±25%) so an item stays in its
        # rough price band rather than walking randomly.
        if rng.random() < 0.7:
            current_price = round(current_price * rng.uniform(0.75, 1.25), 2)
            current_price = max(cfg["price_min"], min(cfg["price_max"], current_price))
        else:
            current_category = rng.choice([c for c in _CATEGORIES if c != current_category])
        versions.append(
            _ProductVersion(
                effective_from=dt.datetime.combine(change_date, dt.time(0, 0, 0), dt.UTC),
                product_name=base.product_name,
                category=current_category,
                price=current_price,
                sku=base.sku,
            )
        )
    return versions


def _current_version_at(versions: list[_ProductVersion], date: dt.date) -> _ProductVersion | None:
    if not versions:
        return None
    cutoff = dt.datetime.combine(date, dt.time(23, 59, 59), dt.UTC)
    valid = [v for v in versions if v.effective_from <= cutoff]
    return valid[-1] if valid else None


def generate_for_date(
    date: dt.date,
    cfg: dict,
    seed: int,
    record_count: int = _DEFAULT_RECORD_COUNT,
) -> list[dict]:
    """Build the day's product snapshot. Idempotent given (date, cfg, seed)."""
    records: list[dict] = []
    for index in range(record_count):
        versions = _build_timeline(index, seed, cfg, date)
        current = _current_version_at(versions, date)
        if current is None:
            continue
        records.append(
            {
                "product_id": _product_id(seed, index),
                "product_name": current.product_name,
                "category": current.category,
                "price": current.price,
                "sku": current.sku,
                "updated_at": current.effective_from.isoformat(),
            }
        )
    return records


def _write_csv(records: list[dict], target_url: str) -> None:
    """Write records to CSV at the given URL.

    Uses csv.DictWriter to guarantee column order and proper quoting; opens
    via fsspec so the same code handles file://, s3://, gs://.
    """
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(_CSV_COLUMNS))
    writer.writeheader()
    writer.writerows(records)
    body = buf.getvalue()

    parsed = urlparse(target_url)
    if parsed.scheme in {"", "file"}:
        local = parsed.path if parsed.scheme == "file" else target_url
        path = Path(local)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body)
        return
    with fsspec.open(target_url, "w") as handle:
        handle.write(body)


def run(
    start_date: dt.date,
    end_date: dt.date,
    bucket: str,
    cfg: dict,
    seed: int,
    record_count: int = _DEFAULT_RECORD_COUNT,
    partition_template: str | None = None,
) -> list[str]:
    products_cfg = cfg["generators"]["products"]
    raw_prefix = cfg["paths"]["raw_prefix"]
    template = partition_template or cfg["paths"]["partitioning"]

    written: list[str] = []
    for date in daily_dates(start_date, end_date):
        records = generate_for_date(date, products_cfg, seed, record_count)
        target_dir = partition_path(bucket, raw_prefix, "products", date, template)
        target_url = f"{target_dir}/products.csv"
        _write_csv(records, target_url)
        written.append(target_url)
    return written


@click.command()
@click.option("--start-date", required=True, type=click.DateTime(["%Y-%m-%d"]))
@click.option("--end-date", required=True, type=click.DateTime(["%Y-%m-%d"]))
@click.option("--env", default=None)
@click.option("--seed", default=None, type=int)
@click.option("--record-count", default=None, type=int)
@click.option("--bucket", default=None)
def main(
    start_date: dt.datetime,
    end_date: dt.datetime,
    env: str | None,
    seed: int | None,
    record_count: int | None,
    bucket: str | None,
) -> None:
    """Generate products snapshots for a date range."""
    cfg = load_config(env=env)
    if seed is None:
        seed = int(get_path(cfg, "generators.random_seed", default=42))
    if bucket is None:
        bucket = get_path(cfg, "storage.bucket")
    if record_count is None:
        record_count = int(
            get_path(cfg, "generators.products.record_count", default=_DEFAULT_RECORD_COUNT)
        )
    # Fill price_min / price_max from config if not present on products subtree
    cfg["generators"]["products"].setdefault(
        "price_min", get_path(cfg, "generators.orders.price_min", default=4.99)
    )
    cfg["generators"]["products"].setdefault(
        "price_max", get_path(cfg, "generators.orders.price_max", default=499.99)
    )

    written = run(
        start_date=start_date.date(),
        end_date=end_date.date(),
        bucket=bucket,
        cfg=cfg,
        seed=seed,
        record_count=record_count,
    )
    click.echo(f"Wrote {len(written)} products partition(s) to {bucket}/raw/products/")
    for path in written:
        click.echo(f"  {path}")


if __name__ == "__main__":
    main()
