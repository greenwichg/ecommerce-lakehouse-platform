"""Synthetic customers generator.

Produces one Parquet snapshot per day at:

    <bucket>/raw/customers/year=YYYY/month=MM/day=DD/customers.parquet

Each customer has a deterministic timeline derived from
``(master_seed, customer_index)``:

- Signed up on some day between ``project_epoch`` and the target date.
- Carries a chain of ``(effective_from, name, email, address)`` versions.
  The version chain is built once per customer and then *queried* per
  generation date — picking the latest version with
  ``effective_from <= date``.
- ~``daily_change_pct``% of customers change email or address per day.
  Names don't change (treated as stable).

For each date D, the daily snapshot contains every customer who has
signed up by D, with attributes as of their latest version <= D and
``updated_at`` = that version's ``effective_from``. Customers with no
change since signup keep ``updated_at`` = signup datetime.

This shape is exactly what the Silver MERGE consumes: one row per
``customer_id`` per day, with ``updated_at`` reflecting the source-of-
truth change time. SCD2 in Gold materialises the version chain from
the stream of these snapshots.
"""

from __future__ import annotations

import datetime as dt
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import click
import pyarrow as pa
from faker import Faker

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

# Fixed reference date: customers can only sign up on or after this. Held
# stable across runs so re-generations don't shift signup dates.
PROJECT_EPOCH = dt.date(2024, 1, 1)

# Customers' signup dates are sampled uniformly across this window from
# PROJECT_EPOCH. Wider than the typical query window so generation against a
# date inside the window yields a realistic mix of "already signed up" and
# "not yet signed up" customers — the latter are filtered out per-day.
_SIGNUP_HORIZON_DAYS = 730  # ~2 years

_DEFAULT_RECORD_COUNT = 10_000

_PARQUET_SCHEMA = pa.schema(
    [
        ("customer_id", pa.string()),
        ("name", pa.string()),
        ("email", pa.string()),
        ("address", pa.string()),
        ("signup_date", pa.date32()),
        ("updated_at", pa.timestamp("us", tz="UTC")),
    ]
)


@dataclass(frozen=True)
class _CustomerVersion:
    effective_from: dt.datetime
    name: str
    email: str
    address: str


def _customer_id(seed: int, index: int) -> str:
    return str(deterministic_uuid(f"customers:{seed}", index))


@lru_cache(maxsize=65_536)
def signup_date_for(seed: int, customer_index: int) -> dt.date:
    """The deterministic signup date for one customer, without building
    the full timeline.

    Event generators (orders, wishlist, clickstream) use this to only
    reference customers who exist at event time — otherwise their PIT
    joins against dim_customer produce NULL surrogates and trip the
    orphan-rate gates (threshold 1%).

    Must consume the RNG stream in exactly the same order as
    ``_build_timeline`` (faker-seed draw, then signup-offset draw);
    ``test_signup_date_for_matches_timeline`` guards the alignment.
    """
    rng = seeded_rng("customer", seed, customer_index)
    rng.randint(0, 1_000_000_000)  # the faker-seed draw in _build_timeline
    return PROJECT_EPOCH + dt.timedelta(days=rng.randint(0, _SIGNUP_HORIZON_DAYS))


def _build_timeline(
    customer_index: int,
    seed: int,
    cfg: dict,
    max_date: dt.date,
    project_epoch: dt.date = PROJECT_EPOCH,
) -> list[_CustomerVersion]:
    """Compute one customer's full SCD2 timeline over the fixed horizon.

    Returns an empty list if the customer's signup date is after ``max_date``
    (i.e., they don't exist yet from the perspective of that date). The
    version chain itself does NOT depend on ``max_date`` — it spans
    (signup, project_epoch + horizon] regardless of the generation date, so
    consecutive daily snapshots agree about the customer's history.
    ``_current_version_at`` picks the version visible at a given date.
    """
    rng = seeded_rng("customer", seed, customer_index)

    # Faker is seeded from the same RNG so two customers don't collide on
    # name/email, but the same customer reproduces identically across runs.
    fake = Faker()
    fake.seed_instance(rng.randint(0, 1_000_000_000))

    # Sample signup offset from the fixed horizon (not from days-since-epoch),
    # so re-generating at a later max_date doesn't shift a customer's signup
    # date — and so some customers genuinely haven't signed up at early
    # generation dates.
    signup_offset = rng.randint(0, _SIGNUP_HORIZON_DAYS)
    signup_date = project_epoch + dt.timedelta(days=signup_offset)
    if signup_date > max_date:
        return []

    base = _CustomerVersion(
        effective_from=dt.datetime.combine(signup_date, dt.time(0, 0, 0), dt.UTC),
        name=fake.name(),
        email=fake.email(),
        address=fake.address().replace("\n", ", "),
    )
    versions: list[_CustomerVersion] = [base]

    # Number of changes over the customer's horizon lifetime. The chain is
    # sampled over the FIXED window (signup, project_epoch + horizon], not
    # up to max_date: if the chain depended on the generation date, two
    # consecutive daily snapshots would disagree about the customer's
    # history (different RNG draws → e.g. a different email at the same
    # updated_at), which downstream SCD2 merges can't reconcile. Expected
    # changes visible at any date D stay ≈ days-since-signup × pct/100
    # either way. Modelled as Poisson via a gamma sample
    # (gammavariate(k, 1) ≈ Poisson(k) for our coarse purposes).
    # Explicit short-circuit for the 0% case: gammavariate's α=0.1 floor
    # has a long enough tail to occasionally fire even at avg_changes=0.
    if cfg["daily_change_pct"] <= 0:
        return versions
    horizon_end = project_epoch + dt.timedelta(days=_SIGNUP_HORIZON_DAYS)
    chain_days = max(1, (horizon_end - signup_date).days)
    avg_changes = chain_days * cfg["daily_change_pct"] / 100.0
    n_changes = int(rng.gammavariate(max(avg_changes, 0.1), 1.0))

    # Change dates: distinct days drawn uniformly from (signup, horizon_end].
    # Sorted so the resulting version chain is monotonically increasing.
    if n_changes == 0:
        return versions
    candidate_offsets = sorted({rng.randint(1, chain_days) for _ in range(n_changes)})

    current_email = base.email
    current_address = base.address
    for offset in candidate_offsets:
        change_date = signup_date + dt.timedelta(days=offset)
        # Change either email or address — both are SCD2-tracked. Pick one
        # per change so each event represents a single attribute moving.
        if rng.random() < 0.5:
            current_email = fake.email()
        else:
            current_address = fake.address().replace("\n", ", ")
        versions.append(
            _CustomerVersion(
                effective_from=dt.datetime.combine(change_date, dt.time(0, 0, 0), dt.UTC),
                name=base.name,
                email=current_email,
                address=current_address,
            )
        )
    return versions


def _current_version_at(versions: list[_CustomerVersion], date: dt.date) -> _CustomerVersion | None:
    """Return the latest version whose ``effective_from <= end-of-date``."""
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
    """Build the day's customer snapshot. Idempotent given (date, cfg, seed)."""
    records: list[dict] = []
    for index in range(record_count):
        versions = _build_timeline(index, seed, cfg, date)
        current = _current_version_at(versions, date)
        if current is None:
            continue  # not yet signed up
        records.append(
            {
                "customer_id": _customer_id(seed, index),
                "name": current.name,
                "email": current.email,
                "address": current.address,
                "signup_date": versions[0].effective_from.date(),
                "updated_at": current.effective_from,
            }
        )
    return records


def _records_to_table(records: Iterable[dict]) -> pa.Table:
    return pa.Table.from_pylist(list(records), schema=_PARQUET_SCHEMA)


def run(
    start_date: dt.date,
    end_date: dt.date,
    bucket: str,
    cfg: dict,
    seed: int,
    record_count: int = _DEFAULT_RECORD_COUNT,
    partition_template: str | None = None,
) -> list[str]:
    """Generate per-day Parquet snapshots and return the paths written."""
    customers_cfg = cfg["generators"]["customers"]
    raw_prefix = cfg["paths"]["raw_prefix"]
    template = partition_template or cfg["paths"]["partitioning"]

    written: list[str] = []
    for date in daily_dates(start_date, end_date):
        records = generate_for_date(date, customers_cfg, seed, record_count)
        target_dir = partition_path(bucket, raw_prefix, "customers", date, template)
        target_url = f"{target_dir}/customers.parquet"
        write_parquet(_records_to_table(records), target_url)
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
    """Generate customers snapshots for a date range."""
    cfg = load_config(env=env)
    if seed is None:
        seed = int(get_path(cfg, "generators.random_seed", default=42))
    if bucket is None:
        bucket = get_path(cfg, "storage.bucket")
    if record_count is None:
        record_count = int(
            get_path(cfg, "generators.customers.record_count", default=_DEFAULT_RECORD_COUNT)
        )

    written = run(
        start_date=start_date.date(),
        end_date=end_date.date(),
        bucket=bucket,
        cfg=cfg,
        seed=seed,
        record_count=record_count,
    )
    click.echo(f"Wrote {len(written)} customers partition(s) to {bucket}/raw/customers/")
    for path in written:
        click.echo(f"  {path}")


if __name__ == "__main__":
    main()
