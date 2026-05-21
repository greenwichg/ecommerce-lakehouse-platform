"""Synthetic wishlist events generator.

Emits ~5,000 wishlist-addition events per day as JSON Lines at:

    <bucket>/raw/wishlist/year=YYYY/month=MM/day=DD/wishlist.json

## Grain

Per-event: one row per (customer_id, product_id, added_at). Append-only.
Re-adds (same customer + product, different timestamps) are kept as
separate events — that's signal, not noise. See
``docs/architecture.md`` § "Slice 4 — factless fact grain" for the
rationale.

## Authentication assumption

Wishlists are an authenticated-only feature in this model — every event
has a non-null ``customer_id``. The UX doesn't expose an anonymous
wishlist button. (Realistic for most ecommerce: anonymous browsers can
save items via local storage but those don't reach the backend.)

## FK alignment

``customer_id`` drawn from the same pool as ``generators.customers``
(``_DEFAULT_RECORD_COUNT = 10_000`` customers, deterministic UUIDv5 from
``("customers:{seed}", index)``). ``product_id`` drawn from
``generators.products`` (``PRD-{seed:04d}-{idx:05d}``). So once both
sources land, the wishlist factless fact's PIT joins to dim_customer
and dim_product resolve.
"""

from __future__ import annotations

import datetime as dt
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import click
import fsspec

_REPO_ROOT = Path(__file__).resolve().parent.parent
_LIBS_DIR = _REPO_ROOT / "databricks"
if str(_LIBS_DIR) not in sys.path:
    sys.path.insert(0, str(_LIBS_DIR))

from libs.config import get_path, load_config  # noqa: E402
from libs.paths import partition_path  # noqa: E402

from generators._common import daily_dates, deterministic_uuid, seeded_rng  # noqa: E402

_DEFAULT_EVENTS_PER_DAY = 5_000
_DEFAULT_CUSTOMER_POOL = 10_000
_DEFAULT_PRODUCT_POOL = 5_000

# Where wishlist additions originate from. Used by analytics to attribute
# wishlist-driven engagement (e.g., "how many wishlists came from the
# product page vs the search results?").
_SOURCES = (
    "/products",  # detail page CTA
    "/search",  # search-results inline CTA
    "/recommendations",  # "you might also like"
    "/cart",  # "save for later"
    "/email-campaign",  # link from marketing email
)


@dataclass(frozen=True)
class _WishlistEvent:
    wishlist_event_id: str
    customer_id: str
    product_id: str
    added_at: dt.datetime
    source: str


def _customer_pool_id(seed: int, index: int) -> str:
    """Mirror customers.py's customer_id derivation so FKs align."""
    return str(deterministic_uuid(f"customers:{seed}", index))


def _product_pool_id(seed: int, index: int) -> str:
    """Mirror products.py's product_id derivation."""
    return f"PRD-{seed:04d}-{index:05d}"


def generate_for_date(
    date: dt.date,
    seed: int,
    events_per_day: int = _DEFAULT_EVENTS_PER_DAY,
    customer_pool_size: int = _DEFAULT_CUSTOMER_POOL,
    product_pool_size: int = _DEFAULT_PRODUCT_POOL,
) -> list[dict]:
    """Build the day's wishlist events. Deterministic from (date, seed)."""
    events: list[dict] = []
    for index in range(events_per_day):
        # Each event is seeded per (date, index, seed) so identical args
        # yield identical events.
        rng = seeded_rng("wishlist", seed, date.isoformat(), index)
        customer_id = _customer_pool_id(seed, rng.randint(0, customer_pool_size - 1))
        product_id = _product_pool_id(seed, rng.randint(0, product_pool_size - 1))
        # added_at: random second within the day, monotonically usable for
        # ordering but not aligned to "real" diurnal traffic shape (kept
        # uniform for simplicity; documented as a Slice 5+ enhancement)
        seconds = rng.randint(0, 24 * 3600 - 1)
        added_at = dt.datetime.combine(date, dt.time(0, 0, 0), dt.UTC) + dt.timedelta(
            seconds=seconds
        )
        source = rng.choice(_SOURCES)
        wishlist_event_id = str(deterministic_uuid("wishlist-event", seed, date.isoformat(), index))
        events.append(
            {
                "wishlist_event_id": wishlist_event_id,
                "customer_id": customer_id,
                "product_id": product_id,
                "added_at": added_at.isoformat(),
                "source": source,
            }
        )
    # Sort by added_at so the file looks like an event stream.
    events.sort(key=lambda e: e["added_at"])
    return events


def _write_jsonl(records: list[dict], target_url: str) -> None:
    body = "\n".join(json.dumps(r, sort_keys=True) for r in records) + ("\n" if records else "")
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
    events_per_day: int = _DEFAULT_EVENTS_PER_DAY,
    customer_pool_size: int = _DEFAULT_CUSTOMER_POOL,
    product_pool_size: int = _DEFAULT_PRODUCT_POOL,
) -> list[str]:
    raw_prefix = cfg["paths"]["raw_prefix"]
    template = cfg["paths"]["partitioning"]
    written: list[str] = []
    for date in daily_dates(start_date, end_date):
        records = generate_for_date(
            date, seed, events_per_day, customer_pool_size, product_pool_size
        )
        target_dir = partition_path(bucket, raw_prefix, "wishlist", date, template)
        target_url = f"{target_dir}/wishlist.json"
        _write_jsonl(records, target_url)
        written.append(target_url)
    return written


@click.command()
@click.option("--start-date", required=True, type=click.DateTime(["%Y-%m-%d"]))
@click.option("--end-date", required=True, type=click.DateTime(["%Y-%m-%d"]))
@click.option("--env", default=None)
@click.option("--seed", default=None, type=int)
@click.option("--events-per-day", default=None, type=int)
@click.option("--bucket", default=None)
def main(
    start_date: dt.datetime,
    end_date: dt.datetime,
    env: str | None,
    seed: int | None,
    events_per_day: int | None,
    bucket: str | None,
) -> None:
    """Generate wishlist events for a date range."""
    cfg = load_config(env=env)
    if seed is None:
        seed = int(get_path(cfg, "generators.random_seed", default=42))
    if bucket is None:
        bucket = get_path(cfg, "storage.bucket")
    if events_per_day is None:
        events_per_day = int(
            get_path(cfg, "generators.wishlist.events_per_day", default=_DEFAULT_EVENTS_PER_DAY)
        )

    written = run(
        start_date=start_date.date(),
        end_date=end_date.date(),
        bucket=bucket,
        cfg=cfg,
        seed=seed,
        events_per_day=events_per_day,
    )
    click.echo(f"Wrote {len(written)} wishlist partition(s) to {bucket}/raw/wishlist/")
    for path in written:
        click.echo(f"  {path}")


if __name__ == "__main__":
    main()
