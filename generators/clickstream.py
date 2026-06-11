"""Synthetic clickstream events generator.

Produces one JSON Lines file per hour at:

    <bucket>/raw/clickstream/year=YYYY/month=MM/day=DD/hour=HH/events.json

Volume: ~50k events/day uniform across hours (so ~2k events/hour). The
diurnal pattern would be more realistic; documented as a Slice 5
enhancement in docs/runbook.md.

## Event / session model

The world has a pool of stable browser cookies (``raw_session_id`` values).
Each cookie has multiple "visits" over time. A visit is a contiguous burst
of activity — N events spaced 30s–5min apart. Two visits from the same
cookie are separated by >30 minutes of inactivity, which is exactly what
the Silver sessionizer's gap-and-island will recover (Silver sub-divides
the cookie into ``silver_session_key`` values; the generator's
``session_id`` is the cookie identity, not the post-sessionization key).

Roughly 30% of cookies are anonymous (``customer_id = NULL`` on every
event). The rest are bound to a customer from the same pool as the
customers generator — but a bound cookie's visits are only *attributed*
once the customer has signed up (pre-signup visits emit NULL), so the
fact_sessions → dim_customer PIT join always resolves.

Funnel: each visit starts with page_views; some add_to_cart, some
checkout, some purchase. Purchase implies prior checkout and add_to_cart.

## Idempotency

Same ``(start_hour, end_hour, seed, record_count, session_pool_size)`` →
same output. Visit timelines per cookie are computed deterministically
from ``seeded_rng("visit", master_seed, session_index)``.
"""

from __future__ import annotations

import datetime as dt
import json
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

from generators._common import deterministic_uuid, seeded_rng  # noqa: E402
from generators.customers import signup_date_for  # noqa: E402

PROJECT_EPOCH = dt.datetime(2024, 1, 1, tzinfo=dt.UTC)
_SESSION_HORIZON_HOURS = 730 * 24  # ~2y of session activity

# Pool sizes. 100k cookies is realistic for a small ecommerce site over 2y.
_DEFAULT_SESSION_POOL = 100_000
# Customer pool aligns with the customers generator so FKs resolve.
_DEFAULT_CUSTOMER_POOL = 10_000
_ANONYMOUS_RATE = 0.30

# Funnel probabilities — given that the visit reached the previous stage.
_P_ADD_TO_CART = 0.30
_P_CHECKOUT_GIVEN_ADD = 0.40  # → 12% of visits checkout
_P_PURCHASE_GIVEN_CHECKOUT = 0.55  # → 6.6% of visits purchase

_EVENT_TYPES = ("page_view", "add_to_cart", "checkout", "purchase")
_DEVICES = ("desktop", "mobile", "tablet")

_PAGE_URLS = (
    "/",
    "/products",
    "/products/featured",
    "/products/sale",
    "/categories/electronics",
    "/categories/books",
    "/categories/clothing",
    "/categories/home_kitchen",
    "/search?q=headphones",
    "/search?q=laptop",
    "/cart",
    "/checkout",
    "/account",
    "/orders",
)


@dataclass(frozen=True)
class _Event:
    event_id: str
    session_id: str
    customer_id: str | None
    event_type: str
    page_url: str
    event_ts: dt.datetime
    device: str
    user_agent: str


def _session_id(seed: int, index: int) -> str:
    return str(deterministic_uuid(f"clickstream-session:{seed}", index))


def _customer_pool_id(seed: int, index: int) -> str:
    """Mirror customers.py's customer_id derivation so FKs align."""
    return str(deterministic_uuid(f"customers:{seed}", index))


@lru_cache(maxsize=200_000)
def _build_visits_for_session_cached(
    session_index: int, seed: int, customer_pool_size: int
) -> tuple[_Event, ...]:
    """Memoised wrapper. Per-cookie timelines never change for fixed args, so
    sweeping many hours over the same pool reuses the work."""
    return tuple(_build_visits_for_session(session_index, seed, customer_pool_size))


def _build_visits_for_session(
    session_index: int, seed: int, customer_pool_size: int
) -> list[_Event]:
    """Generate the full event stream for one cookie over the project lifetime.

    Deterministic from ``(seed, session_index)``. The session's first visit
    happens within ``_SESSION_HORIZON_HOURS`` of PROJECT_EPOCH; subsequent
    visits are gamma-distributed in time. Within each visit, the funnel
    stages are sampled per the Slice 3 design.
    """
    rng = seeded_rng("visit", seed, session_index)
    fake = Faker()
    fake.seed_instance(rng.randint(0, 1_000_000_000))

    raw_session_id = _session_id(seed, session_index)

    # Anonymity is stable per cookie: a cookie is either logged in or not
    # for its entire lifetime. (Real world has cookie-shared-with-account
    # nuances; we don't model that here.) For logged-in cookies, visits
    # that START before the bound customer's signup date are emitted as
    # anonymous — the person hadn't created the account yet, and the
    # Snowflake PIT join (fact_sessions → dim_customer) would otherwise
    # orphan those sessions past the 1% gate.
    is_anonymous = rng.random() < _ANONYMOUS_RATE
    customer_id: str | None
    customer_signup: dt.date | None
    if is_anonymous:
        customer_id = None
        customer_signup = None
    else:
        customer_index = rng.randint(0, customer_pool_size - 1)
        customer_id = _customer_pool_id(seed, customer_index)
        customer_signup = signup_date_for(seed, customer_index)

    # Stable device + user agent per cookie.
    device = rng.choice(_DEVICES)
    user_agent = fake.user_agent()

    # How many visits does this cookie make? Power-law-ish: most cookies
    # have a handful, a few are heavy users. Gamma(2, 5) gives mean 10,
    # median ~7, p95 ~22. Truncate to keep things bounded.
    n_visits = min(int(rng.gammavariate(2.0, 5.0)), 50)
    if n_visits == 0:
        return []

    # Visit start hours, uniform in the horizon. Sorted so the timeline
    # is monotonic and Silver's window functions don't have to reorder
    # across cookie boundaries.
    first_offset_hours = sorted(rng.randint(0, _SESSION_HORIZON_HOURS) for _ in range(n_visits))

    events: list[_Event] = []
    for visit_idx, hour_offset in enumerate(first_offset_hours):
        visit_start = PROJECT_EPOCH + dt.timedelta(hours=hour_offset, minutes=rng.randint(0, 59))
        # Pre-signup visits are anonymous (see note above).
        visit_customer_id = (
            customer_id
            if customer_signup is not None and customer_signup <= visit_start.date()
            else None
        )
        # Stage 1: 1–8 page_views, 30s–5min apart
        n_pv = max(1, int(rng.gammavariate(2.0, 2.0)))
        n_pv = min(n_pv, 8)
        cursor = visit_start
        for pv in range(n_pv):
            events.append(
                _Event(
                    event_id=str(
                        deterministic_uuid("event", seed, session_index, visit_idx, "pv", pv)
                    ),
                    session_id=raw_session_id,
                    customer_id=visit_customer_id,
                    event_type="page_view",
                    page_url=rng.choice(_PAGE_URLS),
                    event_ts=cursor,
                    device=device,
                    user_agent=user_agent,
                )
            )
            cursor += dt.timedelta(seconds=rng.randint(30, 300))

        # Stage 2: maybe add_to_cart
        if rng.random() < _P_ADD_TO_CART:
            cursor += dt.timedelta(seconds=rng.randint(10, 60))
            events.append(
                _Event(
                    event_id=str(
                        deterministic_uuid("event", seed, session_index, visit_idx, "atc")
                    ),
                    session_id=raw_session_id,
                    customer_id=visit_customer_id,
                    event_type="add_to_cart",
                    page_url="/cart",
                    event_ts=cursor,
                    device=device,
                    user_agent=user_agent,
                )
            )
            # Stage 3: maybe checkout
            if rng.random() < _P_CHECKOUT_GIVEN_ADD:
                cursor += dt.timedelta(seconds=rng.randint(20, 180))
                events.append(
                    _Event(
                        event_id=str(
                            deterministic_uuid("event", seed, session_index, visit_idx, "co")
                        ),
                        session_id=raw_session_id,
                        customer_id=visit_customer_id,
                        event_type="checkout",
                        page_url="/checkout",
                        event_ts=cursor,
                        device=device,
                        user_agent=user_agent,
                    )
                )
                # Stage 4: maybe purchase
                if rng.random() < _P_PURCHASE_GIVEN_CHECKOUT:
                    cursor += dt.timedelta(seconds=rng.randint(15, 90))
                    events.append(
                        _Event(
                            event_id=str(
                                deterministic_uuid("event", seed, session_index, visit_idx, "p")
                            ),
                            session_id=raw_session_id,
                            customer_id=visit_customer_id,
                            event_type="purchase",
                            page_url="/orders/confirm",
                            event_ts=cursor,
                            device=device,
                            user_agent=user_agent,
                        )
                    )
    return events


def generate_for_hour(
    hour_start: dt.datetime,
    seed: int,
    session_pool_size: int = _DEFAULT_SESSION_POOL,
    customer_pool_size: int = _DEFAULT_CUSTOMER_POOL,
) -> list[dict]:
    """Build the hour's clickstream events. Idempotent.

    Iterates every cookie in the pool, computes its full visit timeline,
    and keeps only events with ``event_ts`` in ``[hour_start, hour_start+1h)``.
    For large pools this is O(N) per hour but each session timeline build
    is small; tests run with a tiny pool to keep the suite fast.
    """
    if hour_start.tzinfo is None:
        hour_start = hour_start.replace(tzinfo=dt.UTC)
    hour_end = hour_start + dt.timedelta(hours=1)
    records: list[dict] = []
    for session_idx in range(session_pool_size):
        for event in _build_visits_for_session_cached(session_idx, seed, customer_pool_size):
            if hour_start <= event.event_ts < hour_end:
                records.append(
                    {
                        "event_id": event.event_id,
                        "session_id": event.session_id,
                        "customer_id": event.customer_id,
                        "event_type": event.event_type,
                        "page_url": event.page_url,
                        "event_ts": event.event_ts.isoformat(),
                        "device": event.device,
                        "user_agent": event.user_agent,
                    }
                )
    # Sort by event_ts for nicer files; not load-bearing for correctness.
    records.sort(key=lambda r: r["event_ts"])
    return records


def _hourly_dates(start: dt.datetime, end: dt.datetime) -> list[dt.datetime]:
    """Hours in [start, end] inclusive, both aligned to top-of-hour."""
    if start.tzinfo is None:
        start = start.replace(tzinfo=dt.UTC)
    if end.tzinfo is None:
        end = end.replace(tzinfo=dt.UTC)
    if end < start:
        raise ValueError("end must be >= start")
    hours = []
    current = start.replace(minute=0, second=0, microsecond=0)
    end_aligned = end.replace(minute=0, second=0, microsecond=0)
    while current <= end_aligned:
        hours.append(current)
        current += dt.timedelta(hours=1)
    return hours


def _hour_partition_path(
    bucket: str, raw_prefix: str, hour: dt.datetime, partition_template: str
) -> str:
    """Like libs.paths.partition_path but appends hour=HH."""
    base = partition_path(bucket, raw_prefix, "clickstream", hour.date(), partition_template)
    return f"{base}/hour={hour.hour:02d}"


def _write_jsonl(records: list[dict], target_url: str) -> None:
    """Write one JSON object per line. Sorted keys for deterministic bytes."""
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
    start_hour: dt.datetime,
    end_hour: dt.datetime,
    bucket: str,
    cfg: dict,
    seed: int,
    session_pool_size: int = _DEFAULT_SESSION_POOL,
    customer_pool_size: int = _DEFAULT_CUSTOMER_POOL,
) -> list[str]:
    """Generate per-hour JSON Lines files. Returns the paths written."""
    raw_prefix = cfg["paths"]["raw_prefix"]
    template = cfg["paths"]["partitioning"]

    written: list[str] = []
    for hour in _hourly_dates(start_hour, end_hour):
        records = generate_for_hour(hour, seed, session_pool_size, customer_pool_size)
        target_dir = _hour_partition_path(bucket, raw_prefix, hour, template)
        target_url = f"{target_dir}/events.json"
        _write_jsonl(records, target_url)
        written.append(target_url)
    return written


@click.command()
@click.option(
    "--start-hour", required=True, type=click.DateTime(["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H"])
)
@click.option(
    "--end-hour", required=True, type=click.DateTime(["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H"])
)
@click.option("--env", default=None)
@click.option("--seed", default=None, type=int)
@click.option("--session-pool-size", default=None, type=int)
@click.option("--bucket", default=None)
def main(
    start_hour: dt.datetime,
    end_hour: dt.datetime,
    env: str | None,
    seed: int | None,
    session_pool_size: int | None,
    bucket: str | None,
) -> None:
    """Generate clickstream events for an hour range."""
    cfg = load_config(env=env)
    if seed is None:
        seed = int(get_path(cfg, "generators.random_seed", default=42))
    if bucket is None:
        bucket = get_path(cfg, "storage.bucket")
    if session_pool_size is None:
        session_pool_size = int(
            get_path(cfg, "generators.clickstream.session_pool_size", default=_DEFAULT_SESSION_POOL)
        )

    written = run(
        start_hour=start_hour,
        end_hour=end_hour,
        bucket=bucket,
        cfg=cfg,
        seed=seed,
        session_pool_size=session_pool_size,
    )
    click.echo(f"Wrote {len(written)} clickstream partition(s) to {bucket}/raw/clickstream/")
    for path in written:
        click.echo(f"  {path}")


if __name__ == "__main__":
    main()
