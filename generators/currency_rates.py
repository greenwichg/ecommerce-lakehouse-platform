"""Synthetic currency rates generator.

Pulls daily exchange rates from exchangerate.host (free, no API key) and
writes one CSV snapshot per day at:

    <bucket>/raw/currency_rates/year=YYYY/month=MM/day=DD/rates.csv

## API + fallback contract

1. **Primary**: ``GET https://api.exchangerate.host/{date}?base=USD&symbols=...``
   with 3 attempts and exponential backoff (1s → 2s → 4s, ~7s total budget).
2. **Fallback**: deterministic simulated rates if all 3 attempts fail.
   Same shape as the API response so downstream code can't tell.
   Per-(seed, date, currency) seeded; same args → same simulated values.

## `_source` provenance column (Slice 4 observability)

Every emitted row carries a ``_source`` column with one of:

- ``"api"``: row came from a successful exchangerate.host fetch
- ``"simulated"``: row came from the deterministic fallback

This propagates through Bronze → Silver → Gold → Snowflake so the
``validate_currency_freshness`` Airflow gate can answer "what % of
yesterday's rates were API vs simulated?", and analysts can filter
revenue conversion to API-sourced rates only if they need strict
provenance.

## Use case

Orders are stored in USD. The currency_rates table enables multi-currency
revenue reporting: ``revenue_eur = sum(total_amount_usd * rate_usd_to_eur)``
PIT-joined on ``fact_orders.created_at::date = rate_date``.
"""

from __future__ import annotations

import csv
import datetime as dt
import io
import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import click
import fsspec
import requests

_REPO_ROOT = Path(__file__).resolve().parent.parent
_LIBS_DIR = _REPO_ROOT / "databricks"
if str(_LIBS_DIR) not in sys.path:
    sys.path.insert(0, str(_LIBS_DIR))

from libs.config import get_path, load_config  # noqa: E402

from generators._common import daily_dates, seeded_rng  # noqa: E402

log = logging.getLogger(__name__)

_API_URL = "https://api.exchangerate.host/{date}"
_API_TIMEOUT = 5.0
_RETRY_DELAYS = (1.0, 2.0, 4.0)  # ~7s total budget

DEFAULT_BASE = "USD"
DEFAULT_TARGETS = ("EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "SGD", "INR", "BRL", "MXN")

# Hardcoded "true" rates as of project epoch. Simulated fallback walks
# multiplicative jitter around these per day to mimic realistic drift.
_BASE_RATES = {
    "EUR": 0.92,
    "GBP": 0.79,
    "JPY": 149.50,
    "CAD": 1.36,
    "AUD": 1.52,
    "CHF": 0.88,
    "SGD": 1.34,
    "INR": 83.20,
    "BRL": 4.95,
    "MXN": 17.30,
}

_CSV_COLUMNS = (
    "rate_date",
    "base_currency",
    "target_currency",
    "rate",
    "_source",  # 'api' | 'simulated'
    "_fetched_at",
)


@dataclass(frozen=True)
class _Rate:
    rate_date: dt.date
    base_currency: str
    target_currency: str
    rate: float
    source: str
    fetched_at: dt.datetime


def _fetch_api(date: dt.date, base: str, targets: tuple[str, ...]) -> dict[str, float] | None:
    """Single API attempt. Returns rates dict on success, None on failure."""
    url = _API_URL.format(date=date.isoformat())
    params = {"base": base, "symbols": ",".join(targets)}
    try:
        resp = requests.get(url, params=params, timeout=_API_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
        # exchangerate.host returns {"success": true, "rates": {...}} on the date endpoint
        if not payload.get("success", True):  # newer versions omit success key on success
            return None
        rates = payload.get("rates") or {}
        if not all(c in rates for c in targets):
            return None
        return {c: float(rates[c]) for c in targets}
    except (requests.RequestException, ValueError):
        return None


def _fetch_with_retries(
    date: dt.date, base: str, targets: tuple[str, ...]
) -> dict[str, float] | None:
    """Three API attempts with exponential backoff. None if all fail."""
    for attempt, delay in enumerate(_RETRY_DELAYS, start=1):
        rates = _fetch_api(date, base, targets)
        if rates is not None:
            return rates
        if attempt < len(_RETRY_DELAYS):
            time.sleep(delay)
    return None


def _simulate_rates(
    date: dt.date, seed: int, base: str, targets: tuple[str, ...]
) -> dict[str, float]:
    """Deterministic fallback. Walks ±2% multiplicative jitter around base rates,
    seeded per (date, currency, seed)."""
    if base != "USD":
        raise ValueError(f"simulated rates only support base=USD, got {base!r}")
    result: dict[str, float] = {}
    for currency in targets:
        if currency not in _BASE_RATES:
            raise ValueError(f"no simulated base rate for {currency!r}")
        rng = seeded_rng("currency", seed, date.isoformat(), currency)
        jitter = rng.uniform(-0.02, 0.02)  # ±2%
        result[currency] = round(_BASE_RATES[currency] * (1.0 + jitter), 6)
    return result


def fetch_for_date(
    date: dt.date,
    seed: int,
    base: str = DEFAULT_BASE,
    targets: tuple[str, ...] = DEFAULT_TARGETS,
    use_api: bool = True,
) -> list[_Rate]:
    """Fetch (or simulate) currency rates for a single date.

    Args:
        date: the rate date (the API endpoint's day)
        seed: master seed; controls simulated-fallback determinism
        base: base currency, default USD
        targets: tuple of target currencies
        use_api: when False, skip the API entirely and go straight to
            simulation (used in tests where we don't want to hit the network)

    Returns one ``_Rate`` per target currency. Each row's ``source`` is
    either ``"api"`` (live fetch succeeded) or ``"simulated"`` (fallback
    used).
    """
    fetched_at = dt.datetime.now(dt.UTC)
    api_rates = _fetch_with_retries(date, base, targets) if use_api else None
    if api_rates is not None:
        return [
            _Rate(
                rate_date=date,
                base_currency=base,
                target_currency=c,
                rate=api_rates[c],
                source="api",
                fetched_at=fetched_at,
            )
            for c in targets
        ]
    log.warning("currency API unavailable for %s; using simulated rates", date)
    sim = _simulate_rates(date, seed, base, targets)
    return [
        _Rate(
            rate_date=date,
            base_currency=base,
            target_currency=c,
            rate=sim[c],
            source="simulated",
            fetched_at=fetched_at,
        )
        for c in targets
    ]


def _write_csv(rates: list[_Rate], target_url: str) -> None:
    """Write rates to CSV at the URL with the canonical column order."""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(_CSV_COLUMNS))
    writer.writeheader()
    for r in rates:
        writer.writerow(
            {
                "rate_date": r.rate_date.isoformat(),
                "base_currency": r.base_currency,
                "target_currency": r.target_currency,
                "rate": r.rate,
                "_source": r.source,
                "_fetched_at": r.fetched_at.isoformat(),
            }
        )
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
    use_api: bool = True,
) -> list[str]:
    """Generate per-day CSV snapshots and return paths written."""
    from libs.paths import partition_path

    raw_prefix = cfg["paths"]["raw_prefix"]
    template = cfg["paths"]["partitioning"]

    cur_cfg = cfg["generators"].get("currency_rates", {})
    base = cur_cfg.get("base_currency", DEFAULT_BASE)
    targets = tuple(cur_cfg.get("target_currencies", DEFAULT_TARGETS))

    written: list[str] = []
    for date in daily_dates(start_date, end_date):
        rates = fetch_for_date(date, seed=seed, base=base, targets=targets, use_api=use_api)
        target_dir = partition_path(bucket, raw_prefix, "currency_rates", date, template)
        target_url = f"{target_dir}/rates.csv"
        _write_csv(rates, target_url)
        written.append(target_url)
    return written


@click.command()
@click.option("--start-date", required=True, type=click.DateTime(["%Y-%m-%d"]))
@click.option("--end-date", required=True, type=click.DateTime(["%Y-%m-%d"]))
@click.option("--env", default=None)
@click.option("--seed", default=None, type=int)
@click.option("--bucket", default=None)
@click.option("--no-api", is_flag=True, help="Skip API; use simulated rates only.")
def main(
    start_date: dt.datetime,
    end_date: dt.datetime,
    env: str | None,
    seed: int | None,
    bucket: str | None,
    no_api: bool,
) -> None:
    """Generate currency rates snapshots for a date range."""
    cfg = load_config(env=env)
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
        use_api=not no_api,
    )
    click.echo(f"Wrote {len(written)} currency_rates partition(s) to {bucket}/raw/currency_rates/")
    for path in written:
        click.echo(f"  {path}")


if __name__ == "__main__":
    main()
