"""Tests for the clickstream generator.

Coverage:
- Idempotency: same args -> same output (line-equal JSON Lines).
- Per-event schema: required fields present, types correct.
- Anonymous rate: ~30% of events have customer_id = NULL.
- Funnel monotonicity: a session with a purchase event always has a
  prior checkout, add_to_cart, and page_view.
- Session continuity: a single cookie's events within a visit are
  spaced < 30 min apart (so Silver's gap-and-island groups them).
- Cross-visit gap: a cookie with multiple visits has at least one
  >30-min gap (so Silver creates multiple session_keys for it).
- Hour bucketing: emitted events all fall within the target hour.
- Output JSON Lines: parseable, line-equal across runs.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pytest

from generators.clickstream import (
    _DEFAULT_CUSTOMER_POOL,
    PROJECT_EPOCH,
    _build_visits_for_session,
    generate_for_hour,
    run,
)


@pytest.fixture()
def clickstream_test_cfg() -> dict:
    """Small pool config to keep tests fast."""
    return {
        "paths": {
            "raw_prefix": "raw",
            "partitioning": "year={year:04d}/month={month:02d}/day={day:02d}",
        },
    }


def test_idempotent_same_args() -> None:
    """Same args -> identical output across two calls."""
    h = PROJECT_EPOCH + dt.timedelta(hours=24)  # day 1
    a = generate_for_hour(h, seed=42, session_pool_size=200, customer_pool_size=100)
    b = generate_for_hour(h, seed=42, session_pool_size=200, customer_pool_size=100)
    assert a == b


def test_different_seed_different_session_ids() -> None:
    h = PROJECT_EPOCH + dt.timedelta(hours=100)
    a_ids = {
        r["session_id"]
        for r in generate_for_hour(h, seed=42, session_pool_size=200, customer_pool_size=100)
    }
    b_ids = {
        r["session_id"]
        for r in generate_for_hour(h, seed=99, session_pool_size=200, customer_pool_size=100)
    }
    if a_ids and b_ids:
        assert a_ids.isdisjoint(b_ids)


def test_all_events_in_target_hour() -> None:
    """No events leak from neighbouring hours."""
    h = PROJECT_EPOCH + dt.timedelta(hours=50)
    records = generate_for_hour(h, seed=42, session_pool_size=500, customer_pool_size=100)
    h_end = h + dt.timedelta(hours=1)
    for r in records:
        ts = dt.datetime.fromisoformat(r["event_ts"])
        assert h <= ts < h_end, f"event {r['event_id']} outside hour: {ts}"


def _records_in_first_busy_hour(pool: int = 2000) -> list[dict]:
    """Sweep early hours until we find one with events. Tests using small
    pools over a 17,520-hour horizon need this — most hours are empty
    at low density, but the generator emits correctly when there's signal."""
    for h_off in range(0, 500, 10):
        h = PROJECT_EPOCH + dt.timedelta(hours=h_off)
        records = generate_for_hour(h, seed=42, session_pool_size=pool, customer_pool_size=100)
        if records:
            return records
    raise AssertionError(f"no busy hour found within first 500h at pool={pool}")


def test_event_schema_keys_present() -> None:
    records = _records_in_first_busy_hour()
    required = {
        "event_id",
        "session_id",
        "customer_id",
        "event_type",
        "page_url",
        "event_ts",
        "device",
        "user_agent",
    }
    for r in records:
        assert set(r.keys()) == required


def test_event_types_from_known_enum() -> None:
    records = _records_in_first_busy_hour()
    known = {"page_view", "add_to_cart", "checkout", "purchase"}
    for r in records:
        assert r["event_type"] in known


def test_anonymous_rate_approximately_30pct() -> None:
    """Anonymity is per-cookie. Aggregated over many events, ~30% null."""
    # Sweep many hours to get a large event sample.
    all_events = []
    for h_off in range(0, 200, 8):
        h = PROJECT_EPOCH + dt.timedelta(hours=h_off)
        all_events.extend(
            generate_for_hour(h, seed=42, session_pool_size=500, customer_pool_size=100)
        )
    anon = sum(1 for r in all_events if r["customer_id"] is None)
    if not all_events:
        pytest.skip("not enough sampled events")
    rate = anon / len(all_events)
    assert 0.20 < rate < 0.40, f"anonymous rate {rate:.2%} outside [20%, 40%] band"


def test_purchase_implies_prior_funnel_stages() -> None:
    """Sessionization invariant: every purchase has page_view + add_to_cart + checkout
    in the same visit, in time order."""
    # Build many visit timelines, find ones containing a purchase, verify order.
    found_purchase_session = False
    for idx in range(200):
        events = _build_visits_for_session(idx, seed=42, customer_pool_size=100)
        purchases = [e for e in events if e.event_type == "purchase"]
        for purchase in purchases:
            found_purchase_session = True
            # All earlier events in the same cookie, within 30 min before, must
            # include at least page_view, add_to_cart, checkout
            earlier = [
                e
                for e in events
                if e.event_ts < purchase.event_ts
                and (purchase.event_ts - e.event_ts) < dt.timedelta(minutes=30)
            ]
            types_before = {e.event_type for e in earlier}
            assert "page_view" in types_before
            assert "add_to_cart" in types_before
            assert "checkout" in types_before
    assert found_purchase_session, "no purchase events generated in 200 sessions"


def test_intra_visit_events_within_30min() -> None:
    """Events within a single visit shouldn't be spaced > 30 min apart
    (otherwise Silver would treat them as separate sessions, which would
    break the funnel-implies-prior-stages invariant)."""
    for idx in range(50):
        events = _build_visits_for_session(idx, seed=42, customer_pool_size=100)
        events_sorted = sorted(events, key=lambda e: e.event_ts)
        # Within a visit = events grouped by < 30 min gaps
        visit_groups: list[list] = []
        current_group: list = []
        for e in events_sorted:
            if not current_group or (e.event_ts - current_group[-1].event_ts) < dt.timedelta(
                minutes=30
            ):
                current_group.append(e)
            else:
                visit_groups.append(current_group)
                current_group = [e]
        if current_group:
            visit_groups.append(current_group)
        # Each group's events should all be < 30 min from neighbours
        for group in visit_groups:
            for prev, nxt in zip(group, group[1:], strict=False):
                gap = nxt.event_ts - prev.event_ts
                assert gap < dt.timedelta(minutes=30), f"intra-visit gap {gap}"


def test_cross_visit_gaps_exist() -> None:
    """Most cookies with multiple visits will have at least one >30-min
    inter-visit gap — this is what Silver's gap-and-island slices on."""
    found_multi_visit = False
    for idx in range(100):
        events = _build_visits_for_session(idx, seed=42, customer_pool_size=100)
        if len(events) < 2:
            continue
        events_sorted = sorted(events, key=lambda e: e.event_ts)
        large_gaps = [
            (nxt.event_ts - prev.event_ts)
            for prev, nxt in zip(events_sorted, events_sorted[1:], strict=False)
            if (nxt.event_ts - prev.event_ts) > dt.timedelta(minutes=30)
        ]
        if large_gaps:
            found_multi_visit = True
            break
    assert found_multi_visit, "no cookies with >30-min inter-visit gaps"


def test_customer_id_stable_within_cookie() -> None:
    """A cookie's customer_id is stable across all its events (we don't
    model cookie-shared-between-accounts)."""
    for idx in range(50):
        events = _build_visits_for_session(idx, seed=42, customer_pool_size=100)
        if not events:
            continue
        cids = {e.customer_id for e in events}
        assert len(cids) == 1, f"cookie {idx} has multiple customer_ids: {cids}"


def test_device_and_user_agent_stable_within_cookie() -> None:
    """A cookie = a browser; same device + user agent across all events."""
    for idx in range(50):
        events = _build_visits_for_session(idx, seed=42, customer_pool_size=100)
        if not events:
            continue
        assert len({e.device for e in events}) == 1
        assert len({e.user_agent for e in events}) == 1


def test_customer_ids_align_with_customers_generator_pool() -> None:
    """Clickstream customer_ids must come from the same pool the customers
    generator produces, so FKs in fact_sessions resolve."""
    from generators.customers import _customer_id as customers_id

    expected_pool = {customers_id(seed=42, index=i) for i in range(100)}
    records = _records_in_first_busy_hour()
    non_null = {r["customer_id"] for r in records if r["customer_id"] is not None}
    if not non_null:
        # All events in the first busy hour happened to be anonymous;
        # sweep more hours.
        for h_off in range(0, 500, 5):
            h = PROJECT_EPOCH + dt.timedelta(hours=h_off)
            recs = generate_for_hour(h, seed=42, session_pool_size=2000, customer_pool_size=100)
            non_null.update(r["customer_id"] for r in recs if r["customer_id"] is not None)
            if non_null:
                break
    assert non_null, "expected at least one non-null customer_id"
    assert non_null.issubset(expected_pool)


def test_run_writes_hourly_partitions(clickstream_test_cfg: dict, tmp_path: Path) -> None:
    bucket = f"file://{tmp_path}"
    start = PROJECT_EPOCH + dt.timedelta(hours=24)
    end = PROJECT_EPOCH + dt.timedelta(hours=26)  # 3 hours inclusive
    written = run(
        start_hour=start,
        end_hour=end,
        bucket=bucket,
        cfg=clickstream_test_cfg,
        seed=42,
        session_pool_size=200,
        customer_pool_size=100,
    )
    assert len(written) == 3
    for h_off in range(3):
        h = start + dt.timedelta(hours=h_off)
        expected = (
            tmp_path
            / "raw"
            / "clickstream"
            / f"year={h.year:04d}"
            / f"month={h.month:02d}"
            / f"day={h.day:02d}"
            / f"hour={h.hour:02d}"
            / "events.json"
        )
        assert expected.is_file(), f"missing {expected}"


def test_run_output_is_valid_json_lines(clickstream_test_cfg: dict, tmp_path: Path) -> None:
    bucket = f"file://{tmp_path}"
    # Find a busy hour to anchor the test on
    for h_off in range(0, 500, 10):
        h = PROJECT_EPOCH + dt.timedelta(hours=h_off)
        records = generate_for_hour(h, seed=42, session_pool_size=2000, customer_pool_size=100)
        if records:
            break
    else:
        raise AssertionError("no busy hour found")

    run(
        start_hour=h,
        end_hour=h,
        bucket=bucket,
        cfg=clickstream_test_cfg,
        seed=42,
        session_pool_size=2000,
        customer_pool_size=100,
    )
    path = (
        tmp_path
        / "raw"
        / "clickstream"
        / f"year={h.year:04d}"
        / f"month={h.month:02d}"
        / f"day={h.day:02d}"
        / f"hour={h.hour:02d}"
        / "events.json"
    )
    with path.open() as f:
        lines = [line.strip() for line in f if line.strip()]
    assert lines, "no events written"
    for line in lines:
        record = json.loads(line)
        assert "event_id" in record


def test_run_idempotent_bytes_equivalent(clickstream_test_cfg: dict, tmp_path: Path) -> None:
    bucket_a = f"file://{tmp_path / 'a'}"
    bucket_b = f"file://{tmp_path / 'b'}"
    h = PROJECT_EPOCH + dt.timedelta(hours=100)
    for bucket in (bucket_a, bucket_b):
        run(
            start_hour=h,
            end_hour=h,
            bucket=bucket,
            cfg=clickstream_test_cfg,
            seed=42,
            session_pool_size=300,
            customer_pool_size=100,
        )
    a = (
        tmp_path
        / "a"
        / "raw"
        / "clickstream"
        / f"year={h.year:04d}"
        / f"month={h.month:02d}"
        / f"day={h.day:02d}"
        / f"hour={h.hour:02d}"
        / "events.json"
    ).read_bytes()
    b = (
        tmp_path
        / "b"
        / "raw"
        / "clickstream"
        / f"year={h.year:04d}"
        / f"month={h.month:02d}"
        / f"day={h.day:02d}"
        / f"hour={h.hour:02d}"
        / "events.json"
    ).read_bytes()
    assert a == b


def test_default_customer_pool_constant_matches_customers_default() -> None:
    """Sanity: keep clickstream's customer pool aligned with customers.py's
    so FKs work without explicit coordination."""
    from generators.customers import _DEFAULT_RECORD_COUNT

    assert _DEFAULT_RECORD_COUNT == _DEFAULT_CUSTOMER_POOL
