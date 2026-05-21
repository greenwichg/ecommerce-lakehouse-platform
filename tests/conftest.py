"""Top-level pytest fixtures shared across test trees.

This conftest also defensively re-adds the repo root to ``sys.path``. The
``orchestration/tests`` conftest imports Airflow, and some of Airflow's
init machinery clobbers ``sys.path`` in a way that strips the entry
``pytest`` adds via ``[tool.pytest.ini_options].pythonpath``. Without this
guard, running the orchestration suite before / interleaved with the
top-level ``tests/`` suite breaks ``from generators.x import y``.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


@pytest.fixture()
def repo_root() -> Path:
    """Absolute path to the repository root."""
    return _REPO_ROOT


@pytest.fixture()
def orders_test_cfg() -> dict:
    """A small, deterministic orders config suitable for unit tests.

    Mirrors the structure of config/base.yaml so generator code under test
    sees the same shape it would in production, but with tighter volumes
    so tests stay fast.
    """
    return {
        "generators": {
            "orders": {
                "records_per_day": 50,
                "late_arrival_pct": 2.0,
                "same_day_update_pct": 4.0,  # bumped for test signal
                "cancellation_pct": 10.0,
                "avg_items_per_order": 2.3,
                "price_min": 4.99,
                "price_max": 499.99,
                "transition_days_mean": {
                    "placed_to_paid": 0.5,
                    "paid_to_shipped": 1.5,
                    "shipped_to_delivered": 3.5,
                },
            },
            "random_seed": 42,
        },
        "paths": {
            "raw_prefix": "raw",
            "partitioning": "year={year:04d}/month={month:02d}/day={day:02d}",
        },
    }
