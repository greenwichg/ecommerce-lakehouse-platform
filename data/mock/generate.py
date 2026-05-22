"""Mock data generator for the Streamlit dashboard.

Used in two places:
- ``streamlit_app/app.py`` reads these files when ``--mock`` is set
  (the default mode), so the dashboard renders without any cloud
  credentials.
- ``tests/streamlit_app/test_data.py`` regenerates them in a tmpdir
  and asserts the data layer reads them back correctly.

The intent is "deliberately representative": one DAG that's healthy,
one that's been failing recently, freshness ranging from minutes to
hours, costs that show both Snowflake and Databricks line items, and
a quarantine queue with all three SFN-branch reasons represented.

Re-running this script is idempotent — it writes deterministic
content from a fixed random seed.
"""

from __future__ import annotations

import json
import random
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd

_MOCK_DIR = Path(__file__).resolve().parent
_SEED = 42


def _now() -> datetime:
    # Fixed "now" so screenshots are reproducible. The "live" path on the
    # dashboard uses datetime.now(); the mock path uses this anchor and
    # backdates from it.
    return datetime(2025, 5, 21, 14, 30, 0, tzinfo=UTC)


def _airflow_runs() -> pd.DataFrame:
    """50 DAG runs across the three production DAGs. Mix of states."""
    rng = random.Random(_SEED)
    rows = []
    now = _now()
    dag_specs = [
        ("daily_batch_pipeline", timedelta(days=1), 3600, 0.92),  # ~1h target, 92% success
        ("hourly_clickstream_pipeline", timedelta(hours=1), 600, 0.97),  # ~10m target, 97% success
        ("weekly_maintenance", timedelta(days=7), 5400, 1.00),  # ~1.5h, always succeeds
    ]
    for dag_id, cadence, target_secs, success_rate in dag_specs:
        run_count = (
            20 if cadence == timedelta(hours=1) else (15 if cadence == timedelta(days=1) else 4)
        )
        for i in range(run_count):
            start = now - cadence * (i + 1)
            jitter = rng.gauss(1.0, 0.15)
            duration_s = max(60, int(target_secs * jitter))
            state = (
                "success"
                if rng.random() < success_rate
                else rng.choice(["failed", "failed", "running"])
            )
            rows.append(
                {
                    "dag_id": dag_id,
                    "run_id": f"scheduled__{start.strftime('%Y-%m-%dT%H:%M:%S')}",
                    "start_time": start,
                    "end_time": (
                        start + timedelta(seconds=duration_s) if state != "running" else None
                    ),
                    "duration_s": duration_s if state != "running" else None,
                    "state": state,
                    # Rows processed: pulled from XCom in live mode. For mock
                    # we generate plausible numbers per DAG.
                    "rows_processed": (
                        rng.randint(800_000, 1_400_000)
                        if dag_id == "hourly_clickstream_pipeline"
                        else rng.randint(1_500, 4_500) if dag_id == "daily_batch_pipeline" else None
                    ),
                }
            )
    return pd.DataFrame(rows).sort_values("start_time", ascending=False).reset_index(drop=True)


def _freshness() -> pd.DataFrame:
    """Per-table freshness: max(_loaded_at), age, SLA status."""
    now = _now()
    # Tuple: (table, sla_hours, hours_old)
    spec = [
        # Healthy
        ("analytics.fact_orders", 24, 2),
        ("analytics.dim_customer", 24, 2),
        ("analytics.dim_product", 24, 2),
        ("analytics.fact_order_lifecycle", 24, 2),
        ("analytics.currency_rates", 24, 18),
        # Hourly path — fresher
        ("analytics.fact_sessions", 1, 0.5),
        # Slice 4 additions
        ("analytics.fact_customer_wishlist_product", 24, 3),
        ("analytics.customer_ltv", 24, 4),
        # Stale — currency was last refreshed > 24h ago, MV needs refresh
        ("analytics.mv_daily_revenue_by_category", 24, 27),
    ]
    rows = []
    for table, sla_hours, hours_old in spec:
        last_loaded = now - timedelta(hours=hours_old)
        if hours_old < sla_hours * 0.5:
            status = "fresh"
        elif hours_old < sla_hours:
            status = "warn"
        else:
            status = "stale"
        rows.append(
            {
                "table": table,
                "last_loaded_at": last_loaded,
                "age_hours": hours_old,
                "sla_hours": sla_hours,
                "status": status,
            }
        )
    return pd.DataFrame(rows)


def _cost_per_run() -> pd.DataFrame:
    """Snowflake credits + Databricks DBUs by DAG run, last 30 days.

    Stacked-bar friendly: one row per (dag_id, run_date, compute_kind).
    """
    rng = random.Random(_SEED + 1)
    rows = []
    now = _now()
    for day in range(30):
        d = (now - timedelta(days=day)).date()
        # Daily batch hits both Databricks + Snowflake
        rows.append(
            {
                "run_date": d,
                "dag_id": "daily_batch_pipeline",
                "compute_kind": "databricks_dbu",
                "credits_or_dbus": round(rng.uniform(8.0, 14.0), 2),
            }
        )
        rows.append(
            {
                "run_date": d,
                "dag_id": "daily_batch_pipeline",
                "compute_kind": "snowflake_credits",
                "credits_or_dbus": round(rng.uniform(3.0, 6.0), 2),
            }
        )
        # Hourly clickstream: Databricks heavy
        rows.append(
            {
                "run_date": d,
                "dag_id": "hourly_clickstream_pipeline",
                "compute_kind": "databricks_dbu",
                "credits_or_dbus": round(rng.uniform(15.0, 22.0), 2),
            }
        )
        rows.append(
            {
                "run_date": d,
                "dag_id": "hourly_clickstream_pipeline",
                "compute_kind": "snowflake_credits",
                "credits_or_dbus": round(rng.uniform(0.5, 1.5), 2),
            }
        )
        # Weekly: only on Sundays
        if d.weekday() == 6:
            rows.append(
                {
                    "run_date": d,
                    "dag_id": "weekly_maintenance",
                    "compute_kind": "databricks_dbu",
                    "credits_or_dbus": round(rng.uniform(20.0, 30.0), 2),
                }
            )
            rows.append(
                {
                    "run_date": d,
                    "dag_id": "weekly_maintenance",
                    "compute_kind": "snowflake_credits",
                    "credits_or_dbus": round(rng.uniform(2.0, 3.5), 2),
                }
            )
    return pd.DataFrame(rows).sort_values(["run_date", "dag_id"]).reset_index(drop=True)


def _quarantine_queue() -> list[dict]:
    """SFN executions waiting on an operator. JSON to preserve the
    nested task-token + input shape we'd see from describe-execution."""
    now = _now()
    return [
        {
            "execution_name": "exec-2025-05-21-orders-missing-cust",
            "started_at": (now - timedelta(hours=3)).isoformat(),
            "age_hours": 3,
            "quarantine_key": "quarantine/orders/year=2025/month=05/day=21/orders.parquet",
            "source": "orders",
            "reason": "missing_columns: customer_id",
            "task_token": "AAAAKgAAAAIAAAAAAAAAAS9...redacted...mock_token_1",
        },
        {
            "execution_name": "exec-2025-05-21-products-bad-csv",
            "started_at": (now - timedelta(hours=8)).isoformat(),
            "age_hours": 8,
            "quarantine_key": "quarantine/products/year=2025/month=05/day=21/products.csv",
            "source": "products",
            "reason": "unreadable_csv: malformed header",
            "task_token": "AAAAKgAAAAIAAAAAAAAAAS9...redacted...mock_token_2",
        },
        {
            "execution_name": "exec-2025-05-20-currency-zero-rows",
            "started_at": (now - timedelta(hours=26)).isoformat(),
            "age_hours": 26,
            "quarantine_key": "quarantine/currency_rates/year=2025/month=05/day=20/rates.csv",
            "source": "currency_rates",
            "reason": "zero_rows",
            "task_token": "AAAAKgAAAAIAAAAAAAAAAS9...redacted...mock_token_3",
        },
    ]


def _currency_fallback_rate() -> pd.DataFrame:
    """Hour-by-hour share of currency_rates rows sourced from the
    simulated fallback (vs the live API). Sidebar gauge."""
    rng = random.Random(_SEED + 2)
    now = _now()
    rows = []
    for h in range(24):
        ts = now - timedelta(hours=h)
        # Most hours: 0-15% simulated. One bad window: 60% (API outage).
        if 4 <= h <= 7:
            share = round(rng.uniform(0.55, 0.7), 2)
        else:
            share = round(rng.uniform(0.0, 0.15), 2)
        rows.append({"hour": ts, "simulated_share": share})
    return pd.DataFrame(rows).sort_values("hour").reset_index(drop=True)


def _recent_alerts() -> list[dict]:
    """SNS alerts published in the last 24 hours."""
    now = _now()
    return [
        {
            "ts": (now - timedelta(minutes=12)).isoformat(),
            "subject": "[Lakehouse] Quarantine queue depth = 3",
            "severity": "warn",
        },
        {
            "ts": (now - timedelta(hours=2)).isoformat(),
            "subject": "[Lakehouse] currency_freshness: 60% simulated rates last hour",
            "severity": "warn",
        },
        {
            "ts": (now - timedelta(hours=5)).isoformat(),
            "subject": "[Lakehouse] daily_batch_pipeline succeeded",
            "severity": "info",
        },
        {
            "ts": (now - timedelta(hours=23)).isoformat(),
            "subject": "[Lakehouse] file_validator: 1 file quarantined (orders, missing_columns)",
            "severity": "warn",
        },
    ]


def regenerate(target_dir: Path = _MOCK_DIR) -> None:
    """Write all mock files to ``target_dir``. Used by both __main__ and
    by the test fixture to populate a tmp_path."""
    target_dir.mkdir(parents=True, exist_ok=True)

    _airflow_runs().to_parquet(target_dir / "airflow_runs.parquet", index=False)
    _freshness().to_parquet(target_dir / "freshness.parquet", index=False)
    _cost_per_run().to_parquet(target_dir / "cost_per_run.parquet", index=False)
    _currency_fallback_rate().to_parquet(target_dir / "currency_fallback.parquet", index=False)

    (target_dir / "quarantine_queue.json").write_text(json.dumps(_quarantine_queue(), indent=2))
    (target_dir / "recent_alerts.json").write_text(json.dumps(_recent_alerts(), indent=2))

    print(f"Wrote mock data to {target_dir}")


if __name__ == "__main__":
    regenerate()
