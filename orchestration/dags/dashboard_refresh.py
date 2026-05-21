"""Dashboard refresh — Slice 3.

Triggered by the ``DATASET_FACT_SESSIONS`` Dataset, which the hourly
clickstream pipeline publishes on successful completion.

## Why a separate DAG instead of extending the daily

The daily batch DAG does heavyweight work (SCD2 dim MERGEs, Snowflake
COPY+MERGE on fact_orders + lifecycle, DQ gates). If the daily DAG were
also Dataset-triggered, it would fire 24× per day (once per hourly
publish) plus its 02:00 cron run — wasted Snowflake credits for full
dim MERGEs that don't need hourly cadence.

Pattern, documented in docs/architecture.md:
- **Cron for source-of-truth refreshes** (the daily DAG).
- **Dataset triggering for downstream consumers** that only need to react
  to fresh upstream signal (this DAG).

## What this DAG actually does

A placeholder ``refresh_sessions_dashboard`` task. In production this
would:
- Invalidate a Streamlit / BI cache (e.g., POST to the dashboard service)
- Refresh a Snowflake materialized view (``ALTER MATERIALIZED VIEW ...
  REFRESH``)
- Re-run a small dbt model for the homepage metric

For Slice 3 (code-only), it just logs that the refresh would fire.
"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow.decorators import task
from airflow.models import DAG

_DAG_FILE = Path(__file__).resolve()
sys.path.insert(0, str(_DAG_FILE.parent))
from _datasets import DATASET_FACT_SESSIONS  # noqa: E402
from callbacks import sns_failure_callback  # noqa: E402

DAG_ID = "dashboard_refresh"


default_args: dict[str, Any] = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": sns_failure_callback,
    # No SLA: this DAG runs reactively, not on a schedule, so an SLA based
    # on logical_date doesn't carry useful operational meaning.
}


@task
def refresh_sessions_dashboard() -> str:
    """Placeholder for the actual dashboard refresh action.

    In Slice 6 this gets replaced with the Streamlit cache invalidation
    or Snowflake materialized view refresh. For now, log + return.
    """
    print("dashboard refresh: fact_sessions data updated, would invalidate Streamlit cache here")
    return "refreshed"


with DAG(
    dag_id=DAG_ID,
    description="Refresh BI dashboards when fact_sessions has new data.",
    start_date=pendulum.datetime(2025, 5, 1, tz="UTC"),
    schedule=[DATASET_FACT_SESSIONS],
    catchup=False,
    default_args=default_args,
    tags=["dashboard", "dataset-triggered", "slice-3"],
    doc_md=__doc__,
    max_active_runs=1,
) as dag:
    refresh_sessions_dashboard()
