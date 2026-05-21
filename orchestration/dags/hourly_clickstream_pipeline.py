"""Hourly clickstream pipeline — Slice 3.

Schedule: @hourly (00 of every hour).
SLA:      15 minutes end-to-end.

.. mermaid::

    flowchart LR
        S[S3KeySensor: events.json for this hour]
            --> B[emit_batch_id]
            --> BR[bronze.clickstream]
            --> SI[silver.clickstream]
            --> GO[gold.fact_sessions]
            --> P[publish DATASET_FACT_SESSIONS outlet]

The Snowpipe + Streams + Tasks pipeline downstream of S3 gold runs
autonomously in Snowflake — Airflow does not manage it (it's
S3-event-driven and time-driven within Snowflake). See
snowflake/ddl/50_snowpipe_streams_tasks.sql for the full Snowflake-side
flow.

## Dataset publish

The final task ``publish_sessions_dataset`` declares
``outlets=[DATASET_FACT_SESSIONS]``. Airflow records the Dataset update on
successful completion; the dashboard_refresh DAG triggers on that update.

## Why not extend the daily DAG with hourly tasks?

Operational separation. Hourly clickstream has a 15-minute SLA and a
distinct failure-domain (Snowpipe, Streams, Tasks). Burying it inside the
daily DAG would make the daily SLA 24-hour-noisy and confuse on-call
escalation.
"""

from __future__ import annotations

import os
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow.decorators import task
from airflow.models import DAG, Variable
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

_DAG_FILE = Path(__file__).resolve()
_LIBS_DIR = _DAG_FILE.parent.parent.parent / "databricks"
if str(_LIBS_DIR) not in sys.path:
    sys.path.insert(0, str(_LIBS_DIR))

from libs.batch import make_batch_id  # noqa: E402

sys.path.insert(0, str(_DAG_FILE.parent))
from _datasets import DATASET_FACT_SESSIONS  # noqa: E402
from callbacks import sns_failure_callback  # noqa: E402

DAG_ID = "hourly_clickstream_pipeline"
SLA_MINUTES = 15

S3_BUCKET = Variable.get("lakehouse_s3_bucket", default_var="ecommerce-lakehouse-prod")
S3_RAW_PREFIX = "raw"

DATABRICKS_JOBS = {
    "bronze_clickstream": int(Variable.get("databricks_job_bronze_clickstream", default_var=0)),
    "silver_clickstream": int(Variable.get("databricks_job_silver_clickstream", default_var=0)),
    "gold_fact_sessions": int(Variable.get("databricks_job_gold_fact_sessions", default_var=0)),
}

DATABRICKS_CONN_ID = "databricks_default"
AWS_CONN_ID = "aws_default"

default_args: dict[str, Any] = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": int(os.environ.get("AIRFLOW_DEFAULT_RETRIES", "2")),
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "on_failure_callback": sns_failure_callback,
    "sla": timedelta(minutes=SLA_MINUTES),
}

_NOTEBOOK_PARAMS_TEMPLATE = {
    "env": "{{ var.value.lakehouse_env }}",
    "batch_id": "{{ ti.xcom_pull(task_ids='emit_batch_id') }}",
}


@task
def emit_batch_id() -> str:
    """Generate a batch_id and push to XCom."""
    bid = make_batch_id()
    print(f"batch_id={bid}")
    return bid


@task(outlets=[DATASET_FACT_SESSIONS])
def publish_sessions_dataset() -> str:
    """Outlet-only task: signals Airflow that DATASET_FACT_SESSIONS has new
    data so downstream DAGs (dashboard_refresh) can react.

    The task body is intentionally minimal — Datasets in Airflow are
    metadata events, not file moves. The actual data already landed in
    S3 + Snowflake by the time this runs.
    """
    print("Published DATASET_FACT_SESSIONS")
    return "ok"


with DAG(
    dag_id=DAG_ID,
    description="Hourly clickstream pipeline: bronze -> silver (sessionize) -> gold (fact_sessions).",
    start_date=pendulum.datetime(2025, 5, 1, tz="UTC"),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["hourly", "clickstream", "slice-3"],
    doc_md=__doc__,
    max_active_runs=1,
) as dag:

    # 1) Wait for this hour's clickstream JSON Lines file.
    wait_for_clickstream = S3KeySensor(
        task_id="wait_for_clickstream_raw",
        bucket_name=S3_BUCKET,
        bucket_key=(
            f"{S3_RAW_PREFIX}/clickstream/"
            f"year={{{{ logical_date.year }}}}/"
            f"month={{{{ '%02d' | format(logical_date.month) }}}}/"
            f"day={{{{ '%02d' | format(logical_date.day) }}}}/"
            f"hour={{{{ '%02d' | format(logical_date.hour) }}}}/events.json"
        ),
        aws_conn_id=AWS_CONN_ID,
        deferrable=True,
        timeout=60 * 60,  # 1h max wait — tighter than daily because cadence is hourly
        poke_interval=60,
    )

    batch_id = emit_batch_id()

    bronze = DatabricksRunNowOperator(
        task_id="bronze_clickstream",
        job_id=DATABRICKS_JOBS["bronze_clickstream"],
        databricks_conn_id=DATABRICKS_CONN_ID,
        deferrable=True,
        notebook_params=_NOTEBOOK_PARAMS_TEMPLATE,
    )

    silver = DatabricksRunNowOperator(
        task_id="silver_clickstream",
        job_id=DATABRICKS_JOBS["silver_clickstream"],
        databricks_conn_id=DATABRICKS_CONN_ID,
        deferrable=True,
        notebook_params=_NOTEBOOK_PARAMS_TEMPLATE,
    )

    gold = DatabricksRunNowOperator(
        task_id="gold_fact_sessions",
        job_id=DATABRICKS_JOBS["gold_fact_sessions"],
        databricks_conn_id=DATABRICKS_CONN_ID,
        deferrable=True,
        notebook_params=_NOTEBOOK_PARAMS_TEMPLATE,
    )

    publish = publish_sessions_dataset()

    wait_for_clickstream >> batch_id >> bronze >> silver >> gold >> publish
