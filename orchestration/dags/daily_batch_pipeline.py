"""Daily batch pipeline — Slice 1 (orders only).

Schedule: 02:00 UTC daily.
SLA:      4 hours end-to-end.

.. mermaid::

    flowchart LR
        S[S3KeySensor] --> B[bronze.orders]
        B --> SI[silver.orders]
        SI --> G1[gold.fact_orders]
        SI --> G2[gold.fact_order_lifecycle]
        G1 --> SF[snowflake.orders_load]
        G2 --> SF
        SF --> DQ[dq_gate]

Slice 2 adds:
- Dynamic task mapping for additional sources (customers, products)
- Currency rates fetch task
- Threshold moves from hardcoded 20% to config

This DAG passes ``batch_id`` between tasks via XCom so every layer of the
pipeline tags its outputs with the same lineage identifier.
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
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.task_group import TaskGroup

# Make the libs/ importable for batch_id generation. This is the same
# pathing trick generators use; documented in PLAN.md. DAG file lives at
# orchestration/dags/, so up three is the repo root.
_DAG_FILE = Path(__file__).resolve()
_LIBS_DIR = _DAG_FILE.parent.parent.parent / "databricks"
if str(_LIBS_DIR) not in sys.path:
    sys.path.insert(0, str(_LIBS_DIR))

from libs.batch import make_batch_id  # noqa: E402

# Make sibling DAG-helper modules importable.
sys.path.insert(0, str(_DAG_FILE.parent))
from callbacks import sns_failure_callback  # noqa: E402
from dq_gates import check_row_count_drop  # noqa: E402

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

DAG_ID = "daily_batch_pipeline"
SCHEDULE = "0 2 * * *"  # 02:00 UTC daily
SLA_HOURS = 4

# Sources processed by the daily pipeline. Slice 1 ships orders only;
# Slice 2 extends this list, at which point the bronze task group becomes
# a dynamically-mapped task.
SOURCES = ["orders"]

# S3 bucket and prefix structure. Pulled from Airflow Variables so the
# same DAG runs against dev/prod without code edits.
S3_BUCKET = Variable.get("lakehouse_s3_bucket", default_var="ecommerce-lakehouse-prod")
S3_RAW_PREFIX = "raw"

# Databricks job IDs per layer + source. Configured per-environment via
# Variables; defaults trigger a clear validation error if missing.
DATABRICKS_JOBS = {
    "bronze_orders": int(Variable.get("databricks_job_bronze_orders", default_var=0)),
    "silver_orders": int(Variable.get("databricks_job_silver_orders", default_var=0)),
    "gold_fact_orders": int(Variable.get("databricks_job_gold_fact_orders", default_var=0)),
    "gold_fact_order_lifecycle": int(
        Variable.get("databricks_job_gold_fact_order_lifecycle", default_var=0)
    ),
}

DATABRICKS_CONN_ID = "databricks_default"
SNOWFLAKE_CONN_ID = "snowflake_default"
AWS_CONN_ID = "aws_default"

# Snowflake env vars (used in load SQL templating)
SNOWFLAKE_DATABASE = Variable.get("snowflake_database", default_var="prod_lakehouse")
SNOWFLAKE_WAREHOUSE = Variable.get("snowflake_warehouse", default_var="prod_etl_wh")

_LOAD_SQL_PATH = _DAG_FILE.parent.parent.parent / "snowflake" / "dml" / "orders_load.sql"

# -----------------------------------------------------------------------------
# DAG-level defaults
# -----------------------------------------------------------------------------

default_args: dict[str, Any] = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": int(os.environ.get("AIRFLOW_DEFAULT_RETRIES", "2")),
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": sns_failure_callback,
    "sla": timedelta(hours=SLA_HOURS),
}

# -----------------------------------------------------------------------------
# Task: generate the batch_id once at DAG start
# -----------------------------------------------------------------------------


@task
def emit_batch_id() -> str:
    """Generate a batch_id for this run and push to XCom."""
    bid = make_batch_id()
    print(f"batch_id={bid}")
    return bid


@task
def gate_row_count(table_fqn: str) -> dict[str, Any]:
    """Wrap dq_gates.check_row_count_drop so it shows up as an Airflow task."""
    return check_row_count_drop(table_fqn=table_fqn, conn_id=SNOWFLAKE_CONN_ID)


# -----------------------------------------------------------------------------
# DAG definition
# -----------------------------------------------------------------------------

with DAG(
    dag_id=DAG_ID,
    description="Daily batch pipeline: orders end-to-end (Slice 1).",
    start_date=pendulum.datetime(2025, 5, 1, tz="UTC"),
    schedule=SCHEDULE,
    catchup=False,
    default_args=default_args,
    tags=["batch", "orders", "slice-1"],
    doc_md=__doc__,
    max_active_runs=1,
) as dag:

    # 1) Wait for today's raw orders file. Deferrable so the worker slot
    #    is released while we wait.
    wait_for_orders = S3KeySensor(
        task_id="wait_for_orders_raw",
        bucket_name=S3_BUCKET,
        # Templated key uses Airflow's logical_date macro
        bucket_key=(
            f"{S3_RAW_PREFIX}/orders/year={{{{ logical_date.year }}}}/"
            f"month={{{{ '%02d' | format(logical_date.month) }}}}/"
            f"day={{{{ '%02d' | format(logical_date.day) }}}}/orders.parquet"
        ),
        aws_conn_id=AWS_CONN_ID,
        deferrable=True,
        timeout=60 * 60 * 2,  # 2h max wait
        poke_interval=300,
    )

    # 2) Emit the batch_id used by every downstream task.
    batch_id = emit_batch_id()

    # 3) Bronze TaskGroup. Slice 1 has just orders; Slice 2 will replace
    #    this with .expand() across SOURCES.
    with TaskGroup(group_id="bronze", tooltip="Auto Loader ingestion to Delta") as bronze_group:
        bronze_orders = DatabricksRunNowOperator(
            task_id="bronze_orders",
            job_id=DATABRICKS_JOBS["bronze_orders"],
            databricks_conn_id=DATABRICKS_CONN_ID,
            deferrable=True,
            notebook_params={
                "env": "{{ var.value.lakehouse_env }}",
                "batch_id": "{{ ti.xcom_pull(task_ids='emit_batch_id') }}",
            },
        )

    # 4) Silver TaskGroup.
    with TaskGroup(group_id="silver", tooltip="Dedup + DQ quarantine + watermark") as silver_group:
        silver_orders = DatabricksRunNowOperator(
            task_id="silver_orders",
            job_id=DATABRICKS_JOBS["silver_orders"],
            databricks_conn_id=DATABRICKS_CONN_ID,
            deferrable=True,
            notebook_params={
                "env": "{{ var.value.lakehouse_env }}",
                "batch_id": "{{ ti.xcom_pull(task_ids='emit_batch_id') }}",
            },
        )

    # 5) Gold TaskGroup — fact_orders + accumulating snapshot in parallel.
    with TaskGroup(group_id="gold", tooltip="Star schema facts") as gold_group:
        gold_fact_orders = DatabricksRunNowOperator(
            task_id="gold_fact_orders",
            job_id=DATABRICKS_JOBS["gold_fact_orders"],
            databricks_conn_id=DATABRICKS_CONN_ID,
            deferrable=True,
            notebook_params={
                "env": "{{ var.value.lakehouse_env }}",
                "batch_id": "{{ ti.xcom_pull(task_ids='emit_batch_id') }}",
            },
        )
        gold_fact_order_lifecycle = DatabricksRunNowOperator(
            task_id="gold_fact_order_lifecycle",
            job_id=DATABRICKS_JOBS["gold_fact_order_lifecycle"],
            databricks_conn_id=DATABRICKS_CONN_ID,
            deferrable=True,
            notebook_params={
                "env": "{{ var.value.lakehouse_env }}",
                "batch_id": "{{ ti.xcom_pull(task_ids='emit_batch_id') }}",
            },
        )

    # 6) Snowflake load: COPY INTO raw + MERGE into analytics for both facts.
    load_orders_to_snowflake = SQLExecuteQueryOperator(
        task_id="snowflake_load_orders",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=_LOAD_SQL_PATH.read_text(),
        # Substitute the &{...} placeholders via Snowflake session vars.
        # Setting them as a prefix statement is simpler than rewriting the
        # SQL with jinja and works for any SQL backend.
        autocommit=True,
        params={
            "SNOWFLAKE_DATABASE": SNOWFLAKE_DATABASE,
            "SNOWFLAKE_WAREHOUSE": SNOWFLAKE_WAREHOUSE,
            "BATCH_ID": "{{ ti.xcom_pull(task_ids='emit_batch_id') }}",
        },
    )

    # 7) Data quality gate: fail the DAG if today's count drops > 20% vs
    #    the 7-day baseline. Hardcoded threshold for Slice 1.
    dq_gate_fact_orders = gate_row_count.override(task_id="dq_gate_fact_orders")(
        table_fqn=f"{SNOWFLAKE_DATABASE}.analytics.fact_orders",
    )

    # Wiring
    wait_for_orders >> batch_id >> bronze_group
    bronze_group >> silver_group >> gold_group >> load_orders_to_snowflake
    load_orders_to_snowflake >> dq_gate_fact_orders
