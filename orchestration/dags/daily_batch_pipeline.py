"""Daily batch pipeline — Slice 2 (orders + customers + products).

Schedule: 02:00 UTC daily.
SLA:      4 hours end-to-end.

Task-flow diagram: see ``docs/pipelines.md`` (rendered on GitHub).

Slice 2 additions over Slice 1:
- Dynamic task mapping for bronze (.expand_kwargs over SOURCES)
- silver/gold/snowflake tasks for customers + products
- gold.fact_orders depends on both dim_customer and dim_product gold
  tasks (PIT-correct SCD2 surrogate binding requires the dims to be
  current)
- Two DQ gates in parallel: row_count_drop (existing, threshold now
  from config) + orphan_surrogate_rate (new in Slice 2)
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

_DAG_FILE = Path(__file__).resolve()
_LIBS_DIR = _DAG_FILE.parent.parent.parent / "databricks"
if str(_LIBS_DIR) not in sys.path:
    sys.path.insert(0, str(_LIBS_DIR))

from libs.batch import make_batch_id  # noqa: E402

sys.path.insert(0, str(_DAG_FILE.parent))
from callbacks import sns_failure_callback  # noqa: E402
from dq_gates import (  # noqa: E402
    check_currency_freshness,
    check_orphan_surrogate_rate,
    check_row_count_drop,
)

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

DAG_ID = "daily_batch_pipeline"
SCHEDULE = "0 2 * * *"
SLA_HOURS = 4

# Sources processed by bronze (dynamic task mapping). Slice 4 adds
# currency_rates + wishlist alongside the original three.
SOURCES: list[str] = ["orders", "customers", "products", "currency_rates", "wishlist"]

# Source file shapes — used by the per-source S3 sensors.
_SOURCE_FILE = {
    "orders": "orders.parquet",
    "customers": "customers.parquet",
    "products": "products.csv",
    "currency_rates": "rates.csv",
    "wishlist": "wishlist.json",
}

S3_BUCKET = Variable.get("lakehouse_s3_bucket", default_var="ecommerce-lakehouse-prod")
S3_RAW_PREFIX = "raw"

# Databricks job IDs per layer + source.
DATABRICKS_JOBS = {
    f"bronze_{s}": int(Variable.get(f"databricks_job_bronze_{s}", default_var=0)) for s in SOURCES
}
DATABRICKS_JOBS.update(
    {f"silver_{s}": int(Variable.get(f"databricks_job_silver_{s}", default_var=0)) for s in SOURCES}
)
DATABRICKS_JOBS.update(
    {
        "gold_dim_customer": int(Variable.get("databricks_job_gold_dim_customer", default_var=0)),
        "gold_dim_product": int(Variable.get("databricks_job_gold_dim_product", default_var=0)),
        "gold_fact_orders": int(Variable.get("databricks_job_gold_fact_orders", default_var=0)),
        "gold_fact_order_lifecycle": int(
            Variable.get("databricks_job_gold_fact_order_lifecycle", default_var=0)
        ),
        # Slice 4
        "gold_currency_rates": int(
            Variable.get("databricks_job_gold_currency_rates", default_var=0)
        ),
        "gold_fact_customer_wishlist_product": int(
            Variable.get("databricks_job_gold_fact_customer_wishlist_product", default_var=0)
        ),
        "gold_customer_ltv": int(Variable.get("databricks_job_gold_customer_ltv", default_var=0)),
    }
)

DATABRICKS_CONN_ID = "databricks_default"
SNOWFLAKE_CONN_ID = "snowflake_default"
AWS_CONN_ID = "aws_default"

SNOWFLAKE_DATABASE = Variable.get("snowflake_database", default_var="prod_lakehouse")
SNOWFLAKE_WAREHOUSE = Variable.get("snowflake_warehouse", default_var="prod_etl_wh")

_SQL_DIR = _DAG_FILE.parent.parent.parent / "snowflake" / "dml"
_LOAD_ORDERS_SQL = (_SQL_DIR / "orders_load.sql").read_text()
_LOAD_DIM_CUSTOMER_SQL = (_SQL_DIR / "dim_customer_load.sql").read_text()
_LOAD_DIM_PRODUCT_SQL = (_SQL_DIR / "dim_product_load.sql").read_text()
# Slice 4
_LOAD_CURRENCY_RATES_SQL = (_SQL_DIR / "currency_rates_load.sql").read_text()
_LOAD_WISHLIST_SQL = (_SQL_DIR / "wishlist_load.sql").read_text()
_LOAD_CUSTOMER_LTV_SQL = (_SQL_DIR / "customer_ltv_load.sql").read_text()

# -----------------------------------------------------------------------------
# DAG defaults
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

_NOTEBOOK_PARAMS_TEMPLATE = {
    "env": "{{ var.value.lakehouse_env }}",
    "batch_id": "{{ ti.xcom_pull(task_ids='emit_batch_id') }}",
}


@task
def emit_batch_id() -> str:
    """Generate a batch_id for this run and push to XCom."""
    bid = make_batch_id()
    print(f"batch_id={bid}")
    return bid


@task
def gate_row_count(table_fqn: str) -> dict[str, Any]:
    return check_row_count_drop(table_fqn=table_fqn, conn_id=SNOWFLAKE_CONN_ID)


@task
def gate_orphan_rate(table_fqn: str, sk_cols: list[str]) -> dict[str, Any]:
    return check_orphan_surrogate_rate(
        table_fqn=table_fqn, sk_cols=sk_cols, conn_id=SNOWFLAKE_CONN_ID
    )


@task
def gate_currency_freshness(table_fqn: str) -> dict[str, Any]:
    """Slice 4: ensure today's currency rates loaded + simulated share < 50%."""
    return check_currency_freshness(table_fqn=table_fqn, conn_id=SNOWFLAKE_CONN_ID)


# -----------------------------------------------------------------------------
# DAG
# -----------------------------------------------------------------------------

with DAG(
    dag_id=DAG_ID,
    description="Daily batch pipeline: orders + customers + products end-to-end (Slice 2).",
    start_date=pendulum.datetime(2025, 5, 1, tz="UTC"),
    schedule=SCHEDULE,
    catchup=False,
    default_args=default_args,
    tags=["batch", "orders", "customers", "products", "slice-2"],
    doc_md=__doc__,
    max_active_runs=1,
) as dag:

    # 1) Per-source S3 sensors in a TaskGroup. Deferrable.
    with TaskGroup(
        group_id="wait_for_raw", tooltip="Deferrable S3 sensors per source"
    ) as wait_for_raw:
        sensors = []
        for source in SOURCES:
            filename = _SOURCE_FILE[source]
            sensors.append(
                S3KeySensor(
                    task_id=f"wait_{source}",
                    bucket_name=S3_BUCKET,
                    bucket_key=(
                        f"{S3_RAW_PREFIX}/{source}/"
                        f"year={{{{ logical_date.year }}}}/"
                        f"month={{{{ '%02d' | format(logical_date.month) }}}}/"
                        f"day={{{{ '%02d' | format(logical_date.day) }}}}/{filename}"
                    ),
                    aws_conn_id=AWS_CONN_ID,
                    deferrable=True,
                    timeout=60 * 60 * 2,
                    poke_interval=300,
                )
            )

    # 2) Emit batch_id.
    batch_id = emit_batch_id()

    # 3) Bronze: dynamic task mapping over SOURCES (.expand).
    #    Per spec: "Dynamic task mapping to generate one task per source
    #    table in bronze". Each mapped invocation triggers the matching
    #    Databricks job with shared notebook_params.
    #
    #    sla=None override: mapped tasks don't support SLAs (Airflow raises
    #    at parse time). The DAG-level SLA still covers end-to-end via the
    #    final dq_gate task — bronze is not a meaningful SLA boundary on
    #    its own anyway.
    bronze_mapped = DatabricksRunNowOperator.partial(
        task_id="bronze",
        databricks_conn_id=DATABRICKS_CONN_ID,
        deferrable=True,
        notebook_params=_NOTEBOOK_PARAMS_TEMPLATE,
        sla=None,
    ).expand(
        job_id=[DATABRICKS_JOBS[f"bronze_{s}"] for s in SOURCES],
    )

    # 4) Silver: one task per source (no mapping — each source's silver
    #    notebook is distinct since DQ rules and natural keys differ).
    with TaskGroup(group_id="silver") as silver_group:
        silver_orders = DatabricksRunNowOperator(
            task_id="silver_orders",
            job_id=DATABRICKS_JOBS["silver_orders"],
            databricks_conn_id=DATABRICKS_CONN_ID,
            deferrable=True,
            notebook_params=_NOTEBOOK_PARAMS_TEMPLATE,
        )
        silver_customers = DatabricksRunNowOperator(
            task_id="silver_customers",
            job_id=DATABRICKS_JOBS["silver_customers"],
            databricks_conn_id=DATABRICKS_CONN_ID,
            deferrable=True,
            notebook_params=_NOTEBOOK_PARAMS_TEMPLATE,
        )
        silver_products = DatabricksRunNowOperator(
            task_id="silver_products",
            job_id=DATABRICKS_JOBS["silver_products"],
            databricks_conn_id=DATABRICKS_CONN_ID,
            deferrable=True,
            notebook_params=_NOTEBOOK_PARAMS_TEMPLATE,
        )
        # Slice 4 additions
        silver_currency_rates = DatabricksRunNowOperator(
            task_id="silver_currency_rates",
            job_id=DATABRICKS_JOBS["silver_currency_rates"],
            databricks_conn_id=DATABRICKS_CONN_ID,
            deferrable=True,
            notebook_params=_NOTEBOOK_PARAMS_TEMPLATE,
        )
        silver_wishlist = DatabricksRunNowOperator(
            task_id="silver_wishlist",
            job_id=DATABRICKS_JOBS["silver_wishlist"],
            databricks_conn_id=DATABRICKS_CONN_ID,
            deferrable=True,
            notebook_params=_NOTEBOOK_PARAMS_TEMPLATE,
        )

    # 5) Gold dims must materialise BEFORE fact_orders so the PIT join
    #    has the right SCD2 versions to bind to.
    with TaskGroup(group_id="gold_dims") as gold_dims:
        gold_dim_customer = DatabricksRunNowOperator(
            task_id="dim_customer",
            job_id=DATABRICKS_JOBS["gold_dim_customer"],
            databricks_conn_id=DATABRICKS_CONN_ID,
            deferrable=True,
            notebook_params=_NOTEBOOK_PARAMS_TEMPLATE,
        )
        gold_dim_product = DatabricksRunNowOperator(
            task_id="dim_product",
            job_id=DATABRICKS_JOBS["gold_dim_product"],
            databricks_conn_id=DATABRICKS_CONN_ID,
            deferrable=True,
            notebook_params=_NOTEBOOK_PARAMS_TEMPLATE,
        )

    # 6) Gold facts. fact_orders depends on dims; fact_order_lifecycle
    #    doesn't (no surrogate FKs in the lifecycle accumulator).
    with TaskGroup(group_id="gold_facts") as gold_facts:
        gold_fact_orders = DatabricksRunNowOperator(
            task_id="fact_orders",
            job_id=DATABRICKS_JOBS["gold_fact_orders"],
            databricks_conn_id=DATABRICKS_CONN_ID,
            deferrable=True,
            notebook_params=_NOTEBOOK_PARAMS_TEMPLATE,
        )
        gold_fact_order_lifecycle = DatabricksRunNowOperator(
            task_id="fact_order_lifecycle",
            job_id=DATABRICKS_JOBS["gold_fact_order_lifecycle"],
            databricks_conn_id=DATABRICKS_CONN_ID,
            deferrable=True,
            notebook_params=_NOTEBOOK_PARAMS_TEMPLATE,
        )
        # Slice 4 additions: currency_rates reference table, wishlist
        # factless fact (also gated on both dims like fact_orders), and
        # customer_ltv mart (depends on fact_orders)
        gold_currency_rates = DatabricksRunNowOperator(
            task_id="currency_rates",
            job_id=DATABRICKS_JOBS["gold_currency_rates"],
            databricks_conn_id=DATABRICKS_CONN_ID,
            deferrable=True,
            notebook_params=_NOTEBOOK_PARAMS_TEMPLATE,
        )
        gold_fact_wishlist = DatabricksRunNowOperator(
            task_id="fact_customer_wishlist_product",
            job_id=DATABRICKS_JOBS["gold_fact_customer_wishlist_product"],
            databricks_conn_id=DATABRICKS_CONN_ID,
            deferrable=True,
            notebook_params=_NOTEBOOK_PARAMS_TEMPLATE,
        )
        gold_customer_ltv = DatabricksRunNowOperator(
            task_id="customer_ltv",
            job_id=DATABRICKS_JOBS["gold_customer_ltv"],
            databricks_conn_id=DATABRICKS_CONN_ID,
            deferrable=True,
            notebook_params=_NOTEBOOK_PARAMS_TEMPLATE,
        )

    # 7) Snowflake load. Dims first, then facts (so fact MERGEs see the
    #    dim rows that Snowflake's FK constraints reference).
    _SQL_PARAMS = {
        "SNOWFLAKE_DATABASE": SNOWFLAKE_DATABASE,
        "SNOWFLAKE_WAREHOUSE": SNOWFLAKE_WAREHOUSE,
        "BATCH_ID": "{{ ti.xcom_pull(task_ids='emit_batch_id') }}",
    }
    with TaskGroup(group_id="snowflake_load") as snowflake_load:
        load_dim_customer = SQLExecuteQueryOperator(
            task_id="dim_customer",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=_LOAD_DIM_CUSTOMER_SQL,
            autocommit=True,
            params=_SQL_PARAMS,
        )
        load_dim_product = SQLExecuteQueryOperator(
            task_id="dim_product",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=_LOAD_DIM_PRODUCT_SQL,
            autocommit=True,
            params=_SQL_PARAMS,
        )
        load_orders = SQLExecuteQueryOperator(
            task_id="orders",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=_LOAD_ORDERS_SQL,
            autocommit=True,
            params=_SQL_PARAMS,
        )
        load_currency_rates = SQLExecuteQueryOperator(
            task_id="currency_rates",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=_LOAD_CURRENCY_RATES_SQL,
            autocommit=True,
            params=_SQL_PARAMS,
        )
        load_wishlist = SQLExecuteQueryOperator(
            task_id="wishlist",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=_LOAD_WISHLIST_SQL,
            autocommit=True,
            params=_SQL_PARAMS,
        )
        load_customer_ltv = SQLExecuteQueryOperator(
            task_id="customer_ltv",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=_LOAD_CUSTOMER_LTV_SQL,
            autocommit=True,
            params=_SQL_PARAMS,
        )
        # Facts depend on dims (Snowflake FK enforcement is advisory but
        # we order anyway so dq_gate_orphan sees coherent state).
        [load_dim_customer, load_dim_product] >> load_orders
        [load_dim_customer, load_dim_product] >> load_wishlist
        load_orders >> load_customer_ltv

    # 8) DQ gates: row count + orphan surrogate rate + Slice-4 currency freshness.
    dq_row_count = gate_row_count.override(task_id="dq_row_count_fact_orders")(
        table_fqn=f"{SNOWFLAKE_DATABASE}.analytics.fact_orders",
    )
    dq_orphan = gate_orphan_rate.override(task_id="dq_orphan_fact_orders")(
        table_fqn=f"{SNOWFLAKE_DATABASE}.analytics.fact_orders",
        sk_cols=["customer_sk", "product_sk"],
    )
    # Slice 4: rates loaded for today + simulated share < threshold. Runs
    # right after currency_rates loads (don't block other facts on this).
    dq_currency = gate_currency_freshness.override(task_id="dq_currency_freshness")(
        table_fqn=f"{SNOWFLAKE_DATABASE}.analytics.currency_rates",
    )

    # Wiring -----------------------------------------------------------------
    wait_for_raw >> batch_id >> bronze_mapped >> silver_group
    silver_orders >> [gold_fact_orders, gold_fact_order_lifecycle]
    silver_customers >> gold_dim_customer
    silver_products >> gold_dim_product
    silver_currency_rates >> gold_currency_rates
    silver_wishlist >> gold_fact_wishlist
    # PIT-correct surrogate binding requires dims to be current before
    # facts that bind to them run:
    [gold_dim_customer, gold_dim_product] >> gold_fact_orders
    [gold_dim_customer, gold_dim_product] >> gold_fact_wishlist
    # customer_ltv reads fact_orders → must run after gold_fact_orders.
    gold_fact_orders >> gold_customer_ltv

    gold_dim_customer >> load_dim_customer
    gold_dim_product >> load_dim_product
    [gold_fact_orders, gold_fact_order_lifecycle] >> load_orders
    gold_currency_rates >> load_currency_rates
    gold_fact_wishlist >> load_wishlist
    gold_customer_ltv >> load_customer_ltv

    load_orders >> [dq_row_count, dq_orphan]
    load_currency_rates >> dq_currency
