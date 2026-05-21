"""Smoke tests for DAG imports.

A DAG that doesn't import is a DAG that can't run. These tests catch
import errors (missing providers, syntax issues, bad task wiring) without
needing a live Airflow scheduler.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_DAGS_DIR = Path(__file__).resolve().parent.parent / "dags"
sys.path.insert(0, str(_DAGS_DIR))


def _import_all_dag_modules() -> list:
    """Import every DAG file in airflow/dags/ and return the module objects."""
    import importlib

    modules = []
    for path in sorted(_DAGS_DIR.glob("*.py")):
        if path.name in {"__init__.py", "callbacks.py", "dq_gates.py"}:
            continue
        mod = importlib.import_module(path.stem)
        modules.append(mod)
    return modules


def test_no_import_errors() -> None:
    """Every DAG file imports cleanly."""
    modules = _import_all_dag_modules()
    assert len(modules) >= 1, "no DAG files found in airflow/dags/"


def test_daily_batch_pipeline_has_expected_tasks() -> None:
    """The Slice 1 DAG has all the tasks the slice plan declares."""
    from daily_batch_pipeline import dag

    expected = {
        "wait_for_orders_raw",
        "emit_batch_id",
        "bronze.bronze_orders",
        "silver.silver_orders",
        "gold.gold_fact_orders",
        "gold.gold_fact_order_lifecycle",
        "snowflake_load_orders",
        "dq_gate_fact_orders",
    }
    actual = {t.task_id for t in dag.tasks}
    assert actual == expected, f"missing or extra tasks: {expected ^ actual}"


def test_daily_batch_pipeline_has_failure_callback() -> None:
    """on_failure_callback is wired so SNS alerts fire on task failure."""
    from callbacks import sns_failure_callback
    from daily_batch_pipeline import dag

    # Default args are propagated to every task
    cb = dag.default_args.get("on_failure_callback")
    assert cb is sns_failure_callback


def test_daily_batch_pipeline_has_sla() -> None:
    """SLA is set per spec (4 hours end-to-end)."""
    from datetime import timedelta

    from daily_batch_pipeline import dag

    sla = dag.default_args.get("sla")
    assert sla == timedelta(hours=4)


def test_daily_batch_pipeline_wiring() -> None:
    """Sensor -> batch_id -> bronze -> silver -> gold -> snowflake -> dq."""
    from daily_batch_pipeline import dag

    by_id = {t.task_id: t for t in dag.tasks}

    # Sensor feeds batch_id
    assert by_id["wait_for_orders_raw"].downstream_task_ids == {"emit_batch_id"}

    # batch_id feeds the bronze group's tasks (in a TaskGroup, upstreams of
    # the group resolve to the group's leaves)
    assert "bronze.bronze_orders" in by_id["emit_batch_id"].downstream_task_ids

    # Bronze -> silver
    assert "silver.silver_orders" in by_id["bronze.bronze_orders"].downstream_task_ids

    # Silver -> both gold tasks (run in parallel)
    silver_downstream = by_id["silver.silver_orders"].downstream_task_ids
    assert "gold.gold_fact_orders" in silver_downstream
    assert "gold.gold_fact_order_lifecycle" in silver_downstream

    # Both gold -> snowflake load
    assert "snowflake_load_orders" in by_id["gold.gold_fact_orders"].downstream_task_ids
    assert "snowflake_load_orders" in by_id["gold.gold_fact_order_lifecycle"].downstream_task_ids

    # Snowflake -> dq gate
    assert by_id["snowflake_load_orders"].downstream_task_ids == {"dq_gate_fact_orders"}


def test_daily_batch_pipeline_doc_md_set() -> None:
    """DAG-level docstring renders in the UI; required per spec."""
    from daily_batch_pipeline import dag

    assert dag.doc_md is not None and len(dag.doc_md) > 50


@pytest.mark.parametrize(
    "task_id",
    [
        "bronze.bronze_orders",
        "silver.silver_orders",
        "gold.gold_fact_orders",
        "gold.gold_fact_order_lifecycle",
    ],
)
def test_databricks_tasks_are_deferrable(task_id: str) -> None:
    """All Databricks tasks must be deferrable so worker slots free up
    during long Databricks job runs."""
    from daily_batch_pipeline import dag

    task = dag.get_task(task_id)
    # deferrable attribute exists on DatabricksRunNowOperator
    assert getattr(task, "deferrable", False), f"{task_id} is not deferrable"


def test_s3_sensor_is_deferrable() -> None:
    from daily_batch_pipeline import dag

    sensor = dag.get_task("wait_for_orders_raw")
    assert getattr(sensor, "deferrable", False)
