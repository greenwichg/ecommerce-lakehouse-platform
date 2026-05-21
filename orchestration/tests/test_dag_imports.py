"""Smoke tests for DAG imports.

A DAG that doesn't import is a DAG that can't run. These tests catch
import errors (missing providers, syntax issues, bad task wiring) without
needing a live Airflow scheduler.

Slice 2 expands the assertion set: dynamic-task-mapped bronze, new
silver/gold/snowflake/dq tasks for customers + products, PIT dim->fact
ordering.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_DAGS_DIR = Path(__file__).resolve().parent.parent / "dags"
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))


def _import_all_dag_modules() -> list:
    """Import every DAG file in orchestration/dags/ and return the module objects."""
    import importlib

    modules = []
    for path in sorted(_DAGS_DIR.glob("*.py")):
        if path.name in {"__init__.py", "callbacks.py", "dq_gates.py"}:
            continue
        mod = importlib.import_module(path.stem)
        modules.append(mod)
    return modules


def test_no_import_errors() -> None:
    modules = _import_all_dag_modules()
    assert len(modules) >= 1, "no DAG files found in orchestration/dags/"


# Slice 2 task set, by group:
#   wait_for_raw.wait_{orders,customers,products}
#   emit_batch_id
#   bronze (mapped over SOURCES)
#   silver.silver_{orders,customers,products}
#   gold_dims.{dim_customer,dim_product}
#   gold_facts.{fact_orders,fact_order_lifecycle}
#   snowflake_load.{dim_customer,dim_product,orders}
#   dq_row_count_fact_orders, dq_orphan_fact_orders
EXPECTED_TASKS = {
    "wait_for_raw.wait_orders",
    "wait_for_raw.wait_customers",
    "wait_for_raw.wait_products",
    "emit_batch_id",
    "bronze",
    "silver.silver_orders",
    "silver.silver_customers",
    "silver.silver_products",
    "gold_dims.dim_customer",
    "gold_dims.dim_product",
    "gold_facts.fact_orders",
    "gold_facts.fact_order_lifecycle",
    "snowflake_load.dim_customer",
    "snowflake_load.dim_product",
    "snowflake_load.orders",
    "dq_row_count_fact_orders",
    "dq_orphan_fact_orders",
}


def test_daily_batch_pipeline_has_expected_tasks() -> None:
    from daily_batch_pipeline import dag

    actual = {t.task_id for t in dag.tasks}
    missing = EXPECTED_TASKS - actual
    extra = actual - EXPECTED_TASKS
    assert not missing and not extra, f"missing={missing} extra={extra}"


def test_daily_batch_pipeline_has_failure_callback() -> None:
    from callbacks import sns_failure_callback
    from daily_batch_pipeline import dag

    assert dag.default_args.get("on_failure_callback") is sns_failure_callback


def test_daily_batch_pipeline_has_sla() -> None:
    from datetime import timedelta

    from daily_batch_pipeline import dag

    assert dag.default_args.get("sla") == timedelta(hours=4)


def test_daily_batch_pipeline_doc_md_set() -> None:
    from daily_batch_pipeline import dag

    assert dag.doc_md is not None and len(dag.doc_md) > 50


def test_bronze_is_dynamically_mapped() -> None:
    """Per Slice 2 spec: bronze uses .expand() so a new source is one
    list entry, not a copy-pasted operator block."""
    from daily_batch_pipeline import dag

    bronze = dag.get_task("bronze")
    cls_name = type(bronze).__name__
    assert "Mapped" in cls_name or hasattr(
        bronze, "expand_input"
    ), f"bronze should be a MappedOperator, got {cls_name}"


def test_bronze_maps_one_per_source() -> None:
    """The expand iterable has the same arity as SOURCES."""
    from daily_batch_pipeline import SOURCES, dag

    bronze = dag.get_task("bronze")
    import contextlib

    map_count = None
    if hasattr(bronze, "expand_input"):
        with contextlib.suppress(Exception):
            map_count = len(bronze.expand_input.value["job_id"])
    assert map_count == len(SOURCES), f"bronze maps {map_count}, expected {len(SOURCES)}"


def test_dim_tasks_block_fact_orders() -> None:
    """PIT-correct surrogate binding requires both dim tasks to complete
    BEFORE fact_orders runs. If this regresses, fact_orders could see a
    stale dim and bind to the wrong SCD2 version."""
    from daily_batch_pipeline import dag

    fact = dag.get_task("gold_facts.fact_orders")
    upstream_ids = {t.task_id for t in fact.upstream_list}
    assert "gold_dims.dim_customer" in upstream_ids
    assert "gold_dims.dim_product" in upstream_ids


def test_fact_order_lifecycle_does_not_block_on_dims() -> None:
    """The lifecycle fact has no surrogate FKs to dims; gating on them
    would just slow the DAG without buying anything."""
    from daily_batch_pipeline import dag

    lifecycle = dag.get_task("gold_facts.fact_order_lifecycle")
    upstream_ids = {t.task_id for t in lifecycle.upstream_list}
    assert "gold_dims.dim_customer" not in upstream_ids
    assert "gold_dims.dim_product" not in upstream_ids


def test_dq_gates_run_after_snowflake_load() -> None:
    from daily_batch_pipeline import dag

    for gate_id in ("dq_row_count_fact_orders", "dq_orphan_fact_orders"):
        upstream_ids = {t.task_id for t in dag.get_task(gate_id).upstream_list}
        assert (
            "snowflake_load.orders" in upstream_ids
        ), f"{gate_id} should run after snowflake_load.orders"


def test_snowflake_facts_load_after_dims() -> None:
    """FK safety: dim_customer / dim_product must land before fact_orders
    in Snowflake too, regardless of whether FKs are enforced."""
    from daily_batch_pipeline import dag

    fact_load = dag.get_task("snowflake_load.orders")
    upstream_ids = {t.task_id for t in fact_load.upstream_list}
    assert "snowflake_load.dim_customer" in upstream_ids
    assert "snowflake_load.dim_product" in upstream_ids


@pytest.mark.parametrize(
    "task_id",
    [
        "silver.silver_orders",
        "silver.silver_customers",
        "silver.silver_products",
        "gold_dims.dim_customer",
        "gold_dims.dim_product",
        "gold_facts.fact_orders",
        "gold_facts.fact_order_lifecycle",
    ],
)
def test_databricks_tasks_are_deferrable(task_id: str) -> None:
    from daily_batch_pipeline import dag

    task = dag.get_task(task_id)
    assert getattr(task, "deferrable", False), f"{task_id} is not deferrable"


@pytest.mark.parametrize(
    "task_id",
    ["wait_for_raw.wait_orders", "wait_for_raw.wait_customers", "wait_for_raw.wait_products"],
)
def test_sensors_are_deferrable(task_id: str) -> None:
    from daily_batch_pipeline import dag

    sensor = dag.get_task(task_id)
    assert getattr(sensor, "deferrable", False)


def test_sources_constant_matches_spec() -> None:
    from daily_batch_pipeline import SOURCES

    assert SOURCES == ["orders", "customers", "products"]
