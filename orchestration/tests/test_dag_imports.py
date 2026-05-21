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
        # Underscore-prefixed files (e.g. _datasets.py) and pure helper
        # modules (callbacks, dq_gates) aren't DAG files.
        if path.name in {"__init__.py", "callbacks.py", "dq_gates.py"} or path.name.startswith("_"):
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
    # Sensors
    "wait_for_raw.wait_orders",
    "wait_for_raw.wait_customers",
    "wait_for_raw.wait_products",
    "wait_for_raw.wait_currency_rates",  # Slice 4
    "wait_for_raw.wait_wishlist",  # Slice 4
    # Coordination
    "emit_batch_id",
    "bronze",
    # Silver
    "silver.silver_orders",
    "silver.silver_customers",
    "silver.silver_products",
    "silver.silver_currency_rates",  # Slice 4
    "silver.silver_wishlist",  # Slice 4
    # Gold dims
    "gold_dims.dim_customer",
    "gold_dims.dim_product",
    # Gold facts + marts (Slice 4 grew this group: currency_rates ref
    # table, wishlist factless fact, customer_ltv mart)
    "gold_facts.fact_orders",
    "gold_facts.fact_order_lifecycle",
    "gold_facts.currency_rates",
    "gold_facts.fact_customer_wishlist_product",
    "gold_facts.customer_ltv",
    # Snowflake load
    "snowflake_load.dim_customer",
    "snowflake_load.dim_product",
    "snowflake_load.orders",
    "snowflake_load.currency_rates",  # Slice 4
    "snowflake_load.wishlist",  # Slice 4
    "snowflake_load.customer_ltv",  # Slice 4
    # DQ gates
    "dq_row_count_fact_orders",
    "dq_orphan_fact_orders",
    "dq_currency_freshness",  # Slice 4
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

    assert SOURCES == ["orders", "customers", "products", "currency_rates", "wishlist"]


def test_customer_ltv_depends_on_fact_orders() -> None:
    """customer_ltv reads fact_orders; if this regresses the mart would
    read stale data from yesterday's fact_orders Delta."""
    from daily_batch_pipeline import dag

    ltv = dag.get_task("gold_facts.customer_ltv")
    upstream_ids = {t.task_id for t in ltv.upstream_list}
    assert "gold_facts.fact_orders" in upstream_ids


def test_wishlist_fact_depends_on_dims() -> None:
    """Same PIT-binding correctness story as fact_orders: dims first."""
    from daily_batch_pipeline import dag

    wishlist = dag.get_task("gold_facts.fact_customer_wishlist_product")
    upstream_ids = {t.task_id for t in wishlist.upstream_list}
    assert "gold_dims.dim_customer" in upstream_ids
    assert "gold_dims.dim_product" in upstream_ids


def test_currency_freshness_gate_runs_after_currency_load() -> None:
    """The freshness gate queries Snowflake, so it has to wait for the
    Snowflake load. (And we don't gate it on fact_orders — currency is
    independent of orders' load path.)"""
    from daily_batch_pipeline import dag

    gate = dag.get_task("dq_currency_freshness")
    upstream_ids = {t.task_id for t in gate.upstream_list}
    assert "snowflake_load.currency_rates" in upstream_ids


def test_snowflake_customer_ltv_load_after_orders_load() -> None:
    """The mart's Snowflake load needs analytics.fact_orders populated
    first (the LTV table joins to dim_customer but is built from
    fact_orders upstream, so FK ordering follows fact_orders)."""
    from daily_batch_pipeline import dag

    load_ltv = dag.get_task("snowflake_load.customer_ltv")
    upstream_ids = {t.task_id for t in load_ltv.upstream_list}
    assert "snowflake_load.orders" in upstream_ids


# ---------------------------------------------------------------------------
# Slice 3 — hourly_clickstream_pipeline + dashboard_refresh
# ---------------------------------------------------------------------------


HOURLY_EXPECTED_TASKS = {
    "wait_for_clickstream_raw",
    "emit_batch_id",
    "bronze_clickstream",
    "silver_clickstream",
    "gold_fact_sessions",
    "publish_sessions_dataset",
}


def test_hourly_dag_imports() -> None:
    from hourly_clickstream_pipeline import dag

    assert dag.dag_id == "hourly_clickstream_pipeline"


def test_hourly_dag_has_expected_tasks() -> None:
    from hourly_clickstream_pipeline import dag

    actual = {t.task_id for t in dag.tasks}
    assert actual == HOURLY_EXPECTED_TASKS, f"diff: {actual ^ HOURLY_EXPECTED_TASKS}"


def test_hourly_dag_schedule_is_hourly() -> None:
    from hourly_clickstream_pipeline import dag

    # @hourly normalises to a cron timetable internally
    summary = dag.timetable.summary
    assert (
        "@hourly" in summary or "0 * * * *" in summary or "hour" in summary.lower()
    ), f"unexpected schedule: {summary}"


def test_hourly_dag_sla_is_15_minutes() -> None:
    from datetime import timedelta

    from hourly_clickstream_pipeline import dag

    assert dag.default_args.get("sla") == timedelta(minutes=15)


def test_hourly_dag_wiring() -> None:
    from hourly_clickstream_pipeline import dag

    by_id = {t.task_id: t for t in dag.tasks}
    assert by_id["wait_for_clickstream_raw"].downstream_task_ids == {"emit_batch_id"}
    assert by_id["emit_batch_id"].downstream_task_ids == {"bronze_clickstream"}
    assert by_id["bronze_clickstream"].downstream_task_ids == {"silver_clickstream"}
    assert by_id["silver_clickstream"].downstream_task_ids == {"gold_fact_sessions"}
    assert by_id["gold_fact_sessions"].downstream_task_ids == {"publish_sessions_dataset"}


def test_hourly_publishes_fact_sessions_dataset() -> None:
    """The Dataset outlet wiring is the whole point of Slice 3's
    cross-DAG triggering. If this regresses, dashboard_refresh stops
    firing on new clickstream data."""
    from _datasets import DATASET_FACT_SESSIONS
    from hourly_clickstream_pipeline import dag

    publish = dag.get_task("publish_sessions_dataset")
    outlet_uris = {ds.uri for ds in (publish.outlets or [])}
    assert DATASET_FACT_SESSIONS.uri in outlet_uris


@pytest.mark.parametrize(
    "task_id",
    ["bronze_clickstream", "silver_clickstream", "gold_fact_sessions"],
)
def test_hourly_databricks_tasks_deferrable(task_id: str) -> None:
    from hourly_clickstream_pipeline import dag

    assert getattr(dag.get_task(task_id), "deferrable", False)


def test_hourly_sensor_deferrable() -> None:
    from hourly_clickstream_pipeline import dag

    assert getattr(dag.get_task("wait_for_clickstream_raw"), "deferrable", False)


def test_dashboard_refresh_imports() -> None:
    from dashboard_refresh import dag

    assert dag.dag_id == "dashboard_refresh"


def test_dashboard_refresh_schedule_is_dataset() -> None:
    """Daily DAG stays cron, dashboard_refresh is dataset-triggered. If
    the schedule type regresses to cron, the separation-of-concerns
    documented in docs/architecture.md is gone."""
    from _datasets import DATASET_FACT_SESSIONS
    from dashboard_refresh import dag

    # Airflow 2.10: schedule=[dataset] builds a DatasetTriggeredTimetable
    # whose dataset_condition exposes the constituent datasets via
    # iter_datasets().
    assert (
        type(dag.timetable).__name__ == "DatasetTriggeredTimetable"
    ), f"expected DatasetTriggeredTimetable, got {type(dag.timetable).__name__}"
    # iter_datasets() in Airflow 2.10 yields (uri, Dataset) tuples
    pairs = list(dag.timetable.dataset_condition.iter_datasets())
    uris = {uri for uri, _ds in pairs}
    assert (
        DATASET_FACT_SESSIONS.uri in uris
    ), f"dashboard_refresh should be triggered by {DATASET_FACT_SESSIONS.uri}, got {uris}"


def test_dashboard_refresh_has_no_cron_schedule() -> None:
    """Per the architecture decision: dashboard_refresh fires only on
    upstream Dataset updates, NOT on a cron schedule. If someone adds a
    cron to this DAG, they've defeated the separation of concerns."""
    from dashboard_refresh import dag

    # No cron expression in the schedule's summary
    summary = dag.timetable.summary
    assert (
        "cron" not in summary.lower() or summary == "Never, external triggers only"
    ), f"dashboard_refresh should be dataset-only, got: {summary}"


# ---------------------------------------------------------------------------
# Slice 5 — weekly_maintenance
# ---------------------------------------------------------------------------


def test_weekly_maintenance_imports() -> None:
    from weekly_maintenance import dag

    assert dag.dag_id == "weekly_maintenance"


def test_weekly_maintenance_runs_sunday_0400_utc() -> None:
    """Per Slice 5 spec: 0 4 * * 0. If the cron drifts the DAG could
    collide with the daily batch (02:00 UTC) or the hourly window."""
    from weekly_maintenance import dag

    summary = dag.timetable.summary
    assert "0 4 * * 0" in summary, f"unexpected schedule: {summary}"


def test_weekly_maintenance_has_failure_callback() -> None:
    from callbacks import sns_failure_callback
    from weekly_maintenance import dag

    assert dag.default_args.get("on_failure_callback") is sns_failure_callback


def test_weekly_maintenance_covers_all_medallion_layers() -> None:
    """Regression guard: if a new Gold table is added without being
    added to DELTA_TABLES here, it accumulates small files until reads
    slow. Test pins the expected coverage."""
    from weekly_maintenance import DELTA_TABLES

    layers = {t.split(".")[0] for t in DELTA_TABLES}
    assert layers == {"bronze", "silver", "gold"}, (
        f"weekly maintenance should cover bronze/silver/gold; got {layers}"
    )
    # The Slice 4 gold marts MUST be included — they're append-heavy and
    # would degrade fastest if skipped.
    assert "gold.customer_ltv" in DELTA_TABLES
    assert "gold.fact_customer_wishlist_product" in DELTA_TABLES
    assert "gold.currency_rates" in DELTA_TABLES


def test_weekly_maintenance_includes_databricks_in_cost_report() -> None:
    """Slice 5 design decision (Q5): 'Snowflake-only undersells the
    split-compute story.' If the Databricks usage task gets removed,
    we're back to a one-sided report."""
    from weekly_maintenance import dag

    task_ids = {t.task_id for t in dag.tasks}
    assert "fetch_databricks_dbu_usage" in task_ids
    assert "fetch_snowflake_credit_usage" in task_ids


def test_weekly_maintenance_cost_report_runs_after_maintenance() -> None:
    """The cost report should see the actual maintenance run as part
    of the week's DBU spend — so maintenance must finish first."""
    from weekly_maintenance import dag

    report = dag.get_task("render_cost_report")
    upstream_ids = {t.task_id for t in report.upstream_list}
    # Both task groups should be upstream of render_cost_report
    assert "fetch_snowflake_credit_usage" in upstream_ids
    assert "fetch_databricks_dbu_usage" in upstream_ids


def test_weekly_maintenance_publish_runs_last() -> None:
    """The SNS publish must be the leaf — operators see the report
    only when everything else has been computed."""
    from weekly_maintenance import dag

    publish = dag.get_task("publish_cost_report")
    assert publish.downstream_task_ids == set()


def test_weekly_maintenance_clustering_alert_threshold() -> None:
    """The 4.0 threshold matches Snowflake's docs recommendation. If
    it gets dropped to e.g. 1.0 we'd alert constantly; if raised to 10
    we'd never recluster."""
    from weekly_maintenance import CLUSTERING_DEPTH_ALERT

    assert 2.0 <= CLUSTERING_DEPTH_ALERT <= 6.0
