"""Ecommerce Lakehouse Platform — operations dashboard.

Run::

    # Default (mock mode — no credentials needed, screenshot-ready)
    streamlit run streamlit_app/app.py

    # Live mode (set SNOWFLAKE_*, AWS_*, AIRFLOW_* env vars)
    LAKEHOUSE_DASHBOARD_MODE=live streamlit run streamlit_app/app.py

Four widgets cover the operational story:
    A. Pipeline run history (Airflow DAG state)
    B. Data freshness (Snowflake MAX(_loaded_at) vs SLA)
    C. Cost per pipeline run (Snowflake credits + Databricks DBUs)
    D. Quarantine queue (Step Functions executions awaiting decision)

Plus a sidebar with the currency-fallback-rate gauge and recent SNS
alerts.

This file is intentionally thin: it picks the mode, calls the data
layer for each widget, and hands the result to the corresponding
renderer in ``widgets.py``.
"""

from __future__ import annotations

import os

import streamlit as st

from streamlit_app import data as data_module
from streamlit_app import widgets


def _resolve_mode() -> str:
    """Resolve mock vs live mode.

    Default is "mock". The ``LAKEHOUSE_DASHBOARD_MODE`` env var
    overrides; falls back to mock if "live" is requested but no
    Snowflake / AWS creds are visible.
    """
    requested = os.environ.get("LAKEHOUSE_DASHBOARD_MODE", "mock").lower()
    if requested != "live":
        return "mock"

    cfg = data_module.LiveConfig.from_env()
    if not (cfg.snowflake_ready or cfg.aws_ready or cfg.airflow_ready):
        st.warning(
            "Dashboard mode is 'live' but no Snowflake / AWS / Airflow "
            "credentials are set. Falling back to mock mode."
        )
        return "mock"
    return "live"


def main() -> None:
    st.set_page_config(
        page_title="Lakehouse — Operations",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    mode = _resolve_mode()

    # Header
    title_col, status_col = st.columns([5, 1])
    title_col.title("Ecommerce Lakehouse")
    title_col.caption("Operations dashboard — pipeline health, freshness, cost, and the quarantine queue.")
    status_col.markdown(
        f"<div style='text-align:right; font-size:0.85rem; padding-top:1.5rem;'>"
        f"Mode: <code>{mode}</code></div>",
        unsafe_allow_html=True,
    )
    st.divider()

    # Main grid — two columns, two rows. Order top-left → bottom-right
    # by what someone looking for "is the platform OK?" cares about most.
    row1_left, row1_right = st.columns([1, 1])

    with row1_left:
        widgets.render_pipeline_runs(data_module.airflow_runs(mode=mode))
    with row1_right:
        widgets.render_freshness(data_module.freshness(mode=mode))

    st.divider()

    row2_left, row2_right = st.columns([1, 1])
    with row2_left:
        widgets.render_cost_per_run(data_module.cost_per_run(mode=mode))
    with row2_right:
        widgets.render_quarantine_queue(data_module.quarantine_queue(mode=mode), mode=mode)

    # Sidebar
    widgets.render_sidebar_currency_gauge(data_module.currency_fallback_rate(mode=mode))
    st.sidebar.divider()
    widgets.render_sidebar_alerts(data_module.recent_alerts(mode=mode))
    st.sidebar.divider()
    st.sidebar.markdown(
        "**About**  \nProduction-grade ecommerce lakehouse demo.  \n"
        "[Architecture](docs/architecture.md) · "
        "[Runbook](docs/runbook.md) · "
        "[Data model](docs/data_model.md)"
    )


if __name__ == "__main__":
    main()
