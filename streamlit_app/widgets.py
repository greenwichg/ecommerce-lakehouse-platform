"""Widget render functions.

Each widget is a function that takes a DataFrame (already loaded by
``data.py``) and emits Streamlit components. Keeping rendering separate
from data loading lets us unit-test the data layer against fixtures
without booting Streamlit's runtime.
"""

from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from streamlit_app import data as data_module

# ---------------------------------------------------------------------------
# Widget A — Pipeline run history
# ---------------------------------------------------------------------------


def render_pipeline_runs(df: pd.DataFrame) -> None:
    st.subheader("Pipeline run history")
    st.caption("Last DAG runs across the three production pipelines.")

    if df.empty:
        st.info("No DAG runs available.")
        return

    # Top-line counters: 24-h success rate and median duration. These
    # are the numbers a viewer cares about before drilling into rows.
    recent = df[df["start_time"] >= (df["start_time"].max() - pd.Timedelta(days=1))]
    success_rate = (recent["state"] == "success").mean() if not recent.empty else 0.0
    median_dur = recent["duration_s"].dropna().median() if not recent.empty else 0
    cols = st.columns(3)
    cols[0].metric("24h success rate", f"{success_rate:.0%}")
    cols[1].metric("Median duration", f"{median_dur:.0f}s" if median_dur else "—")
    cols[2].metric("Runs (last 24h)", len(recent))

    # Per-DAG run-state chart. Stacked bar by state per DAG.
    chart_df = (
        df.assign(state_label=df["state"].fillna("unknown"))
        .groupby(["dag_id", "state_label"])
        .size()
        .reset_index(name="count")
    )
    chart = (
        alt.Chart(chart_df)
        .mark_bar()
        .encode(
            x=alt.X("dag_id:N", title=None),
            y=alt.Y("count:Q", title="Runs"),
            color=alt.Color(
                "state_label:N",
                scale=alt.Scale(
                    domain=["success", "running", "failed", "unknown"],
                    range=["#22c55e", "#3b82f6", "#ef4444", "#94a3b8"],
                ),
                legend=alt.Legend(title="State"),
            ),
            tooltip=["dag_id", "state_label", "count"],
        )
        .properties(height=200)
    )
    st.altair_chart(chart, use_container_width=True)

    # Detail table — limited rows, sorted by recency, formatted.
    display = df.head(15).copy()
    display["start_time"] = display["start_time"].dt.strftime("%Y-%m-%d %H:%M")
    display["duration"] = display["duration_s"].apply(
        lambda s: f"{int(s)}s" if pd.notnull(s) else "running"
    )
    display = display[["dag_id", "run_id", "start_time", "duration", "state", "rows_processed"]]
    st.dataframe(display, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Widget B — Data freshness
# ---------------------------------------------------------------------------


_STATUS_ICONS = {"fresh": "✅", "warn": "⚠️", "stale": "❌"}


def render_freshness(df: pd.DataFrame) -> None:
    st.subheader("Data freshness")
    st.caption("Hours since the last successful load, vs. the table's SLA.")

    if df.empty:
        st.info("No freshness data available.")
        return

    display = df.copy()
    display["status_icon"] = display["status"].map(_STATUS_ICONS).fillna("❓")
    display["last_loaded_at"] = pd.to_datetime(display["last_loaded_at"]).dt.strftime(
        "%Y-%m-%d %H:%M"
    )
    display["age"] = display["age_hours"].apply(lambda h: f"{h:.1f}h")
    display["sla"] = display["sla_hours"].apply(lambda h: f"{int(h)}h")
    display = display[["status_icon", "table", "last_loaded_at", "age", "sla"]]
    display.columns = ["", "Table", "Last loaded", "Age", "SLA"]
    st.dataframe(display, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Widget C — Cost per pipeline run
# ---------------------------------------------------------------------------


def render_cost_per_run(df: pd.DataFrame) -> None:
    st.subheader("Cost per pipeline (last 30 days)")
    st.caption(
        "Snowflake credits + Databricks DBUs stacked by DAG. The split-compute story should show two stripes per bar."
    )

    if df.empty:
        st.info("No cost data available.")
        return

    # Aggregate by dag_id + compute_kind so the chart is a stacked
    # column per DAG, not a per-day timeline (the timeline goes in the
    # tooltip).
    agg = df.groupby(["dag_id", "compute_kind"])["credits_or_dbus"].sum().reset_index()

    chart = (
        alt.Chart(agg)
        .mark_bar()
        .encode(
            x=alt.X("dag_id:N", title=None),
            y=alt.Y("credits_or_dbus:Q", title="Credits / DBUs (30d total)"),
            color=alt.Color(
                "compute_kind:N",
                scale=alt.Scale(
                    domain=["snowflake_credits", "databricks_dbu"],
                    range=["#29B5E8", "#FF3621"],  # Snowflake blue + Databricks red
                ),
                legend=alt.Legend(title=None, orient="top"),
            ),
            tooltip=["dag_id", "compute_kind", alt.Tooltip("credits_or_dbus:Q", format=".2f")],
        )
        .properties(height=240)
    )
    st.altair_chart(chart, use_container_width=True)

    # Bottom-line totals
    total_credits = df[df["compute_kind"] == "snowflake_credits"]["credits_or_dbus"].sum()
    total_dbus = df[df["compute_kind"] == "databricks_dbu"]["credits_or_dbus"].sum()
    cols = st.columns(2)
    cols[0].metric("Snowflake credits (30d)", f"{total_credits:,.1f}")
    cols[1].metric("Databricks DBUs (30d)", f"{total_dbus:,.1f}")


# ---------------------------------------------------------------------------
# Widget D — Quarantine queue
# ---------------------------------------------------------------------------


def render_quarantine_queue(queue: list[dict], *, mode: str) -> None:
    st.subheader("Quarantine queue")
    st.caption(
        "Files awaiting operator decision. Buttons POST `SendTaskSuccess` "
        "to the Step Functions execution."
        + (" *(Mock mode: button clicks logged, no live state change.)*" if mode == "mock" else "")
    )

    if not queue:
        st.success("Queue is empty — no quarantined files waiting.")
        return

    for item in queue:
        with st.expander(
            f"⚠️ {item['source']} — {item['reason']}  ·  {item['age_hours']:.1f}h ago",
            expanded=False,
        ):
            st.write(f"**Key:** `{item['quarantine_key']}`")
            st.write(f"**Execution:** `{item['execution_name']}`")
            st.write(f"**Started:** {item['started_at']}")

            # Three buttons matching the SFN decision branches. Each
            # POSTs SendTaskSuccess with the appropriate decision payload.
            cols = st.columns(3)
            token = item["task_token"]
            if cols[0].button("Replay", key=f"replay-{item['execution_name']}"):
                resp = data_module.send_quarantine_decision(
                    token, "replay", operator=_current_operator(), mode=mode
                )
                _toast_outcome("Replay sent", resp)
            if cols[1].button("Discard", key=f"discard-{item['execution_name']}"):
                resp = data_module.send_quarantine_decision(
                    token,
                    "discard",
                    operator=_current_operator(),
                    mode=mode,
                    extra={"reason": "manually discarded via dashboard"},
                )
                _toast_outcome("Discard sent", resp)
            if cols[2].button("Fix-and-replay", key=f"fix-{item['execution_name']}"):
                resp = data_module.send_quarantine_decision(
                    token,
                    "fix-and-replay",
                    operator=_current_operator(),
                    mode=mode,
                )
                _toast_outcome("Fix-and-replay sent — upload to _fixed/ to trigger replay", resp)


def _current_operator() -> str:
    """Operator identifier recorded in the audit log + task output.

    In Slice 6 we use the env var ``LAKEHOUSE_OPERATOR`` or fall back
    to ``dashboard-user`` so the audit log isn't anonymous. A real
    deployment plugs in SSO / Streamlit auth here.
    """
    import os

    return os.environ.get("LAKEHOUSE_OPERATOR", "dashboard-user")


def _toast_outcome(headline: str, resp: dict) -> None:
    if resp.get("mocked"):
        st.toast(f"{headline} (mock)", icon="🧪")
    else:
        st.toast(headline, icon="✅")


# ---------------------------------------------------------------------------
# Sidebar widgets
# ---------------------------------------------------------------------------


def render_sidebar_currency_gauge(df: pd.DataFrame) -> None:
    st.sidebar.subheader("Currency fallback rate (24h)")
    if df.empty:
        st.sidebar.info("No data")
        return
    avg = df["simulated_share"].mean()
    color = "🟢" if avg < 0.10 else "🟡" if avg < 0.30 else "🔴"
    st.sidebar.metric(
        f"{color} Simulated share (mean)",
        f"{avg:.0%}",
        help="Share of currency_rates rows sourced from the deterministic fallback "
        "vs the live exchangerate API. >30% = API is having a bad day.",
    )
    line = (
        alt.Chart(df)
        .mark_area(opacity=0.6, color="#fbbf24")
        .encode(
            x=alt.X("hour:T", title=None, axis=alt.Axis(format="%H:%M")),
            y=alt.Y("simulated_share:Q", title=None, scale=alt.Scale(domain=[0, 1])),
        )
        .properties(height=80)
    )
    st.sidebar.altair_chart(line, use_container_width=True)


def render_sidebar_alerts(alerts: list[dict]) -> None:
    st.sidebar.subheader("Recent alerts")
    if not alerts:
        st.sidebar.info("No recent alerts.")
        return
    for a in alerts[:10]:
        icon = {"warn": "⚠️", "info": "ℹ️", "error": "🚨"}.get(a.get("severity", "info"), "•")
        st.sidebar.markdown(f"{icon} **{a['subject']}**  \n_{a['ts']}_")
