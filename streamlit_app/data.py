"""Data layer for the lakehouse dashboard.

Each public function returns a pandas DataFrame (or list[dict] for the
JSON-shaped queue) that the widget renderers consume. The functions
take a ``mode`` flag — ``"mock"`` reads pre-generated files from
``data/mock/``; ``"live"`` queries Snowflake / Airflow REST / SFN.

Why this shape: the widgets don't know about the source. Tests cover
the mock path end-to-end; the live path is exercised by an integration
test marked ``live`` (skipped in CI without creds).

The live-mode imports are lazy so the dashboard renders in mock mode
without snowflake-connector-python or boto3 installed.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pandas as pd

log = logging.getLogger(__name__)

Mode = Literal["mock", "live"]

_REPO_ROOT = Path(__file__).resolve().parent.parent
_MOCK_DIR = _REPO_ROOT / "data" / "mock"


@dataclass(frozen=True)
class LiveConfig:
    """Connection parameters for live mode.

    Pulled from environment variables, not asked of the user at runtime,
    so the same dashboard can be redeployed without code changes.
    """

    snowflake_account: str | None
    snowflake_user: str | None
    snowflake_password: str | None
    snowflake_warehouse: str | None
    snowflake_database: str | None
    aws_region: str | None
    sfn_state_machine_arn: str | None
    airflow_base_url: str | None
    airflow_username: str | None
    airflow_password: str | None

    @classmethod
    def from_env(cls) -> LiveConfig:
        return cls(
            snowflake_account=os.environ.get("SNOWFLAKE_ACCOUNT"),
            snowflake_user=os.environ.get("SNOWFLAKE_USER"),
            snowflake_password=os.environ.get("SNOWFLAKE_PASSWORD"),
            snowflake_warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
            snowflake_database=os.environ.get("SNOWFLAKE_DATABASE"),
            aws_region=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"),
            sfn_state_machine_arn=os.environ.get("SFN_STATE_MACHINE_ARN"),
            airflow_base_url=os.environ.get("AIRFLOW_BASE_URL"),
            airflow_username=os.environ.get("AIRFLOW_USERNAME"),
            airflow_password=os.environ.get("AIRFLOW_PASSWORD"),
        )

    @property
    def snowflake_ready(self) -> bool:
        return all(
            [
                self.snowflake_account,
                self.snowflake_user,
                self.snowflake_password,
                self.snowflake_warehouse,
                self.snowflake_database,
            ]
        )

    @property
    def aws_ready(self) -> bool:
        return all([self.aws_region, self.sfn_state_machine_arn])

    @property
    def airflow_ready(self) -> bool:
        return all([self.airflow_base_url, self.airflow_username, self.airflow_password])


# ---------------------------------------------------------------------------
# Mock loaders
# ---------------------------------------------------------------------------


def _load_parquet(name: str) -> pd.DataFrame:
    path = _MOCK_DIR / name
    if not path.exists():
        # Reasonable error: the mock files are checked in, but if someone
        # ran ``rm -rf data/`` we can regenerate them rather than crash.
        from data.mock.generate import regenerate

        regenerate(_MOCK_DIR)
    return pd.read_parquet(path)


def _load_json(name: str) -> list[dict]:
    path = _MOCK_DIR / name
    if not path.exists():
        from data.mock.generate import regenerate

        regenerate(_MOCK_DIR)
    return json.loads(path.read_text())


# ---------------------------------------------------------------------------
# Public API — each widget calls the corresponding function with a mode.
# ---------------------------------------------------------------------------


def airflow_runs(mode: Mode = "mock", *, cfg: LiveConfig | None = None) -> pd.DataFrame:
    """Last ~50 DAG runs across all DAGs."""
    if mode == "mock":
        return _load_parquet("airflow_runs.parquet")
    cfg = cfg or LiveConfig.from_env()
    return _airflow_runs_live(cfg)


def freshness(mode: Mode = "mock", *, cfg: LiveConfig | None = None) -> pd.DataFrame:
    """Per-table MAX(_loaded_at) + SLA status."""
    if mode == "mock":
        return _load_parquet("freshness.parquet")
    cfg = cfg or LiveConfig.from_env()
    return _freshness_live(cfg)


def cost_per_run(mode: Mode = "mock", *, cfg: LiveConfig | None = None) -> pd.DataFrame:
    """Snowflake credits + Databricks DBUs by DAG run, last 30 days."""
    if mode == "mock":
        return _load_parquet("cost_per_run.parquet")
    cfg = cfg or LiveConfig.from_env()
    return _cost_live(cfg)


def quarantine_queue(mode: Mode = "mock", *, cfg: LiveConfig | None = None) -> list[dict]:
    """SFN executions waiting on operator input."""
    if mode == "mock":
        return _load_json("quarantine_queue.json")
    cfg = cfg or LiveConfig.from_env()
    return _quarantine_queue_live(cfg)


def currency_fallback_rate(mode: Mode = "mock", *, cfg: LiveConfig | None = None) -> pd.DataFrame:
    """Hourly share of currency_rates rows with _source='simulated'."""
    if mode == "mock":
        return _load_parquet("currency_fallback.parquet")
    cfg = cfg or LiveConfig.from_env()
    return _currency_fallback_live(cfg)


def recent_alerts(mode: Mode = "mock", *, cfg: LiveConfig | None = None) -> list[dict]:
    """Last 24h of SNS alerts."""
    if mode == "mock":
        return _load_json("recent_alerts.json")
    cfg = cfg or LiveConfig.from_env()
    return _recent_alerts_live(cfg)


def send_quarantine_decision(
    task_token: str,
    decision: Literal["replay", "discard", "fix-and-replay"],
    operator: str,
    *,
    mode: Mode = "mock",
    cfg: LiveConfig | None = None,
    extra: dict | None = None,
) -> dict:
    """POST a SendTaskSuccess to the SFN execution.

    In mock mode this is a no-op that returns the payload that would
    have been sent — useful for the test asserting button-click wiring
    without needing real AWS.
    """
    payload = {"decision": decision, "operator": operator, **(extra or {})}
    if mode == "mock":
        log.info("MOCK send-task-success: %s", payload)
        return {"mocked": True, "task_token": task_token, "payload": payload}

    cfg = cfg or LiveConfig.from_env()
    if not cfg.aws_ready:
        raise RuntimeError("AWS env vars not set; cannot send live SFN decision")
    import boto3

    client = boto3.client("stepfunctions", region_name=cfg.aws_region)
    client.send_task_success(taskToken=task_token, output=json.dumps(payload))
    return {"mocked": False, "task_token": task_token, "payload": payload}


# ---------------------------------------------------------------------------
# Live-mode adapters. Each tolerates partial credentials by falling back
# to mock data and logging a warning — the dashboard always renders.
# ---------------------------------------------------------------------------


def _airflow_runs_live(cfg: LiveConfig) -> pd.DataFrame:
    if not cfg.airflow_ready:
        log.warning("Airflow creds not set; falling back to mock airflow_runs")
        return _load_parquet("airflow_runs.parquet")
    # The Airflow REST API exposes /api/v1/dags/{dag_id}/dagRuns. We
    # paginate across the three DAGs and flatten. Lazy import so this
    # file is import-cheap.
    import requests
    from requests.auth import HTTPBasicAuth

    auth = HTTPBasicAuth(cfg.airflow_username, cfg.airflow_password)
    rows: list[dict] = []
    for dag_id in ("daily_batch_pipeline", "hourly_clickstream_pipeline", "weekly_maintenance"):
        url = f"{cfg.airflow_base_url}/api/v1/dags/{dag_id}/dagRuns?order_by=-start_date&limit=20"
        resp = requests.get(url, auth=auth, timeout=10)
        resp.raise_for_status()
        for r in resp.json().get("dag_runs", []):
            rows.append(
                {
                    "dag_id": dag_id,
                    "run_id": r["dag_run_id"],
                    "start_time": pd.to_datetime(r["start_date"]),
                    "end_time": pd.to_datetime(r["end_date"]) if r.get("end_date") else None,
                    "duration_s": (
                        (
                            pd.to_datetime(r["end_date"]) - pd.to_datetime(r["start_date"])
                        ).total_seconds()
                        if r.get("end_date")
                        else None
                    ),
                    "state": r["state"],
                    "rows_processed": None,  # filled from XCom in a future iteration
                }
            )
    return pd.DataFrame(rows).sort_values("start_time", ascending=False).reset_index(drop=True)


def _freshness_live(cfg: LiveConfig) -> pd.DataFrame:
    if not cfg.snowflake_ready:
        log.warning("Snowflake creds not set; falling back to mock freshness")
        return _load_parquet("freshness.parquet")
    import snowflake.connector

    tables = [
        ("analytics.fact_orders", 24),
        ("analytics.dim_customer", 24),
        ("analytics.dim_product", 24),
        ("analytics.fact_order_lifecycle", 24),
        ("analytics.currency_rates", 24),
        ("analytics.fact_sessions", 1),
        ("analytics.fact_customer_wishlist_product", 24),
        ("analytics.customer_ltv", 24),
    ]
    conn = snowflake.connector.connect(
        account=cfg.snowflake_account,
        user=cfg.snowflake_user,
        password=cfg.snowflake_password,
        warehouse=cfg.snowflake_warehouse,
        database=cfg.snowflake_database,
    )
    rows = []
    try:
        cur = conn.cursor()
        for table, sla_h in tables:
            cur.execute(
                f"SELECT MAX(_loaded_at) AS last_loaded, "  # noqa: S608 - table list is hardcoded
                f"DATEDIFF('hour', MAX(_loaded_at), CURRENT_TIMESTAMP()) AS age_h "
                f"FROM {table}"
            )
            row = cur.fetchone()
            last_loaded, age_h = row[0], float(row[1] or 0)
            status = "fresh" if age_h < sla_h * 0.5 else "warn" if age_h < sla_h else "stale"
            rows.append(
                {
                    "table": table,
                    "last_loaded_at": last_loaded,
                    "age_hours": age_h,
                    "sla_hours": sla_h,
                    "status": status,
                }
            )
    finally:
        conn.close()
    return pd.DataFrame(rows)


def _cost_live(cfg: LiveConfig) -> pd.DataFrame:
    if not cfg.snowflake_ready:
        log.warning("Snowflake creds not set; falling back to mock cost_per_run")
        return _load_parquet("cost_per_run.parquet")
    import snowflake.connector

    conn = snowflake.connector.connect(
        account=cfg.snowflake_account,
        user=cfg.snowflake_user,
        password=cfg.snowflake_password,
        warehouse=cfg.snowflake_warehouse,
        database=cfg.snowflake_database,
    )
    # Per-DAG attribution requires query_tag conventions on the Snowflake
    # side; we group by warehouse here and label by warehouse-to-DAG
    # mapping. The mock dataset uses the same shape so the renderer is
    # source-agnostic.
    query = """
        SELECT
            DATE(start_time) AS run_date,
            warehouse_name   AS dag_id,
            'snowflake_credits' AS compute_kind,
            SUM(credits_used)   AS credits_or_dbus
        FROM snowflake.account_usage.warehouse_metering_history
        WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
        GROUP BY 1, 2
        ORDER BY 1, 2
    """
    rows = []
    try:
        cur = conn.cursor()
        cur.execute(query)
        for r in cur.fetchall():
            rows.append(
                {
                    "run_date": r[0],
                    "dag_id": r[1],
                    "compute_kind": r[2],
                    "credits_or_dbus": float(r[3] or 0),
                }
            )
    finally:
        conn.close()
    # Databricks DBUs aren't queryable from Snowflake — would come from
    # the Databricks billable-usage API. Live mode returns Snowflake-only
    # until that's wired (Slice 5 deviation #5).
    log.info("live cost report is Snowflake-only; Databricks DBUs require the billable-usage API")
    return pd.DataFrame(rows)


def _quarantine_queue_live(cfg: LiveConfig) -> list[dict]:
    if not cfg.aws_ready:
        log.warning("AWS env vars not set; falling back to mock quarantine_queue")
        return _load_json("quarantine_queue.json")
    import boto3

    client = boto3.client("stepfunctions", region_name=cfg.aws_region)
    paginator = client.get_paginator("list_executions")
    queue = []
    for page in paginator.paginate(
        stateMachineArn=cfg.sfn_state_machine_arn, statusFilter="RUNNING"
    ):
        for ex in page.get("executions", []):
            detail = client.describe_execution(executionArn=ex["executionArn"])
            input_obj = json.loads(detail.get("input", "{}"))
            started = detail["startDate"]
            queue.append(
                {
                    "execution_name": ex["name"],
                    "started_at": started.isoformat(),
                    "age_hours": (
                        pd.Timestamp.now(tz="UTC") - pd.Timestamp(started)
                    ).total_seconds()
                    / 3600,
                    "quarantine_key": input_obj.get("quarantine_key", "(unknown)"),
                    "source": input_obj.get("source", "(unknown)"),
                    "reason": input_obj.get("reason", "(unknown)"),
                    # Task tokens come from inside the WaitForOperatorDecision
                    # state; getActivityTask retrieves them. Real implementation
                    # tracks tokens in a sidecar table keyed by execution name.
                    "task_token": input_obj.get("_task_token", "(retrieve from activity poller)"),
                }
            )
    return queue


def _currency_fallback_live(cfg: LiveConfig) -> pd.DataFrame:
    if not cfg.snowflake_ready:
        log.warning("Snowflake creds not set; falling back to mock currency_fallback")
        return _load_parquet("currency_fallback.parquet")
    import snowflake.connector

    conn = snowflake.connector.connect(
        account=cfg.snowflake_account,
        user=cfg.snowflake_user,
        password=cfg.snowflake_password,
        warehouse=cfg.snowflake_warehouse,
        database=cfg.snowflake_database,
    )
    query = """
        SELECT
            DATE_TRUNC('hour', _loaded_at) AS hour,
            SUM(CASE WHEN _source = 'simulated' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS simulated_share
        FROM analytics.currency_rates
        WHERE _loaded_at >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
        GROUP BY 1
        ORDER BY 1
    """
    try:
        cur = conn.cursor()
        cur.execute(query)
        return pd.DataFrame(cur.fetchall(), columns=["hour", "simulated_share"])
    finally:
        conn.close()


def _recent_alerts_live(cfg: LiveConfig) -> list[dict]:
    # SNS doesn't have a "list recent published messages" API. In
    # production this widget reads from a CloudWatch log group that
    # mirrors SNS publishes (configured in CloudWatch module). For now,
    # live mode returns the same mock until that log group is plumbed.
    log.info("recent_alerts live mode requires CloudWatch log integration; using mock")
    return _load_json("recent_alerts.json")
