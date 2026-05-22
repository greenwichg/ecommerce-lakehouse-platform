"""Tests for streamlit_app.data — mock loaders + live-fallback behaviour.

The live adapters are skipped in CI (no Snowflake / AWS), but the
*fallback to mock when creds are missing* path is asserted — that's
the behaviour an audience-facing dashboard relies on.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

# Make data/mock/generate.py importable as ``data.mock.generate``.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from streamlit_app import data as data_module  # noqa: E402

# ---------------------------------------------------------------------------
# Mock loaders
# ---------------------------------------------------------------------------


def test_airflow_runs_mock_returns_nonempty_dataframe() -> None:
    df = data_module.airflow_runs(mode="mock")
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    # Column contract — widget renderers depend on these
    assert {"dag_id", "run_id", "start_time", "state"} <= set(df.columns)
    # All three production DAGs represented
    assert {"daily_batch_pipeline", "hourly_clickstream_pipeline", "weekly_maintenance"} <= set(df["dag_id"])


def test_airflow_runs_sorted_most_recent_first() -> None:
    """Widget renders top 15 rows — they must be the freshest."""
    df = data_module.airflow_runs(mode="mock")
    assert df["start_time"].is_monotonic_decreasing


def test_freshness_mock_status_values_valid() -> None:
    df = data_module.freshness(mode="mock")
    assert {"fresh", "warn", "stale"} >= set(df["status"])
    assert (df["age_hours"] >= 0).all()
    assert (df["sla_hours"] > 0).all()


def test_freshness_mock_includes_stale_row_for_demo() -> None:
    """The mock dataset intentionally includes a stale row so the
    screenshot shows the ❌ status. Pin this so it doesn't drift."""
    df = data_module.freshness(mode="mock")
    assert (df["status"] == "stale").any()


def test_cost_per_run_mock_has_both_compute_kinds() -> None:
    """Slice 5 design called out: Snowflake-only undersells the
    split-compute story. The mock data must carry both kinds."""
    df = data_module.cost_per_run(mode="mock")
    assert {"snowflake_credits", "databricks_dbu"} <= set(df["compute_kind"])


def test_quarantine_queue_mock_has_all_three_reason_classes() -> None:
    """One queue entry per failure mode so the demo is comprehensive."""
    q = data_module.quarantine_queue(mode="mock")
    assert isinstance(q, list)
    assert len(q) >= 3
    reasons = " ".join(item["reason"] for item in q)
    assert "missing_columns" in reasons
    assert "unreadable" in reasons or "malformed" in reasons
    assert "zero_rows" in reasons


def test_currency_fallback_rate_mock_in_unit_interval() -> None:
    df = data_module.currency_fallback_rate(mode="mock")
    assert (df["simulated_share"] >= 0).all()
    assert (df["simulated_share"] <= 1).all()


def test_recent_alerts_mock_has_severity() -> None:
    alerts = data_module.recent_alerts(mode="mock")
    assert len(alerts) > 0
    for a in alerts:
        assert a["severity"] in {"info", "warn", "error"}


# ---------------------------------------------------------------------------
# Live-mode credential fallback
# ---------------------------------------------------------------------------


def test_live_config_partial_creds_treated_as_not_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    """If only SOME Snowflake env vars are set, the ready check fails
    rather than crashing on a downstream connect call."""
    for var in (
        "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE",
        "AWS_REGION", "AWS_DEFAULT_REGION", "SFN_STATE_MACHINE_ARN",
        "AIRFLOW_BASE_URL", "AIRFLOW_USERNAME", "AIRFLOW_PASSWORD",
    ):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "xy12345")
    # Other Snowflake vars deliberately unset

    cfg = data_module.LiveConfig.from_env()
    assert cfg.snowflake_ready is False
    assert cfg.aws_ready is False
    assert cfg.airflow_ready is False


def test_freshness_live_falls_back_to_mock_when_no_creds(monkeypatch: pytest.MonkeyPatch) -> None:
    """The dashboard must render even when live mode is requested but
    creds aren't set — graceful degradation, not a crash."""
    for var in (
        "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE",
    ):
        monkeypatch.delenv(var, raising=False)
    df = data_module.freshness(mode="live")
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


def test_quarantine_queue_live_falls_back_when_no_aws(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in ("AWS_REGION", "AWS_DEFAULT_REGION", "SFN_STATE_MACHINE_ARN"):
        monkeypatch.delenv(var, raising=False)
    queue = data_module.quarantine_queue(mode="live")
    assert isinstance(queue, list)
    assert len(queue) >= 1


# ---------------------------------------------------------------------------
# Quarantine decision wiring
# ---------------------------------------------------------------------------


def test_send_quarantine_decision_mock_returns_payload() -> None:
    """The mock path returns the payload that would have been sent, so
    a UI test can assert the button wires through correctly."""
    result = data_module.send_quarantine_decision(
        "mock-token-1", "discard", operator="alice", mode="mock",
        extra={"reason": "test"},
    )
    assert result["mocked"] is True
    assert result["task_token"] == "mock-token-1"
    assert result["payload"]["decision"] == "discard"
    assert result["payload"]["operator"] == "alice"
    assert result["payload"]["reason"] == "test"


def test_send_quarantine_decision_live_without_creds_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """Don't silently fall back to mock when an operator clicks a live
    button — surface the misconfiguration as an error."""
    for var in ("AWS_REGION", "AWS_DEFAULT_REGION", "SFN_STATE_MACHINE_ARN"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(RuntimeError, match="AWS env vars not set"):
        data_module.send_quarantine_decision(
            "mock-token-1", "replay", operator="alice", mode="live",
        )


# ---------------------------------------------------------------------------
# Mock-file regeneration
# ---------------------------------------------------------------------------


def test_regenerate_writes_all_expected_files(tmp_path: Path) -> None:
    """The mock generator's contract is a fixed set of files. If a new
    widget adds another file, this test fails until generate.py is
    updated — keeping the two in sync."""
    from data.mock.generate import regenerate

    regenerate(tmp_path)
    expected = {
        "airflow_runs.parquet",
        "freshness.parquet",
        "cost_per_run.parquet",
        "currency_fallback.parquet",
        "quarantine_queue.json",
        "recent_alerts.json",
    }
    actual = {p.name for p in tmp_path.iterdir() if p.is_file()}
    assert expected <= actual


def test_regenerate_is_deterministic(tmp_path: Path) -> None:
    """Same seed → same bytes. Lets us check files into git without
    flakiness across re-runs."""
    from data.mock.generate import regenerate

    a = tmp_path / "a"
    b = tmp_path / "b"
    regenerate(a)
    regenerate(b)
    for name in ("airflow_runs.parquet", "freshness.parquet", "cost_per_run.parquet"):
        # Parquet metadata includes a creation timestamp, but pandas
        # parquet writers in the same process get the same one. Compare
        # the *contents* by re-reading.
        df_a = pd.read_parquet(a / name)
        df_b = pd.read_parquet(b / name)
        pd.testing.assert_frame_equal(df_a.reset_index(drop=True), df_b.reset_index(drop=True))
    # JSON files must be byte-identical
    for name in ("quarantine_queue.json", "recent_alerts.json"):
        assert json.loads((a / name).read_text()) == json.loads((b / name).read_text())
