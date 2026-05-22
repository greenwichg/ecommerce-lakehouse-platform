"""End-to-end tests for the Streamlit app using AppTest.

AppTest runs the script in-process and exposes the resulting component
tree. We assert that all four widgets render and the sidebar gauge
shows up — without booting a browser.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from streamlit.testing.v1 import AppTest  # noqa: E402


@pytest.fixture()
def app() -> AppTest:
    """Run the app in mock mode and return the populated AppTest."""
    at = AppTest.from_file(str(_REPO_ROOT / "streamlit_app" / "app.py"), default_timeout=15)
    at.run()
    return at


def test_app_renders_without_exceptions(app: AppTest) -> None:
    """The most basic guard — if any widget raises, AppTest captures it."""
    assert not app.exception, f"App raised: {app.exception}"


def test_app_title_present(app: AppTest) -> None:
    titles = [t.value for t in app.title]
    assert any("Ecommerce Lakehouse" in t for t in titles)


def test_all_four_widget_subheaders_present(app: AppTest) -> None:
    """The four widgets each emit a st.subheader. If a widget silently
    fails to render, this catches it."""
    subheaders = [s.value for s in app.subheader]
    expected = {
        "Pipeline run history",
        "Data freshness",
        "Cost per pipeline (last 30 days)",
        "Quarantine queue",
        "Currency fallback rate (24h)",
        "Recent alerts",
    }
    actual = set(subheaders)
    missing = expected - actual
    assert not missing, f"missing widget subheaders: {missing}; got: {actual}"


def test_pipeline_runs_widget_has_metrics(app: AppTest) -> None:
    """Pipeline-runs widget emits three top-line metrics."""
    metric_labels = [m.label for m in app.metric]
    assert "24h success rate" in metric_labels
    assert "Median duration" in metric_labels
    assert "Runs (last 24h)" in metric_labels


def test_cost_widget_emits_split_compute_totals(app: AppTest) -> None:
    """The 30-day totals widget must show BOTH Snowflake credits and
    Databricks DBUs — that's the split-compute story."""
    metric_labels = [m.label for m in app.metric]
    assert "Snowflake credits (30d)" in metric_labels
    assert "Databricks DBUs (30d)" in metric_labels


def test_quarantine_queue_renders_three_expanders_in_mock(app: AppTest) -> None:
    """Each queue entry becomes a collapsible expander. Mock has 3."""
    # AppTest exposes expanders via .get("expander"); each has a label
    expanders = app.get("expander")
    assert len(expanders) >= 3, f"expected 3+ quarantine expanders, got {len(expanders)}"


def test_mode_falls_back_to_mock_with_no_creds(monkeypatch: pytest.MonkeyPatch) -> None:
    """LAKEHOUSE_DASHBOARD_MODE=live + no creds → mock with a warning."""
    for var in (
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
        "SFN_STATE_MACHINE_ARN",
        "AIRFLOW_BASE_URL",
        "AIRFLOW_USERNAME",
        "AIRFLOW_PASSWORD",
    ):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("LAKEHOUSE_DASHBOARD_MODE", "live")

    at = AppTest.from_file(str(_REPO_ROOT / "streamlit_app" / "app.py"), default_timeout=15)
    at.run()

    assert not at.exception
    # The fallback warning is emitted via st.warning
    warning_texts = [w.value for w in at.warning]
    assert any(
        "Falling back to mock" in w for w in warning_texts
    ), f"expected fallback warning, got warnings={warning_texts}"


def test_quarantine_button_in_mock_mode_does_not_crash(app: AppTest) -> None:
    """Clicking the Replay button in mock mode should log + toast, not raise."""
    # Locate the first Replay button by its key prefix
    buttons = [b for b in app.button if b.key and b.key.startswith("replay-")]
    assert len(buttons) >= 1, "no replay buttons rendered"

    # Click and re-run — should not raise an exception
    buttons[0].click()
    app.run()
    assert not app.exception
