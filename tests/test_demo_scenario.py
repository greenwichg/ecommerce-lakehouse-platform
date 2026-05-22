"""Smoke test for demo/inject_bad_file.py.

The demo is part of the deliverable, not a one-shot script. If a future
change to the validator or quarantine_helper signatures breaks the
demo's call-sites, we want CI to catch it before someone tries to run
it live.

The test invokes ``main(["--auto-replay"])`` in mock mode and asserts
the exit code is 0. It does NOT assert on printed output (that's the
demo's job, not the contract); it asserts the *flow* completes cleanly.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))


def test_demo_mock_full_loop_runs_cleanly(capsys: pytest.CaptureFixture[str]) -> None:
    from demo.inject_bad_file import main

    exit_code = main(["--auto-replay"])
    assert exit_code == 0, f"demo failed with exit code {exit_code}"

    captured = capsys.readouterr()
    # Spot-check the demo hits all six phases. If any of these markers
    # disappear, the demo's documentation is no longer accurate.
    for marker in (
        "[1] Dropped malformed file",
        "[2] Validator processed",
        "[3] File moved to quarantine",
        "[4] Step Functions execution started",
        "[5] AUTO-REPLAY",
        "[6] Replay validation",
        "DEMO COMPLETE",
    ):
        assert marker in captured.out, f"missing marker in demo output: {marker!r}"


def test_demo_mock_without_auto_replay_stops_after_quarantine(capsys: pytest.CaptureFixture[str]) -> None:
    """The default run stops after step 4 — confirm step 5 doesn't fire."""
    from demo.inject_bad_file import main

    exit_code = main([])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "[4] Step Functions execution started" in captured.out
    assert "[5] AUTO-REPLAY" not in captured.out
    assert "DEMO COMPLETE" not in captured.out
