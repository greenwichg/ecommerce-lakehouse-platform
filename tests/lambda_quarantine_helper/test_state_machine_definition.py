"""Static validation of the ASL JSON for the quarantine-replay state machine.

We can't run a real SFN execution in tests, but we CAN validate:
- The JSON parses
- All States referenced by Next / Default actually exist
- Every reachable terminal state has an End / Error / TimeoutSeconds
- The expected three decision branches (replay/discard/fix-and-replay) exist
- The fix-and-replay loop has the WaitForFixedFile → PollForFixedFile → Choice
  structure (regression guard)
"""

from __future__ import annotations

import json
from pathlib import Path

_DEFINITION_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "infrastructure"
    / "terraform"
    / "modules"
    / "step_functions"
    / "definition.asl.json"
)


def _load_definition() -> dict:
    # Read the file as text; it has ${...} placeholders that Terraform
    # substitutes at apply time, so json.loads on the raw file is fine
    # — those placeholders are inside JSON string values, not bare.
    return json.loads(_DEFINITION_PATH.read_text())


def test_definition_parses() -> None:
    d = _load_definition()
    assert d["StartAt"] == "NotifyOpsForReview"
    assert "States" in d


def test_all_next_targets_exist() -> None:
    d = _load_definition()
    state_names = set(d["States"].keys())
    for name, state in d["States"].items():
        if "Next" in state:
            assert (
                state["Next"] in state_names
            ), f"State {name} → Next={state['Next']} doesn't exist"
        if "Choices" in state:
            for choice in state["Choices"]:
                assert (
                    choice["Next"] in state_names
                ), f"Choice in {name} → {choice['Next']} doesn't exist"
            if "Default" in state:
                assert state["Default"] in state_names
        if "Catch" in state:
            for catch in state["Catch"]:
                assert catch["Next"] in state_names


def test_three_decision_branches_exist() -> None:
    """Approved scenario: replay, discard, fix-and-replay."""
    d = _load_definition()
    router = d["States"]["RouteOnDecision"]
    decisions = {c["StringEquals"] for c in router["Choices"]}
    assert decisions == {"replay", "discard", "fix-and-replay"}


def test_wait_for_operator_decision_uses_task_token() -> None:
    """WaitForOperatorDecision must use waitForTaskToken; otherwise the
    operator wait isn't actually a wait."""
    d = _load_definition()
    state = d["States"]["WaitForOperatorDecision"]
    assert state["Resource"].endswith(".waitForTaskToken")
    # Timeout is the 7-day operator-decision window
    assert state["TimeoutSeconds"] == 604800


def test_fix_and_replay_polling_loop_exists() -> None:
    """The fix-and-replay branch must loop on Wait → Poll → Choice → Wait
    until the fixed file appears."""
    d = _load_definition()
    # Wait state goes to Poll
    assert d["States"]["WaitForFixedFile"]["Next"] == "PollForFixedFile"
    # Poll goes to Choice
    assert d["States"]["PollForFixedFile"]["Next"] == "FixedFilePresentChoice"
    # Choice's Default goes back to Wait (the polling loop)
    choice = d["States"]["FixedFilePresentChoice"]
    assert choice["Default"] == "WaitForFixedFile"
    # The true branch must replay the FIXED file (action=replay_fixed),
    # NOT MoveBackToRaw — moving the original bytes back to raw/ would
    # just re-quarantine them.
    true_branch = next(c for c in choice["Choices"] if c.get("BooleanEquals") is True)
    assert true_branch["Next"] == "ReplayFixedFile"
    replay_fixed = d["States"]["ReplayFixedFile"]
    assert replay_fixed["Parameters"]["Payload"]["action"] == "replay_fixed"
    assert replay_fixed["Next"] == "TriggerAirflowDAG"


def test_fix_path_notification_quotes_exact_upload_key() -> None:
    """The operator notification must quote the exact key poll_for_fix
    checks (computed by the helper, surfaced via $.poll.Payload.key) —
    a hand-formatted path drifted from the poller once already."""
    d = _load_definition()
    # The branch first computes the fix path via the helper...
    router = d["States"]["RouteOnDecision"]
    fix_choice = next(c for c in router["Choices"] if c["StringEquals"] == "fix-and-replay")
    assert fix_choice["Next"] == "ComputeFixPath"
    compute = d["States"]["ComputeFixPath"]
    assert compute["Parameters"]["Payload"]["action"] == "poll_for_fix"
    assert compute["Next"] == "NotifyFixPath"
    # ...then the SNS message interpolates that computed key.
    notify = d["States"]["NotifyFixPath"]
    assert "$.poll.Payload.key" in notify["Parameters"]["Message.$"]


def test_replay_path_triggers_airflow() -> None:
    """The replay path must call trigger_airflow after moving the file."""
    d = _load_definition()
    assert d["States"]["MoveBackToRaw"]["Next"] == "TriggerAirflowDAG"


def test_all_terminal_states_publish_to_sns() -> None:
    """Every terminal end-state in the success / failure / timeout paths
    publishes to SNS so the operator sees the outcome."""
    d = _load_definition()
    terminal_states = [name for name, s in d["States"].items() if s.get("End") is True]
    # Should include ReplaySuccess, ReplayTriggerFailed, DiscardLogged,
    # OperatorTimedOut, OperatorDecisionInvalid
    assert {
        "ReplaySuccess",
        "ReplayTriggerFailed",
        "DiscardLogged",
        "OperatorTimedOut",
        "OperatorDecisionInvalid",
    } <= set(terminal_states)
    for name in terminal_states:
        state = d["States"][name]
        assert (
            state["Resource"] == "arn:aws:states:::sns:publish"
        ), f"Terminal state {name} should publish to SNS"


def test_top_level_timeout_set() -> None:
    """Defensive: the whole state machine has an overall timeout
    matching the operator-wait window (7 days)."""
    d = _load_definition()
    assert d["TimeoutSeconds"] == 604800


def test_no_orphan_states() -> None:
    """Every non-start state should be reachable from some Next/Choice/Catch.
    Catches dead-code states that don't get linked in."""
    d = _load_definition()
    state_names = set(d["States"].keys())
    reachable = {d["StartAt"]}
    # BFS over Next / Choices / Catch
    frontier = [d["StartAt"]]
    while frontier:
        name = frontier.pop()
        state = d["States"][name]
        next_states: list[str] = []
        if "Next" in state:
            next_states.append(state["Next"])
        if "Choices" in state:
            next_states.extend(c["Next"] for c in state["Choices"])
            if "Default" in state:
                next_states.append(state["Default"])
        if "Catch" in state:
            next_states.extend(c["Next"] for c in state["Catch"])
        for n in next_states:
            if n not in reachable:
                reachable.add(n)
                frontier.append(n)
    orphans = state_names - reachable
    assert not orphans, f"unreachable states: {orphans}"
