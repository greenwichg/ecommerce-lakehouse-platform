"""Callbacks shared across all DAGs.

Centralises the on_failure_callback that publishes an SNS alert with the
task context and the URL to the failed task's log. Keeping this in one
module means every DAG behaves the same way under failure and the alert
format only needs to be edited in one place.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from airflow.providers.amazon.aws.hooks.sns import SnsHook

log = logging.getLogger(__name__)


def _format_alert(context: dict[str, Any]) -> str:
    """Render the failure context as a human-readable JSON payload.

    SNS messages should be short and self-contained — operators reading the
    alert on a phone need the task id, run id, and a clickable log URL
    without scrolling. Anything richer goes in the DAG run page.
    """
    task_instance = context.get("task_instance")
    dag = context.get("dag")
    exception = context.get("exception")

    payload = {
        "dag_id": getattr(dag, "dag_id", "unknown"),
        "task_id": getattr(task_instance, "task_id", "unknown"),
        "run_id": getattr(task_instance, "run_id", "unknown"),
        "try_number": getattr(task_instance, "try_number", -1),
        "execution_date": str(context.get("logical_date") or context.get("execution_date") or ""),
        "log_url": getattr(task_instance, "log_url", ""),
        "exception": str(exception) if exception else "",
    }
    return json.dumps(payload, indent=2, default=str)


def sns_failure_callback(context: dict[str, Any]) -> None:
    """Publish a failure alert to the configured SNS topic.

    Reads the topic ARN from the Airflow Variable ``sns_alert_topic_arn``
    so it can be swapped per-environment without code changes. The hook
    uses ``aws_default`` connection which Slice 5 wires to the
    SecretsManagerBackend.
    """
    from airflow.models import Variable  # imported lazily so DAG file load is cheap

    try:
        topic_arn = Variable.get("sns_alert_topic_arn", default_var=None)
    except Exception:  # noqa: BLE001 — variable lookup must not itself raise
        log.exception("Failed to look up sns_alert_topic_arn variable")
        return

    if not topic_arn:
        log.warning("sns_alert_topic_arn not set; skipping failure SNS publish")
        return

    message = _format_alert(context)
    subject = f"[Airflow] {context.get('dag').dag_id} task failed"[:100]  # SNS limit
    try:
        SnsHook(aws_conn_id="aws_default").publish_to_target(
            target_arn=topic_arn,
            message=message,
            subject=subject,
        )
        log.info("Published failure alert to %s", topic_arn)
    except Exception:  # noqa: BLE001 — alerting must never re-fail the task
        log.exception("SNS publish failed; falling through")
