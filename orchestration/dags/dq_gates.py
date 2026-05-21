"""Data-quality gates used by the daily batch pipeline.

The headline rule (per spec):

    Fail the DAG if today's row count for a fact is more than 20% lower
    than the 7-day rolling average.

Implemented as a function that runs a SQL query against Snowflake (or any
common-SQL connection) and raises ``AirflowFailException`` on violation —
no retries, since "row count too low" is not a transient error.

Hardcoded threshold (20%) for Slice 1; promoted to config in Slice 2 per
the slice plan.
"""

from __future__ import annotations

import logging
from typing import Any

from airflow.exceptions import AirflowFailException
from airflow.providers.common.sql.hooks.sql import DbApiHook

log = logging.getLogger(__name__)

# Hardcoded for Slice 1; moves to config in Slice 2.
_DROP_THRESHOLD_PCT = 20.0
_BASELINE_DAYS = 7


def _row_count_query(table_fqn: str, baseline_days: int) -> str:
    """SQL: returns (today_count, baseline_avg_count) as two columns.

    Uses Snowflake Time Travel to look back ``baseline_days`` days. The
    AT (OFFSET ...) syntax queries the table state at that point in
    history; we then aggregate the count.

    In Snowflake free tier / Time Travel retention limits, OFFSET beyond
    7 days requires Enterprise. The 7-day default is intentional.
    """
    seconds = baseline_days * 86400
    return f"""
        SELECT
            (SELECT COUNT(*) FROM {table_fqn}) AS today_count,
            (SELECT COUNT(*) FROM {table_fqn} AT (OFFSET => -{seconds})) AS baseline_count
    """


def check_row_count_drop(
    table_fqn: str,
    conn_id: str = "snowflake_default",
    threshold_pct: float = _DROP_THRESHOLD_PCT,
    baseline_days: int = _BASELINE_DAYS,
) -> dict[str, Any]:
    """Run the row-count drop check.

    Returns counts as a dict (also goes to XCom) on success; raises
    ``AirflowFailException`` if today's count is below the threshold.
    Treats baseline_count == 0 (no historical data yet) as a pass.
    """
    from airflow.hooks.base import BaseHook

    hook: DbApiHook = BaseHook.get_hook(conn_id)
    sql = _row_count_query(table_fqn, baseline_days)
    log.info("Row count check on %s: %s", table_fqn, sql)
    row = hook.get_first(sql)
    today, baseline = int(row[0] or 0), int(row[1] or 0)

    if baseline == 0:
        log.info("Baseline count is 0 (no history); skipping threshold check")
        return {"table": table_fqn, "today": today, "baseline": baseline, "drop_pct": 0.0}

    drop_pct = max(0.0, (baseline - today) / baseline * 100.0)
    payload = {
        "table": table_fqn,
        "today": today,
        "baseline": baseline,
        "drop_pct": round(drop_pct, 2),
    }
    log.info("Row count result: %s", payload)

    if drop_pct > threshold_pct:
        raise AirflowFailException(
            f"Row-count drop {drop_pct:.1f}% exceeds threshold {threshold_pct}% "
            f"for {table_fqn} (today={today}, baseline={baseline})"
        )
    return payload
