"""Data-quality gates used by the daily batch pipeline.

Two gates today:

- ``check_row_count_drop``: today's count vs N-day baseline via Snowflake
  Time Travel; fails the DAG if drop > threshold.
- ``check_orphan_surrogate_rate``: % of fact rows with any NULL surrogate
  key after the SCD2 PIT join; fails the DAG if rate > threshold. A spike
  indicates the upstream source produced an order referencing a customer
  or product that never landed in the dim source.

Both gates read their thresholds from ``config/base.yaml`` via
``libs.config.load_config`` (Slice 2 promotion from Slice 1's hardcoded
constants). The function defaults are kept for ease of unit-testing without
loading the config layer.
"""

from __future__ import annotations

import logging
from typing import Any

from airflow.exceptions import AirflowFailException
from airflow.providers.common.sql.hooks.sql import DbApiHook

log = logging.getLogger(__name__)


def _load_dq_thresholds() -> dict[str, float | int]:
    """Read DQ thresholds from layered config.

    Imported lazily inside the function so the module loads cleanly even
    when the libs/ path isn't on sys.path yet (e.g. during the DAG's first
    import in unit tests). Falls back to Slice 1 defaults if config can't
    be loaded — that way a misconfigured environment never silently
    disables the gates.
    """
    try:
        from libs.config import get_path, load_config

        cfg = load_config(include_local=False)
        return {
            "row_count_drop_pct": float(
                get_path(cfg, "data_quality.row_count_drop_threshold_pct", default=20.0)
            ),
            "row_count_baseline_days": int(
                get_path(cfg, "data_quality.row_count_baseline_days", default=7)
            ),
            "orphan_surrogate_pct": float(
                get_path(cfg, "data_quality.orphan_surrogate_rate_pct", default=1.0)
            ),
        }
    except Exception:  # noqa: BLE001 — never let config issues silently widen the gate
        log.exception("DQ threshold config load failed; using Slice 1 defaults")
        return {
            "row_count_drop_pct": 20.0,
            "row_count_baseline_days": 7,
            "orphan_surrogate_pct": 1.0,
        }


def _row_count_query(table_fqn: str, baseline_days: int) -> str:
    """SQL: returns (today_count, baseline_count) as two columns.

    Uses Snowflake Time Travel to look back ``baseline_days`` days. The
    AT (OFFSET ...) syntax queries the table state at that point in
    history.

    On Snowflake free tier / Time Travel retention limits, OFFSET beyond
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
    threshold_pct: float | None = None,
    baseline_days: int | None = None,
) -> dict[str, Any]:
    """Fail the DAG if today's row count dropped > threshold vs baseline.

    Thresholds come from ``config/base.yaml`` unless overridden explicitly
    (the kwarg defaults are ``None`` so tests can inject without loading
    config).
    """
    from airflow.hooks.base import BaseHook

    thresholds = _load_dq_thresholds()
    if threshold_pct is None:
        threshold_pct = thresholds["row_count_drop_pct"]
    if baseline_days is None:
        baseline_days = int(thresholds["row_count_baseline_days"])

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


def _orphan_rate_query(table_fqn: str, sk_cols: list[str]) -> str:
    """SQL: returns (total_rows, orphan_rows) for a fact table.

    A row is an orphan if ANY of the named surrogate-key columns is NULL.
    """
    null_predicate = " OR ".join(f"{c} IS NULL" for c in sk_cols)
    return f"""
        SELECT
            COUNT(*) AS total_rows,
            SUM(CASE WHEN {null_predicate} THEN 1 ELSE 0 END) AS orphan_rows
        FROM {table_fqn}
    """


def check_orphan_surrogate_rate(
    table_fqn: str,
    sk_cols: list[str],
    conn_id: str = "snowflake_default",
    threshold_pct: float | None = None,
) -> dict[str, Any]:
    """Fail the DAG if the share of fact rows with NULL surrogates exceeds the threshold.

    A non-zero orphan rate means the SCD2 PIT join in gold didn't find a
    matching dim version. Causes worth investigating:
      - dim load lagging behind fact load (race condition)
      - upstream emitted a customer_id / product_id that was never sent
        to the dim source generator (referential integrity break)
      - the dim source skipped a customer that the fact references
    """
    from airflow.hooks.base import BaseHook

    if threshold_pct is None:
        threshold_pct = _load_dq_thresholds()["orphan_surrogate_pct"]

    hook: DbApiHook = BaseHook.get_hook(conn_id)
    sql = _orphan_rate_query(table_fqn, sk_cols)
    log.info("Orphan-surrogate check on %s: %s", table_fqn, sql)
    row = hook.get_first(sql)
    total, orphan = int(row[0] or 0), int(row[1] or 0)

    if total == 0:
        log.info("Table is empty; skipping orphan check")
        return {"table": table_fqn, "total": 0, "orphan": 0, "orphan_pct": 0.0}

    orphan_pct = orphan / total * 100.0
    payload = {
        "table": table_fqn,
        "total": total,
        "orphan": orphan,
        "orphan_pct": round(orphan_pct, 4),
        "sk_cols": sk_cols,
    }
    log.info("Orphan check result: %s", payload)

    if orphan_pct > threshold_pct:
        raise AirflowFailException(
            f"Orphan surrogate rate {orphan_pct:.2f}% exceeds threshold {threshold_pct}% "
            f"for {table_fqn} ({orphan}/{total} rows have NULL {sk_cols})"
        )
    return payload
