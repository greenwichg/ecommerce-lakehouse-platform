"""Silver layer transformations.

Silver materialises a *current state* view of each source: one row per
natural key, latest known values, with bad records quarantined and very
late records dropped (watermark).

Functions here are designed for testability:

- ``dedup_by_key`` is a pure DataFrame transform — window function plus filter.
- ``apply_watermark`` is a pure transform — computes a high-water mark and
  filters anything older than the configured window.
- ``merge_into_silver`` wraps Delta's MERGE; the only I/O.
- ``append_quarantine`` is a plain Delta append.

The MERGE uses a ``s.updated_at > t.updated_at`` predicate so out-of-order
arrivals can never overwrite newer state with older state.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Sequence

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def dedup_by_key(df: DataFrame, keys: Sequence[str], latest_ts_col: str) -> DataFrame:
    """Keep one row per ``keys`` combination: the one with the largest
    ``latest_ts_col``. Tie-break is non-deterministic but stable within a
    single SparkSession on the same input."""
    window = Window.partitionBy(*keys).orderBy(F.col(latest_ts_col).desc())
    return (
        df.withColumn("__row_num", F.row_number().over(window))
        .filter("__row_num = 1")
        .drop("__row_num")
    )


def apply_watermark(df: DataFrame, ts_col: str, window_days: int) -> DataFrame:
    """Drop rows whose ``ts_col`` is older than ``max(ts_col) - window_days``.

    Returns the DataFrame unchanged if it's empty (no high-water mark to
    compute).
    """
    if window_days <= 0:
        return df
    max_row = df.agg(F.max(ts_col).alias("__max_ts")).first()
    max_ts = max_row["__max_ts"] if max_row is not None else None
    if max_ts is None:
        return df
    cutoff = max_ts - dt.timedelta(days=window_days)
    return df.filter(F.col(ts_col) >= F.lit(cutoff))


def merge_into_silver(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    merge_keys: Sequence[str],
    timestamp_col: str = "updated_at",
) -> None:
    """MERGE ``source_df`` into the Delta table at ``target_path``.

    Semantics:

    - WHEN MATCHED AND ``s.{timestamp_col} > t.{timestamp_col}``:
      update every column from source. The predicate prevents older
      records (e.g., late-arriving "placed" events for an already-paid
      order) from clobbering newer state.
    - WHEN NOT MATCHED: insert.

    Caller is responsible for ensuring the target table exists with a
    schema compatible with ``source_df``.
    """
    target = DeltaTable.forPath(spark, target_path)
    on = " AND ".join(f"t.{k} = s.{k}" for k in merge_keys)
    update_cond = f"s.{timestamp_col} > t.{timestamp_col}"
    (
        target.alias("t")
        .merge(source_df.alias("s"), on)
        .whenMatchedUpdateAll(condition=update_cond)
        .whenNotMatchedInsertAll()
        .execute()
    )


def append_quarantine(df: DataFrame, path: str) -> None:
    """Append quarantine records to a Delta table.

    ``mergeSchema=true`` so adding a new DQ rule (and therefore a new
    possible value in ``_quarantine_reason``) doesn't fail the write.
    """
    (df.write.format("delta").mode("append").option("mergeSchema", "true").save(path))


def ensure_silver_table(spark: SparkSession, path: str, schema_df: DataFrame) -> None:
    """Create the silver Delta table at ``path`` if it doesn't exist.

    Uses ``schema_df.schema`` as the table schema. A no-op if the table
    already exists.
    """
    if DeltaTable.isDeltaTable(spark, path):
        return
    empty = spark.createDataFrame([], schema_df.schema)
    empty.write.format("delta").save(path)
