"""Bronze layer transformations.

The bronze layer is append-only and intentionally minimal: read raw files
via Auto Loader (when running in Databricks) or plain Parquet (when running
in tests / replays), attach lineage metadata, and land to Delta with
``mergeSchema=true`` so new source columns are picked up automatically.

These functions are pure DataFrame transformations — no I/O — so they can
be unit-tested against a local SparkSession. The streaming options are
encoded as a dict for documentation / testability rather than as a hidden
side effect inside a notebook.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType

METADATA_INGESTION_TIMESTAMP = "_ingestion_timestamp"
METADATA_SOURCE_FILE = "_source_file"
METADATA_BATCH_ID = "_batch_id"
BRONZE_METADATA_COLS = (
    METADATA_INGESTION_TIMESTAMP,
    METADATA_SOURCE_FILE,
    METADATA_BATCH_ID,
)


def add_bronze_metadata(
    df: DataFrame,
    batch_id: str,
    source_file_col: str | None = "_metadata.file_path",
    ingestion_timestamp: dt.datetime | None = None,
) -> DataFrame:
    """Attach the three Bronze lineage columns.

    Args:
        df: Source DataFrame. May be streaming or static.
        batch_id: Lineage identifier (see ``libs.batch.make_batch_id``).
        source_file_col: Column expression yielding the source file path.
            Defaults to Auto Loader's ``_metadata.file_path``. Pass ``None``
            to set a literal NULL (useful in tests where the input has no
            ``_metadata`` column).
        ingestion_timestamp: Wall-clock UTC time for
            ``_ingestion_timestamp``. Defaults to ``current_timestamp()``
            (preferred in streaming).

    Returns:
        DataFrame with ``_ingestion_timestamp``, ``_source_file``, and
        ``_batch_id`` appended.
    """
    source_expr = (
        F.col(source_file_col)
        if source_file_col is not None
        else F.lit(None).cast(StringType())
    )
    ts_expr: Any
    if ingestion_timestamp is None:
        ts_expr = F.current_timestamp()
    else:
        # Cast a literal string so the test gets a deterministic value
        # rather than wall-clock.
        ts_expr = F.lit(ingestion_timestamp.isoformat()).cast(TimestampType())

    return df.select(
        "*",
        ts_expr.alias(METADATA_INGESTION_TIMESTAMP),
        source_expr.alias(METADATA_SOURCE_FILE),
        F.lit(batch_id).alias(METADATA_BATCH_ID),
    )


def autoloader_options(schema_location: str, file_format: str = "parquet") -> dict[str, str]:
    """The canonical cloudFiles option set for Bronze ingestion.

    Returned as a plain dict so it can be passed to ``.options(**opts)`` and
    also asserted-on in tests / docs without launching Spark.

    - ``schemaEvolutionMode=addNewColumns``: new source columns are added to
      the bronze schema automatically; column removal still requires manual
      intervention.
    - ``inferColumnTypes=true``: read types from data on first encounter
      and persist to ``schemaLocation``; subsequent runs reuse the persisted
      schema unless evolution kicks in.
    - ``includeExistingFiles=true``: on first run, backfill from any files
      already in the source directory (rather than starting from "now").
    """
    return {
        "cloudFiles.format": file_format,
        "cloudFiles.schemaLocation": schema_location,
        "cloudFiles.schemaEvolutionMode": "addNewColumns",
        "cloudFiles.inferColumnTypes": "true",
        "cloudFiles.includeExistingFiles": "true",
    }


def read_raw_parquet_static(spark: SparkSession, source_path: str) -> DataFrame:
    """Read raw Parquet files recursively. Used for tests and replay paths
    where we don't have a live Auto Loader stream."""
    return spark.read.option("recursiveFileLookup", "true").parquet(source_path)


def write_delta_batch(
    df: DataFrame,
    path: str,
    mode: str = "append",
    merge_schema: bool = True,
    partition_by: list[str] | None = None,
) -> None:
    """Batch write to a Delta path with optional schema merge.

    Streaming writes are configured at the notebook level so this stays a
    plain transform-friendly helper.
    """
    writer = (
        df.write.format("delta")
        .mode(mode)
        .option("mergeSchema", "true" if merge_schema else "false")
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)
