"""Tests for libs.bronze.

Static-DataFrame tests for the lineage column attachment; an integration-style
round-trip through Delta verifies write_delta_batch.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest
from chispa.dataframe_comparer import assert_df_equality
from libs.bronze import (
    METADATA_BATCH_ID,
    METADATA_INGESTION_TIMESTAMP,
    METADATA_SOURCE_FILE,
    add_bronze_metadata,
    autoloader_options,
    read_raw_parquet_static,
    write_delta_batch,
)
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType


def test_add_bronze_metadata_adds_three_columns(spark: SparkSession) -> None:
    df = spark.createDataFrame([("o1", "placed"), ("o2", "paid")], ["order_id", "status"])
    out = add_bronze_metadata(
        df,
        batch_id="20250520T143022Z-deadbeef",
        source_file_col=None,
        ingestion_timestamp=dt.datetime(2025, 5, 20, 14, 30, 22, tzinfo=dt.UTC),
    )
    assert METADATA_INGESTION_TIMESTAMP in out.columns
    assert METADATA_SOURCE_FILE in out.columns
    assert METADATA_BATCH_ID in out.columns


def test_add_bronze_metadata_preserves_original_columns(spark: SparkSession) -> None:
    df = spark.createDataFrame([("o1", 5)], ["order_id", "qty"])
    out = add_bronze_metadata(df, batch_id="b1", source_file_col=None)
    # Original cols still present and in original order at the front
    assert out.columns[:2] == ["order_id", "qty"]


def test_batch_id_is_literal(spark: SparkSession) -> None:
    df = spark.createDataFrame([("o1",), ("o2",)], ["order_id"])
    out = add_bronze_metadata(df, batch_id="b-123", source_file_col=None)
    batch_ids = {row[METADATA_BATCH_ID] for row in out.collect()}
    assert batch_ids == {"b-123"}


def test_explicit_ingestion_timestamp_is_used(spark: SparkSession) -> None:
    when = dt.datetime(2025, 5, 20, 14, 30, 22, tzinfo=dt.UTC)
    df = spark.createDataFrame([("o1",)], ["order_id"])
    out = add_bronze_metadata(df, batch_id="b", source_file_col=None, ingestion_timestamp=when)
    row = out.first()
    # Spark may strip tz on cast; compare wall-clock components
    assert row[METADATA_INGESTION_TIMESTAMP].replace(tzinfo=None) == when.replace(tzinfo=None)


def test_source_file_col_can_be_null(spark: SparkSession) -> None:
    df = spark.createDataFrame([("o1",)], ["order_id"])
    out = add_bronze_metadata(df, batch_id="b", source_file_col=None)
    assert out.first()[METADATA_SOURCE_FILE] is None


def test_source_file_col_reads_from_named_column(spark: SparkSession) -> None:
    """When the source DF already has a file path column (e.g., from a static
    read), we can attach that as the lineage source_file."""
    schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("file_path", StringType(), True),
        ]
    )
    df = spark.createDataFrame([("o1", "/data/raw/orders/y=2025/m=05/d=01/orders.parquet")], schema)
    out = add_bronze_metadata(df, batch_id="b", source_file_col="file_path")
    assert out.first()[METADATA_SOURCE_FILE].endswith("orders.parquet")


def test_autoloader_options_canonical_set() -> None:
    opts = autoloader_options(schema_location="s3://b/_schemas/orders")
    assert opts["cloudFiles.format"] == "parquet"
    assert opts["cloudFiles.schemaEvolutionMode"] == "addNewColumns"
    assert opts["cloudFiles.schemaLocation"] == "s3://b/_schemas/orders"
    assert opts["cloudFiles.inferColumnTypes"] == "true"
    assert opts["cloudFiles.includeExistingFiles"] == "true"


def test_autoloader_options_format_override() -> None:
    opts = autoloader_options(schema_location="s3://b/_schemas/cs", file_format="json")
    assert opts["cloudFiles.format"] == "json"


def test_round_trip_to_delta(spark: SparkSession, tmp_path: Path) -> None:
    """Static write/read round trip: write a small DF, read it back, compare."""
    df = spark.createDataFrame([("o1", "placed"), ("o2", "paid")], ["order_id", "status"])
    bronze_path = str(tmp_path / "bronze" / "orders")
    write_delta_batch(df, bronze_path)
    back = spark.read.format("delta").load(bronze_path)
    assert_df_equality(back, df, ignore_row_order=True)


def test_round_trip_supports_merge_schema(spark: SparkSession, tmp_path: Path) -> None:
    """Appending with a new column merges into the existing schema."""
    bronze_path = str(tmp_path / "bronze" / "orders")
    df1 = spark.createDataFrame([("o1", "placed")], ["order_id", "status"])
    write_delta_batch(df1, bronze_path)
    df2 = spark.createDataFrame([("o2", "paid", 9.99)], ["order_id", "status", "price"])
    write_delta_batch(df2, bronze_path, merge_schema=True)
    back = spark.read.format("delta").load(bronze_path)
    assert "price" in back.columns


def test_read_raw_parquet_recursive(spark: SparkSession, tmp_path: Path) -> None:
    """Verify recursive Parquet reading across Hive-style partition dirs."""
    raw = tmp_path / "raw" / "orders"
    p1 = raw / "year=2025" / "month=05" / "day=01"
    p2 = raw / "year=2025" / "month=05" / "day=02"
    p1.mkdir(parents=True)
    p2.mkdir(parents=True)
    spark.createDataFrame([("o1", "placed")], ["order_id", "status"]).write.parquet(
        str(p1 / "orders.parquet")
    )
    spark.createDataFrame([("o2", "paid")], ["order_id", "status"]).write.parquet(
        str(p2 / "orders.parquet")
    )
    out = read_raw_parquet_static(spark, str(raw))
    ids = {row["order_id"] for row in out.select("order_id").collect()}
    assert ids == {"o1", "o2"}


@pytest.mark.parametrize("batch_id", ["20250520T143022Z-deadbeef", "any-string-works"])
def test_batch_id_passthrough(spark: SparkSession, batch_id: str) -> None:
    df = spark.createDataFrame([("o1",)], ["order_id"])
    out = add_bronze_metadata(df, batch_id=batch_id, source_file_col=None)
    assert out.first()[METADATA_BATCH_ID] == batch_id
