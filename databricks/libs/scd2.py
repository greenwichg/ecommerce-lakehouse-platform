"""Slowly Changing Dimension Type 2 (SCD2) merge primitive.

The pattern materialises a *version history* table from a stream of
"current state" snapshots. Each natural-key value gets one row per change
to any tracked attribute, with ``effective_from`` / ``effective_to``
bracketing the version's validity window. Exactly one row per natural
key has ``is_current = true``.

Surrogate-key strategy: ``sk_col = sha256(natural_key | '|' | updated_at)``.

The SHA-256 string surrogate is chosen over a BIGINT IDENTITY:
- Deterministic across runs and across clusters: re-running yields the
  same surrogate, so downstream caching and upstream point-in-time joins
  stay stable.
- No central sequence to coordinate when silver and gold run on different
  clusters or different sessions.
- VARCHAR(64) vs INT8: 8x the bytes on disk, but Delta's columnar storage
  with dictionary encoding compresses the sk column down to almost the
  same footprint. Join performance is O(1) either way once the dim is
  dictionary-encoded. At our data volume (10k customers, low-thousands
  of SCD2 versions over the project lifetime), the trade-off lands
  squarely on the deterministic side.

MERGE strategy: stage a synthetic source DF that contains, per change:
- For new customers (not yet in the dim): one row with ``__merge_key = NULL``
  (no existing current to close; goes through WHEN NOT MATCHED -> INSERT).
- For changed customers: two rows — one with ``__merge_key = NULL`` for
  the INSERT branch, one with ``__merge_key = natural_key`` for the
  WHEN MATCHED branch that closes the existing current row.
- For unchanged customers: zero rows (no-op).

This is the canonical Delta SCD2 pattern; it lets a single MERGE statement
do both halves of the version transition.
"""

from __future__ import annotations

from collections.abc import Sequence

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

EFFECTIVE_FROM = "effective_from"
EFFECTIVE_TO = "effective_to"
IS_CURRENT = "is_current"
# Far-future sentinel for open-ended versions. Picking the maximum
# Snowflake-friendly TIMESTAMP_TZ that Spark also accepts.
_FAR_FUTURE_SQL = "CAST('9999-12-31 23:59:59' AS TIMESTAMP)"


def compute_surrogate(
    df: DataFrame, natural_key: str, ts_col: str, sk_col: str
) -> DataFrame:
    """Attach the SCD2 surrogate key to ``df``.

    sk = sha256(natural_key || '|' || updated_at_iso). This couples the SK
    uniquely to the (natural_key, version) pair, so two source records
    that differ only in unrelated attributes (which we don't end up
    versioning) still hash the same and won't accidentally create
    spurious surrogate churn.
    """
    return df.withColumn(
        sk_col,
        F.sha2(
            F.concat_ws("|", F.col(natural_key), F.col(ts_col).cast("string")),
            256,
        ),
    )


def _scd2_schema(
    source_with_sk: DataFrame,
    natural_key: str,
    sk_col: str,
    attribute_cols: Sequence[str],
) -> StructType:
    """Build the target schema = sk + natural_key + attrs + SCD2 metadata."""
    src_types = {f.name: f.dataType for f in source_with_sk.schema.fields}
    fields = [
        StructField(sk_col, src_types.get(sk_col, StringType()), nullable=False),
        StructField(natural_key, src_types[natural_key], nullable=False),
    ]
    for col in attribute_cols:
        fields.append(StructField(col, src_types[col], nullable=True))
    fields.extend(
        [
            StructField(EFFECTIVE_FROM, TimestampType(), nullable=False),
            StructField(EFFECTIVE_TO, TimestampType(), nullable=False),
            StructField(IS_CURRENT, BooleanType(), nullable=False),
        ]
    )
    return StructType(fields)


def ensure_scd2_table(
    spark: SparkSession,
    path: str,
    source_with_sk: DataFrame,
    natural_key: str,
    sk_col: str,
    attribute_cols: Sequence[str],
) -> None:
    """Idempotent create of an empty SCD2 Delta table at ``path``."""
    if DeltaTable.isDeltaTable(spark, path):
        return
    schema = _scd2_schema(source_with_sk, natural_key, sk_col, attribute_cols)
    spark.createDataFrame([], schema).write.format("delta").save(path)


def _stage_for_merge(
    source_with_sk: DataFrame,
    target_current: DataFrame,
    natural_key: str,
    tracked_cols: Sequence[str],
) -> DataFrame:
    """Build the staged source for the SCD2 MERGE.

    For each source row, emit:
      - 1 INSERT row (__merge_key = NULL) if NEW
      - 2 rows (INSERT + CLOSE) if CHANGED
      - 0 rows if UNCHANGED
    """
    s = source_with_sk.alias("s")
    t = target_current.alias("t")

    # LEFT join source onto current target on natural_key
    joined = s.join(t, F.col(f"s.{natural_key}") == F.col(f"t.{natural_key}"), "left")

    # Classify: new (no match) vs changed (match but tracked_cols differ) vs
    # unchanged (match and all tracked_cols equal under null-safe equality).
    # Use the eqNullSafe (<=>) operator so NULLs compare equal — otherwise a
    # row where (s.email IS NULL, t.email IS NULL) would trigger a spurious
    # version churn.
    null_safe_equal = " AND ".join(f"s.{c} <=> t.{c}" for c in tracked_cols)
    is_new = F.col(f"t.{natural_key}").isNull()
    is_changed = F.col(f"t.{natural_key}").isNotNull() & ~F.expr(null_safe_equal)

    # Source-only columns we need to carry to the INSERT side. Drop join's t.* duplicates.
    source_cols = [F.col(f"s.{c}") for c in source_with_sk.columns]

    inserts_new = joined.where(is_new).select(*source_cols).withColumn(
        "__merge_key", F.lit(None).cast(StringType())
    )
    inserts_changed = joined.where(is_changed).select(*source_cols).withColumn(
        "__merge_key", F.lit(None).cast(StringType())
    )
    closes = joined.where(is_changed).select(*source_cols).withColumn(
        "__merge_key", F.col(natural_key)
    )

    return inserts_new.unionByName(inserts_changed).unionByName(closes)


def apply_scd2_merge(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    natural_key: str,
    attribute_cols: Sequence[str],
    tracked_cols: Sequence[str],
    sk_col: str,
    ts_col: str = "updated_at",
) -> None:
    """Apply an SCD2 MERGE to a Delta dim table.

    Args:
        spark: SparkSession.
        source_df: Current-state silver snapshot, one row per ``natural_key``.
        target_path: Delta path for the SCD2 dim.
        natural_key: Source-of-truth natural key column (e.g. ``customer_id``).
        attribute_cols: All non-key, non-metadata columns that get *stored*
            in the dim. Must include ``tracked_cols`` plus any "carried
            but not versioned" descriptors (e.g. ``signup_date``).
        tracked_cols: Subset of ``attribute_cols`` that, when changed,
            trigger a new SCD2 version. Other ``attribute_cols`` changes
            are silently absorbed into the current version.
        sk_col: Name for the surrogate-key column. Computed inside.
        ts_col: Source's change-time column; becomes ``effective_from``.
    """
    source_with_sk = compute_surrogate(source_df, natural_key, ts_col, sk_col)
    ensure_scd2_table(spark, target_path, source_with_sk, natural_key, sk_col, attribute_cols)

    target = DeltaTable.forPath(spark, target_path)
    target_current = target.toDF().filter(F.col(IS_CURRENT))
    staged = _stage_for_merge(source_with_sk, target_current, natural_key, tracked_cols)

    merge_pred = f"t.{natural_key} = s.__merge_key AND t.{IS_CURRENT} = TRUE"

    insert_values: dict[str, str] = {
        sk_col: f"s.{sk_col}",
        natural_key: f"s.{natural_key}",
    }
    for col in attribute_cols:
        insert_values[col] = f"s.{col}"
    insert_values[EFFECTIVE_FROM] = f"s.{ts_col}"
    insert_values[EFFECTIVE_TO] = _FAR_FUTURE_SQL
    insert_values[IS_CURRENT] = "TRUE"

    (
        target.alias("t")
        .merge(staged.alias("s"), merge_pred)
        .whenMatchedUpdate(
            set={
                EFFECTIVE_TO: f"s.{ts_col}",
                IS_CURRENT: "FALSE",
            }
        )
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )
