"""Clickstream sessionization + fact_sessions builder.

Two transforms live here:

1. ``sessionize_events`` runs the gap-and-island window function over
   raw events to recover session boundaries. The generator's ``session_id``
   is the upstream tracker / cookie identity; one cookie can host multiple
   sessions over time, separated by >30 minutes of inactivity. The
   ``silver_session_key`` is the post-sessionization key:

       silver_session_key = sha256(raw_session_id || '|' || session_seq)

   where ``session_seq`` is the cumulative count of "new session"
   boundaries (gap > threshold OR first event for the cookie).

2. ``build_fact_sessions`` aggregates sessionized events into one row
   per session: session_start, session_end, event_count, attributed
   customer_id (via ``max_by(customer_id, event_ts)``), conversion flag,
   first/last page URL, device.

Both are pure DataFrame transforms — Spark window functions on the input,
no I/O. Tested with chispa against a local SparkSession.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# Default gap threshold per spec (30 minutes of inactivity → new session).
DEFAULT_SESSION_GAP_MINUTES = 30


def sessionize_events(
    events: DataFrame,
    raw_session_id_col: str = "session_id",
    event_ts_col: str = "event_ts",
    gap_minutes: int = DEFAULT_SESSION_GAP_MINUTES,
) -> DataFrame:
    """Attach ``silver_session_key`` and ``session_seq`` to each event.

    Gap-and-island: partition by ``raw_session_id``, order by ``event_ts``,
    lag the timestamp, mark "new session" where the gap exceeds the
    threshold (or there's no prior event), cumulative-sum the boundary
    flag to assign an ascending sequence number per cookie.

    Returns ``events`` with two new columns:
      - ``session_seq`` (int): 1, 2, 3, ... per cookie
      - ``silver_session_key`` (string): SHA-256 hex of
        ``raw_session_id || '|' || session_seq``
    """
    window_per_cookie = Window.partitionBy(raw_session_id_col).orderBy(event_ts_col)

    return (
        events.withColumn("__prev_ts", F.lag(event_ts_col).over(window_per_cookie))
        .withColumn(
            "__gap_minutes",
            F.when(
                F.col("__prev_ts").isNotNull(),
                (F.col(event_ts_col).cast("long") - F.col("__prev_ts").cast("long")) / 60,
            ),
        )
        .withColumn(
            "__is_new_session",
            F.col("__prev_ts").isNull() | (F.col("__gap_minutes") > gap_minutes),
        )
        .withColumn(
            "session_seq",
            F.sum(F.col("__is_new_session").cast("int")).over(window_per_cookie),
        )
        .withColumn(
            "silver_session_key",
            F.sha2(
                F.concat_ws(
                    "|", F.col(raw_session_id_col), F.col("session_seq").cast("string")
                ),
                256,
            ),
        )
        .drop("__prev_ts", "__gap_minutes", "__is_new_session")
    )


def build_fact_sessions(sessionized_events: DataFrame) -> DataFrame:
    """Aggregate sessionized events into one row per session.

    Customer attribution: the most recent *non-null* ``customer_id``
    seen in the session. Implemented as
    ``max_by(customer_id, IF(customer_id IS NOT NULL, event_ts))`` —
    we null out the rank key when the value is null so ``max_by`` picks
    from only the rows where ``customer_id`` is set. (Plain
    ``max_by(customer_id, event_ts)`` would pick the *latest* row even
    if its ``customer_id`` is NULL, defeating the attribution; that's
    a subtle Spark semantic worth flagging.)

    Conversion flag: any event with ``event_type='purchase'``.

    The output is a current-state fact: one row per session_key, latest
    aggregates. The Silver-side MERGE into the fact_sessions Delta table
    handles late-arriving events by re-aggregating any session that
    grows.
    """
    return sessionized_events.groupBy("silver_session_key").agg(
        F.min("session_id").alias("raw_session_id"),
        F.min("event_ts").alias("session_start"),
        F.max("event_ts").alias("session_end"),
        F.count("*").alias("event_count"),
        F.count_distinct("event_type").alias("distinct_event_types"),
        # Attribute to the LATEST non-null customer_id. We rank-by-null
        # the rows where customer_id is itself null (max_by ignores NULL
        # ranks), which means a trailing anonymous event after a sign-in
        # doesn't blank out attribution.
        F.max_by(
            F.col("customer_id"),
            F.when(F.col("customer_id").isNotNull(), F.col("event_ts")),
        ).alias("attributed_customer_id"),
        F.max(F.when(F.col("event_type") == "purchase", F.lit(1)).otherwise(F.lit(0)))
        .cast("boolean")
        .alias("converted"),
        F.min_by(F.col("page_url"), F.col("event_ts")).alias("first_page_url"),
        F.max_by(F.col("page_url"), F.col("event_ts")).alias("last_page_url"),
        F.min_by(F.col("device"), F.col("event_ts")).alias("device"),
        # session_end doubles as the "watermark" column for the Silver
        # MERGE — a re-emitted session with more events has a strictly
        # later session_end than the prior version.
        F.max("event_ts").alias("updated_at"),
    )
