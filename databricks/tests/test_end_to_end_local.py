"""Local end-to-end: generators → Bronze → Silver → Gold on real files.

This is the runbook's "Running end-to-end locally" recipe made
executable: the Databricks notebook bodies are ported onto the exact
``libs`` functions they call, against a ``file://`` bucket with real
generated Parquet/CSV/JSONL and local Delta tables.

Scenario:

- A dim warm-up snapshot 14 days before day 1 (matching the orders
  generator's transition lookback) so the SCD2 dims hold the history
  the PIT joins need for transition/late-arrival orders.
- Two consecutive daily batches over all six sources, each with its own
  ``batch_id`` — exercising the Bronze append, Silver MERGE dedup,
  SCD2 versioning across days, PIT surrogate binding, sessionization
  across hourly files, the marts, and the factless fact.
- One hand-injected bad order row (NULL quantity) — exercising the
  quarantine split end-to-end.
- A re-run of the final day's Silver+Gold pass — exercising idempotency.

Then every cross-layer invariant the platform promises is asserted on
the final state. Runtime is dominated by Delta MERGE round-trips
(~2-4 min); marked ``integration`` accordingly.
"""

from __future__ import annotations

import datetime as dt

import pytest
from libs.bronze import add_bronze_metadata, write_delta_batch
from libs.gold import (
    build_fact_order_lifecycle,
    build_fact_orders,
    ensure_gold_table,
    merge_into_gold,
    orphan_surrogate_rate,
)
from libs.marts import build_customer_ltv, write_mart_overwrite
from libs.quality import (
    CLICKSTREAM_RULES,
    CURRENCY_RATES_RULES,
    CUSTOMERS_RULES,
    ORDERS_RULES,
    PRODUCTS_RULES,
    WISHLIST_RULES,
    split_quarantine,
)
from libs.scd2 import apply_scd2_merge
from libs.sessionize import build_fact_sessions, sessionize_events
from libs.silver import (
    append_quarantine,
    apply_watermark,
    dedup_by_key,
    ensure_silver_table,
    merge_into_silver,
)
from libs.wishlist import build_fact_customer_wishlist_product
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

pytestmark = [pytest.mark.spark, pytest.mark.integration]

_SEED = 42
_N_CUSTOMERS = 120
_N_PRODUCTS = 60
_DAY_1 = dt.date(2025, 5, 1)
_DAY_2 = dt.date(2025, 5, 2)
# Dim history must reach back as far as the oldest order the day-1 file
# can reference (the transition lookback), or PIT joins on entities that
# changed inside that window can't resolve.
_WARMUP_DAY = _DAY_1 - dt.timedelta(days=14)

_GEN_CFG = {
    "generators": {
        "orders": {
            "records_per_day": 30,
            "late_arrival_pct": 5.0,
            "same_day_update_pct": 5.0,
            "cancellation_pct": 10.0,
            "avg_items_per_order": 2.3,
            "price_min": 4.99,
            "price_max": 499.99,
            "transition_days_mean": {
                "placed_to_paid": 0.5,
                "paid_to_shipped": 1.5,
                "shipped_to_delivered": 3.5,
            },
        },
        "customers": {"daily_change_pct": 2.0},
        "products": {"daily_change_pct": 2.0, "price_min": 4.99, "price_max": 499.99},
        "random_seed": _SEED,
    },
    "paths": {
        "raw_prefix": "raw",
        "partitioning": "year={year:04d}/month={month:02d}/day={day:02d}",
    },
}


def _bronze(spark: SparkSession, df: DataFrame, root: str, source: str, batch_id: str) -> None:
    """The bronze notebook body: lineage columns + Delta append."""
    enriched = add_bronze_metadata(df, batch_id=batch_id, source_file_col=None)
    write_delta_batch(enriched, f"{root}/bronze/{source}")


def _bronze_batch(spark: SparkSession, root: str, source: str, batch_id: str) -> DataFrame:
    return (
        spark.read.format("delta")
        .load(f"{root}/bronze/{source}")
        .filter(F.col("_batch_id") == batch_id)
    )


def _silver_merge_dedup(
    spark: SparkSession,
    root: str,
    source: str,
    batch_id: str,
    rules: tuple,
    keys: list[str],
    ts_col: str,
    watermark: bool = True,
) -> None:
    """The shared silver notebook body: DQ split → watermark → dedup → MERGE."""
    batch_df = _bronze_batch(spark, root, source, batch_id)
    good, bad = split_quarantine(batch_df, rules)
    if bad.count() > 0:
        append_quarantine(bad, f"{root}/quarantine/{source}")
    if watermark:
        good = apply_watermark(good, ts_col, window_days=30)
    deduped = dedup_by_key(good, keys, ts_col)
    silver_path = f"{root}/silver/{source}"
    ensure_silver_table(spark, silver_path, deduped)
    merge_into_silver(spark, deduped, silver_path, merge_keys=keys, timestamp_col=ts_col)


def _generate_raw(bucket: str, day: dt.date) -> None:
    """One day's raw files for all six sources (clickstream: 3 hourly files)."""
    from generators import clickstream, currency_rates, customers, orders, products, wishlist

    orders.run(day, day, bucket, _GEN_CFG, _SEED, _N_CUSTOMERS, _N_PRODUCTS)
    customers.run(day, day, bucket, _GEN_CFG, _SEED, record_count=_N_CUSTOMERS)
    products.run(day, day, bucket, _GEN_CFG, _SEED, record_count=_N_PRODUCTS)
    currency_rates.run(day, day, bucket, _GEN_CFG, _SEED, use_api=False)
    wishlist.run(
        day,
        day,
        bucket,
        _GEN_CFG,
        _SEED,
        events_per_day=40,
        customer_pool_size=_N_CUSTOMERS,
        product_pool_size=_N_PRODUCTS,
    )
    noon = dt.datetime.combine(day, dt.time(12, 0), dt.UTC)
    clickstream.run(
        noon,
        noon + dt.timedelta(hours=2),
        bucket,
        _GEN_CFG,
        _SEED,
        session_pool_size=2500,
        customer_pool_size=_N_CUSTOMERS,
    )


def _day_partition(bucket: str, source: str, day: dt.date) -> str:
    return f"{bucket}/raw/{source}/year={day.year:04d}/month={day.month:02d}/day={day.day:02d}"


def _ingest_dims(spark: SparkSession, bucket: str, root: str, day: dt.date, batch_id: str) -> None:
    """Bronze + Silver + Gold-SCD2 for customers and products only.

    Dimension snapshots are NOT watermarked (see silver_customers.py): a
    snapshot row's updated_at is the entity's last change, so a watermark
    would silently drop every long-stable entity from the dims.
    """
    cust = spark.read.parquet(f"{_day_partition(bucket, 'customers', day)}/customers.parquet")
    _bronze(spark, cust, root, "customers", batch_id)
    _silver_merge_dedup(
        spark,
        root,
        "customers",
        batch_id,
        CUSTOMERS_RULES,
        ["customer_id"],
        "updated_at",
        watermark=False,
    )

    prod = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{_day_partition(bucket, 'products', day)}/products.csv")
        .withColumn("updated_at", F.col("updated_at").cast("timestamp"))
        .withColumn("price", F.col("price").cast("double"))
    )
    _bronze(spark, prod, root, "products", batch_id)
    _silver_merge_dedup(
        spark,
        root,
        "products",
        batch_id,
        PRODUCTS_RULES,
        ["product_id"],
        "updated_at",
        watermark=False,
    )

    apply_scd2_merge(
        spark=spark,
        source_df=spark.read.format("delta").load(f"{root}/silver/customers"),
        target_path=f"{root}/gold/dim_customer",
        natural_key="customer_id",
        attribute_cols=["name", "email", "address", "signup_date"],
        tracked_cols=["email", "address"],
        sk_col="customer_sk",
        ts_col="updated_at",
    )
    apply_scd2_merge(
        spark=spark,
        source_df=spark.read.format("delta").load(f"{root}/silver/products"),
        target_path=f"{root}/gold/dim_product",
        natural_key="product_id",
        attribute_cols=["product_name", "category", "price", "sku"],
        tracked_cols=["price", "category"],
        sk_col="product_sk",
        ts_col="updated_at",
    )


def _ingest_facts(spark: SparkSession, bucket: str, root: str, day: dt.date, batch_id: str) -> None:
    """Bronze + Silver for the four event sources, then the Gold builds."""
    # --- orders (Parquet) ---
    raw_orders = spark.read.parquet(f"{_day_partition(bucket, 'orders', day)}/orders.parquet")
    _bronze(spark, raw_orders, root, "orders", batch_id)
    _silver_merge_dedup(spark, root, "orders", batch_id, ORDERS_RULES, ["order_id"], "updated_at")

    # --- currency rates (CSV) ---
    raw_rates = (
        spark.read.option("header", "true")
        .csv(f"{_day_partition(bucket, 'currency_rates', day)}/rates.csv")
        .withColumn("rate_date", F.col("rate_date").cast("date"))
        .withColumn("rate", F.col("rate").cast("double"))
        .withColumn("_fetched_at", F.col("_fetched_at").cast("timestamp"))
    )
    _bronze(spark, raw_rates, root, "currency_rates", batch_id)
    _silver_merge_dedup(
        spark,
        root,
        "currency_rates",
        batch_id,
        CURRENCY_RATES_RULES,
        ["rate_date", "target_currency"],
        "_fetched_at",
        watermark=False,
    )

    # --- wishlist (JSONL) ---
    raw_wishlist = spark.read.json(
        f"{_day_partition(bucket, 'wishlist', day)}/wishlist.json"
    ).withColumn("added_at", F.col("added_at").cast("timestamp"))
    _bronze(spark, raw_wishlist, root, "wishlist", batch_id)
    _silver_merge_dedup(
        spark,
        root,
        "wishlist",
        batch_id,
        WISHLIST_RULES,
        ["wishlist_event_id"],
        "added_at",
        watermark=False,
    )

    # --- clickstream (hourly JSONL → sessionized events) ---
    raw_clicks = (
        spark.read.option("recursiveFileLookup", "true")
        .json(_day_partition(bucket, "clickstream", day))
        .withColumn("event_ts", F.col("event_ts").cast("timestamp"))
    )
    _bronze(spark, raw_clicks, root, "clickstream", batch_id)
    clicks_batch = _bronze_batch(spark, root, "clickstream", batch_id)
    good_clicks, bad_clicks = split_quarantine(clicks_batch, CLICKSTREAM_RULES)
    if bad_clicks.count() > 0:
        append_quarantine(bad_clicks, f"{root}/quarantine/clickstream")
    sessionized = sessionize_events(good_clicks)
    silver_clicks_path = f"{root}/silver/clickstream"
    ensure_silver_table(spark, silver_clicks_path, sessionized)
    merge_into_silver(
        spark, sessionized, silver_clicks_path, merge_keys=["event_id"], timestamp_col="event_ts"
    )

    # --- gold ---
    _build_gold(spark, root)


def _build_gold(spark: SparkSession, root: str) -> None:
    """The gold notebook bodies, in DAG dependency order."""
    silver_orders = spark.read.format("delta").load(f"{root}/silver/orders")
    dim_customer = spark.read.format("delta").load(f"{root}/gold/dim_customer")
    dim_product = spark.read.format("delta").load(f"{root}/gold/dim_product")

    fact = build_fact_orders(silver_orders, dim_customer, dim_product)
    ensure_gold_table(spark, f"{root}/gold/fact_orders", fact)
    merge_into_gold(spark, fact, f"{root}/gold/fact_orders", ["order_id"], "updated_at")

    lifecycle = build_fact_order_lifecycle(silver_orders)
    ensure_gold_table(spark, f"{root}/gold/fact_order_lifecycle", lifecycle)
    merge_into_gold(
        spark, lifecycle, f"{root}/gold/fact_order_lifecycle", ["order_id"], "updated_at"
    )

    silver_rates = spark.read.format("delta").load(f"{root}/silver/currency_rates")
    silver_rates.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        f"{root}/gold/currency_rates"
    )

    silver_wishlist = spark.read.format("delta").load(f"{root}/silver/wishlist")
    wishlist_fact = build_fact_customer_wishlist_product(silver_wishlist, dim_customer, dim_product)
    ensure_gold_table(spark, f"{root}/gold/fact_customer_wishlist_product", wishlist_fact)
    merge_into_gold(
        spark,
        wishlist_fact,
        f"{root}/gold/fact_customer_wishlist_product",
        ["wishlist_event_sk"],
        "updated_at",
    )

    silver_clicks = spark.read.format("delta").load(f"{root}/silver/clickstream")
    sessions = build_fact_sessions(silver_clicks)
    ensure_gold_table(spark, f"{root}/gold/fact_sessions", sessions)
    merge_into_gold(
        spark, sessions, f"{root}/gold/fact_sessions", ["silver_session_key"], "updated_at"
    )

    gold_fact_orders = spark.read.format("delta").load(f"{root}/gold/fact_orders")
    ltv = build_customer_ltv(gold_fact_orders.filter(F.col("customer_sk").isNotNull()))
    write_mart_overwrite(ltv, f"{root}/gold/customer_ltv")


def _assert_scd2_invariants(dim: DataFrame, natural_key: str) -> None:
    """Exactly one current row per key; version windows form a clean chain."""
    per_key_current = dim.groupBy(natural_key).agg(
        F.sum(F.col("is_current").cast("int")).alias("n_current")
    )
    bad_current = per_key_current.filter(F.col("n_current") != 1).count()
    assert bad_current == 0, f"{bad_current} {natural_key}s without exactly one current version"

    assert dim.filter(F.col("effective_from") >= F.col("effective_to")).count() == 0

    versions = dim.select(natural_key, "effective_from", "effective_to").collect()
    by_key: dict[str, list] = {}
    for row in versions:
        by_key.setdefault(row[natural_key], []).append(row)
    for key, rows in by_key.items():
        rows.sort(key=lambda r: r["effective_from"])
        for prev, nxt in zip(rows, rows[1:], strict=False):
            assert prev["effective_to"] <= nxt["effective_from"], f"overlapping versions for {key}"


def test_local_end_to_end(spark: SparkSession, tmp_path) -> None:
    from libs.batch import make_batch_id

    bucket = f"file://{tmp_path}/raw_bucket"
    root = f"{tmp_path}/lake"

    # ---- warm-up: dims only, 14 days before day 1 -------------------------
    from generators import customers as customers_gen
    from generators import products as products_gen

    customers_gen.run(_WARMUP_DAY, _WARMUP_DAY, bucket, _GEN_CFG, _SEED, record_count=_N_CUSTOMERS)
    products_gen.run(_WARMUP_DAY, _WARMUP_DAY, bucket, _GEN_CFG, _SEED, record_count=_N_PRODUCTS)
    warmup_batch = make_batch_id(dt.datetime.combine(_WARMUP_DAY, dt.time(2, 0)))
    _ingest_dims(spark, bucket, root, _WARMUP_DAY, warmup_batch)

    # ---- day 1 + day 2: all six sources ------------------------------------
    batch_ids = {}
    for day in (_DAY_1, _DAY_2):
        _generate_raw(bucket, day)
        batch_id = make_batch_id(dt.datetime.combine(day, dt.time(2, 0)))
        batch_ids[day] = batch_id
        _ingest_dims(spark, bucket, root, day, batch_id)

        if day == _DAY_2:
            # Inject one malformed order into the day-2 bronze batch: NULL
            # quantity must be quarantined (NULL-condition DQ semantics),
            # never reach silver. Template a real day-1 bronze row and
            # re-tag it into the day-2 batch (day-2 orders aren't in
            # bronze yet at this point).
            template = _bronze_batch(spark, root, "orders", batch_ids[_DAY_1]).limit(1)
            bad_row = (
                template.withColumn("order_id", F.lit("bad-e2e-1"))
                .withColumn("quantity", F.lit(None).cast("int"))
                .withColumn("_batch_id", F.lit(batch_id))
            )
            assert bad_row.count() == 1
            write_delta_batch(bad_row, f"{root}/bronze/orders")

        _ingest_facts(spark, bucket, root, day, batch_id)

    # ---- idempotency: re-running the final batch is a no-op ----------------
    fact_orders_path = f"{root}/gold/fact_orders"
    count_before = spark.read.format("delta").load(fact_orders_path).count()
    silver_before = spark.read.format("delta").load(f"{root}/silver/orders").count()
    _silver_merge_dedup(
        spark, root, "orders", batch_ids[_DAY_2], ORDERS_RULES, ["order_id"], "updated_at"
    )
    _build_gold(spark, root)
    assert spark.read.format("delta").load(fact_orders_path).count() == count_before
    assert spark.read.format("delta").load(f"{root}/silver/orders").count() == silver_before

    # ---- invariants ---------------------------------------------------------
    silver_orders = spark.read.format("delta").load(f"{root}/silver/orders")
    fact_orders = spark.read.format("delta").load(fact_orders_path)
    dim_customer = spark.read.format("delta").load(f"{root}/gold/dim_customer")
    dim_product = spark.read.format("delta").load(f"{root}/gold/dim_product")

    # Silver: one row per order, DQ-clean.
    assert silver_orders.count() == silver_orders.select("order_id").distinct().count()
    assert silver_orders.filter(F.col("quantity").isNull() | (F.col("quantity") <= 0)).count() == 0
    assert silver_orders.filter(F.col("price").isNull() | (F.col("price") <= 0)).count() == 0

    # Quarantine: the injected bad row is there, and never reached silver.
    quarantined = spark.read.format("delta").load(f"{root}/quarantine/orders")
    bad = quarantined.filter(F.col("order_id") == "bad-e2e-1")
    assert bad.count() >= 1
    assert "quantity_positive" in bad.first()["_quarantine_reason"]
    assert silver_orders.filter(F.col("order_id") == "bad-e2e-1").count() == 0

    # SCD2 dims: clean version chains. The 2%-daily change rate over two
    # snapshot days makes multi-version entities near-certain at pool size.
    _assert_scd2_invariants(dim_customer, "customer_id")
    _assert_scd2_invariants(dim_product, "product_id")

    # fact_orders: complete, PIT-resolved, arithmetically consistent.
    assert fact_orders.count() == silver_orders.count()
    assert orphan_surrogate_rate(fact_orders, ["customer_sk", "product_sk"]) == 0.0
    assert (
        fact_orders.filter(
            F.abs(F.col("total_amount") - F.col("quantity") * F.col("price")) > 0.001
        ).count()
        == 0
    )
    # PIT bracket: every bound surrogate's dim version covers created_at.
    pit_violations = (
        fact_orders.alias("f")
        .join(dim_customer.alias("d"), F.col("f.customer_sk") == F.col("d.customer_sk"))
        .filter(
            (F.col("f.created_at") < F.col("d.effective_from"))
            | (F.col("f.created_at") >= F.col("d.effective_to"))
        )
        .count()
    )
    assert pit_violations == 0, "fact bound to a dim version not valid at order placement"

    # Accumulating snapshot: durations non-negative, milestones ordered.
    lifecycle = spark.read.format("delta").load(f"{root}/gold/fact_order_lifecycle")
    for col in (
        "days_placed_to_paid",
        "days_paid_to_shipped",
        "days_shipped_to_delivered",
        "days_placed_to_delivered",
    ):
        assert lifecycle.filter(F.col(col) < 0).count() == 0, f"negative {col}"

    # Sessions: every silver event in exactly one session; keys unique;
    # multi-day cookies did NOT collapse into one session (cross-batch keys).
    silver_clicks = spark.read.format("delta").load(f"{root}/silver/clickstream")
    sessions = spark.read.format("delta").load(f"{root}/gold/fact_sessions")
    assert sessions.count() == sessions.select("silver_session_key").distinct().count()
    assert (
        sessions.agg(F.sum("event_count")).first()[0] == silver_clicks.count()
    ), "every event must belong to exactly one session"
    assert sessions.filter(F.col("session_end") < F.col("session_start")).count() == 0
    multi_day_cookies = (
        silver_clicks.groupBy("session_id")
        .agg(F.count_distinct(F.to_date("event_ts")).alias("days"))
        .filter(F.col("days") > 1)
        .count()
    )
    if multi_day_cookies > 0:
        glued = sessions.filter(F.to_date("session_start") != F.to_date("session_end")).count()
        assert glued == 0, "a session spans both generated days — cross-batch key collision"

    # Wishlist factless fact: per-event grain preserved, PIT-resolved.
    wishlist_fact = spark.read.format("delta").load(f"{root}/gold/fact_customer_wishlist_product")
    assert wishlist_fact.count() == wishlist_fact.select("wishlist_event_sk").distinct().count()
    assert orphan_surrogate_rate(wishlist_fact, ["customer_sk", "product_sk"]) == 0.0

    # Currency reference: both days × configured targets, all positive.
    gold_rates = spark.read.format("delta").load(f"{root}/gold/currency_rates")
    assert gold_rates.select("rate_date").distinct().count() == 2
    assert gold_rates.filter(F.col("rate") <= 0).count() == 0

    # customer_ltv mart: one row per customer, revenue ties out.
    ltv = spark.read.format("delta").load(f"{root}/gold/customer_ltv")
    assert ltv.count() == ltv.select("customer_sk").distinct().count()
    expected_revenue = (
        fact_orders.filter((F.col("status") != "cancelled") & F.col("customer_sk").isNotNull())
        .agg(F.sum("total_amount"))
        .first()[0]
    )
    actual_revenue = ltv.agg(F.sum("total_revenue")).first()[0]
    assert abs(actual_revenue - expected_revenue) < 0.01
