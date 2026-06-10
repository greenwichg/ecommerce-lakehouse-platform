"""Wishlist factless-fact builder.

``build_fact_customer_wishlist_product`` constructs the per-event
factless fact: one row per (customer, product, added_at) wishlist
addition. Surrogate keys for customer + product come from PIT joins
against dim_customer + dim_product at ``added_at`` — same pattern as
``libs.gold.build_fact_orders``.

Factless means **no measures, only FK relationships + dates**. The
business questions this fact answers are "did this happen?" style:

    -- customers who wishlisted but didn't buy
    SELECT DISTINCT w.customer_sk, w.product_sk
    FROM analytics.fact_customer_wishlist_product w
    LEFT JOIN analytics.fact_orders o
        ON o.customer_sk = w.customer_sk
        AND o.product_sk = w.product_sk
    WHERE o.order_sk IS NULL;

The ``DISTINCT`` is required because per-event grain allows multiple
rows per (customer, product) pair — see Slice 4 design rationale in
``docs/architecture.md``.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from libs.gold import pit_join_scd2


def build_fact_customer_wishlist_product(
    silver_wishlist: DataFrame,
    dim_customer: DataFrame,
    dim_product: DataFrame,
) -> DataFrame:
    """Build the wishlist factless fact.

    Args:
        silver_wishlist: deduplicated silver wishlist events with at least
            ``wishlist_event_id``, ``customer_id``, ``product_id``,
            ``added_at``, ``source``.
        dim_customer: SCD2 customer dim with effective_from/effective_to.
        dim_product: SCD2 product dim.

    Output columns:
        wishlist_event_sk   — SHA-256 of wishlist_event_id (deterministic)
        wishlist_event_id   — natural key, carried for audit
        customer_id         — natural
        customer_sk         — PIT surrogate (NULL if dim_customer missing this customer)
        product_id          — natural
        product_sk          — PIT surrogate (NULL if dim_product missing this product)
        added_at            — event timestamp
        added_date          — DATE(added_at), useful for time-window filters
        source              — origin (e.g. "/products", "/email-campaign")
        updated_at          — = added_at; events are immutable, so MERGE
                              on wishlist_event_sk no-ops on replay
    """
    base = silver_wishlist.select(
        F.sha2(F.col("wishlist_event_id"), 256).alias("wishlist_event_sk"),
        F.col("wishlist_event_id"),
        F.col("customer_id"),
        F.col("product_id"),
        F.col("added_at"),
        F.col("added_at").cast("date").alias("added_date"),
        F.col("source"),
        F.col("added_at").alias("updated_at"),
    )
    with_customer = pit_join_scd2(base, dim_customer, "customer_id", "customer_sk", "added_at")
    with_both = pit_join_scd2(with_customer, dim_product, "product_id", "product_sk", "added_at")
    # Reorder so surrogates sit next to their natural keys
    return with_both.select(
        "wishlist_event_sk",
        "wishlist_event_id",
        "customer_id",
        "customer_sk",
        "product_id",
        "product_sk",
        "added_at",
        "added_date",
        "source",
        "updated_at",
    )
