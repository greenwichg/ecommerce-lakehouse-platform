"""Data quality rules and quarantine splitting.

Rules are declared as ``Rule(name, condition)`` tuples where ``condition``
is a SQL expression that must evaluate to TRUE for a valid row. This keeps
rules declarative and inspectable: the runbook can list every rule by name
without reading transformation code.

Bad rows go to a quarantine Delta table with a ``_quarantine_reason``
column listing every failed rule name (comma-separated). That way an
operator can see at a glance whether a quarantine spike is one bad rule
or many.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


@dataclass(frozen=True)
class Rule:
    """One data quality rule.

    ``name`` is surfaced in the quarantine table for diagnostics.
    ``condition`` is a SQL expression interpreted by ``pyspark.sql.functions.expr``;
    a row is valid iff the expression evaluates to TRUE.
    """

    name: str
    condition: str


# Orders DQ rules. Order matters only for human readability; evaluation is
# all-at-once. Keep names short and snake_cased — they end up in alerts.
ORDERS_RULES: tuple[Rule, ...] = (
    Rule("order_id_not_null", "order_id IS NOT NULL"),
    Rule("customer_id_not_null", "customer_id IS NOT NULL"),
    Rule("product_id_not_null", "product_id IS NOT NULL"),
    Rule("quantity_positive", "quantity > 0"),
    Rule("price_positive", "price > 0"),
    Rule(
        "status_known",
        "status IN ('placed', 'paid', 'shipped', 'delivered', 'cancelled')",
    ),
    # Allow a 1-hour skew for clock drift between source and Spark driver.
    Rule(
        "created_at_not_future",
        "created_at <= current_timestamp() + INTERVAL 1 HOUR",
    ),
    Rule("updated_at_not_before_created", "updated_at >= created_at"),
)


# Customers DQ rules (Slice 2). Mirrors the orders contract but for the
# customer snapshot shape.
CUSTOMERS_RULES: tuple[Rule, ...] = (
    Rule("customer_id_not_null", "customer_id IS NOT NULL"),
    Rule("name_not_null", "name IS NOT NULL"),
    # email format check: simple "@ with non-empty local + domain" regex.
    # Looser than RFC 5322 but catches obviously-broken values.
    Rule(
        "email_well_formed",
        "email IS NULL OR email RLIKE '^[^@\\\\s]+@[^@\\\\s]+\\\\.[^@\\\\s]+$'",
    ),
    Rule("signup_date_not_future", "signup_date <= current_date()"),
    Rule(
        "updated_at_not_before_signup",
        "updated_at >= CAST(signup_date AS TIMESTAMP)",
    ),
)


# Products DQ rules (Slice 2).
PRODUCTS_RULES: tuple[Rule, ...] = (
    Rule("product_id_not_null", "product_id IS NOT NULL"),
    Rule("product_name_not_null", "product_name IS NOT NULL"),
    Rule("price_positive", "price > 0"),
    Rule(
        "category_known",
        "category IN ('electronics', 'books', 'clothing', 'home_kitchen', "
        "'sports_outdoors', 'toys_games', 'beauty', 'grocery', 'automotive', "
        "'office')",
    ),
    Rule("sku_not_null", "sku IS NOT NULL"),
)


_QUARANTINE_COL = "_quarantine_reason"


def split_quarantine(
    df: DataFrame, rules: Sequence[Rule]
) -> tuple[DataFrame, DataFrame]:
    """Split ``df`` into (good, bad) by applying ``rules``.

    Returns:
        good: rows that satisfy every rule (without the ``_quarantine_reason`` col).
        bad: rows that failed at least one rule, with a ``_quarantine_reason``
            column listing every failed rule name, comma-separated.

    If ``rules`` is empty, every row is good; ``bad`` is empty but has the
    ``_quarantine_reason`` column added so callers that ``unionByName`` the
    two halves don't trip on schema differences.
    """
    if not rules:
        empty_bad = df.limit(0).withColumn(_QUARANTINE_COL, F.lit(None).cast(StringType()))
        return df, empty_bad

    # For each row, build an array of the failed rule names (NULLs filtered out).
    fail_exprs = [
        F.when(F.expr(f"NOT ({rule.condition})"), F.lit(rule.name)) for rule in rules
    ]
    flagged = df.withColumn(
        "__quarantine_reasons_array",
        F.array_compact(F.array(*fail_exprs)),
    )

    good = flagged.filter(F.size("__quarantine_reasons_array") == 0).drop(
        "__quarantine_reasons_array"
    )
    bad = (
        flagged.filter(F.size("__quarantine_reasons_array") > 0)
        .withColumn(_QUARANTINE_COL, F.concat_ws(",", "__quarantine_reasons_array"))
        .drop("__quarantine_reasons_array")
    )
    return good, bad
