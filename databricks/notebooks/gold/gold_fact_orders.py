# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — fact_orders (transactional grain, SCD2-aware)
# MAGIC
# MAGIC One row per order, current state, with surrogate keys bound via
# MAGIC point-in-time SCD2 joins against dim_customer and dim_product.
# MAGIC
# MAGIC ## Surrogate keys
# MAGIC
# MAGIC - `order_sk` = sha256(order_id) — deterministic and stable
# MAGIC - `customer_sk` / `product_sk` resolved via `pit_join_scd2` on
# MAGIC   `created_at` (the order's placement timestamp). This binds each
# MAGIC   order to the dim version that was current AT PLACEMENT, regardless
# MAGIC   of when status updates arrive — see libs.gold.pit_join_scd2 for
# MAGIC   why created_at is the right binding timestamp.
# MAGIC
# MAGIC ## Orphan-rate gate
# MAGIC
# MAGIC If any order references a customer_id or product_id absent from the
# MAGIC corresponding dim, the SK comes through NULL (LEFT join). We compute
# MAGIC the orphan rate after the join and `raise` if it exceeds
# MAGIC `data_quality.orphan_surrogate_rate_pct` from config — fails the
# MAGIC notebook job, which fails the Airflow task, which fires the SNS alert.
# MAGIC
# MAGIC ## OPTIMIZE
# MAGIC
# MAGIC ZORDER BY (order_id) at the end — natural join key for Snowflake's
# MAGIC downstream MERGE and the dashboard's drill-through filter.

# COMMAND ----------

import sys
from pathlib import Path

_libs_root = Path.cwd().parent.parent
if (_libs_root / "libs").is_dir() and str(_libs_root) not in sys.path:
    sys.path.insert(0, str(_libs_root))

# COMMAND ----------

dbutils.widgets.text("env", "dev")  # noqa: F821
dbutils.widgets.text("batch_id", "")  # noqa: F821
env = dbutils.widgets.get("env")  # noqa: F821
batch_id = dbutils.widgets.get("batch_id")  # noqa: F821
assert batch_id

# COMMAND ----------

from libs.config import get_path, load_config  # noqa: E402
from libs.gold import (  # noqa: E402
    build_fact_orders,
    ensure_gold_table,
    merge_into_gold,
    optimize_zorder,
    orphan_surrogate_rate,
)
from libs.paths import layer_root  # noqa: E402

cfg = load_config(env=env)
bucket = get_path(cfg, "storage.bucket")
silver_orders_path = layer_root(bucket, cfg["paths"]["silver_prefix"], "orders")
dim_customer_path = layer_root(bucket, cfg["paths"]["gold_prefix"], "dim_customer")
dim_product_path = layer_root(bucket, cfg["paths"]["gold_prefix"], "dim_product")
fact_orders_path = layer_root(bucket, cfg["paths"]["gold_prefix"], "fact_orders")
orphan_threshold = float(
    get_path(cfg, "data_quality.orphan_surrogate_rate_pct", default=1.0)
)

print(f"silver_orders = {silver_orders_path}")
print(f"dim_customer  = {dim_customer_path}")
print(f"dim_product   = {dim_product_path}")
print(f"fact_orders   = {fact_orders_path}")
print(f"orphan_threshold = {orphan_threshold}%")

# COMMAND ----------

silver = spark.read.format("delta").load(silver_orders_path)  # noqa: F821
dim_customer = spark.read.format("delta").load(dim_customer_path)  # noqa: F821
dim_product = spark.read.format("delta").load(dim_product_path)  # noqa: F821

fact = build_fact_orders(silver, dim_customer, dim_product)

# COMMAND ----------

# Orphan-rate gate: fail the notebook (and therefore the Airflow task) if
# the SCD2 PIT join couldn't resolve too many surrogates. Causes:
#   - dim load lagging behind fact load
#   - upstream emitted a customer_id/product_id not in the dim source
orphan_rate = orphan_surrogate_rate(fact, ["customer_sk", "product_sk"])
print(f"orphan_surrogate_rate = {orphan_rate:.4f}%")
if orphan_rate > orphan_threshold:
    raise RuntimeError(
        f"Orphan surrogate rate {orphan_rate:.2f}% exceeds threshold "
        f"{orphan_threshold}% — investigate dim load lag or upstream FK break"
    )

# COMMAND ----------

ensure_gold_table(spark, fact_orders_path, fact)  # noqa: F821
merge_into_gold(
    spark=spark,  # noqa: F821
    source_df=fact,
    target_path=fact_orders_path,
    merge_keys=["order_id"],
    timestamp_col="updated_at",
)
row_count = spark.read.format("delta").load(fact_orders_path).count()  # noqa: F821
print(f"fact_orders rows: {row_count}")

# COMMAND ----------

optimize_zorder(spark, fact_orders_path, ["order_id"])  # noqa: F821

# COMMAND ----------

import json  # noqa: E402

dbutils.notebook.exit(  # noqa: F821
    json.dumps(
        {
            "batch_id": batch_id,
            "row_count": row_count,
            "orphan_rate_pct": round(orphan_rate, 4),
        }
    )
)
