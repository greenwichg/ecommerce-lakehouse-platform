# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — fact_order_lifecycle (accumulating snapshot)
# MAGIC
# MAGIC One row per `order_id`, carrying every milestone the order has
# MAGIC reached so far (placed → paid → shipped → delivered, or cancelled
# MAGIC at some stage) plus decimal-day durations between consecutive
# MAGIC milestones.
# MAGIC
# MAGIC ## Why a separate fact?
# MAGIC
# MAGIC `fact_orders` is the *current state* — one row per order with the
# MAGIC latest status. The accumulating snapshot is the *cycle time view* —
# MAGIC same grain (one row per order) but every milestone column carries
# MAGIC its own timestamp so analytics can compute distributions like
# MAGIC "p50 days from placed to delivered, by month".
# MAGIC
# MAGIC ## MERGE semantics
# MAGIC
# MAGIC The Silver fix (cancellation snapshots carrying paid_at/shipped_at
# MAGIC when the order actually progressed) means we can `whenMatchedUpdateAll`
# MAGIC without losing milestones: silver always re-emits the complete set
# MAGIC of timestamps the order has reached so far.

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
    build_fact_order_lifecycle,
    ensure_gold_table,
    merge_into_gold,
    optimize_zorder,
)
from libs.paths import layer_root  # noqa: E402

cfg = load_config(env=env)
bucket = get_path(cfg, "storage.bucket")
silver_path = layer_root(bucket, cfg["paths"]["silver_prefix"], "orders")
gold_path = layer_root(bucket, cfg["paths"]["gold_prefix"], "fact_order_lifecycle")
print(f"silver = {silver_path}")
print(f"gold   = {gold_path}")

# COMMAND ----------

silver = spark.read.format("delta").load(silver_path)  # noqa: F821
fact = build_fact_order_lifecycle(silver)
ensure_gold_table(spark, gold_path, fact)  # noqa: F821

merge_into_gold(
    spark=spark,  # noqa: F821
    source_df=fact,
    target_path=gold_path,
    merge_keys=["order_id"],
    timestamp_col="updated_at",
)
row_count = spark.read.format("delta").load(gold_path).count()  # noqa: F821
print(f"fact_order_lifecycle rows: {row_count}")

# COMMAND ----------

optimize_zorder(spark, gold_path, ["order_id"])  # noqa: F821

# COMMAND ----------

import json  # noqa: E402

dbutils.notebook.exit(json.dumps({"batch_id": batch_id, "row_count": row_count}))  # noqa: F821
