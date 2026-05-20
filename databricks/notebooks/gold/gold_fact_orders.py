# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — fact_orders (transactional grain)
# MAGIC
# MAGIC One row per order, current state, with surrogate keys.
# MAGIC
# MAGIC ## Surrogate keys
# MAGIC
# MAGIC - `order_sk` = sha256(order_id) — deterministic and stable, so re-running
# MAGIC   gold against the same silver state yields identical keys.
# MAGIC - `customer_sk` / `product_sk` are NULL until Slice 2 ships
# MAGIC   `dim_customer` / `dim_product`. Schema is locked in now so
# MAGIC   downstream consumers don't break when those columns get populated.
# MAGIC
# MAGIC ## OPTIMIZE
# MAGIC
# MAGIC `ZORDER BY (order_id)` is run at the end. `order_id` is the natural
# MAGIC join key for Snowflake's downstream MERGE and the highest-cardinality
# MAGIC filter the dashboard issues.

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
)
from libs.paths import layer_root  # noqa: E402

cfg = load_config(env=env)
bucket = get_path(cfg, "storage.bucket")
silver_path = layer_root(bucket, cfg["paths"]["silver_prefix"], "orders")
gold_path = layer_root(bucket, cfg["paths"]["gold_prefix"], "fact_orders")
print(f"silver = {silver_path}")
print(f"gold   = {gold_path}")

# COMMAND ----------

silver = spark.read.format("delta").load(silver_path)  # noqa: F821
fact = build_fact_orders(silver)
ensure_gold_table(spark, gold_path, fact)  # noqa: F821

merge_into_gold(
    spark=spark,  # noqa: F821
    source_df=fact,
    target_path=gold_path,
    merge_keys=["order_id"],
    timestamp_col="updated_at",
)
row_count = spark.read.format("delta").load(gold_path).count()  # noqa: F821
print(f"fact_orders rows: {row_count}")

# COMMAND ----------

# OPTIMIZE + ZORDER on the merge/filter key. Cheap on small tables, big
# benefit on production volumes where Z-ordering data-skipping kicks in.
optimize_zorder(spark, gold_path, ["order_id"])  # noqa: F821

# COMMAND ----------

import json  # noqa: E402

dbutils.notebook.exit(json.dumps({"batch_id": batch_id, "row_count": row_count}))  # noqa: F821
