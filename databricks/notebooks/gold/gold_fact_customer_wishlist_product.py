# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — fact_customer_wishlist_product (factless fact)
# MAGIC
# MAGIC Per-event grain: one row per (customer, product, added_at) wishlist
# MAGIC addition. PIT-binds customer_sk and product_sk via the SCD2 dims at
# MAGIC added_at. The MERGE on wishlist_event_sk no-ops on replay (events
# MAGIC are immutable by construction).

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
from libs.gold import ensure_gold_table, merge_into_gold, optimize_zorder  # noqa: E402
from libs.paths import layer_root  # noqa: E402
from libs.wishlist import build_fact_customer_wishlist_product  # noqa: E402

cfg = load_config(env=env)
bucket = get_path(cfg, "storage.bucket")
silver_path = layer_root(bucket, cfg["paths"]["silver_prefix"], "wishlist")
dim_customer_path = layer_root(bucket, cfg["paths"]["gold_prefix"], "dim_customer")
dim_product_path = layer_root(bucket, cfg["paths"]["gold_prefix"], "dim_product")
gold_path = layer_root(bucket, cfg["paths"]["gold_prefix"], "fact_customer_wishlist_product")

# COMMAND ----------

silver = spark.read.format("delta").load(silver_path)  # noqa: F821
dim_customer = spark.read.format("delta").load(dim_customer_path)  # noqa: F821
dim_product = spark.read.format("delta").load(dim_product_path)  # noqa: F821

fact = build_fact_customer_wishlist_product(silver, dim_customer, dim_product)

ensure_gold_table(spark, gold_path, fact)  # noqa: F821
merge_into_gold(
    spark=spark,  # noqa: F821
    source_df=fact,
    target_path=gold_path,
    merge_keys=["wishlist_event_sk"],
    timestamp_col="updated_at",
)

optimize_zorder(spark, gold_path, ["customer_sk", "product_sk"])  # noqa: F821

import json  # noqa: E402

row_count = spark.read.format("delta").load(gold_path).count()  # noqa: F821
dbutils.notebook.exit(  # noqa: F821
    json.dumps({"batch_id": batch_id, "row_count": row_count})
)
