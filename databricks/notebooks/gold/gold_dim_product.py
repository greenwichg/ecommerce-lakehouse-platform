# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — dim_product (SCD2)
# MAGIC
# MAGIC Tracked attributes: ``price``, ``category``. Stable: ``product_name``,
# MAGIC ``sku``. Same pattern as dim_customer.

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
from libs.gold import optimize_zorder  # noqa: E402
from libs.paths import layer_root  # noqa: E402
from libs.scd2 import apply_scd2_merge  # noqa: E402

cfg = load_config(env=env)
bucket = get_path(cfg, "storage.bucket")
silver_path = layer_root(bucket, cfg["paths"]["silver_prefix"], "products")
gold_path = layer_root(bucket, cfg["paths"]["gold_prefix"], "dim_product")

# COMMAND ----------

silver = spark.read.format("delta").load(silver_path)  # noqa: F821

apply_scd2_merge(
    spark=spark,  # noqa: F821
    source_df=silver,
    target_path=gold_path,
    natural_key="product_id",
    attribute_cols=["product_name", "category", "price", "sku"],
    tracked_cols=["price", "category"],
    sk_col="product_sk",
    ts_col="updated_at",
)
row_count = spark.read.format("delta").load(gold_path).count()  # noqa: F821

# COMMAND ----------

optimize_zorder(spark, gold_path, ["product_id"])  # noqa: F821

# COMMAND ----------

import json  # noqa: E402

dbutils.notebook.exit(  # noqa: F821
    json.dumps({"batch_id": batch_id, "row_count": row_count})
)
