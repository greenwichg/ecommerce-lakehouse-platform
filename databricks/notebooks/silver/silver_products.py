# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Products
# MAGIC
# MAGIC Same pipeline as silver_customers, keyed on ``product_id`` — and,
# MAGIC like silver_customers, with NO late-arrival watermark: product
# MAGIC snapshots carry ``updated_at`` = the product's last change, so a
# MAGIC watermark would silently drop every long-stable product from the
# MAGIC dim (see the rationale in silver_customers).

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
from libs.paths import layer_root  # noqa: E402
from libs.quality import PRODUCTS_RULES, split_quarantine  # noqa: E402
from libs.silver import (  # noqa: E402
    append_quarantine,
    dedup_by_key,
    ensure_silver_table,
    merge_into_silver,
)

cfg = load_config(env=env)
bucket = get_path(cfg, "storage.bucket")
bronze_path = layer_root(bucket, cfg["paths"]["bronze_prefix"], "products")
silver_path = layer_root(bucket, cfg["paths"]["silver_prefix"], "products")
quarantine_path = layer_root(bucket, cfg["paths"]["quarantine_prefix"], "products")

# COMMAND ----------

from pyspark.sql import functions as F  # noqa: E402

bronze = spark.read.format("delta").load(bronze_path)  # noqa: F821
batch_df = bronze.filter(F.col("_batch_id") == batch_id)
input_count = batch_df.count()

good, bad = split_quarantine(batch_df, PRODUCTS_RULES)
bad_count = bad.count()
good_count = good.count()
if bad_count > 0:
    append_quarantine(bad, quarantine_path)

deduped = dedup_by_key(good, ["product_id"], "updated_at")
merge_count = deduped.count()

ensure_silver_table(spark, silver_path, deduped)  # noqa: F821
merge_into_silver(
    spark=spark,  # noqa: F821
    source_df=deduped,
    target_path=silver_path,
    merge_keys=["product_id"],
    timestamp_col="updated_at",
)

# COMMAND ----------

import json  # noqa: E402

result = {
    "batch_id": batch_id,
    "input_count": input_count,
    "good_count": good_count,
    "bad_count": bad_count,
    "merged_count": merge_count,
}
print(json.dumps(result))
dbutils.notebook.exit(json.dumps(result))  # noqa: F821
