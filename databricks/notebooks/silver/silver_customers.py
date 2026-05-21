# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Customers
# MAGIC
# MAGIC Same shape as silver_orders: bronze (filtered by current _batch_id)
# MAGIC -> split_quarantine -> watermark -> dedup -> MERGE into silver Delta.
# MAGIC The natural key here is ``customer_id``; the SCD2 trigger fields
# MAGIC (email, address) are NOT the dedup key — Silver keeps "current state
# MAGIC per customer_id", and Gold's apply_scd2_merge materialises history.

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
assert batch_id, "batch_id widget required (from Airflow)"

# COMMAND ----------

from libs.config import get_path, load_config  # noqa: E402
from libs.paths import layer_root  # noqa: E402
from libs.quality import CUSTOMERS_RULES, split_quarantine  # noqa: E402
from libs.silver import (  # noqa: E402
    append_quarantine,
    apply_watermark,
    dedup_by_key,
    ensure_silver_table,
    merge_into_silver,
)

cfg = load_config(env=env)
bucket = get_path(cfg, "storage.bucket")
bronze_path = layer_root(bucket, cfg["paths"]["bronze_prefix"], "customers")
silver_path = layer_root(bucket, cfg["paths"]["silver_prefix"], "customers")
quarantine_path = layer_root(bucket, cfg["paths"]["quarantine_prefix"], "customers")
watermark_days = int(get_path(cfg, "data_quality.late_arrival_window_days", default=10))

# COMMAND ----------

from pyspark.sql import functions as F  # noqa: E402

bronze = spark.read.format("delta").load(bronze_path)  # noqa: F821
batch_df = bronze.filter(F.col("_batch_id") == batch_id)
input_count = batch_df.count()
print(f"bronze rows for batch_id={batch_id}: {input_count}")

# COMMAND ----------

good, bad = split_quarantine(batch_df, CUSTOMERS_RULES)
bad_count = bad.count()
good_count = good.count()
print(f"good={good_count}  bad={bad_count}")

if bad_count > 0:
    append_quarantine(bad, quarantine_path)

# COMMAND ----------

watermarked = apply_watermark(good, "updated_at", window_days=watermark_days)
deduped = dedup_by_key(watermarked, ["customer_id"], "updated_at")
merge_count = deduped.count()
print(f"after watermark + dedup: {merge_count}")

# COMMAND ----------

ensure_silver_table(spark, silver_path, deduped)  # noqa: F821
merge_into_silver(
    spark=spark,  # noqa: F821
    source_df=deduped,
    target_path=silver_path,
    merge_keys=["customer_id"],
    timestamp_col="updated_at",
)
print(f"merged into silver: {silver_path}")

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
