# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Orders
# MAGIC
# MAGIC Transforms Bronze orders into a deduplicated, quality-filtered, and
# MAGIC watermark-bounded current-state table.
# MAGIC
# MAGIC ## Pipeline
# MAGIC
# MAGIC ```
# MAGIC bronze.orders (raw + lineage)
# MAGIC     │   filter by _batch_id
# MAGIC     ▼
# MAGIC split_quarantine(ORDERS_RULES)
# MAGIC     │
# MAGIC     ├── bad  → quarantine.orders (Delta, append-only)
# MAGIC     │
# MAGIC     └── good
# MAGIC          │   apply_watermark(updated_at, window_days=10)
# MAGIC          ▼
# MAGIC          dedup_by_key([order_id], updated_at)   ← intra-batch dedup
# MAGIC          │
# MAGIC          ▼
# MAGIC          MERGE INTO silver.orders ON order_id
# MAGIC              WHEN MATCHED AND s.updated_at > t.updated_at: UPDATE *
# MAGIC              WHEN NOT MATCHED:                              INSERT *
# MAGIC ```
# MAGIC
# MAGIC ## Late-arrival policy
# MAGIC
# MAGIC The watermark drops records whose `updated_at` is older than
# MAGIC `max(updated_at) - 10 days`. With the orders trajectory means
# MAGIC (placed→paid 0.5d, paid→shipped 1.5d, shipped→delivered 3.5d), a 10-day
# MAGIC window captures all natural transitions plus a 4-5 day safety margin.

# COMMAND ----------

import sys
from pathlib import Path

_libs_root = Path.cwd().parent.parent  # databricks/
if (_libs_root / "libs").is_dir() and str(_libs_root) not in sys.path:
    sys.path.insert(0, str(_libs_root))

# COMMAND ----------

dbutils.widgets.text("env", "dev", "Environment")  # noqa: F821
dbutils.widgets.text("batch_id", "", "Batch ID (from Airflow)")  # noqa: F821

env = dbutils.widgets.get("env")  # noqa: F821
batch_id = dbutils.widgets.get("batch_id")  # noqa: F821
assert batch_id, "batch_id widget is required — Airflow passes via DatabricksRunNowOperator"
print(f"env={env} batch_id={batch_id}")

# COMMAND ----------

from libs.config import get_path, load_config  # noqa: E402
from libs.paths import layer_root  # noqa: E402
from libs.quality import ORDERS_RULES, split_quarantine  # noqa: E402
from libs.silver import (  # noqa: E402
    append_quarantine,
    apply_watermark,
    dedup_by_key,
    ensure_silver_table,
    merge_into_silver,
)

cfg = load_config(env=env)
bucket = get_path(cfg, "storage.bucket")
bronze_path = layer_root(bucket, cfg["paths"]["bronze_prefix"], "orders")
silver_path = layer_root(bucket, cfg["paths"]["silver_prefix"], "orders")
quarantine_path = layer_root(bucket, cfg["paths"]["quarantine_prefix"], "orders")
watermark_days = int(get_path(cfg, "data_quality.late_arrival_window_days", default=10))

print(f"bronze     = {bronze_path}")
print(f"silver     = {silver_path}")
print(f"quarantine = {quarantine_path}")
print(f"watermark_days = {watermark_days}")

# COMMAND ----------

# MAGIC %md ## Read Bronze for this batch
# MAGIC
# MAGIC Filter on `_batch_id` so we only process records produced by the
# MAGIC current Airflow run. The MERGE handles idempotency for re-runs.

# COMMAND ----------

from pyspark.sql import functions as F  # noqa: E402

bronze = spark.read.format("delta").load(bronze_path)  # noqa: F821
batch_df = bronze.filter(F.col("_batch_id") == batch_id)
input_count = batch_df.count()
print(f"bronze rows for batch_id={batch_id}: {input_count}")

# COMMAND ----------

# MAGIC %md ## Data quality split

# COMMAND ----------

good, bad = split_quarantine(batch_df, ORDERS_RULES)
bad_count = bad.count()
good_count = good.count()
print(f"good={good_count}  bad={bad_count}")

if bad_count > 0:
    append_quarantine(bad, quarantine_path)
    print(f"appended {bad_count} bad rows to {quarantine_path}")

# COMMAND ----------

# MAGIC %md ## Watermark + dedup
# MAGIC
# MAGIC Watermark first (cheaper to filter old rows before the window op),
# MAGIC then dedup so the MERGE sees at most one row per order_id.

# COMMAND ----------

watermarked = apply_watermark(good, "updated_at", window_days=watermark_days)
deduped = dedup_by_key(watermarked, ["order_id"], "updated_at")
merge_count = deduped.count()
print(f"after watermark + dedup: {merge_count}")

# COMMAND ----------

# MAGIC %md ## MERGE into silver.orders

# COMMAND ----------

ensure_silver_table(spark, silver_path, deduped)  # noqa: F821
merge_into_silver(
    spark=spark,  # noqa: F821
    source_df=deduped,
    target_path=silver_path,
    merge_keys=["order_id"],
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
