# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Wishlist Events
# MAGIC
# MAGIC DQ rules + dedup on wishlist_event_id (events are immutable; dedup
# MAGIC is a no-op on first load, replay-safe).

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
from libs.quality import WISHLIST_RULES, split_quarantine  # noqa: E402
from libs.silver import (  # noqa: E402
    append_quarantine,
    dedup_by_key,
    ensure_silver_table,
    merge_into_silver,
)
from pyspark.sql import functions as F  # noqa: E402

cfg = load_config(env=env)
bucket = get_path(cfg, "storage.bucket")
bronze_path = layer_root(bucket, cfg["paths"]["bronze_prefix"], "wishlist")
silver_path = layer_root(bucket, cfg["paths"]["silver_prefix"], "wishlist")
quarantine_path = layer_root(bucket, cfg["paths"]["quarantine_prefix"], "wishlist")

# COMMAND ----------

bronze = spark.read.format("delta").load(bronze_path)  # noqa: F821
batch_df = bronze.filter(F.col("_batch_id") == batch_id).withColumn(
    "added_at", F.col("added_at").cast("timestamp")
)

good, bad = split_quarantine(batch_df, WISHLIST_RULES)
if bad.count() > 0:
    append_quarantine(bad, quarantine_path)

# Events are immutable: dedup on wishlist_event_id, picking the
# first-seen if a replay somehow re-emits (timestamp_col doesn't matter
# here because events are unique by ID).
deduped = dedup_by_key(good, ["wishlist_event_id"], "added_at")

ensure_silver_table(spark, silver_path, deduped)  # noqa: F821
merge_into_silver(
    spark=spark,  # noqa: F821
    source_df=deduped,
    target_path=silver_path,
    merge_keys=["wishlist_event_id"],
    timestamp_col="added_at",
)

import json  # noqa: E402

dbutils.notebook.exit(  # noqa: F821
    json.dumps({"batch_id": batch_id, "row_count": deduped.count()})
)
