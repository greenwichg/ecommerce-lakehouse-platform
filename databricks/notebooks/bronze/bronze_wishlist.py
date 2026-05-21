# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Wishlist Events
# MAGIC
# MAGIC Auto Loader JSON ingestion (same pattern as clickstream). ~5k events
# MAGIC per day, append-only.

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
if not batch_id:
    from libs.batch import make_batch_id

    batch_id = make_batch_id()

# COMMAND ----------

from libs.bronze import add_bronze_metadata, autoloader_options  # noqa: E402
from libs.config import get_path, load_config  # noqa: E402
from libs.paths import checkpoint_path, layer_root  # noqa: E402

cfg = load_config(env=env)
bucket = get_path(cfg, "storage.bucket")
source_path = layer_root(bucket, cfg["paths"]["raw_prefix"], "wishlist")
target_path = layer_root(bucket, cfg["paths"]["bronze_prefix"], "wishlist")
schema_location = checkpoint_path(
    bucket, cfg["paths"]["checkpoint_prefix"], "wishlist", "bronze_schema"
)
checkpoint_location = checkpoint_path(
    bucket, cfg["paths"]["checkpoint_prefix"], "wishlist", "bronze"
)

# COMMAND ----------

src_stream = (
    spark.readStream.format("cloudFiles")  # noqa: F821
    .options(**autoloader_options(schema_location=schema_location, file_format="json"))
    .load(source_path)
)
enriched = add_bronze_metadata(
    src_stream, batch_id=batch_id, source_file_col="_metadata.file_path"
)

query = (
    enriched.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_location)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .start(target_path)
)
query.awaitTermination()

import json  # noqa: E402

rows_written = sum(p.numInputRows for p in query.recentProgress)
dbutils.notebook.exit(  # noqa: F821
    json.dumps({"batch_id": batch_id, "rows_written": rows_written})
)
