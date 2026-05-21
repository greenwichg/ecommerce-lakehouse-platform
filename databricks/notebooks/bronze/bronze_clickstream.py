# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Clickstream
# MAGIC
# MAGIC Auto Loader streaming ingestion of `raw/clickstream/year=/month=/day=/hour=/events.json`
# MAGIC into the Bronze Delta table.
# MAGIC
# MAGIC ## Why this is the first source to *really* exercise Auto Loader
# MAGIC
# MAGIC Orders / customers / products are stable schemas — Auto Loader's
# MAGIC `addNewColumns` evolution mode never has to do anything. Clickstream is
# MAGIC the first source where the tracking team plausibly adds a new event
# MAGIC attribute mid-quarter (e.g., `experiment_variant_id`). When that
# MAGIC happens, Auto Loader picks it up automatically; downstream notebooks
# MAGIC don't need a redeploy.
# MAGIC
# MAGIC ## Hourly cadence
# MAGIC
# MAGIC Triggered hourly by `hourly_clickstream_pipeline.py` via
# MAGIC `DatabricksRunNowOperator(deferrable=True)`. The trigger is
# MAGIC `availableNow=True`: process all currently-available files and stop,
# MAGIC same exactly-once-file-processing guarantee as Slice 1's bronze
# MAGIC notebooks.

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
raw_prefix = cfg["paths"]["raw_prefix"]
bronze_prefix = cfg["paths"]["bronze_prefix"]
checkpoint_prefix = cfg["paths"]["checkpoint_prefix"]

source_path = layer_root(bucket, raw_prefix, "clickstream")
target_path = layer_root(bucket, bronze_prefix, "clickstream")
schema_location = checkpoint_path(bucket, checkpoint_prefix, "clickstream", "bronze_schema")
checkpoint_location = checkpoint_path(bucket, checkpoint_prefix, "clickstream", "bronze")

# COMMAND ----------

src_stream = (
    spark.readStream.format("cloudFiles")  # noqa: F821
    .options(**autoloader_options(schema_location=schema_location, file_format="json"))
    .load(source_path)
)

enriched = add_bronze_metadata(
    src_stream,
    batch_id=batch_id,
    source_file_col="_metadata.file_path",
)

# COMMAND ----------

query = (
    enriched.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_location)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .start(target_path)
)
query.awaitTermination()

# COMMAND ----------

rows_written = sum(p.numInputRows for p in query.recentProgress)
import json  # noqa: E402

dbutils.notebook.exit(  # noqa: F821
    json.dumps({"batch_id": batch_id, "rows_written": rows_written})
)
