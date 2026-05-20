# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Orders
# MAGIC
# MAGIC Streaming ingestion of `raw/orders/year=.../month=.../day=.../orders.parquet`
# MAGIC into the Bronze Delta table via Auto Loader (`cloudFiles`).
# MAGIC
# MAGIC ## Contract
# MAGIC
# MAGIC - **Source**: `<bucket>/raw/orders/` (partitioned by year/month/day)
# MAGIC - **Sink**: `<bucket>/bronze/orders/` (Delta, append-only)
# MAGIC - **Schema evolution**: `addNewColumns` — new source columns appear
# MAGIC   automatically; column removal is not auto-applied.
# MAGIC - **Lineage columns added**: `_ingestion_timestamp`, `_source_file`,
# MAGIC   `_batch_id` (see `libs.bronze.add_bronze_metadata`).
# MAGIC - **Trigger**: `availableNow=True` — process all currently-available
# MAGIC   files in one pass, then stop. Airflow re-invokes the job per run.
# MAGIC
# MAGIC ## Idempotency
# MAGIC
# MAGIC Auto Loader's checkpoint tracks processed files; re-running this
# MAGIC notebook is a no-op if no new files have appeared since last run.

# COMMAND ----------

import sys
from pathlib import Path

# When deployed, the libs directory is uploaded alongside this notebook and
# appended to sys.path so `from libs.X import Y` works. In local notebook
# development on Databricks Repos, the workspace root is already on the path.
_notebook_dir = Path.cwd()
_libs_root = _notebook_dir.parent.parent  # databricks/
if (_libs_root / "libs").is_dir() and str(_libs_root) not in sys.path:
    sys.path.insert(0, str(_libs_root))

# COMMAND ----------

dbutils.widgets.text("env", "dev", "Environment (dev/prod)")  # noqa: F821
dbutils.widgets.text("batch_id", "", "Batch ID (Airflow XCom)")  # noqa: F821

env = dbutils.widgets.get("env")  # noqa: F821
batch_id = dbutils.widgets.get("batch_id")  # noqa: F821
if not batch_id:
    from libs.batch import make_batch_id

    batch_id = make_batch_id()
print(f"env={env} batch_id={batch_id}")

# COMMAND ----------

from libs.bronze import add_bronze_metadata, autoloader_options  # noqa: E402
from libs.config import get_path, load_config  # noqa: E402
from libs.paths import checkpoint_path, layer_root  # noqa: E402

cfg = load_config(env=env)
bucket = get_path(cfg, "storage.bucket")
raw_prefix = cfg["paths"]["raw_prefix"]
bronze_prefix = cfg["paths"]["bronze_prefix"]
checkpoint_prefix = cfg["paths"]["checkpoint_prefix"]

source_path = layer_root(bucket, raw_prefix, "orders")
target_path = layer_root(bucket, bronze_prefix, "orders")
schema_location = checkpoint_path(bucket, checkpoint_prefix, "orders", "bronze_schema")
checkpoint_location = checkpoint_path(bucket, checkpoint_prefix, "orders", "bronze")

print(f"source = {source_path}")
print(f"target = {target_path}")
print(f"schema = {schema_location}")
print(f"checkp = {checkpoint_location}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream definition
# MAGIC
# MAGIC `cloudFiles.schemaEvolutionMode = addNewColumns` means the first run
# MAGIC infers types from data; subsequent runs reuse the persisted schema
# MAGIC unless new columns appear, in which case they're added (the stream
# MAGIC restarts; Airflow's retry policy handles that).

# COMMAND ----------

src_stream = (
    spark.readStream.format("cloudFiles")  # noqa: F821
    .options(**autoloader_options(schema_location=schema_location, file_format="parquet"))
    .load(source_path)
)

# Attach lineage. _source_file comes from Auto Loader's _metadata struct,
# which is present on every row of a cloudFiles stream.
enriched = add_bronze_metadata(
    src_stream,
    batch_id=batch_id,
    source_file_col="_metadata.file_path",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sink: Delta with mergeSchema
# MAGIC
# MAGIC `trigger(availableNow=True)` processes any pending files and stops —
# MAGIC equivalent to a batch job but lets us reuse the streaming checkpoint
# MAGIC machinery. Airflow's `DatabricksRunNowOperator` waits for completion.

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

# Surface row count + batch_id back to Airflow via job exit value
rows_written = sum(p.numInputRows for p in query.recentProgress)
print(f"rows_written={rows_written} batch_id={batch_id}")
dbutils.notebook.exit(  # noqa: F821
    f'{{"batch_id":"{batch_id}","rows_written":{rows_written}}}'
)
