# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Customers
# MAGIC
# MAGIC Streaming ingestion of `raw/customers/year=.../month=.../day=.../customers.parquet`
# MAGIC into the Bronze Delta table via Auto Loader (`cloudFiles`).
# MAGIC
# MAGIC ## Contract
# MAGIC
# MAGIC - **Source**: `<bucket>/raw/customers/` (partitioned by year/month/day;
# MAGIC   one current-state snapshot per customer per day)
# MAGIC - **Sink**: `<bucket>/bronze/customers/` (Delta, append-only)
# MAGIC - **Schema evolution**: `addNewColumns` — new source columns appear
# MAGIC   automatically; column removal is not auto-applied.
# MAGIC - **Lineage columns added**: `_ingestion_timestamp`, `_source_file`,
# MAGIC   `_batch_id` (see `libs.bronze.add_bronze_metadata`).
# MAGIC - **Trigger**: `availableNow=True` — process all currently-available
# MAGIC   files in one pass, then stop. Airflow re-invokes the job per run.
# MAGIC
# MAGIC Downstream, `silver_customers` filters on `_batch_id`, applies
# MAGIC CUSTOMERS_RULES, and MERGEs the current state per `customer_id`;
# MAGIC `gold_dim_customer` materialises the SCD2 history from that stream.

# COMMAND ----------

import sys
from pathlib import Path

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
source_path = layer_root(bucket, cfg["paths"]["raw_prefix"], "customers")
target_path = layer_root(bucket, cfg["paths"]["bronze_prefix"], "customers")
schema_location = checkpoint_path(
    bucket, cfg["paths"]["checkpoint_prefix"], "customers", "bronze_schema"
)
checkpoint_location = checkpoint_path(
    bucket, cfg["paths"]["checkpoint_prefix"], "customers", "bronze"
)

print(f"source = {source_path}")
print(f"target = {target_path}")

# COMMAND ----------

src_stream = (
    spark.readStream.format("cloudFiles")  # noqa: F821
    .options(**autoloader_options(schema_location=schema_location, file_format="parquet"))
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

import json  # noqa: E402

rows_written = sum(p.numInputRows for p in query.recentProgress)
print(f"rows_written={rows_written} batch_id={batch_id}")
dbutils.notebook.exit(  # noqa: F821
    json.dumps({"batch_id": batch_id, "rows_written": rows_written})
)
