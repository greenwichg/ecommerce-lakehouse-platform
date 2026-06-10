# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Products
# MAGIC
# MAGIC Auto Loader CSV ingest of `raw/products/year=.../products.csv` into
# MAGIC the Bronze Delta table. The products generator writes CSV (per the
# MAGIC Slice 2 spec — exercising a second file format through the same
# MAGIC pipeline shape); Auto Loader infers column types from the header +
# MAGIC data on first contact and persists them to the schema location.
# MAGIC
# MAGIC ## Contract
# MAGIC
# MAGIC - **Source**: `<bucket>/raw/products/` (daily snapshots, CSV with header)
# MAGIC - **Sink**: `<bucket>/bronze/products/` (Delta, append-only)
# MAGIC - **Schema evolution**: `addNewColumns`
# MAGIC - **Lineage columns added**: `_ingestion_timestamp`, `_source_file`,
# MAGIC   `_batch_id`
# MAGIC - **Trigger**: `availableNow=True`; Airflow re-invokes per run
# MAGIC
# MAGIC Downstream, `silver_products` filters on `_batch_id`, applies
# MAGIC PRODUCTS_RULES, and MERGEs current state per `product_id`;
# MAGIC `gold_dim_product` materialises the SCD2 history.

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
print(f"env={env} batch_id={batch_id}")

# COMMAND ----------

from libs.bronze import add_bronze_metadata, autoloader_options  # noqa: E402
from libs.config import get_path, load_config  # noqa: E402
from libs.paths import checkpoint_path, layer_root  # noqa: E402

cfg = load_config(env=env)
bucket = get_path(cfg, "storage.bucket")
source_path = layer_root(bucket, cfg["paths"]["raw_prefix"], "products")
target_path = layer_root(bucket, cfg["paths"]["bronze_prefix"], "products")
schema_location = checkpoint_path(
    bucket, cfg["paths"]["checkpoint_prefix"], "products", "bronze_schema"
)
checkpoint_location = checkpoint_path(
    bucket, cfg["paths"]["checkpoint_prefix"], "products", "bronze"
)

print(f"source = {source_path}")
print(f"target = {target_path}")

# COMMAND ----------

src_stream = (
    spark.readStream.format("cloudFiles")  # noqa: F821
    .options(**autoloader_options(schema_location=schema_location, file_format="csv"))
    .option("header", "true")
    .load(source_path)
)
enriched = add_bronze_metadata(src_stream, batch_id=batch_id, source_file_col="_metadata.file_path")

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
