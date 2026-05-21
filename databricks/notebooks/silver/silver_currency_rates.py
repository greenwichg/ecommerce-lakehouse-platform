# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Currency Rates
# MAGIC
# MAGIC Tiny pass-through: DQ rules + MERGE on natural key (rate_date,
# MAGIC target_currency). No SCD2 needed — rate_date IS the version. The
# MAGIC `_source` column from bronze carries through unchanged so the
# MAGIC Airflow freshness gate can distinguish API-fetched from simulated
# MAGIC rates downstream.

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
from libs.quality import CURRENCY_RATES_RULES, split_quarantine  # noqa: E402
from libs.silver import (  # noqa: E402
    append_quarantine,
    dedup_by_key,
    ensure_silver_table,
    merge_into_silver,
)
from pyspark.sql import functions as F  # noqa: E402

cfg = load_config(env=env)
bucket = get_path(cfg, "storage.bucket")
bronze_path = layer_root(bucket, cfg["paths"]["bronze_prefix"], "currency_rates")
silver_path = layer_root(bucket, cfg["paths"]["silver_prefix"], "currency_rates")
quarantine_path = layer_root(bucket, cfg["paths"]["quarantine_prefix"], "currency_rates")

# COMMAND ----------

bronze = spark.read.format("delta").load(bronze_path)  # noqa: F821
batch_df = (
    bronze.filter(F.col("_batch_id") == batch_id)
    .withColumn("rate_date", F.col("rate_date").cast("date"))
    .withColumn("rate", F.col("rate").cast("double"))
    .withColumn("_fetched_at", F.col("_fetched_at").cast("timestamp"))
)

good, bad = split_quarantine(batch_df, CURRENCY_RATES_RULES)
if bad.count() > 0:
    append_quarantine(bad, quarantine_path)

# Dedup on (rate_date, target_currency); same (date, currency) appearing
# twice (e.g., a re-run) keeps the latest by _fetched_at.
deduped = dedup_by_key(good, ["rate_date", "target_currency"], "_fetched_at")

ensure_silver_table(spark, silver_path, deduped)  # noqa: F821
merge_into_silver(
    spark=spark,  # noqa: F821
    source_df=deduped,
    target_path=silver_path,
    merge_keys=["rate_date", "target_currency"],
    timestamp_col="_fetched_at",
)

import json  # noqa: E402

dbutils.notebook.exit(  # noqa: F821
    json.dumps({"batch_id": batch_id, "row_count": deduped.count()})
)
