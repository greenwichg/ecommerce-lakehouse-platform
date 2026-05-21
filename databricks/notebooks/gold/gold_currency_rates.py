# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Currency Rates
# MAGIC
# MAGIC Currency rates are a reference table — silver IS gold in shape. This
# MAGIC notebook is a thin overwrite copy that lands in the gold path so
# MAGIC Snowflake's external stage reads from the gold prefix, consistent
# MAGIC with every other source. Re-emits the `_source` provenance column.

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

cfg = load_config(env=env)
bucket = get_path(cfg, "storage.bucket")
silver_path = layer_root(bucket, cfg["paths"]["silver_prefix"], "currency_rates")
gold_path = layer_root(bucket, cfg["paths"]["gold_prefix"], "currency_rates")

# Full refresh — tiny table.
silver = spark.read.format("delta").load(silver_path)  # noqa: F821
silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_path)

import json  # noqa: E402

dbutils.notebook.exit(  # noqa: F821
    json.dumps({"batch_id": batch_id, "row_count": silver.count()})
)
