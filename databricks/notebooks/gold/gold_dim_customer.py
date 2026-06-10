# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — dim_customer (SCD2)
# MAGIC
# MAGIC Materialises the SCD2 history of customers from silver's "current
# MAGIC state per customer_id" snapshot. Tracked attributes (changes trigger
# MAGIC a new version): ``email``, ``address``. Stable attributes carried
# MAGIC without versioning: ``name``, ``signup_date``.
# MAGIC
# MAGIC OPTIMIZE ZORDER BY (customer_id) — the dashboard's hot path is
# MAGIC "current row for a given customer_id" (`WHERE customer_id = X AND
# MAGIC is_current = true`); Z-ordering by customer_id lets micro-partition
# MAGIC pruning skip irrelevant files.

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
from libs.gold import optimize_zorder  # noqa: E402
from libs.paths import layer_root  # noqa: E402
from libs.scd2 import apply_scd2_merge  # noqa: E402

cfg = load_config(env=env)
bucket = get_path(cfg, "storage.bucket")
silver_path = layer_root(bucket, cfg["paths"]["silver_prefix"], "customers")
gold_path = layer_root(bucket, cfg["paths"]["gold_prefix"], "dim_customer")
print(f"silver = {silver_path}")
print(f"gold   = {gold_path}")

# COMMAND ----------

silver = spark.read.format("delta").load(silver_path)  # noqa: F821

apply_scd2_merge(
    spark=spark,  # noqa: F821
    source_df=silver,
    target_path=gold_path,
    natural_key="customer_id",
    attribute_cols=["name", "email", "address", "signup_date"],
    tracked_cols=["email", "address"],
    sk_col="customer_sk",
    ts_col="updated_at",
)
row_count = spark.read.format("delta").load(gold_path).count()  # noqa: F821
print(f"dim_customer rows: {row_count}")

# COMMAND ----------

optimize_zorder(spark, gold_path, ["customer_id"])  # noqa: F821

# COMMAND ----------

import json  # noqa: E402

dbutils.notebook.exit(json.dumps({"batch_id": batch_id, "row_count": row_count}))  # noqa: F821
