# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — customer_ltv mart
# MAGIC
# MAGIC Per-customer lifetime aggregates from fact_orders. Lives in
# MAGIC Databricks Gold (not as a Snowflake MV) because the avg_days_between_
# MAGIC orders calculation uses window functions (LAG over per-customer
# MAGIC timeline), which disqualify it from Snowflake's MV restrictions.
# MAGIC
# MAGIC Full-refresh per run — small target (10k customers), refresh cost
# MAGIC is in the noise relative to fact_orders' own computation.

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
from libs.marts import build_customer_ltv, write_mart_overwrite  # noqa: E402
from libs.paths import layer_root  # noqa: E402

cfg = load_config(env=env)
bucket = get_path(cfg, "storage.bucket")
fact_orders_path = layer_root(bucket, cfg["paths"]["gold_prefix"], "fact_orders")
gold_path = layer_root(bucket, cfg["paths"]["gold_prefix"], "customer_ltv")

fact_orders = spark.read.format("delta").load(fact_orders_path)  # noqa: F821
ltv = build_customer_ltv(fact_orders)
write_mart_overwrite(ltv, gold_path)

optimize_zorder(spark, gold_path, ["customer_sk"])  # noqa: F821

import json  # noqa: E402

row_count = spark.read.format("delta").load(gold_path).count()  # noqa: F821
dbutils.notebook.exit(json.dumps({"batch_id": batch_id, "row_count": row_count}))  # noqa: F821
