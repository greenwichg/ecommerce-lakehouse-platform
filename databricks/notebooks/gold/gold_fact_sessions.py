# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — fact_sessions
# MAGIC
# MAGIC Aggregates sessionized silver events into one row per session_key.
# MAGIC Output is the natural-key fact (no `customer_sk`). The Snowflake-side
# MAGIC Streams + Tasks pipeline does the PIT customer enrichment after
# MAGIC Snowpipe auto-ingest — see `snowflake/ddl/snowpipe_streams_tasks.sql`
# MAGIC for the parallel pattern; the design intentionally mirrors the
# MAGIC Databricks SCD2 PIT join used for fact_orders so reviewers can
# MAGIC compare both flavors of the same enrichment.
# MAGIC
# MAGIC Per the orphan-rate gate design: the gate filters for "sessions
# MAGIC WITH attributed_customer_id where the binding failed" — anonymous
# MAGIC sessions are *expected* to have NULL customer attribution and
# MAGIC must not trip the gate.

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
from libs.gold import ensure_gold_table, merge_into_gold, optimize_zorder  # noqa: E402
from libs.paths import layer_root  # noqa: E402
from libs.sessionize import build_fact_sessions  # noqa: E402

cfg = load_config(env=env)
bucket = get_path(cfg, "storage.bucket")
silver_path = layer_root(bucket, cfg["paths"]["silver_prefix"], "clickstream")
gold_path = layer_root(bucket, cfg["paths"]["gold_prefix"], "fact_sessions")

# COMMAND ----------

silver = spark.read.format("delta").load(silver_path)  # noqa: F821
fact = build_fact_sessions(silver)

# COMMAND ----------

ensure_gold_table(spark, gold_path, fact)  # noqa: F821
# Merge key = silver_session_key. updated_at = session_end, so a session
# that grew in a later batch (event arrived after the original session_end)
# correctly overwrites the prior fact row.
merge_into_gold(
    spark=spark,  # noqa: F821
    source_df=fact,
    target_path=gold_path,
    merge_keys=["silver_session_key"],
    timestamp_col="updated_at",
)
row_count = spark.read.format("delta").load(gold_path).count()  # noqa: F821

# COMMAND ----------

optimize_zorder(spark, gold_path, ["silver_session_key"])  # noqa: F821

# COMMAND ----------

import json  # noqa: E402

dbutils.notebook.exit(json.dumps({"batch_id": batch_id, "row_count": row_count}))  # noqa: F821
