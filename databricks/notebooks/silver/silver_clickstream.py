# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Clickstream (sessionize + quarantine)
# MAGIC
# MAGIC ```
# MAGIC bronze.clickstream (raw events + lineage)
# MAGIC     │   filter by _batch_id
# MAGIC     ▼
# MAGIC split_quarantine(CLICKSTREAM_RULES)
# MAGIC     │
# MAGIC     ├── bad  → quarantine.clickstream
# MAGIC     │
# MAGIC     └── good
# MAGIC          │   sessionize_events (gap-and-island, 30-min threshold)
# MAGIC          ▼
# MAGIC          MERGE INTO silver.clickstream ON event_id
# MAGIC ```
# MAGIC
# MAGIC ## Watermark semantics (READ ME before changing)
# MAGIC
# MAGIC The spec's "10-minute watermark" is NOT a current-batch filter that
# MAGIC drops late events. We run batch with `availableNow=True`, not
# MAGIC structured streaming — and in batch mode, every event in the current
# MAGIC bronze batch must be processed, no matter how late it arrived.
# MAGIC
# MAGIC The 10-minute window means: when the NEXT batch arrives, any session
# MAGIC whose end is within 10 minutes of THIS batch's `max(event_ts)` may
# MAGIC still grow — i.e., events from this batch's tail-end could land in
# MAGIC the next batch and extend the same session. The Silver MERGE handles
# MAGIC that naturally because session re-emission with a later session_end
# MAGIC wins over the previous version.
# MAGIC
# MAGIC So in practice this notebook does NOT call `apply_watermark()`. The
# MAGIC watermark concept matters operationally (for runbook escalation:
# MAGIC "an event arriving > 10 min after its session ended is anomalous")
# MAGIC but it doesn't filter data here. Documented this misconception
# MAGIC trap explicitly because we hit it during design.

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
from libs.quality import CLICKSTREAM_RULES, split_quarantine  # noqa: E402
from libs.sessionize import sessionize_events  # noqa: E402
from libs.silver import (  # noqa: E402
    append_quarantine,
    ensure_silver_table,
    merge_into_silver,
)

cfg = load_config(env=env)
bucket = get_path(cfg, "storage.bucket")
bronze_path = layer_root(bucket, cfg["paths"]["bronze_prefix"], "clickstream")
silver_path = layer_root(bucket, cfg["paths"]["silver_prefix"], "clickstream")
quarantine_path = layer_root(bucket, cfg["paths"]["quarantine_prefix"], "clickstream")

# COMMAND ----------

from pyspark.sql import functions as F  # noqa: E402

bronze = spark.read.format("delta").load(bronze_path)  # noqa: F821
batch_df = bronze.filter(F.col("_batch_id") == batch_id)
# Cast event_ts from JSON's string to TIMESTAMP. Auto Loader infers it as
# string by default; the cast lets downstream window functions work.
batch_df = batch_df.withColumn("event_ts", F.col("event_ts").cast("timestamp"))
input_count = batch_df.count()

# COMMAND ----------

good, bad = split_quarantine(batch_df, CLICKSTREAM_RULES)
bad_count = bad.count()
good_count = good.count()
if bad_count > 0:
    append_quarantine(bad, quarantine_path)

# COMMAND ----------

# Sessionize (no watermark filter — see docstring above)
sessionized = sessionize_events(good)

# COMMAND ----------

ensure_silver_table(spark, silver_path, sessionized)  # noqa: F821
# Silver merge key is event_id. Each event is immutable; if the same
# event_id appears in two batches (replay), the MERGE no-ops.
merge_into_silver(
    spark=spark,  # noqa: F821
    source_df=sessionized,
    target_path=silver_path,
    merge_keys=["event_id"],
    timestamp_col="event_ts",
)

# COMMAND ----------

import json  # noqa: E402

dbutils.notebook.exit(  # noqa: F821
    json.dumps(
        {
            "batch_id": batch_id,
            "input_count": input_count,
            "good_count": good_count,
            "bad_count": bad_count,
        }
    )
)
