# Pipeline task-flow diagrams

Rendered task graphs for the Airflow DAGs in `orchestration/dags/`. GitHub
renders the Mermaid blocks below; the DAG source files link here rather than
embedding the diagrams, because a `.py` docstring never renders on GitHub and
the Sphinx `.. mermaid::` directive needs a docs build this repo doesn't have.

## `daily_batch_pipeline` — orders + customers + products

Schedule: 02:00 UTC daily · SLA: 4 hours end-to-end.

```mermaid
flowchart TB
    S1[sensor: orders] --> B[bronze.expand]
    S2[sensor: customers] --> B
    S3[sensor: products] --> B
    B --> SiO[silver.orders]
    B --> SiC[silver.customers]
    B --> SiP[silver.products]
    SiC --> GD1[gold.dim_customer]
    SiP --> GD2[gold.dim_product]
    SiO --> GF1[gold.fact_orders]
    GD1 --> GF1
    GD2 --> GF1
    SiO --> GF2[gold.fact_order_lifecycle]
    GD1 --> SLD1[snowflake.dim_customer_load]
    GD2 --> SLD2[snowflake.dim_product_load]
    GF1 --> SLF[snowflake.orders_load]
    GF2 --> SLF
    SLF --> DQ1[dq.row_count]
    SLF --> DQ2[dq.orphan_rate]
```

## `hourly_clickstream_pipeline` — clickstream sessionization

Schedule: @hourly · SLA: 15 minutes end-to-end.

```mermaid
flowchart LR
    S[S3KeySensor: events.json for this hour]
        --> B[emit_batch_id]
        --> BR[bronze.clickstream]
        --> SI[silver.clickstream]
        --> GO[gold.fact_sessions]
        --> P[publish DATASET_FACT_SESSIONS outlet]
```

The Snowpipe + Streams + Tasks pipeline downstream of S3 gold runs
autonomously in Snowflake — Airflow does not manage it (it's S3-event-driven
and time-driven within Snowflake). See
`snowflake/ddl/50_snowpipe_streams_tasks.sql` for the full Snowflake-side flow.
