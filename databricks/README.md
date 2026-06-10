# Databricks — medallion transforms

```
databricks/
├── bundle.yml      Databricks Asset Bundle: one job per notebook, dev/prod targets
├── libs/           Importable transform logic (pure functions; unit-tested)
├── notebooks/      {bronze,silver,gold}/ job notebooks (thin wrappers over libs/)
└── tests/          pytest + chispa suites for libs/ (local SparkSession + Delta)
```

## Bundle layout

`bundle.yml` defines the 20 ETL jobs the Airflow DAGs trigger — six
bronze (one per source), six silver, eight gold. Each job is a single
notebook task on an ephemeral spot-first job cluster (cost model:
`docs/architecture.md`). The shared cluster spec lives on the first job
as a YAML anchor (`&etl_cluster`); every other job aliases it.

Targets:

- `dev` (default) — `mode: development`: deploys under the deploying
  user's workspace folder with name prefixes, safe to iterate on.
- `prod` — `mode: production`: deploys to
  `/Shared/.bundle/prod/ecommerce-lakehouse-platform`.

## Deploying

CI path: trigger the **Deploy** workflow (`deploy.yml`) with the
`databricks` component selected. It runs, from this directory:

```bash
databricks bundle validate --target <env>
databricks bundle deploy   --target <env>
```

with `DATABRICKS_HOST` / `DATABRICKS_TOKEN` from repository secrets.
Local deploys use the same two commands with a configured CLI profile.

## Wiring job IDs into Airflow

The DAGs look up job IDs from Airflow Variables named
`databricks_job_<job-key>` (e.g. `databricks_job_bronze_orders`,
`databricks_job_gold_fact_sessions`) — the bundle's job keys match the
variable suffixes one-to-one. After a first deploy to a workspace:

```bash
databricks jobs list -o json | jq -r '.[] | "\(.settings.name)\t\(.job_id)"'
# then, per job:
airflow variables set databricks_job_bronze_orders <job_id>
```

The `env` notebook parameter defaults to the bundle target; `batch_id`
is supplied per run by Airflow's `DatabricksRunNowOperator`
(`notebook_params`), falling back to a generated ID for manual runs.

## Notebook ↔ libs contract

Notebooks contain no transform logic — they read widgets, resolve paths
from the layered config (`libs.config`), call the pure functions in
`libs/`, and exit with a JSON payload for Airflow. That keeps the logic
unit-testable without a Databricks workspace: `databricks/tests/` runs
against a local SparkSession with `delta-spark`.
