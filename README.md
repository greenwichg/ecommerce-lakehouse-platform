# Ecommerce Lakehouse Platform

Production-grade ecommerce lakehouse: synthetic generators → S3 → Databricks medallion (Bronze/Silver/Gold Delta) → Snowflake star schema → Streamlit dashboard, orchestrated by Airflow, with Terraform-managed AWS infra and a Step Functions human-in-the-loop quarantine workflow.

A portfolio project demonstrating production patterns end-to-end: schema-evolving Auto Loader, MERGE-based dedup, gap-and-island sessionization, SCD Type 2 dimensions, PIT-correct surrogate joins, transactional + accumulating snapshot + factless facts, deferrable orchestration with dataset triggering, IAM least-privilege, container-image Lambdas, `waitForTaskToken` operator workflows, cost guardrails, and CI-gated tests.

## Status — complete

Built in seven vertical slices — one source flowing end-to-end before adding the next. **313 passing tests** (288 from Slices 1–5 + 25 new dashboard / demo tests). Lint / fmt all clean.

| Slice | Scope | Status |
|-------|-------|--------|
| 0 | Project scaffolding, CI lint, layered configs | ✅ done |
| 1 | **Orders** end-to-end: generator → S3 → Bronze → Silver → Gold → Snowflake → Airflow → tests | ✅ done |
| 2 | **Customers + Products** (SCD2 dimensions, PIT-correct surrogate joins) | ✅ done |
| 3 | **Clickstream** + hourly DAG + sessionization + Snowpipe/Streams/Tasks + Dataset triggering | ✅ done |
| 4 | **Currency rates** + customer_ltv mart + wishlist factless fact + MV | ✅ done |
| 5 | **Infra hardening**: Terraform (7 modules), validator Lambda (Container Image), quarantine-replay Step Functions, IAM least-privilege, Snowflake resource monitors, weekly maintenance DAG + cost report | ✅ done |
| 6 | **Streamlit dashboard** (4 widgets + sidebar), CI/CD (test.yml + deploy.yml), docs (architecture, runbook, data_model, demo_failure_scenario), end-to-end demo script | ✅ done |

## Architecture

```mermaid
flowchart LR
    G[Generators<br/>Python + Faker] --> S3R[(S3<br/>raw/source/year=…)]
    S3R -->|S3 → SQS → Lambda<br/>file_validator| V{Schema<br/>valid?}
    V -->|yes| B[Bronze Delta<br/>Auto Loader]
    V -->|no| Q[(quarantine/)]
    Q -->|SNS alert| SFN[Step Functions<br/>waitForTaskToken]
    SFN -->|operator: replay| S3R
    SFN -->|operator: discard| AUD[audit log]
    SFN -->|operator: fix-and-replay| S3R
    B -->|MERGE dedup<br/>+ DQ gate| SI[Silver Delta]
    SI -->|SCD2 + PIT joins| GO[Gold Delta<br/>star schema]
    GO -->|COPY INTO + MERGE| SF[(Snowflake<br/>RAW → STAGING → ANALYTICS)]
    SF -->|MV refresh| MV[(Materialized<br/>view)]
    SF --> ST[Streamlit<br/>dashboard]
    SF --> CW[CloudWatch<br/>dashboards + alarms]
    A[Airflow 2.10<br/>deferrable] -. daily / hourly / weekly .-> B & SI & GO & SF
```

The four key story arcs in one diagram:

1. **The happy path** (top half): generators → Bronze → Silver → Gold → Snowflake → BI.
2. **The unhappy path** (left middle): the validator quarantines a bad file before it pollutes Bronze.
3. **The recovery path** (left top→middle): operator decides via SFN, the file gets replayed.
4. **The observability path** (right): Streamlit + CloudWatch read the same provenance columns that Bronze/Silver write.

## Tech stack

| Layer | Tool | Version |
|-------|------|---------|
| Language | CPython | 3.11 |
| Generators | Faker, pandas, pyarrow | latest stable |
| Lake format | Delta Lake | 3.2.x |
| Compute | Databricks Runtime / Apache Spark | 15.x LTS / 3.5.x |
| Warehouse | Snowflake | account-managed |
| Orchestrator | Apache Airflow | 2.10.x |
| IaC | Terraform | 1.7+ |
| Dashboard | Streamlit | 1.36+ |
| CI | GitHub Actions | — |
| Lint | ruff, black, sqlfluff | per `pyproject.toml` / `.sqlfluff` |
| Test | pytest, chispa | per `pyproject.toml` |

## Repository layout

```
.
├── orchestration/         Airflow DAGs (named non-"airflow" to avoid
│   ├── dags/              shadowing the airflow PyPI package on sys.path)
│   └── tests/
├── config/                Layered YAML configs (base + per-env)
├── databricks/
│   ├── libs/              Importable Python modules (shared transform logic)
│   ├── notebooks/         {bronze,silver,gold}/ notebooks
│   └── tests/             pytest + chispa tests for libs
├── data/                  Local dev data (gitignored except mock/)
├── docs/                  Architecture + runbook
├── generators/            Source data generators
├── infrastructure/        Terraform (Slice 5)
├── snowflake/             DDL, DML, SQL tests
├── streamlit_app/         Dashboard (Slice 6)
├── tests/                 Generator tests + cross-cutting tests
├── conftest.py            Root conftest: sys.path + eager imports
├── PLAN.md                Implementation plan
└── README.md              This file
```

## Setup (local dev)

Requires Python 3.11.

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e ".[dev]"          # generators + lint + test
# Optional extras as you exercise each layer:
pip install -e ".[spark]"        # Databricks libs unit tests
pip install -e ".[airflow]"      # DAG import validation
pip install -e ".[streamlit]"    # dashboard
```

Set the dev env vars:

```bash
export PROJECT_ROOT="$(pwd)"
export LAKEHOUSE_ENV=dev
```

## Running

### Generators

```bash
python -m generators.orders --start-date 2025-05-01 --end-date 2025-05-07
```

Outputs land under `data/raw/orders/year=YYYY/month=MM/day=DD/orders.parquet`.

### Unit tests

```bash
pytest                                  # all tests
pytest tests/generators                 # just generator tests
pytest -m "not spark"                   # skip Spark-dependent tests
```

### Airflow DAG validation

```bash
pip install -e ".[airflow]"
export AIRFLOW_HOME="$(pwd)/.airflow"
airflow db migrate
airflow dags list-import-errors          # must be empty
airflow dags test daily_batch_pipeline 2025-05-01
```

The DAG-import tests in `orchestration/tests/test_dag_imports.py` cover
the same ground without needing an initialised Airflow metadata DB:

```bash
pytest orchestration/tests -q
```

### Streamlit dashboard

```bash
pip install -e ".[streamlit]"
streamlit run streamlit_app/app.py
```

Default is **mock mode** — reads `data/mock/*.parquet` so all four widgets render without any cloud credentials. Set `LAKEHOUSE_DASHBOARD_MODE=live` plus the `SNOWFLAKE_*` / `AWS_*` / `AIRFLOW_*` env vars to query real systems. The dashboard gracefully falls back to mock with a banner if live is requested but credentials are missing.

### Demo failure scenario

```bash
python demo/inject_bad_file.py             # quarantine flow only
python demo/inject_bad_file.py --auto-replay  # full quarantine → operator → replay loop
```

Runs entirely in-process against moto. Walks through the malformed-file → validator → quarantine → Step Functions operator-decision flow → helper-Lambda replay path. See [`docs/demo_failure_scenario.md`](docs/demo_failure_scenario.md) for expected output at each step.

### Local end-to-end (Slice 1)

See [`docs/runbook.md`](docs/runbook.md) → "Running end-to-end locally".

### Cloud end-to-end

See [`docs/runbook.md`](docs/runbook.md) → "Running end-to-end in cloud".
Requires AWS, Databricks, and Snowflake accounts with credentials in
the appropriate secret stores.

## Configuration

`config/base.yaml` holds shared defaults; `config/<env>.yaml` layers
environment-specific overrides on top. `${VAR_NAME}` references are
resolved against the process environment at load time.

```python
from libs.config import load_config
cfg = load_config(env="dev")
```

Secrets (Databricks PAT, Snowflake creds) never live in config files —
they're sourced from environment variables in dev and AWS Secrets
Manager in prod via Airflow's secrets backend.

## Cost estimates

Per-component cost notes appear in `docs/architecture.md` as each
slice lands. Headline assumptions:

- S3 raw stays "Standard" for 90 days, then transitions to Glacier
  Flexible Retrieval (lifecycle policy in `infrastructure/terraform/`).
- Databricks jobs use job clusters (not all-purpose) for spot-priced
  ephemeral compute.
- Snowflake serving uses an XS warehouse with 60s auto-suspend; per-
  warehouse + account-level resource monitors cap spend with NOTIFY
  at 75%/90% and SUSPEND at 100% (see
  `snowflake/ddl/01_resource_monitors.sql`).
- Quarantine review uses Step Functions `waitForTaskToken` for the
  7-day operator-decision window — zero compute cost while waiting,
  vs ~$X/hour for an Airflow worker slot held open over the same span.

## Infrastructure

Slice 5 added a Terraform tree under `infrastructure/terraform/`:

```
infrastructure/terraform/
├── main.tf              # top-level wiring; thin
├── variables.tf         # env, bucket name, retention, etc.
├── environments/        # dev.tfvars / prod.tfvars
└── modules/
    ├── s3/              # bucket + KMS + lifecycle
    ├── sns/             # alerts topic + email subscription
    ├── cloudwatch/      # log groups + metric alarms + dashboard
    ├── secrets/         # 4 placeholder secrets (databricks, snowflake, exchangerate, airflow)
    ├── iam/             # 5 least-privilege roles + walkthrough README
    ├── lambda_validator/  # SQS + DLQ + Container Image Lambda
    └── step_functions/  # ASL JSON + quarantine_helper Lambda
```

The state machine implements a three-branch human-in-the-loop
quarantine workflow (replay / discard / fix-and-replay) — see
`infrastructure/terraform/modules/step_functions/README.md` for the
scenario walkthrough and the why-not-Airflow cost comparison.

## Documentation

- [`PLAN.md`](PLAN.md) — vertical slice plan, deviations, and project-complete summary
- [`docs/architecture.md`](docs/architecture.md) — the "why" decisions, every slice
- [`docs/data_model.md`](docs/data_model.md) — star schema, grains, SCD strategy, surrogate keys
- [`docs/pipelines.md`](docs/pipelines.md) — rendered task-flow diagrams for the Airflow DAGs
- [`docs/runbook.md`](docs/runbook.md) — failure modes, backfill, replay, escalation, quarantine workflow
- [`docs/demo_failure_scenario.md`](docs/demo_failure_scenario.md) — end-to-end drill: bad file → quarantine → operator → replay
- `infrastructure/terraform/modules/*/README.md` — per-module deep dives (IAM walkthrough, Step Functions scenario, etc.)
