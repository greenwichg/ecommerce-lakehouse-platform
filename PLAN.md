# Ecommerce Lakehouse Platform — Implementation Plan

## 1. Project Understanding (in my own words)

The goal is to build a realistic, end-to-end ecommerce data platform that
demonstrates production-grade lakehouse patterns. The data flows like this:

```
Synthetic generators ──► S3 (raw, partitioned by date)
                              │
                              ▼
                    Databricks (medallion: Bronze → Silver → Gold, Delta)
                              │
                              ▼
                    Snowflake (RAW → STAGING → ANALYTICS, star schema)
                              │
                              ▼
                    Streamlit dashboard + CloudWatch monitoring
```

Airflow orchestrates the whole thing (daily batch, hourly clickstream,
weekly maintenance). Terraform provisions the AWS pieces. Everything is
parameterized, tested, and documented.

The platform must showcase several distinct competencies:

- **Generators**: realistic synthetic data with deliberate complications
  (late arrivals, updates, SCD-driving changes) so downstream logic has
  something to handle.
- **Bronze**: Auto Loader streaming ingestion, append-only, schema
  evolution.
- **Silver**: deduplication via MERGE, sessionization, DQ quarantine,
  watermarks for late data.
- **Gold**: SCD2 dimensions, transactional + accumulating snapshot facts,
  pre-aggregated marts.
- **Orchestration**: deferrable sensors, dynamic task mapping, datasets,
  DQ gates, SLA, alerting.
- **Infra**: least-privilege IAM, lifecycle policies, event-driven Lambda
  validation, Step Functions for DR (deliberately distinct from Airflow).
- **Serving**: Snowflake with stages, Snowpipe, Streams + Tasks, MVs,
  resource monitors.
- **Observability**: dashboards + a runbook with real failure scenarios.

The hardest constraint is that nothing can be placeholder — every piece
has to be a working implementation against either real cloud services or
documented local alternatives (LocalStack, docker-compose, Databricks
Community Edition).

## 2. Implementation Order (Vertical Slices)

I'll build vertically per source, end-to-end, so each slice produces
something demoable before the next one starts. Within each slice I'll
follow the test-as-you-go rule.

### Slice 0 — Scaffolding (foundation for everything else)
Repo layout, `config/` with env YAMLs, `requirements.txt` /
`pyproject.toml`, CI lint workflow, `.gitignore`, base `README.md`
skeleton. Nothing functional yet, but everything that follows has a home.

### Slice 1 — Orders end-to-end (the reference slice)
This is the most complex source (transactional, with updates and late
arrivals), so it forces every cross-cutting concern to surface early:

1. `generators/orders.py` writing partitioned Parquet to local
   `data/raw/orders/year=.../...` (S3-compatible path via fsspec)
2. Terraform skeleton + LocalStack S3 bucket for local dev
3. Databricks Bronze notebook for orders (Auto Loader)
4. Databricks Silver notebook for orders (MERGE dedup, DQ quarantine,
   watermark)
5. Databricks Gold (`fact_orders` + accumulating snapshot)
6. `databricks/libs/` shared modules + pytest + chispa unit tests
7. Snowflake stage + COPY INTO + MERGE for orders fact
8. Airflow DAG skeleton (`daily_batch_pipeline.py`) wired up for just
   orders, with deferrable sensors, DQ gate, on_failure_callback
9. Smoke test: run generator → DAG → see rows in Snowflake → see counts
   in Streamlit

This slice is the template the rest copy.

### Slice 2 — Customers + Products (SCD2 dimensions)
Now we layer on the SCD2 pattern, which the orders slice didn't need:

- `generators/customers.py` (Parquet, occasional address/email change)
- `generators/products.py` (CSV, occasional price/category change)
- Silver: standard dedup
- Gold: `dim_customer` + `dim_product` SCD2 via Delta MERGE
- Snowflake: SCD2 dim load (one dim) + clustering key + before/after
  `SYSTEM$CLUSTERING_INFORMATION`
- Dynamic task mapping in the Airflow DAG (one task per source)
- Wire `fact_orders` to surrogate keys from these dims

### Slice 3 — Clickstream (volume + sessionization + hourly path)
Different shape: JSON, high volume, no updates, needs sessionization
and a separate hourly DAG.

- `generators/clickstream.py` (JSON, ~50k/day, session gaps)
- Bronze for JSON
- Silver sessionization with 30-min inactivity window
- Gold `fact_sessions` (one row per session)
- `hourly_clickstream_pipeline.py` DAG with Snowpipe trigger
- Snowflake Snowpipe + Streams + Tasks demo (staging → analytics)
- `Dataset` triggering between hourly DAG and daily DAG

### Slice 4 — Currency rates + remaining marts
The lightest source closes out the data side:

- `generators/currency_rates.py` (free API with simulated fallback)
- `daily_revenue_by_category` and `customer_ltv` marts
- Materialized view in Snowflake (the expensive agg)
- Factless fact for wishlist relationships

### Slice 5 — Hardening (infra + observability + DR)
With all data flowing, fill in the remaining infrastructure:

- Lambda validator + SQS + S3 event notifications (Terraform)
- Step Functions DR/replay workflow (documented contrast to Airflow)
- IAM least-privilege roles
- Secrets Manager wiring
- SNS topic + alert subscriptions
- CloudWatch dashboard JSON + log retention
- `weekly_maintenance.py` DAG (OPTIMIZE/VACUUM/cost report)
- Snowflake resource monitors

### Slice 6 — Streamlit + CI/CD + Docs
Close out the user-facing pieces:

- Streamlit dashboard (4 widgets per spec)
- GitHub Actions test + deploy workflows
- `docs/architecture.md` with the "why" decisions
- `docs/runbook.md` with failure modes, backfill, replay, escalation
- README architecture mermaid + cost estimates + tech stack version table
- Demo failure scenario (bad file → quarantine → alert → replay)

## 3. Estimated Effort per Slice

These are rough order-of-magnitude estimates for a focused agent
session. They assume LocalStack/docker-compose stand in for real cloud
where needed, and that I'm writing code that would deploy cleanly
without actually executing it against billed accounts.

| Slice | Description | Effort (est.) | Files (est.) |
|-------|-------------|---------------|--------------|
| 0 | Scaffolding | S | ~15 |
| 1 | Orders end-to-end | XL | ~30 |
| 2 | Customers + Products + SCD2 | L | ~20 |
| 3 | Clickstream + hourly DAG | L | ~18 |
| 4 | Currency + remaining marts | M | ~10 |
| 5 | Infra hardening + DR + weekly DAG | L | ~25 |
| 6 | Streamlit + CI/CD + docs | M | ~15 |
| | **Total** | **~130 files** | |

Effort key: S = <1 hour, M = 1–3 hours, L = 3–6 hours, XL = 6+ hours of
focused work. Slice 1 is large because it establishes every pattern;
later slices benefit from the templates.

## 4. Clarifying Questions

Before I start I'd like to resolve these. A few are scope questions
where a smaller answer means a much smaller commit; the rest are
preferences that affect the shape of the code.

### Q1. Scope — generators only, or the full platform?

The branch is named `claude/create-data-generators-k0lHJ`, which reads
as "Section 1 only." The prompt describes all eight sections. These
are very different deliverables:

- **(a) Data generators only**: ship Slice 0 + the generators portion of
  Slice 1 (no Databricks, no Airflow, no Snowflake, no infra). ~1 day.
- **(b) Full platform per the spec**: all 8 sections, ~all 6 slices
  above. Multi-day effort, ~130 files.
- **(c) Generators + a clearly-stubbed reference slice** showing how a
  downstream layer would consume them, with the rest deferred. ~1–1.5
  days.

I'd recommend (b) only if you actually want the whole platform; (a) if
the branch name reflects intent and the broader prompt is just context;
(c) if you want a complete, demoable artifact that scopes cleanly to
one PR.

### Q2. Execution target — code-only, LocalStack, or real cloud?

I don't have credentials for AWS / Databricks / Snowflake from this
environment, so I can write code that would deploy and run cleanly, but
I can't prove end-to-end execution unless we use local stand-ins. Three
options:

- **(a) Code-only**: write everything to spec, validate syntactically
  (lint, dag-validate, terraform validate, pytest on transform
  functions), document local stand-ins, but don't actually run the
  pipeline end-to-end.
- **(b) LocalStack + docker-compose Airflow + Databricks Community
  Edition runner**: real execution for the AWS + Airflow + Spark parts;
  Snowflake remains code-only (no good local stand-in).
- **(c) Real cloud**: you provide credentials and I run against your
  accounts. Highest fidelity but cost and access concerns.

I'd default to (b) for the parts where it's free and reliable, and (a)
for Snowflake unless you have a free-trial account to point me at.

### Q3. Databricks Community Edition — known limitations OK?

CE doesn't support Unity Catalog, has limited cluster options, and
won't run `OPTIMIZE ZORDER` the same way as a real workspace. I'll
write the code per spec and gate CE-incompatible bits behind a
`config/env.yaml` flag, with notes in `docs/runbook.md`. Acceptable, or
do you want a different approach?

### Q4. Airflow version + provider versions

I'll target Airflow 2.10.x (deferrable operators are mature there).
`apache-airflow-providers-databricks`, `…-amazon`, `…-snowflake`,
`…-common-sql` at the versions compatible with 2.10. Push back if you
need 2.9.x or 3.x.

### Q5. Snowflake account model

For the demo, do you have a Snowflake trial account I should target, or
should I assume "code that would work against any account; reviewer
runs the SQL manually"? Affects whether I include connection strings in
config (with placeholders) vs writing pure DDL files.

### Q6. Commit cadence — single PR or multiple?

"Small, focused commits" was specified. I'll commit per logical change
(one commit per file or tight group of related files), pushed to
`claude/create-data-generators-k0lHJ`. Do you want me to open a PR at
the end, or leave the branch unpushed-as-PR for you to review?

### Q7. Streamlit data source

The dashboard reads from Snowflake per spec. If we go with code-only
Snowflake, the Streamlit app needs a way to demo without a live
connection. I can add a `--mock` flag that reads pre-generated Parquet
to give a working visual. OK?

---

**Awaiting your review and answers to Q1–Q7 before proceeding.** I'll
not write any further code until you confirm scope and the unresolved
choices above.
