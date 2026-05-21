"""Shared Airflow Dataset definitions.

Datasets are first-class in Airflow 2.10 and let downstream DAGs trigger
on producer-DAG completion. Centralising the definitions here keeps
producers and consumers in sync — if you rename the URI, both ends update
together.
"""

from __future__ import annotations

from airflow.datasets import Dataset

# Published by hourly_clickstream_pipeline at the end of each successful
# hourly run. Consumed by dashboard_refresh.
#
# URI is a logical identifier, not necessarily a real S3 path the
# scheduler checks — Airflow uses Datasets for triggering only, never
# physically reads them.
DATASET_FACT_SESSIONS = Dataset("s3://lakehouse-prod/gold/fact_sessions/")
