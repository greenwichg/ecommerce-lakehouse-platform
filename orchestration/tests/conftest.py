"""Shared fixtures for orchestration/tests.

Airflow needs:

1. ``AIRFLOW_HOME`` set to a writable scratch dir before its config init runs.
2. The metadata DB initialised so ``Variable.get`` queries don't error out
   at DAG import time. We init once per AIRFLOW_HOME using a sentinel file
   so the cost is paid only on first conftest load.

(Slice 1 happened to work without (2) because some test runs reused an
AIRFLOW_HOME that had been initialised by a prior interactive
``airflow db migrate``. Slice 2 has more ``Variable.get`` calls — for the
``databricks_job_*`` IDs — and a fresh AIRFLOW_HOME exposed the gap.)
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile

_AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME") or tempfile.mkdtemp(prefix="airflow-test-")
os.environ["AIRFLOW_HOME"] = _AIRFLOW_HOME
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")

_DB_SENTINEL = os.path.join(_AIRFLOW_HOME, ".db_initialised")
if not os.path.exists(_DB_SENTINEL):
    # `airflow db migrate` is idempotent and the cheapest way to materialise
    # the metadata schema without importing airflow ourselves (which would
    # trip the same Variable.get path under test).
    subprocess.run(
        ["airflow", "db", "migrate"],
        check=True,
        capture_output=True,
        env=os.environ.copy(),
    )
    with open(_DB_SENTINEL, "w") as handle:
        handle.write("ok\n")
    print(f"[conftest] initialised airflow metadata DB at {_AIRFLOW_HOME}", file=sys.stderr)
