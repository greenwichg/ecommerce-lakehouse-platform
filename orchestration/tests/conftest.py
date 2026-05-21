"""Shared fixtures for airflow/tests.

Two things have to happen before any airflow import:

1. The project root must be removed from ``sys.path``. Otherwise our local
   ``airflow/`` directory acts as a namespace package and shadows the real
   ``airflow`` package from site-packages — every ``from airflow.X import Y``
   in the DAGs would fail with ModuleNotFoundError.

2. ``AIRFLOW_HOME`` must be set to a writable scratch dir; without it,
   importing airflow trips on its config initialisation.
"""

from __future__ import annotations

import os
import sys
import tempfile

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
while _REPO_ROOT in sys.path:
    sys.path.remove(_REPO_ROOT)

os.environ.setdefault("AIRFLOW_HOME", tempfile.mkdtemp(prefix="airflow-test-"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
