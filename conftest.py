"""Root conftest.

Pytest's ``[tool.pytest.ini_options].pythonpath`` setting *should* handle
this, but interleaving the orchestration test tree (which imports Airflow)
with the generators test tree occasionally results in the project root
falling off ``sys.path``. Explicitly inserting it here, in a conftest at
the rootdir, runs once at the very start of collection — before any test
file is imported — and guarantees ``from generators.x import y`` resolves
regardless of test ordering.
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent
for path in [_REPO_ROOT, _REPO_ROOT / "databricks", _REPO_ROOT / "orchestration" / "dags"]:
    str_path = str(path)
    if str_path not in sys.path:
        sys.path.insert(0, str_path)

# Eagerly import the local packages so they're registered in ``sys.modules``
# before any test file's imports run. Pytest with importmode=importlib
# occasionally drops sys.path entries between conftest load and test
# collection (root cause unclear, but reproducible when generators and
# orchestration test trees are interleaved). Forcing the import here
# inoculates against that.
import libs  # noqa: E402,F401

import generators  # noqa: E402,F401
