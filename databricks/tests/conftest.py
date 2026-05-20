"""Shared pytest fixtures for ``databricks/libs`` tests.

Includes a session-scoped SparkSession with Delta enabled for the
chispa-based transformation tests. The session is reused across tests for
speed.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# PySpark 3.5 requires Java <= 17. If the system default is Java 21 (Ubuntu
# 24.04+ ships it), force JAVA_HOME to a Java 17 install if available before
# pyspark imports it.
_JAVA17 = "/usr/lib/jvm/java-17-openjdk-amd64"
if "JAVA_HOME" not in os.environ and os.path.isdir(_JAVA17):
    os.environ["JAVA_HOME"] = _JAVA17


@pytest.fixture()
def tmp_config_dir(tmp_path: Path) -> Path:
    """An empty temporary config directory. Tests populate ``base.yaml`` etc."""
    cfg = tmp_path / "config"
    cfg.mkdir()
    return cfg


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """A local SparkSession with Delta enabled.

    Scope=session because Spark startup is slow (~5s). Tests must clean up
    any tables they create or use per-test ``tmp_path`` for warehouse paths.
    """
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    warehouse = Path("/tmp/spark-warehouse-tests")
    if warehouse.exists():
        shutil.rmtree(warehouse, ignore_errors=True)

    builder = (
        SparkSession.builder.appName("lakehouse-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Java 17 module-access for Spark's use of Unsafe/Arrow internals.
        .config(
            "spark.driver.extraJavaOptions",
            "--add-opens=java.base/java.lang=ALL-UNNAMED "
            "--add-opens=java.base/java.nio=ALL-UNNAMED "
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
            "--add-opens=java.base/java.util=ALL-UNNAMED",
        )
    )
    builder = configure_spark_with_delta_pip(builder)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
