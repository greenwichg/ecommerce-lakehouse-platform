"""Shared transform logic for the lakehouse.

This package is imported the same way from notebooks and tests: with
``databricks/`` on ``sys.path``, write ``from libs.x import y``. Pure-Python
modules (config, paths, batch) live alongside Spark-dependent ones (bronze,
silver, gold); the latter import pyspark lazily so unit tests for the former
do not require a Spark installation.
"""
