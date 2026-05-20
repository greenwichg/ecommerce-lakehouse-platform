"""Shared pytest fixtures for ``databricks/libs`` tests."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture()
def tmp_config_dir(tmp_path: Path) -> Path:
    """An empty temporary config directory. Tests populate ``base.yaml`` etc."""
    cfg = tmp_path / "config"
    cfg.mkdir()
    return cfg
