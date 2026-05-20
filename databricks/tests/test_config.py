"""Tests for libs.config: layering, env-var resolution, dotted access."""

from __future__ import annotations

from pathlib import Path

import pytest
from libs.config import _resolve_env_vars, get_path, load_config


def _write(path: Path, content: str) -> None:
    path.write_text(content)


def test_loads_base_only(tmp_config_dir: Path) -> None:
    _write(tmp_config_dir / "base.yaml", "a: 1\nb: 2\n")
    _write(tmp_config_dir / "dev.yaml", "")
    cfg = load_config(env="dev", config_dir=tmp_config_dir, include_local=False)
    assert cfg == {"a": 1, "b": 2}


def test_env_overrides_base(tmp_config_dir: Path) -> None:
    _write(tmp_config_dir / "base.yaml", "a: 1\nb: 2\n")
    _write(tmp_config_dir / "dev.yaml", "b: 99\nc: 3\n")
    cfg = load_config(env="dev", config_dir=tmp_config_dir, include_local=False)
    assert cfg == {"a": 1, "b": 99, "c": 3}


def test_deep_merge_preserves_nested_keys(tmp_config_dir: Path) -> None:
    _write(tmp_config_dir / "base.yaml", "x:\n  a: 1\n  b: 2\n")
    _write(tmp_config_dir / "dev.yaml", "x:\n  b: 99\n  c: 3\n")
    cfg = load_config(env="dev", config_dir=tmp_config_dir, include_local=False)
    assert cfg == {"x": {"a": 1, "b": 99, "c": 3}}


def test_lists_are_replaced_not_merged(tmp_config_dir: Path) -> None:
    _write(tmp_config_dir / "base.yaml", "items: [a, b, c]\n")
    _write(tmp_config_dir / "dev.yaml", "items: [x]\n")
    cfg = load_config(env="dev", config_dir=tmp_config_dir, include_local=False)
    assert cfg == {"items": ["x"]}


def test_local_overrides_env_when_included(tmp_config_dir: Path) -> None:
    _write(tmp_config_dir / "base.yaml", "a: 1\n")
    _write(tmp_config_dir / "dev.yaml", "a: 2\n")
    _write(tmp_config_dir / "local.yaml", "a: 3\n")
    cfg = load_config(env="dev", config_dir=tmp_config_dir, include_local=True)
    assert cfg["a"] == 3


def test_local_excluded_when_disabled(tmp_config_dir: Path) -> None:
    _write(tmp_config_dir / "base.yaml", "a: 1\n")
    _write(tmp_config_dir / "dev.yaml", "a: 2\n")
    _write(tmp_config_dir / "local.yaml", "a: 3\n")
    cfg = load_config(env="dev", config_dir=tmp_config_dir, include_local=False)
    assert cfg["a"] == 2


def test_resolves_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FOO", "hello")
    assert _resolve_env_vars("${FOO}") == "hello"
    assert _resolve_env_vars("${FOO}/world") == "hello/world"


def test_env_var_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MISSING", raising=False)
    assert _resolve_env_vars("${MISSING:-fallback}") == "fallback"


def test_unresolved_env_var_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MISSING", raising=False)
    with pytest.raises(KeyError, match="MISSING"):
        _resolve_env_vars("${MISSING}")


def test_resolves_inside_nested_structure(
    tmp_config_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MY_BUCKET", "test-bucket")
    _write(tmp_config_dir / "base.yaml", "")
    _write(tmp_config_dir / "dev.yaml", "storage:\n  bucket: s3://${MY_BUCKET}\n")
    cfg = load_config(env="dev", config_dir=tmp_config_dir, include_local=False)
    assert cfg == {"storage": {"bucket": "s3://test-bucket"}}


def test_resolves_inside_list(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("X", "x-val")
    assert _resolve_env_vars(["a", "${X}", {"k": "${X}"}]) == ["a", "x-val", {"k": "x-val"}]


def test_missing_env_file_raises(tmp_config_dir: Path) -> None:
    _write(tmp_config_dir / "base.yaml", "a: 1\n")
    with pytest.raises(FileNotFoundError, match="env=prod"):
        load_config(env="prod", config_dir=tmp_config_dir, include_local=False)


def test_get_path_dotted_access() -> None:
    cfg = {"a": {"b": {"c": 42}}}
    assert get_path(cfg, "a.b.c") == 42


def test_get_path_returns_default_when_missing() -> None:
    cfg = {"a": 1}
    assert get_path(cfg, "x.y.z", default="fallback") == "fallback"
    assert get_path(cfg, "x.y.z", default=None) is None


def test_get_path_raises_without_default() -> None:
    cfg = {"a": 1}
    with pytest.raises(KeyError, match="x.y.z"):
        get_path(cfg, "x.y.z")


def test_env_defaults_when_unset(monkeypatch: pytest.MonkeyPatch, tmp_config_dir: Path) -> None:
    monkeypatch.delenv("LAKEHOUSE_ENV", raising=False)
    _write(tmp_config_dir / "base.yaml", "a: 1\n")
    _write(tmp_config_dir / "dev.yaml", "")
    cfg = load_config(config_dir=tmp_config_dir, include_local=False)
    assert cfg == {"a": 1}  # defaulted to "dev"
