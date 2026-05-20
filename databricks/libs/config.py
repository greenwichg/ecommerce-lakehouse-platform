"""Layered YAML configuration loader.

Configs are merged in this order (later overrides earlier):

    config/base.yaml  <  config/<env>.yaml  <  config/local.yaml (optional)

Values may reference environment variables as ``${VAR}`` or
``${VAR:-default}``. References are resolved at load time; an undefined
``${VAR}`` without a default raises ``KeyError`` rather than silently
producing a literal string.

Usage::

    from libs.config import load_config, get_path
    cfg = load_config(env="dev")
    rpd = get_path(cfg, "generators.orders.records_per_day")
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

_ENV_PATTERN = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)(?::-([^}]*))?\}")
_DEFAULT_ENV_VAR = "LAKEHOUSE_ENV"
_CONFIG_DIR_VAR = "LAKEHOUSE_CONFIG_DIR"


def _find_config_dir() -> Path:
    """Locate the config/ directory containing base.yaml.

    Search order:
      1. ``$LAKEHOUSE_CONFIG_DIR``
      2. ``<cwd>/config``
      3. Walk up from this file looking for ``config/base.yaml``
    """
    explicit = os.environ.get(_CONFIG_DIR_VAR)
    if explicit:
        return Path(explicit).resolve()
    cwd_candidate = Path.cwd() / "config"
    if (cwd_candidate / "base.yaml").is_file():
        return cwd_candidate
    here = Path(__file__).resolve()
    for ancestor in here.parents:
        candidate = ancestor / "config"
        if (candidate / "base.yaml").is_file():
            return candidate
    raise FileNotFoundError(
        "Could not locate config/base.yaml. Set $LAKEHOUSE_CONFIG_DIR or run from the repo root."
    )


def _resolve_env_vars(value: Any) -> Any:
    """Recursively resolve ``${VAR}`` references in strings; pass through others."""
    if isinstance(value, str):

        def replace(match: re.Match[str]) -> str:
            var, default = match.group(1), match.group(2)
            if var in os.environ:
                return os.environ[var]
            if default is not None:
                return default
            raise KeyError(f"Undefined env var ${{{var}}} referenced in config")

        return _ENV_PATTERN.sub(replace, value)
    if isinstance(value, Mapping):
        return {k: _resolve_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_env_vars(v) for v in value]
    return value


def _deep_merge(base: dict[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    """Recursively merge ``override`` into a copy of ``base``.

    Dict-into-dict merges; everything else replaces. Lists are replaced
    wholesale — config layering shouldn't have to reason about partial list
    concatenation.
    """
    out = dict(base)
    for key, value in override.items():
        if key in out and isinstance(out[key], dict) and isinstance(value, Mapping):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open() as handle:
        return yaml.safe_load(handle) or {}


def load_config(
    env: str | None = None,
    config_dir: Path | str | None = None,
    include_local: bool = True,
) -> dict[str, Any]:
    """Load and merge layered config, resolving ``${ENV_VAR}`` references.

    Args:
        env: Environment name (e.g. "dev", "prod"). Falls back to
            ``$LAKEHOUSE_ENV`` then ``"dev"``.
        config_dir: Optional path to the config directory. Defaults to
            discovery (see ``_find_config_dir``).
        include_local: When True (default), ``config/local.yaml`` is merged
            on top if it exists. Set False in CI/tests to keep results
            independent of any local overrides.

    Returns:
        Merged configuration dict with all ``${VAR}`` references resolved.
    """
    env = env or os.environ.get(_DEFAULT_ENV_VAR, "dev")
    cfg_dir = Path(config_dir) if config_dir else _find_config_dir()

    base = _load_yaml(cfg_dir / "base.yaml")
    env_path = cfg_dir / f"{env}.yaml"
    if not env_path.is_file():
        raise FileNotFoundError(f"No config for env={env}: {env_path}")

    merged = _deep_merge(base, _load_yaml(env_path))

    if include_local:
        local_path = cfg_dir / "local.yaml"
        if local_path.is_file():
            merged = _deep_merge(merged, _load_yaml(local_path))

    return _resolve_env_vars(merged)


_MISSING = object()


def get_path(cfg: Mapping[str, Any], dotted: str, default: Any = _MISSING) -> Any:
    """Read a nested config value by dotted path.

    >>> get_path({"a": {"b": 1}}, "a.b")
    1
    >>> get_path({"a": 1}, "x.y", default=None) is None
    True
    """
    node: Any = cfg
    for part in dotted.split("."):
        if not isinstance(node, Mapping) or part not in node:
            if default is _MISSING:
                raise KeyError(f"Config key not found: {dotted}")
            return default
        node = node[part]
    return node
