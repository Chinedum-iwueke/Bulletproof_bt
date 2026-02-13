"""Configuration loading and override utilities."""
from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import yaml


class ConfigError(ValueError):
    """Raised when user-provided config files are missing or invalid."""


def load_yaml(path: str | Path) -> dict[str, Any]:
    yaml_path = Path(path)
    if not yaml_path.exists():
        raise ConfigError(f"Config path not found: {yaml_path}")

    try:
        with yaml_path.open("r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"Failed to parse YAML config at {yaml_path}: {exc}") from exc

    if not isinstance(data, dict):
        raise ConfigError(f"Invalid YAML mapping at {yaml_path}: expected a mapping")
    return data


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def resolve_paths_relative_to(base_path_dir: str | Path, override_paths: list[str] | None) -> list[Path]:
    _ = base_path_dir
    if not override_paths:
        return []

    resolved_paths: list[Path] = []
    for raw_path in override_paths:
        candidate = Path(raw_path)
        if not candidate.is_absolute():
            candidate = Path.cwd() / candidate
        resolved_paths.append(candidate)
    return resolved_paths


def load_config_with_overrides(base_path: str, override_paths: list[str] | None) -> dict[str, Any]:
    base_file = Path(base_path)
    config = load_yaml(base_file)

    for override_file in resolve_paths_relative_to(base_file.parent, override_paths):
        override_cfg = load_yaml(override_file)
        config = deep_merge(config, override_cfg)

    return config
