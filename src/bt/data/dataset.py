"""Dataset manifest normalization and validation utilities."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from bt.core.errors import DataError


@dataclass(frozen=True)
class DatasetManifest:
    version: int
    format: str
    symbols: list[str]
    files_by_symbol: dict[str, str]


def _err(dataset_dir: Path, manifest_path: Path, detail: str) -> DataError:
    return DataError(
        f"Dataset manifest validation failed for dataset_dir='{dataset_dir}' "
        f"manifest='{manifest_path}': {detail}"
    )


def _normalize_requested_symbols(value: Any, *, key_path: str) -> list[str]:
    if not isinstance(value, list):
        raise DataError(f"Invalid config: {key_path} must be a non-empty list of strings (got: {value!r})")

    normalized: list[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            raise DataError(f"Invalid config: {key_path} must be a non-empty list of strings (got: {value!r})")
        symbol = item.strip()
        if not symbol:
            continue
        if symbol not in seen:
            seen.add(symbol)
            normalized.append(symbol)

    if not normalized:
        raise DataError(f"Invalid config: {key_path} must be a non-empty list of strings (got: {value!r})")

    return normalized


def _load_manifest_yaml(dataset_dir: Path, manifest_path: Path) -> dict[str, Any]:
    if not dataset_dir.is_dir():
        raise _err(dataset_dir, manifest_path, "dataset_dir is not an existing directory")
    if not manifest_path.is_file():
        raise _err(dataset_dir, manifest_path, "manifest.yaml is missing")

    try:
        with manifest_path.open("r", encoding="utf-8") as handle:
            raw_manifest = yaml.safe_load(handle)
    except yaml.YAMLError as exc:
        raise _err(dataset_dir, manifest_path, "manifest.yaml is invalid YAML") from exc

    if not isinstance(raw_manifest, dict):
        raise _err(dataset_dir, manifest_path, "manifest root must be a mapping")

    return raw_manifest


def _validate_relative_file_exists(dataset_dir: Path, manifest_path: Path, rel_path: str) -> None:
    resolved_path = (dataset_dir / rel_path).resolve()
    dataset_root = dataset_dir.resolve()
    if not str(resolved_path).startswith(str(dataset_root)):
        raise _err(
            dataset_dir,
            manifest_path,
            f"file path '{rel_path}' resolves outside dataset_dir",
        )
    if not resolved_path.is_file():
        raise _err(
            dataset_dir,
            manifest_path,
            f"referenced file is missing: '{rel_path}'",
        )


def _normalize_v1_manifest(
    dataset_dir: Path,
    manifest_path: Path,
    manifest: dict[str, Any],
) -> DatasetManifest:
    if manifest.get("version") != 1:
        raise _err(dataset_dir, manifest_path, "version must be 1 for strict manifests")
    if manifest.get("format") != "parquet":
        raise _err(dataset_dir, manifest_path, "format must be 'parquet' for version=1 manifests")

    files = manifest.get("files")
    if not isinstance(files, list) or not files:
        raise _err(dataset_dir, manifest_path, "files must be a non-empty list")

    if all(isinstance(entry, str) for entry in files):
        files_by_symbol: dict[str, str] = {}
        symbols: list[str] = []
        for index, rel_path_raw in enumerate(files, start=1):
            rel_path = rel_path_raw.strip()
            if not rel_path:
                raise _err(dataset_dir, manifest_path, "files entries must be non-empty strings")
            symbol = f"__file_{index:03d}__"
            symbols.append(symbol)
            files_by_symbol[symbol] = rel_path
            _validate_relative_file_exists(dataset_dir, manifest_path, rel_path)

        # TODO: replace synthetic per-file symbols with true multi-file concat streaming.
        return DatasetManifest(version=1, format="parquet", symbols=symbols, files_by_symbol=files_by_symbol)

    files_by_symbol = {}
    symbols = []
    for index, entry in enumerate(files, start=1):
        if not isinstance(entry, dict):
            raise _err(
                dataset_dir,
                manifest_path,
                f"files[{index}] must be either a string path or an object with symbol/path",
            )

        symbol_raw = entry.get("symbol")
        path_raw = entry.get("path")
        if not isinstance(symbol_raw, str) or not symbol_raw.strip():
            raise _err(dataset_dir, manifest_path, f"files[{index}].symbol must be a non-empty string")
        if not isinstance(path_raw, str) or not path_raw.strip():
            raise _err(dataset_dir, manifest_path, f"files[{index}].path must be a non-empty string")

        symbol = symbol_raw.strip()
        rel_path = path_raw.strip()
        if symbol in files_by_symbol:
            raise _err(dataset_dir, manifest_path, f"duplicate symbol in files list: '{symbol}'")

        symbols.append(symbol)
        files_by_symbol[symbol] = rel_path
        _validate_relative_file_exists(dataset_dir, manifest_path, rel_path)

    return DatasetManifest(version=1, format="parquet", symbols=symbols, files_by_symbol=files_by_symbol)


def _normalize_legacy_manifest(
    dataset_dir: Path,
    manifest_path: Path,
    manifest: dict[str, Any],
) -> DatasetManifest:
    if manifest.get("format") != "per_symbol_parquet":
        raise _err(
            dataset_dir,
            manifest_path,
            "unsupported schema: expected strict v1 parquet or legacy per_symbol_parquet",
        )

    symbols = manifest.get("symbols")
    if not isinstance(symbols, list) or not symbols:
        raise _err(dataset_dir, manifest_path, "symbols must be a non-empty list")

    normalized_symbols: list[str] = []
    seen_symbols: set[str] = set()
    for index, symbol_raw in enumerate(symbols, start=1):
        if not isinstance(symbol_raw, str) or not symbol_raw.strip():
            raise _err(dataset_dir, manifest_path, f"symbols[{index}] must be a non-empty string")
        symbol = symbol_raw.strip()
        if symbol in seen_symbols:
            raise _err(dataset_dir, manifest_path, f"duplicate symbol in symbols list: '{symbol}'")
        seen_symbols.add(symbol)
        normalized_symbols.append(symbol)

    path_template = manifest.get("path")
    if not isinstance(path_template, str) or not path_template.strip():
        raise _err(dataset_dir, manifest_path, "path must be a non-empty string")
    if "{symbol}" not in path_template:
        raise _err(dataset_dir, manifest_path, "path must include '{symbol}' placeholder")

    files_by_symbol = {
        symbol: path_template.format(symbol=symbol)
        for symbol in normalized_symbols
    }
    for rel_path in files_by_symbol.values():
        _validate_relative_file_exists(dataset_dir, manifest_path, rel_path)

    return DatasetManifest(
        version=1,
        format="per_symbol_parquet",
        symbols=normalized_symbols,
        files_by_symbol=files_by_symbol,
    )


def _apply_optional_filters(
    manifest: DatasetManifest,
    dataset_dir: Path,
    manifest_path: Path,
    config: dict[str, Any] | None,
) -> DatasetManifest:
    data_cfg: dict[str, Any] = {}
    if isinstance(config, dict) and isinstance(config.get("data"), dict):
        data_cfg = config["data"]

    symbols = list(manifest.symbols)

    subset = data_cfg.get("symbols_subset")
    symbols_alias = data_cfg.get("symbols")

    normalized_subset = None if subset is None else _normalize_requested_symbols(subset, key_path="data.symbols_subset")
    normalized_symbols_alias = (
        None if symbols_alias is None else _normalize_requested_symbols(symbols_alias, key_path="data.symbols")
    )

    requested_subset = normalized_subset
    if normalized_subset is None and normalized_symbols_alias is not None:
        requested_subset = normalized_symbols_alias
    elif normalized_subset is not None and normalized_symbols_alias is not None and normalized_subset != normalized_symbols_alias:
        raise _err(
            dataset_dir,
            manifest_path,
            "Config conflict: data.symbols and data.symbols_subset both set but differ. "
            f"Use only one. data.symbols={symbols_alias!r} data.symbols_subset={subset!r}",
        )

    if requested_subset is not None:
        unknown_symbols = [symbol for symbol in requested_subset if symbol not in manifest.files_by_symbol]
        if unknown_symbols:
            raise _err(
                dataset_dir,
                manifest_path,
                f"data.symbols_subset contains unknown symbol(s): {unknown_symbols}",
            )
        symbols = requested_subset

    max_symbols = data_cfg.get("max_symbols")
    if max_symbols is not None:
        if not isinstance(max_symbols, int) or max_symbols <= 0:
            raise _err(dataset_dir, manifest_path, "data.max_symbols must be a positive integer")
        symbols = symbols[:max_symbols]

    files_by_symbol = {symbol: manifest.files_by_symbol[symbol] for symbol in symbols}
    return DatasetManifest(
        version=manifest.version,
        format=manifest.format,
        symbols=symbols,
        files_by_symbol=files_by_symbol,
    )


def load_dataset_manifest(dataset_dir: str, config: dict | None = None) -> DatasetManifest:
    dataset_path = Path(dataset_dir)
    manifest_path = dataset_path / "manifest.yaml"
    manifest = _load_manifest_yaml(dataset_path, manifest_path)

    if "version" in manifest:
        normalized = _normalize_v1_manifest(dataset_path, manifest_path, manifest)
    else:
        normalized = _normalize_legacy_manifest(dataset_path, manifest_path, manifest)

    return _apply_optional_filters(normalized, dataset_path, manifest_path, config)
