"""Data loader for parquet/csv into in-memory format."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from bt.data.schema import BAR_COLUMNS, DTYPES, normalize_columns
from bt.data.validation import validate_bars_df


def _ensure_utc(ts: pd.Series) -> None:
    if isinstance(ts.dtype, pd.DatetimeTZDtype):
        if str(ts.dt.tz) != "UTC":
            raise ValueError("ts must be in UTC")
        return
    if pd.api.types.is_datetime64_dtype(ts):
        raise ValueError("ts must be timezone-aware UTC")
    raise ValueError("ts must be timezone-aware UTC")


def load_bars(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    if path.suffix == ".parquet":
        df = pd.read_parquet(path)
    elif path.suffix == ".csv":
        df = pd.read_csv(path)
    else:
        raise ValueError("Unsupported file type")

    df = normalize_columns(df)
    if "ts" not in df.columns:
        raise ValueError("Missing ts column after normalization")

    df["ts"] = pd.to_datetime(df["ts"], errors="raise")
    _ensure_utc(df["ts"])

    for col in BAR_COLUMNS:
        if col == "ts":
            continue
        if col in df.columns and col in DTYPES:
            df[col] = df[col].astype(DTYPES[col])

    df = df.sort_values(["symbol", "ts"], kind="mergesort").reset_index(drop=True)
    validate_bars_df(df)
    return df


def _normalize_and_validate(df: pd.DataFrame, *, sort_columns: list[str]) -> pd.DataFrame:
    df = normalize_columns(df)
    if "ts" not in df.columns:
        raise ValueError("Missing ts column after normalization")

    df["ts"] = pd.to_datetime(df["ts"], errors="raise")
    _ensure_utc(df["ts"])

    for col in BAR_COLUMNS:
        if col == "ts":
            continue
        if col in df.columns and col in DTYPES:
            df[col] = df[col].astype(DTYPES[col])

    df = df.sort_values(sort_columns, kind="mergesort").reset_index(drop=True)
    validate_bars_df(df)
    return df


@dataclass(frozen=True)
class _ParsedManifest:
    manifest_type: str
    file_list: list[Path]


def _manifest_error(manifest_path: Path, detail: str) -> ValueError:
    expected = (
        "Expected v1 {version, format, files} or legacy "
        "{format: per_symbol_parquet, symbols, path}."
    )
    return ValueError(f"Invalid manifest.yaml at {manifest_path}: {detail}. {expected}")


def _parse_manifest(manifest_path: Path) -> _ParsedManifest:
    dataset_dir = manifest_path.parent
    try:
        with manifest_path.open("r", encoding="utf-8") as handle:
            raw_manifest: Any = yaml.safe_load(handle)
    except yaml.YAMLError as exc:
        raise _manifest_error(manifest_path, "invalid YAML") from exc

    if not isinstance(raw_manifest, dict):
        raise _manifest_error(manifest_path, "expected a mapping")

    if "version" in raw_manifest:
        if raw_manifest.get("version") != 1:
            raise _manifest_error(manifest_path, "version must be 1")
        if raw_manifest.get("format") != "parquet":
            raise _manifest_error(manifest_path, "format must be parquet for version=1 manifests")

        files = raw_manifest.get("files")
        if not isinstance(files, list) or not files:
            raise _manifest_error(manifest_path, "files must be a non-empty list")
        if not all(isinstance(file_path, str) and file_path for file_path in files):
            raise _manifest_error(manifest_path, "files entries must be non-empty strings")

        resolved_files = [dataset_dir / file_path for file_path in files]
        for file_path in resolved_files:
            if not file_path.is_file():
                raise _manifest_error(manifest_path, f"missing file listed in files: {file_path}")

        return _ParsedManifest(manifest_type="v1_files", file_list=resolved_files)

    if raw_manifest.get("format") != "per_symbol_parquet":
        raise ValueError(
            f"Unsupported manifest schema at {manifest_path}: "
            "Expected v1 {version,format,files} or legacy "
            "{format: per_symbol_parquet, symbols, path}."
        )

    symbols = raw_manifest.get("symbols")
    if not isinstance(symbols, list) or not symbols:
        raise _manifest_error(manifest_path, "symbols must be a non-empty list for legacy manifests")
    if not all(isinstance(symbol, str) and symbol for symbol in symbols):
        raise _manifest_error(manifest_path, "symbols entries must be non-empty strings")

    path_template = raw_manifest.get("path")
    if not isinstance(path_template, str) or not path_template:
        raise _manifest_error(manifest_path, "path must be a non-empty string for legacy manifests")
    if "{symbol}" not in path_template:
        raise _manifest_error(manifest_path, "path must include {symbol} placeholder for legacy manifests")

    resolved_files = [dataset_dir / path_template.format(symbol=symbol) for symbol in symbols]
    missing_files = [file_path for file_path in resolved_files if not file_path.is_file()]
    if missing_files:
        raise _manifest_error(
            manifest_path,
            (
                "missing legacy parquet files; first missing "
                f"{missing_files[0]} (missing {len(missing_files)} total)"
            ),
        )

    return _ParsedManifest(manifest_type="legacy_per_symbol", file_list=resolved_files)


def load_dataset(dataset_path: str) -> pd.DataFrame:
    """Load either a single bars file or a manifest-based parquet dataset directory."""
    path = Path(dataset_path)
    if path.is_file():
        return load_bars(path)

    if not path.is_dir():
        raise ValueError(f"Dataset path does not exist or is not a file/directory: {dataset_path}")

    manifest_path = path / "manifest.yaml"
    if not manifest_path.exists():
        raise ValueError(f"Dataset manifest missing: {manifest_path}")

    parsed_manifest = _parse_manifest(manifest_path)
    frames: list[pd.DataFrame] = []
    for parquet_path in parsed_manifest.file_list:
        if parquet_path.suffix != ".parquet":
            raise _manifest_error(manifest_path, f"only parquet files are supported, got: {parquet_path}")
        frames.append(pd.read_parquet(parquet_path))

    combined = pd.concat(frames, ignore_index=True)
    # TODO: Add chunked loading/streaming concat for huge universes.
    # TODO: Add optional symbol-subset loading.
    # TODO: Read manifest counts metadata for quick sanity checks.
    return _normalize_and_validate(combined, sort_columns=["ts", "symbol"])
