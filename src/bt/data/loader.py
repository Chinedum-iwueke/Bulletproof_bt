"""Data loader for parquet/csv into in-memory format."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from bt.data.schema import BAR_COLUMNS, DTYPES, normalize_columns
from bt.data.validation import validate_bars_df


def _ensure_utc(ts: pd.Series) -> None:
    if pd.api.types.is_datetime64tz_dtype(ts):
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


def _parse_manifest(manifest_path: Path) -> list[str]:
    try:
        with manifest_path.open("r", encoding="utf-8") as handle:
            raw_manifest: Any = yaml.safe_load(handle)
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid manifest.yaml at {manifest_path}: invalid YAML") from exc

    if not isinstance(raw_manifest, dict):
        raise ValueError(f"Invalid manifest.yaml at {manifest_path}: expected a mapping")

    if raw_manifest.get("version") != 1:
        raise ValueError(f"Invalid manifest.yaml at {manifest_path}: version must be 1")

    if raw_manifest.get("format") != "parquet":
        raise ValueError(f"Invalid manifest.yaml at {manifest_path}: format must be parquet")

    files = raw_manifest.get("files")
    if not isinstance(files, list) or not files:
        raise ValueError(f"Invalid manifest.yaml at {manifest_path}: files must be a non-empty list")
    if not all(isinstance(file_path, str) and file_path for file_path in files):
        raise ValueError(f"Invalid manifest.yaml at {manifest_path}: files entries must be non-empty strings")

    return files


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

    manifest_files = _parse_manifest(manifest_path)
    frames: list[pd.DataFrame] = []
    for relative_file in manifest_files:
        parquet_path = path / relative_file
        if parquet_path.suffix != ".parquet":
            raise ValueError(f"Invalid manifest.yaml at {manifest_path}: only parquet files are supported")
        if not parquet_path.is_file():
            raise ValueError(f"Dataset file listed in manifest not found: {parquet_path}")
        frames.append(pd.read_parquet(parquet_path))

    combined = pd.concat(frames, ignore_index=True)
    # TODO: Add support for additional manifest-level options (e.g., partition filters).
    return _normalize_and_validate(combined, sort_columns=["ts", "symbol"])
