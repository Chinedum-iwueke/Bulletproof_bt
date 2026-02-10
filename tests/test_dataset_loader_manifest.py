"""Tests for manifest-aware dataset loading."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import yaml

from bt.data.loader import load_dataset


def _valid_rows() -> list[dict[str, object]]:
    return [
        {
            "ts": pd.Timestamp("2025-01-01 00:00:00", tz="UTC"),
            "symbol": "AAA",
            "open": 1.0,
            "high": 1.2,
            "low": 0.9,
            "close": 1.1,
            "volume": 10.0,
        }
    ]


def test_load_dataset_file_passthrough_csv(tmp_path: Path) -> None:
    csv_path = tmp_path / "bars.csv"
    pd.DataFrame(_valid_rows()).to_csv(csv_path, index=False)

    df = load_dataset(str(csv_path))

    assert len(df) == 1
    assert str(df["ts"].dt.tz) == "UTC"
    assert list(df["symbol"]) == ["AAA"]


def test_load_dataset_manifest_directory_concatenates_and_sorts(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir()

    manifest = {
        "version": 1,
        "format": "parquet",
        "files": ["bars_2025_02.parquet", "bars_2025_01.parquet"],
    }
    (dataset_dir / "manifest.yaml").write_text(yaml.safe_dump(manifest), encoding="utf-8")

    feb_df = pd.DataFrame(
        [
            {
                "ts": pd.Timestamp("2025-02-01 00:01:00", tz="UTC"),
                "symbol": "BBB",
                "open": 2.0,
                "high": 2.3,
                "low": 1.9,
                "close": 2.1,
                "volume": 20.0,
            }
        ]
    )
    jan_df = pd.DataFrame(
        [
            {
                "ts": pd.Timestamp("2025-01-01 00:01:00", tz="UTC"),
                "symbol": "AAA",
                "open": 1.0,
                "high": 1.4,
                "low": 0.8,
                "close": 1.2,
                "volume": 10.0,
            },
            {
                "ts": pd.Timestamp("2025-02-01 00:01:00", tz="UTC"),
                "symbol": "AAA",
                "open": 3.0,
                "high": 3.4,
                "low": 2.8,
                "close": 3.2,
                "volume": 30.0,
            },
        ]
    )

    feb_df.to_parquet(dataset_dir / "bars_2025_02.parquet", index=False)
    jan_df.to_parquet(dataset_dir / "bars_2025_01.parquet", index=False)

    df = load_dataset(str(dataset_dir))

    assert len(df) == 3
    assert str(df["ts"].dt.tz) == "UTC"
    assert list(zip(df["ts"], df["symbol"])) == [
        (pd.Timestamp("2025-01-01 00:01:00", tz="UTC"), "AAA"),
        (pd.Timestamp("2025-02-01 00:01:00", tz="UTC"), "AAA"),
        (pd.Timestamp("2025-02-01 00:01:00", tz="UTC"), "BBB"),
    ]


def test_load_dataset_manifest_missing_raises(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir()

    with pytest.raises(ValueError, match="manifest"):
        load_dataset(str(dataset_dir))


@pytest.mark.parametrize(
    "manifest",
    [
        {"version": 2, "format": "parquet", "files": ["bars.parquet"]},
        {"version": 1, "format": "csv", "files": ["bars.parquet"]},
        {"version": 1, "format": "parquet", "files": []},
    ],
)
def test_load_dataset_manifest_invalid_raises(tmp_path: Path, manifest: dict[str, object]) -> None:
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir()
    (dataset_dir / "manifest.yaml").write_text(yaml.safe_dump(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="Invalid manifest.yaml"):
        load_dataset(str(dataset_dir))


def test_load_dataset_manifest_legacy_per_symbol_loads(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    symbols_dir = dataset_dir / "symbols"
    symbols_dir.mkdir(parents=True)

    manifest = {
        "dataset_name": "legacy_dataset",
        "format": "per_symbol_parquet",
        "path": "symbols/{symbol}.parquet",
        "symbols": ["AAA", "BBB"],
    }
    (dataset_dir / "manifest.yaml").write_text(yaml.safe_dump(manifest), encoding="utf-8")

    aaa_df = pd.DataFrame(
        [
            {
                "ts": pd.Timestamp("2025-01-01 00:00:00", tz="UTC"),
                "symbol": "AAA",
                "open": 1.0,
                "high": 1.1,
                "low": 0.9,
                "close": 1.05,
                "volume": 10.0,
            },
            {
                "ts": pd.Timestamp("2025-01-01 00:02:00", tz="UTC"),
                "symbol": "AAA",
                "open": 1.2,
                "high": 1.3,
                "low": 1.1,
                "close": 1.25,
                "volume": 11.0,
            },
        ]
    )
    bbb_df = pd.DataFrame(
        [
            {
                "ts": pd.Timestamp("2025-01-01 00:01:00", tz="UTC"),
                "symbol": "BBB",
                "open": 2.0,
                "high": 2.1,
                "low": 1.9,
                "close": 2.05,
                "volume": 12.0,
            }
        ]
    )
    aaa_df.to_parquet(symbols_dir / "AAA.parquet", index=False)
    bbb_df.to_parquet(symbols_dir / "BBB.parquet", index=False)

    df = load_dataset(str(dataset_dir))

    assert len(df) == 3
    assert str(df["ts"].dt.tz) == "UTC"
    assert list(zip(df["ts"], df["symbol"])) == [
        (pd.Timestamp("2025-01-01 00:00:00", tz="UTC"), "AAA"),
        (pd.Timestamp("2025-01-01 00:01:00", tz="UTC"), "BBB"),
        (pd.Timestamp("2025-01-01 00:02:00", tz="UTC"), "AAA"),
    ]
    assert (
        df[(df["symbol"] == "BBB") & (df["ts"] == pd.Timestamp("2025-01-01 00:02:00", tz="UTC"))].empty
    )


def test_load_dataset_manifest_legacy_missing_symbol_placeholder_raises(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir()

    manifest = {
        "format": "per_symbol_parquet",
        "path": "symbols/AAA.parquet",
        "symbols": ["AAA", "BBB"],
    }
    (dataset_dir / "manifest.yaml").write_text(yaml.safe_dump(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match=r"path must include \{symbol\} placeholder"):
        load_dataset(str(dataset_dir))


def test_load_dataset_manifest_legacy_missing_parquet_files_raises(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    symbols_dir = dataset_dir / "symbols"
    symbols_dir.mkdir(parents=True)

    manifest = {
        "format": "per_symbol_parquet",
        "path": "symbols/{symbol}.parquet",
        "symbols": ["AAA", "BBB"],
    }
    (dataset_dir / "manifest.yaml").write_text(yaml.safe_dump(manifest), encoding="utf-8")

    pd.DataFrame(_valid_rows()).to_parquet(symbols_dir / "AAA.parquet", index=False)

    with pytest.raises(ValueError, match=r"first missing .*BBB.parquet.*missing 1 total"):
        load_dataset(str(dataset_dir))


def test_load_dataset_manifest_unsupported_schema_raises(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir()

    manifest = {
        "dataset_name": "stable_data_1m_canonical",
        "format": "csv",
        "symbols": ["AAA"],
    }
    (dataset_dir / "manifest.yaml").write_text(yaml.safe_dump(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="Unsupported manifest schema"):
        load_dataset(str(dataset_dir))
