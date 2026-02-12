from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from bt.data.dataset import load_dataset_manifest


def _write_minimal_parquet(path: Path, symbol: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "ts": ["2025-01-01T00:00:00Z"],
            "symbol": [symbol],
            "open": [1.0],
            "high": [1.1],
            "low": [0.9],
            "close": [1.05],
            "volume": [100.0],
        }
    )
    pq.write_table(table, path)


def test_load_strict_v1_manifest_normalizes(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir()

    manifest = {
        "version": 1,
        "format": "parquet",
        "files": [
            {"symbol": "BTCUSDT", "path": "symbols/BTCUSDT.parquet"},
            {"symbol": "ETHUSDT", "path": "symbols/ETHUSDT.parquet"},
        ],
    }
    (dataset_dir / "manifest.yaml").write_text(yaml.safe_dump(manifest), encoding="utf-8")

    _write_minimal_parquet(dataset_dir / "symbols/BTCUSDT.parquet", "BTCUSDT")
    _write_minimal_parquet(dataset_dir / "symbols/ETHUSDT.parquet", "ETHUSDT")

    loaded = load_dataset_manifest(str(dataset_dir))

    assert loaded.version == 1
    assert loaded.format == "parquet"
    assert loaded.symbols == ["BTCUSDT", "ETHUSDT"]
    assert loaded.files_by_symbol == {
        "BTCUSDT": "symbols/BTCUSDT.parquet",
        "ETHUSDT": "symbols/ETHUSDT.parquet",
    }


def test_load_legacy_manifest_normalizes(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir()

    manifest = {
        "format": "per_symbol_parquet",
        "symbols": ["BBB", "AAA"],
        "path": "symbols/{symbol}.parquet",
    }
    (dataset_dir / "manifest.yaml").write_text(yaml.safe_dump(manifest), encoding="utf-8")

    _write_minimal_parquet(dataset_dir / "symbols/BBB.parquet", "BBB")
    _write_minimal_parquet(dataset_dir / "symbols/AAA.parquet", "AAA")

    loaded = load_dataset_manifest(str(dataset_dir))

    assert loaded.symbols == ["BBB", "AAA"]
    assert loaded.files_by_symbol["BBB"] == "symbols/BBB.parquet"
    assert loaded.files_by_symbol["AAA"] == "symbols/AAA.parquet"


def test_missing_file_raises_valueerror(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir()

    manifest = {
        "version": 1,
        "format": "parquet",
        "files": [{"symbol": "BTCUSDT", "path": "symbols/BTCUSDT.parquet"}],
    }
    (dataset_dir / "manifest.yaml").write_text(yaml.safe_dump(manifest), encoding="utf-8")

    with pytest.raises(ValueError) as exc_info:
        load_dataset_manifest(str(dataset_dir))

    message = str(exc_info.value)
    assert str(dataset_dir) in message
    assert "symbols/BTCUSDT.parquet" in message
    assert "manifest.yaml" in message


def test_symbols_subset_applied_and_sorted_deterministically(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir()

    manifest = {
        "format": "per_symbol_parquet",
        "symbols": ["A", "B", "C"],
        "path": "symbols/{symbol}.parquet",
    }
    (dataset_dir / "manifest.yaml").write_text(yaml.safe_dump(manifest), encoding="utf-8")

    _write_minimal_parquet(dataset_dir / "symbols/A.parquet", "A")
    _write_minimal_parquet(dataset_dir / "symbols/B.parquet", "B")
    _write_minimal_parquet(dataset_dir / "symbols/C.parquet", "C")

    loaded = load_dataset_manifest(
        str(dataset_dir),
        config={"data": {"symbols_subset": ["C", "A"]}},
    )
    assert loaded.symbols == ["C", "A"]

    with pytest.raises(ValueError, match="unknown symbol"):
        load_dataset_manifest(
            str(dataset_dir),
            config={"data": {"symbols_subset": ["C", "Z"]}},
        )

    max_limited = load_dataset_manifest(
        str(dataset_dir),
        config={"data": {"max_symbols": 2}},
    )
    assert max_limited.symbols == ["A", "B"]
    assert len(max_limited.symbols) == 2
