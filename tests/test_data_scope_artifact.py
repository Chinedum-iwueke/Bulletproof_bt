from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from bt.logging.trades import write_data_scope


def _write_legacy_dataset(dataset_dir: Path) -> None:
    symbols_dir = dataset_dir / "symbols"
    symbols_dir.mkdir(parents=True, exist_ok=True)
    (dataset_dir / "manifest.yaml").write_text(
        "format: per_symbol_parquet\n"
        "symbols: [AAA, BBB, CCC]\n"
        'path: "symbols/{symbol}.parquet"\n',
        encoding="utf-8",
    )

    rows = [
        {
            "ts": pd.Timestamp("2024-01-01T00:00:00Z"),
            "open": 1.0,
            "high": 1.2,
            "low": 0.9,
            "close": 1.1,
            "volume": 10.0,
        }
    ]

    for symbol in ("AAA", "BBB", "CCC"):
        frame = pd.DataFrame(rows)
        frame["symbol"] = symbol
        frame.to_parquet(symbols_dir / f"{symbol}.parquet", index=False)


def test_write_data_scope_writes_when_scope_knobs_active(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    run_dir = tmp_path / "run"
    run_dir.mkdir(parents=True, exist_ok=True)
    _write_legacy_dataset(dataset_dir)

    config = {"data": {"symbols_subset": ["BBB", "AAA", "BBB"], "max_symbols": 2}}

    write_data_scope(run_dir, config=config, dataset_dir=str(dataset_dir))

    artifact = run_dir / "data_scope.json"
    assert artifact.exists()

    payload = json.loads(artifact.read_text(encoding="utf-8"))
    assert payload["requested_symbols_subset"] == ["BBB", "AAA", "BBB"]
    assert payload["requested_max_symbols"] == 2
    assert payload["effective_symbols"] == ["BBB", "AAA"]
    assert payload["effective_symbol_count"] == 2


def test_write_data_scope_no_file_without_scope_knobs(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir(parents=True, exist_ok=True)

    write_data_scope(run_dir, config={}, dataset_dir=None)

    assert not (run_dir / "data_scope.json").exists()


def test_write_data_scope_serializes_date_range(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir(parents=True, exist_ok=True)

    config = {
        "data": {
            "date_range": {
                "start": "2025-01-01T00:00:00Z",
                "end": "2025-01-02T00:00:00Z",
            }
        }
    }

    write_data_scope(run_dir, config=config, dataset_dir=None)

    payload = json.loads((run_dir / "data_scope.json").read_text(encoding="utf-8"))
    assert payload["date_range"] == {
        "start": "2025-01-01T00:00:00+00:00",
        "end": "2025-01-02T00:00:00+00:00",
    }
