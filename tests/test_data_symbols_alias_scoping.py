from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from bt.core.config_resolver import resolve_config
from bt.data.load_feed import load_feed
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

    for symbol in ("AAA", "BBB", "CCC"):
        frame = pd.DataFrame(
            {
                "ts": [pd.Timestamp("2024-01-01T00:00:00Z")],
                "symbol": [symbol],
                "open": [1.0],
                "high": [1.1],
                "low": [0.9],
                "close": [1.05],
                "volume": [100.0],
            }
        )
        frame.to_parquet(symbols_dir / f"{symbol}.parquet", index=False)


def test_data_symbols_alias_scopes_dataset_dir_streaming_runs(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    run_dir = tmp_path / "run"
    run_dir.mkdir(parents=True, exist_ok=True)
    _write_legacy_dataset(dataset_dir)

    config = resolve_config({"data": {"mode": "streaming", "symbols": ["BBB"]}})

    feed = load_feed(str(dataset_dir), config)
    assert feed.symbols() == ["BBB"]

    write_data_scope(run_dir, config=config, dataset_dir=str(dataset_dir))
    payload = json.loads((run_dir / "data_scope.json").read_text(encoding="utf-8"))

    assert payload["requested_symbols"] == ["BBB"]
    assert payload["effective_symbols"] == ["BBB"]
    assert payload["effective_symbol_count"] == 1


def test_data_symbols_alias_conflict_raises() -> None:
    with pytest.raises(ValueError, match="data.symbols") as exc_info:
        resolve_config({"data": {"symbols": ["AAA"], "symbols_subset": ["BBB"]}})

    message = str(exc_info.value)
    assert "data.symbols_subset" in message
    assert "['AAA']" in message
    assert "['BBB']" in message


def test_identical_alias_and_canonical_allowed(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    _write_legacy_dataset(dataset_dir)

    config = resolve_config(
        {
            "data": {
                "mode": "streaming",
                "symbols": ["BBB", "BBB"],
                "symbols_subset": ["BBB"],
            }
        }
    )

    feed = load_feed(str(dataset_dir), config)
    assert feed.symbols() == ["BBB"]


def test_invalid_data_symbols_type_raises() -> None:
    with pytest.raises(ValueError, match="list of strings") as exc_info:
        resolve_config({"data": {"symbols": "BTCUSDT"}})

    assert "data.symbols" in str(exc_info.value)
