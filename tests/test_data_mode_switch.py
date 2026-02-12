"""Tests for data feed routing and data.mode defaults."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from bt.data.load_feed import load_feed


def test_dataset_directory_defaults_to_streaming_requires_manifest(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset_dir"
    dataset_dir.mkdir()

    with pytest.raises(ValueError, match="manifest.yaml"):
        load_feed(str(dataset_dir), {})


def test_single_file_defaults_to_dataframe_and_yields_bars(tmp_path: Path) -> None:
    csv_path = tmp_path / "bars.csv"
    rows = [
        {
            "ts": pd.Timestamp("2025-01-01 00:00:00", tz="UTC"),
            "symbol": "AAA",
            "open": 1.0,
            "high": 1.2,
            "low": 0.9,
            "close": 1.1,
            "volume": 10.0,
        },
        {
            "ts": pd.Timestamp("2025-01-01 00:01:00", tz="UTC"),
            "symbol": "AAA",
            "open": 1.1,
            "high": 1.3,
            "low": 1.0,
            "close": 1.2,
            "volume": 11.0,
        },
    ]
    pd.DataFrame(rows).to_csv(csv_path, index=False)

    feed = load_feed(str(csv_path), {})

    assert hasattr(feed, "next")
    first_batch = feed.next()
    assert first_batch is not None
    assert len(first_batch) >= 1

    first_bar = first_batch[0]
    assert first_bar.ts == pd.Timestamp("2025-01-01 00:00:00", tz="UTC")
    assert first_bar.open == pytest.approx(1.0)
    assert first_bar.high == pytest.approx(1.2)
    assert first_bar.low == pytest.approx(0.9)
    assert first_bar.close == pytest.approx(1.1)
    assert first_bar.volume == pytest.approx(10.0)


def test_invalid_data_mode_raises_value_error(tmp_path: Path) -> None:
    csv_path = tmp_path / "bars.csv"
    pd.DataFrame(
        [
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
    ).to_csv(csv_path, index=False)

    with pytest.raises(ValueError, match="data.mode must be one of"):
        load_feed(str(csv_path), {"data": {"mode": "banana"}})


def test_missing_path_raises_value_error(tmp_path: Path) -> None:
    missing_path = tmp_path / "does_not_exist.csv"

    with pytest.raises(ValueError, match="Data path not found"):
        load_feed(str(missing_path), {})
