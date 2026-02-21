from __future__ import annotations

import json
from pathlib import Path
from collections.abc import Mapping
from typing import Any

import pandas as pd
import pytest
import yaml

from bt.api import _build_engine, run_backtest
from bt.core.config_resolver import resolve_config
from bt.core.enums import Side
from bt.core.types import Signal
from bt.data.load_feed import load_feed


def _write_streaming_dataset(dataset_dir: Path, *, minutes: list[int], symbol: str = "AAA") -> None:
    dataset_dir.mkdir(parents=True, exist_ok=True)
    start = pd.Timestamp("2025-01-01 00:00:00", tz="UTC")
    rows = []
    for minute in minutes:
        ts = start + pd.Timedelta(minutes=minute)
        rows.append(
            {
                "ts": ts,
                "open": 100.0 + minute,
                "high": 101.0 + minute,
                "low": 99.0 + minute,
                "close": 100.5 + minute,
                "volume": 10.0,
            }
        )

    bars_path = dataset_dir / f"{symbol}.parquet"
    pd.DataFrame(rows).to_parquet(bars_path, index=False)
    manifest = {
        "format": "per_symbol_parquet",
        "symbols": [symbol],
        "path": "{symbol}.parquet",
    }
    (dataset_dir / "manifest.yaml").write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")


def _base_config() -> dict[str, Any]:
    return {
        "strategy": {"name": "coinflip", "p_enter": 0.0},
        "risk": {"mode": "r_fixed", "r_per_trade": 0.01},
        "data": {"mode": "streaming"},
    }


def _equity_timestamps(run_dir: Path) -> list[pd.Timestamp]:
    equity = pd.read_csv(run_dir / "equity.csv")
    return [pd.Timestamp(value) for value in equity["ts"].tolist()]


def test_mode_a_resamples_engine_clock_to_15m(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    _write_streaming_dataset(dataset_dir, minutes=list(range(0, 46)))

    config = _base_config()
    config["data"].update({"engine_timeframe": "15m", "resample_strict": True})
    config_path = tmp_path / "mode_a.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    run_dir = Path(
        run_backtest(
            config_path=str(config_path),
            data_path=str(dataset_dir),
            out_dir=str(tmp_path / "outputs"),
            run_name="mode-a",
        )
    )

    ts_values = _equity_timestamps(run_dir)
    expected = [
        pd.Timestamp("2025-01-01 00:00:00+00:00"),
        pd.Timestamp("2025-01-01 00:15:00+00:00"),
        pd.Timestamp("2025-01-01 00:30:00+00:00"),
    ]
    assert sorted(set(ts_values)) == expected
    assert all(ts.minute in {0, 15, 30, 45} for ts in ts_values)


def test_strict_completeness_suppresses_incomplete_bucket(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    minutes = [minute for minute in range(0, 46) if minute != 7]
    _write_streaming_dataset(dataset_dir, minutes=minutes)

    config = _base_config()
    config["data"].update({"engine_timeframe": "15m", "resample_strict": True})
    config_path = tmp_path / "strict.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    run_dir = Path(
        run_backtest(
            config_path=str(config_path),
            data_path=str(dataset_dir),
            out_dir=str(tmp_path / "outputs"),
            run_name="strict",
        )
    )

    ts_values = _equity_timestamps(run_dir)
    assert pd.Timestamp("2025-01-01 00:00:00+00:00") not in ts_values
    assert ts_values == [
        pd.Timestamp("2025-01-01 00:15:00+00:00"),
        pd.Timestamp("2025-01-01 00:30:00+00:00"),
    ]


def test_mode_b_entry_gating_15m_exits_still_1m(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    class _AlwaysEntryExitStrategy:
        def on_bars(self, ts, bars_by_symbol, tradeable, ctx):
            symbol = "AAA"
            positions = ctx.get("positions") if hasattr(ctx, "get") else {}
            position = ((positions or {}).get(symbol) or {}) if isinstance(positions, Mapping) else {}
            qty = float(position.get("qty", 0.0))
            if qty == 0.0:
                return [Signal(ts=ts, symbol=symbol, side=Side.BUY, signal_type="entry", confidence=1.0, metadata={"stop_price": 99.0})]
            return [
                Signal(
                    ts=ts,
                    symbol=symbol,
                    side=Side.SELL,
                    signal_type="entry_exit",
                    confidence=1.0,
                    metadata={"is_exit": True, "close_only": True},
                )
            ]

    monkeypatch.setattr("bt.strategy.make_strategy", lambda *args, **kwargs: _AlwaysEntryExitStrategy())

    dataset_dir = tmp_path / "dataset"
    _write_streaming_dataset(dataset_dir, minutes=list(range(0, 36)))

    config = _base_config()
    config["data"].update({"entry_timeframe": "15m", "exit_timeframe": "1m"})
    resolved = resolve_config(config)

    feed = load_feed(str(dataset_dir), resolved)
    run_dir = tmp_path / "run_b"
    run_dir.mkdir(parents=True, exist_ok=True)
    engine = _build_engine(resolved, feed, run_dir)
    engine.run()

    decisions = [json.loads(line) for line in (run_dir / "decisions.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]

    approved_entries = [
        pd.Timestamp(row["ts"])
        for row in decisions
        if row.get("approved") is True and str((row.get("signal") or {}).get("signal_type")) == "entry"
    ]
    approved_exits = [
        pd.Timestamp(row["ts"])
        for row in decisions
        if row.get("approved") is True and bool(((row.get("signal") or {}).get("metadata") or {}).get("is_exit"))
    ]

    assert approved_entries
    assert all(ts == ts.floor("15min") for ts in approved_entries)
    assert approved_exits
    assert any(ts != ts.floor("15min") for ts in approved_exits)


def test_invalid_timeframe_mode_combinations_raise() -> None:
    config = _base_config()
    config["data"].update({"engine_timeframe": "15m", "entry_timeframe": "15m"})
    with pytest.raises(ValueError, match="choose one mode"):
        _build_engine(resolve_config(config), datafeed=object(), run_dir=Path("."))

    config = _base_config()
    config["data"].update({"entry_timeframe": "15m", "exit_timeframe": "5m"})
    with pytest.raises(ValueError, match="not supported yet"):
        _build_engine(resolve_config(config), datafeed=object(), run_dir=Path("."))
