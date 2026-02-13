from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import yaml

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.experiments.grid_runner import run_grid
from bt.strategy.base import Strategy


class _NoStopSignalStrategy(Strategy):
    def __init__(self) -> None:
        self._fired: set[str] = set()

    def on_bars(self, ts: pd.Timestamp, bars_by_symbol: dict[str, Bar], tradeable: set[str], ctx: dict) -> list[Signal]:
        del ctx
        signals: list[Signal] = []
        for symbol in sorted(tradeable):
            if symbol in self._fired:
                continue
            self._fired.add(symbol)
            signals.append(
                Signal(
                    ts=ts,
                    symbol=symbol,
                    side=Side.BUY,
                    signal_type="test_no_stop",
                    confidence=1.0,
                    metadata={"strategy": "test_no_stop_signal"},
                )
            )
        return signals


class _ExplicitStopSignalStrategy(Strategy):
    def __init__(self, stop_offset: float = 1.0) -> None:
        self._fired: set[str] = set()
        self._stop_offset = float(stop_offset)

    def on_bars(self, ts: pd.Timestamp, bars_by_symbol: dict[str, Bar], tradeable: set[str], ctx: dict) -> list[Signal]:
        del ctx
        signals: list[Signal] = []
        for symbol in sorted(tradeable):
            if symbol in self._fired:
                continue
            bar = bars_by_symbol[symbol]
            self._fired.add(symbol)
            signals.append(
                Signal(
                    ts=ts,
                    symbol=symbol,
                    side=Side.BUY,
                    signal_type="test_explicit_stop",
                    confidence=1.0,
                    metadata={"strategy": "test_explicit_stop_signal", "stop_price": bar.close - self._stop_offset},
                )
            )
        return signals


def _write_dataset(dataset_dir: Path) -> Path:
    dataset_dir.mkdir(parents=True, exist_ok=True)
    ts_index = pd.date_range("2024-01-01", periods=4, freq="min", tz="UTC")
    rows: list[dict[str, object]] = []
    for i, ts in enumerate(ts_index):
        rows.append(
            {
                "ts": ts,
                "symbol": "AAA",
                "open": float(100 + i),
                "high": float(101 + i),
                "low": float(99 + i),
                "close": float(100.5 + i),
                "volume": float(1000 + i),
            }
        )
    bars = pd.DataFrame(rows)
    bars.to_parquet(dataset_dir / "bars.parquet", index=False)
    with (dataset_dir / "manifest.yaml").open("w", encoding="utf-8") as handle:
        yaml.safe_dump({"version": 1, "format": "parquet", "files": ["bars.parquet"]}, handle, sort_keys=False)
    return dataset_dir / "bars.parquet"


def _base_config(strategy_name: str) -> dict:
    return {
        "initial_cash": 10000.0,
        "max_leverage": 10.0,
        "risk": {"max_positions": 1, "mode": "r_fixed", "r_per_trade": 0.01, "risk_per_trade_pct": 0.01, "stop": {}},
        "signal_delay_bars": 0,
        "strategy": {"name": strategy_name},
        "maker_fee_bps": 0.0,
        "taker_fee_bps": 0.0,
        "slippage_k": 0.0,
    }


def _run_single(tmp_path: Path, *, strategy_name: str, out_name: str) -> dict:
    import bt.strategy as strategy_module

    strategy_module.STRATEGY_REGISTRY["test_no_stop_signal"] = _NoStopSignalStrategy
    strategy_module.STRATEGY_REGISTRY["test_explicit_stop_signal"] = _ExplicitStopSignalStrategy

    exp = {"version": 1, "grid": {"strategy.seed": [1]}, "run_naming": {"template": "seed{strategy.seed}"}}
    out_path = tmp_path / out_name
    data_path = _write_dataset(tmp_path / f"{out_name}_dataset")
    run_grid(config=_base_config(strategy_name), experiment_cfg=exp, data_path=str(data_path), out_path=out_path)
    status_path = out_path / "runs" / "run_001__seed1" / "run_status.json"
    return json.loads(status_path.read_text(encoding="utf-8"))


def test_run_status_records_legacy_proxy(tmp_path: Path) -> None:
    payload = _run_single(tmp_path, strategy_name="test_no_stop_signal", out_name="legacy")
    assert payload["stop_resolution"] == "legacy_high_low_proxy"
    assert payload["used_legacy_stop_proxy"] is True
    assert payload["r_metrics_valid"] is False
    assert any("Legacy stop proxy used" in note for note in payload["notes"])


def test_run_status_records_explicit_stop(tmp_path: Path) -> None:
    payload = _run_single(tmp_path, strategy_name="test_explicit_stop_signal", out_name="explicit")
    assert payload["stop_resolution"] == "explicit_stop_price"
    assert payload["used_legacy_stop_proxy"] is False
    assert payload["r_metrics_valid"] is True


def test_run_status_new_keys_are_deterministic(tmp_path: Path) -> None:
    payload_a = _run_single(tmp_path, strategy_name="test_explicit_stop_signal", out_name="determinism_a")
    payload_b = _run_single(tmp_path, strategy_name="test_explicit_stop_signal", out_name="determinism_b")

    keys = ["stop_resolution", "used_legacy_stop_proxy", "r_metrics_valid", "notes"]
    assert {key: payload_a[key] for key in keys} == {key: payload_b[key] for key in keys}
