from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import yaml

from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.execution.intrabar import (
    IntrabarSpec,
    parse_intrabar_spec,
    resolve_ambiguous_tp_sl,
)
from bt.experiments.grid_runner import run_grid
from bt.strategy.base import Strategy


class _SingleSignalStrategy(Strategy):
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
                    signal_type="intrabar_mode_test",
                    confidence=1.0,
                    metadata={"strategy": "intrabar_mode_test_strategy", "stop_price": bar.close - self._stop_offset},
                )
            )
        return signals


def _ambiguous_bar() -> Bar:
    return Bar(
        ts=pd.Timestamp("2024-01-01T00:00:00Z"),
        symbol="AAA",
        open=100.0,
        high=105.0,
        low=95.0,
        close=101.0,
        volume=1000.0,
    )


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
    return dataset_dir


def test_default_intrabar_mode_is_worst_case() -> None:
    spec = parse_intrabar_spec({"execution": {}})
    assert spec.mode == "worst_case"


def test_invalid_intrabar_mode_raises() -> None:
    try:
        parse_intrabar_spec({"execution": {"intrabar_mode": "banana"}})
    except ValueError as exc:
        assert "execution.intrabar_mode" in str(exc)
    else:
        raise AssertionError("Expected ValueError for invalid execution.intrabar_mode")


def test_intrabar_tie_break_worst_case_long() -> None:
    outcome, price = resolve_ambiguous_tp_sl(
        position_side=Side.BUY,
        take_profit=104.0,
        stop_loss=96.0,
        bar=_ambiguous_bar(),
        intrabar_spec=IntrabarSpec(mode="worst_case"),
    )
    assert outcome == "sl"
    assert price == 96.0


def test_intrabar_tie_break_best_case_long() -> None:
    outcome, price = resolve_ambiguous_tp_sl(
        position_side=Side.BUY,
        take_profit=104.0,
        stop_loss=96.0,
        bar=_ambiguous_bar(),
        intrabar_spec=IntrabarSpec(mode="best_case"),
    )
    assert outcome == "tp"
    assert price == 104.0


def test_intrabar_midpoint_is_deterministic() -> None:
    bar = _ambiguous_bar()
    spec = IntrabarSpec(mode="midpoint")
    outcomes = [
        resolve_ambiguous_tp_sl(
            position_side=Side.BUY,
            take_profit=104.0,
            stop_loss=96.0,
            bar=bar,
            intrabar_spec=spec,
        )
        for _ in range(3)
    ]
    assert all(outcome == "midpoint" for outcome, _ in outcomes)
    assert all(price == (bar.high + bar.low) / 2.0 for _, price in outcomes)
    assert outcomes[0] == outcomes[1] == outcomes[2]


def test_run_status_records_intrabar_mode(tmp_path: Path) -> None:
    import bt.strategy as strategy_module

    strategy_module.STRATEGY_REGISTRY["intrabar_mode_test_strategy"] = _SingleSignalStrategy
    config = {
        "initial_cash": 10000.0,
        "risk": {"max_positions": 1, "mode": "r_fixed", "r_per_trade": 0.01, "risk_per_trade_pct": 0.01, "stop": {}},
        "signal_delay_bars": 0,
        "strategy": {"name": "intrabar_mode_test_strategy"},
        "maker_fee_bps": 0.0,
        "taker_fee_bps": 0.0,
        "slippage_k": 0.0,
        "execution": {"intrabar_mode": "best_case"},
    }
    exp = {"version": 1, "grid": {"strategy.seed": [1]}, "run_naming": {"template": "seed{strategy.seed}"}}

    out_path = tmp_path / "intrabar_status"
    run_grid(
        config=config,
        experiment_cfg=exp,
        data_path=str(_write_dataset(tmp_path / "dataset")),
        out_path=out_path,
    )

    status_path = out_path / "runs" / "run_001__seed1" / "run_status.json"
    payload = json.loads(status_path.read_text(encoding="utf-8"))
    assert payload["intrabar_mode"] == "best_case"
