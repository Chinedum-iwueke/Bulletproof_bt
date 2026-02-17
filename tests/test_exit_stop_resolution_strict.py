from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import yaml

from bt.api import run_backtest
from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.risk.risk_engine import RiskEngine
from bt.strategy.base import Strategy


class _EntryThenExitNoStopStrategy(Strategy):
    def __init__(self) -> None:
        self._step = 0

    def on_bars(self, ts: pd.Timestamp, bars_by_symbol: dict[str, Bar], tradeable: set[str], ctx: dict) -> list[Signal]:
        del ctx
        if not tradeable:
            return []
        symbol = sorted(tradeable)[0]
        bar = bars_by_symbol[symbol]
        self._step += 1
        if self._step == 1:
            return [
                Signal(
                    ts=ts,
                    symbol=symbol,
                    side=Side.BUY,
                    signal_type="entry_with_stop",
                    confidence=1.0,
                    metadata={"strategy": "entry_then_exit", "stop_price": bar.close - 1.0},
                )
            ]
        if self._step == 2:
            return [
                Signal(
                    ts=ts,
                    symbol=symbol,
                    side=Side.SELL,
                    signal_type="entry_then_exit_exit",
                    confidence=1.0,
                    metadata={"strategy": "entry_then_exit"},
                )
            ]
        return []


def test_exit_signals_do_not_get_stop_rejections_in_strict_mode(tmp_path: Path) -> None:
    import bt.strategy as strategy_module

    strategy_module.STRATEGY_REGISTRY["entry_then_exit_no_stop"] = _EntryThenExitNoStopStrategy

    override = {
        "signal_delay_bars": 0,
        "strategy": {"name": "entry_then_exit_no_stop"},
        "risk": {"mode": "r_fixed", "r_per_trade": 0.01, "stop_resolution": "strict", "stop": {}},
    }
    override_path = tmp_path / "override.yaml"
    override_path.write_text(yaml.safe_dump(override, sort_keys=False), encoding="utf-8")

    run_dir = Path(
        run_backtest(
            config_path="configs/engine.yaml",
            data_path="data/curated/sample.csv",
            out_dir=str(tmp_path / "out"),
            override_paths=[str(override_path)],
            run_name="strict-exit-bypass",
        )
    )

    decisions = [
        json.loads(line)
        for line in (run_dir / "decisions.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    rejected_exit_stop = [
        row
        for row in decisions
        if row.get("approved") is False
        and str((row.get("signal") or {}).get("signal_type", "")).endswith("_exit")
        and (
            "stop_unresolvable" in str(row.get("reason", ""))
            or "stop_too_small" in str(row.get("reason", ""))
        )
    ]
    assert rejected_exit_stop == []

    approved_exit = [
        row
        for row in decisions
        if row.get("approved") is True and str((row.get("signal") or {}).get("signal_type", "")).endswith("_exit")
    ]
    assert approved_exit
    assert approved_exit[0]["order"]["metadata"]["stop_resolution_skipped"] is True
    assert approved_exit[0]["order"]["metadata"]["stop_resolution_skip_reason"] == "exit_signal"


def test_entry_without_stop_rejects_with_strict_reason_and_hint() -> None:
    engine = RiskEngine(
        max_positions=5,
        config={"risk": {"mode": "r_fixed", "r_per_trade": 0.01, "qty_rounding": "none", "stop": {}}},
    )
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    bar = Bar(ts=ts, symbol="BTC", open=100.0, high=110.0, low=100.0, close=105.0, volume=1.0)
    signal = Signal(ts=ts, symbol="BTC", side=Side.BUY, signal_type="unit", confidence=1.0, metadata={})

    order_intent, reason = engine.signal_to_order_intent(
        ts=ts,
        signal=signal,
        bar=bar,
        equity=10_000.0,
        free_margin=10_000.0,
        open_positions=0,
        max_leverage=2.0,
        current_qty=0.0,
    )

    assert order_intent is None
    assert reason.startswith("risk_rejected:stop_unresolvable:strict")
    assert "signal_type=unit" in reason
    assert "ENTRY_requires_explicit_stop_price_or_strategy_must_provide_resolvable_stop_metadata" in reason
