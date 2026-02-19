from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
import yaml

from bt import run_backtest
from bt.core.enums import Side
from bt.core.types import Bar, Signal
from bt.strategy.base import Strategy
from bt.strategy.signal_conflicts import resolve_signal_conflicts


def _ts() -> pd.Timestamp:
    return pd.Timestamp("2025-01-01T00:00:00Z")


def _entry_signal(*, side: Side, confidence: float = 1.0) -> Signal:
    return Signal(
        ts=_ts(),
        symbol="BTCUSDT",
        side=side,
        signal_type="entry",
        confidence=confidence,
        metadata={"stop_price": 99.0 if side == Side.BUY else 101.0},
    )


def _exit_signal() -> Signal:
    return Signal(
        ts=_ts(),
        symbol="BTCUSDT",
        side=Side.SELL,
        signal_type="entry_exit",
        confidence=1.0,
        metadata={"reduce_only": True, "close_only": True},
    )


def test_reject_raises_on_conflict() -> None:
    with pytest.raises(ValueError, match="strategy.signal_conflict_policy") as exc:
        resolve_signal_conflicts([_entry_signal(side=Side.BUY), _entry_signal(side=Side.SELL)], policy="reject")

    message = str(exc.value)
    assert "BTCUSDT" in message
    assert "2025-01-01T00:00:00+00:00" in message


def test_first_wins_deterministic() -> None:
    first = _entry_signal(side=Side.BUY, confidence=0.1)
    second = _entry_signal(side=Side.BUY, confidence=0.9)

    resolved, summaries = resolve_signal_conflicts([first, second], policy="first_wins")

    assert resolved == [first]
    assert len(summaries) == 1
    assert summaries[0].dropped_count == 1


def test_last_wins_deterministic() -> None:
    first = _entry_signal(side=Side.BUY, confidence=0.1)
    second = _entry_signal(side=Side.BUY, confidence=0.9)

    resolved, _ = resolve_signal_conflicts([first, second], policy="last_wins")

    assert resolved == [second]


def test_net_out_entry_entry_opposite_sides_drops_all_entries() -> None:
    resolved, summaries = resolve_signal_conflicts(
        [_entry_signal(side=Side.BUY), _entry_signal(side=Side.SELL)],
        policy="net_out",
    )

    assert resolved == []
    assert len(summaries) == 1
    assert summaries[0].kept is None


def test_net_out_exit_wins_over_entries() -> None:
    entry = _entry_signal(side=Side.BUY)
    exit_signal = _exit_signal()

    resolved, _ = resolve_signal_conflicts([entry, exit_signal], policy="net_out")

    assert resolved == [exit_signal]


def _write_config(tmp_path: Path, *, policy: str) -> Path:
    base_config = yaml.safe_load(Path("configs/engine.yaml").read_text(encoding="utf-8"))
    base_config["strategy"] = {
        "name": "coinflip",
        "signal_conflict_policy": policy,
    }
    config_path = tmp_path / f"config_{policy}.yaml"
    config_path.write_text(yaml.safe_dump(base_config), encoding="utf-8")
    return config_path


class _ConflictEmitterStrategy(Strategy):
    def __init__(self) -> None:
        self._emitted = False

    def on_bars(
        self,
        ts: pd.Timestamp,
        bars_by_symbol: dict[str, Bar],
        tradeable: set[str],
        ctx: dict[str, object],
    ) -> list[Signal]:
        if self._emitted or not tradeable:
            return []
        self._emitted = True
        symbol = sorted(tradeable)[0]
        bar = bars_by_symbol[symbol]
        return [
            Signal(
                ts=ts,
                symbol=symbol,
                side=Side.BUY,
                signal_type="entry",
                confidence=1.0,
                metadata={"stop_price": bar.close - 0.5},
            ),
            Signal(
                ts=ts,
                symbol=symbol,
                side=Side.SELL,
                signal_type="entry",
                confidence=1.0,
                metadata={"stop_price": bar.close + 0.5},
            ),
        ]


def test_engine_smoke_conflict_policy_rejects(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import bt.strategy as strategy_module

    monkeypatch.setitem(strategy_module.STRATEGY_REGISTRY, "coinflip", _ConflictEmitterStrategy)
    config_path = _write_config(tmp_path, policy="reject")

    with pytest.raises(ValueError, match="strategy.signal_conflict_policy"):
        run_backtest(
            config_path=str(config_path),
            data_path="data/curated/sample.csv",
            out_dir=str(tmp_path / "runs"),
        )


def test_engine_smoke_conflict_policy_last_wins(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import bt.strategy as strategy_module

    monkeypatch.setitem(strategy_module.STRATEGY_REGISTRY, "coinflip", _ConflictEmitterStrategy)
    config_path = _write_config(tmp_path, policy="last_wins")

    run_dir = Path(
        run_backtest(
            config_path=str(config_path),
            data_path="data/curated/sample.csv",
            out_dir=str(tmp_path / "runs"),
        )
    )

    decisions_path = run_dir / "decisions.jsonl"
    records = [json.loads(line) for line in decisions_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    approved = [record for record in records if record.get("approved") is True]

    assert len(approved) == 1
    assert approved[0]["signal"]["side"] == "SELL"
