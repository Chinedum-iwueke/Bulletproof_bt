"""Stress testing suite for backtest execution."""
from __future__ import annotations

from dataclasses import dataclass, replace
import datetime as dt
import random
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from bt.core.enums import Side
from bt.core.types import Fill
from bt.core.engine import BacktestEngine
from bt.data.feed import HistoricalDataFeed
from bt.data.loader import load_bars
from bt.execution.execution_model import ExecutionModel
from bt.execution.fees import FeeModel
from bt.execution.slippage import SlippageModel
from bt.logging.jsonl import JsonlWriter
from bt.logging.trades import TradesCsvWriter, prepare_run_dir, write_config_used
from bt.portfolio.portfolio import Portfolio
from bt.risk.risk_engine import RiskEngine
from bt.risk.spec import parse_risk_spec
from bt.strategy.coinflip import CoinFlipStrategy
from bt.universe.universe import UniverseEngine


@dataclass(frozen=True)
class StressScenario:
    name: str
    fee_mult: float = 1.0
    slippage_mult: float = 1.0
    add_delay_bars: int = 0
    drop_fill_prob: float = 0.0
    seed_offset: int = 0


@dataclass(frozen=True)
class StressResult:
    name: str
    run_id: str
    run_dir: Path
    final_equity: float
    n_trades: int
    n_fills: int


class StressExecutionWrapper:
    def __init__(
        self,
        *,
        base_execution: ExecutionModel,
        fee_mult: float,
        slippage_mult: float,
        drop_fill_prob: float,
        rng: random.Random,
    ) -> None:
        if fee_mult < 0:
            raise ValueError("fee_mult must be >= 0")
        if slippage_mult < 0:
            raise ValueError("slippage_mult must be >= 0")
        if not 0.0 <= drop_fill_prob <= 1.0:
            raise ValueError("drop_fill_prob must be in [0, 1]")
        self._base_execution = base_execution
        self._fee_mult = fee_mult
        self._slippage_mult = slippage_mult
        self._drop_fill_prob = drop_fill_prob
        self._rng = rng

    def process(
        self,
        *,
        ts: pd.Timestamp,
        bars_by_symbol: dict[str, Any],
        open_orders: list[Any],
    ) -> tuple[list[Any], list[Fill]]:
        updated_orders, fills = self._base_execution.process(
            ts=ts,
            bars_by_symbol=bars_by_symbol,
            open_orders=open_orders,
        )

        stressed_fills: list[Fill] = []
        for fill in fills:
            if self._drop_fill_prob > 0 and self._rng.random() < self._drop_fill_prob:
                continue
            stressed_fill = self._apply_fee_multiplier(fill)
            stressed_fill = self._apply_slippage_multiplier(stressed_fill)
            stressed_fills.append(stressed_fill)

        return updated_orders, stressed_fills

    def _apply_fee_multiplier(self, fill: Fill) -> Fill:
        if self._fee_mult == 1.0:
            return fill
        return replace(fill, fee=fill.fee * self._fee_mult)

    def _apply_slippage_multiplier(self, fill: Fill) -> Fill:
        if self._slippage_mult == 1.0:
            return fill
        qty = max(abs(fill.qty), 1e-12)
        old_slip_px = fill.slippage / qty
        new_slippage = fill.slippage * self._slippage_mult
        new_slip_px = new_slippage / qty

        if fill.side == Side.BUY:
            base_price = fill.price - old_slip_px
            new_price = base_price + new_slip_px
        elif fill.side == Side.SELL:
            base_price = fill.price + old_slip_px
            new_price = base_price - new_slip_px
        else:
            raise ValueError(f"Unsupported side: {fill.side}")

        return replace(fill, price=new_price, slippage=new_slippage)


def run_stress_suite(
    *,
    data_path: str | Path,
    config: dict[str, Any],
    scenarios: Iterable[StressScenario],
    output_root: Path = Path("outputs/runs"),
    seed: int = 42,
) -> dict[str, Any]:
    """Run backtests under multiple stress scenarios."""
    bars_df = load_bars(data_path)
    base_config = dict(config)
    base_config.setdefault("seed", seed)

    results: list[StressResult] = []
    summary: dict[str, dict[str, Any]] = {}
    scenario_list = list(scenarios)

    for idx, scenario in enumerate(scenario_list):
        scenario_config = dict(base_config)
        scenario_config["signal_delay_bars"] = int(
            scenario_config.get("signal_delay_bars", 1)
        ) + int(scenario.add_delay_bars)

        run_id = _make_scenario_run_id(scenario.name, idx)
        run_dir = prepare_run_dir(output_root, run_id)
        write_config_used(run_dir, scenario_config)

        datafeed = HistoricalDataFeed(bars_df)
        universe = UniverseEngine(
            min_history_bars=int(scenario_config.get("min_history_bars", 1)),
            lookback_bars=int(scenario_config.get("lookback_bars", 1)),
            min_avg_volume=float(scenario_config.get("min_avg_volume", 0.0)),
            lag_bars=int(scenario_config.get("lag_bars", 0)),
        )
        strategy = CoinFlipStrategy(
            seed=int(scenario_config.get("seed", seed)),
            p_trade=float(scenario_config.get("p_trade", 0.2)),
            cooldown_bars=int(scenario_config.get("cooldown_bars", 0)),
        )
        risk_cfg = scenario_config.get("risk", {}) if isinstance(scenario_config.get("risk"), dict) else {}
        risk_cfg_for_spec = dict(risk_cfg)
        if "max_positions" not in risk_cfg_for_spec and "max_positions" in scenario_config:
            risk_cfg_for_spec["max_positions"] = scenario_config["max_positions"]
        if "risk_per_trade_pct" not in risk_cfg_for_spec and "risk_per_trade_pct" in scenario_config:
            risk_cfg_for_spec["risk_per_trade_pct"] = scenario_config["risk_per_trade_pct"]
        if "mode" not in risk_cfg_for_spec:
            risk_cfg_for_spec["mode"] = "equity_pct"
        if "r_per_trade" not in risk_cfg_for_spec and "risk_per_trade_pct" in risk_cfg_for_spec:
            risk_cfg_for_spec["r_per_trade"] = risk_cfg_for_spec["risk_per_trade_pct"]
        risk_spec = parse_risk_spec({"risk": risk_cfg_for_spec})
        risk = RiskEngine(
            max_positions=int(risk_cfg_for_spec.get("max_positions", 5)),
            risk_per_trade_pct=risk_spec.r_per_trade,
            max_notional_per_symbol=scenario_config.get("max_notional_per_symbol"),
            config={"risk": risk_cfg_for_spec},
        )
        fee_model = FeeModel(
            maker_fee_bps=float(scenario_config.get("maker_fee_bps", 0.0)),
            taker_fee_bps=float(scenario_config.get("taker_fee_bps", 0.0)),
        )
        slippage_model = SlippageModel(
            k=float(scenario_config.get("slippage_k", 1.0)),
            atr_pct_cap=float(scenario_config.get("atr_pct_cap", 0.20)),
            impact_cap=float(scenario_config.get("impact_cap", 0.05)),
        )
        execution = ExecutionModel(
            fee_model=fee_model,
            slippage_model=slippage_model,
            delay_bars=int(scenario_config.get("signal_delay_bars", 1)),
        )
        rng = random.Random(seed + scenario.seed_offset)
        execution_wrapper = StressExecutionWrapper(
            base_execution=execution,
            fee_mult=scenario.fee_mult,
            slippage_mult=scenario.slippage_mult,
            drop_fill_prob=scenario.drop_fill_prob,
            rng=rng,
        )
        portfolio = Portfolio(
            initial_cash=float(scenario_config.get("initial_cash", 100000.0)),
            max_leverage=float(scenario_config.get("max_leverage", 2.0)),
        )

        decisions_writer = JsonlWriter(run_dir / "decisions.jsonl")
        fills_writer = JsonlWriter(run_dir / "fills.jsonl")
        trades_writer = TradesCsvWriter(run_dir / "trades.csv")
        equity_path = run_dir / "equity.csv"

        engine = BacktestEngine(
            datafeed=datafeed,
            universe=universe,
            strategy=strategy,
            risk=risk,
            execution=execution_wrapper,
            portfolio=portfolio,
            decisions_writer=decisions_writer,
            fills_writer=fills_writer,
            trades_writer=trades_writer,
            equity_path=equity_path,
            config={"risk": risk_cfg_for_spec},
        )
        engine.run()

        n_fills = _count_lines(run_dir / "fills.jsonl")
        n_trades = _count_csv_rows(run_dir / "trades.csv")

        result = StressResult(
            name=scenario.name,
            run_id=run_id,
            run_dir=run_dir,
            final_equity=portfolio.equity,
            n_trades=n_trades,
            n_fills=n_fills,
        )
        results.append(result)
        summary[scenario.name] = {
            "run_id": run_id,
            "run_dir": str(run_dir),
            "final_equity": portfolio.equity,
            "n_trades": n_trades,
            "n_fills": n_fills,
        }

    return {
        "seed": seed,
        "scenarios": [scenario.name for scenario in scenario_list],
        "results": results,
        "summary": summary,
    }


def _make_scenario_run_id(name: str, index: int) -> str:
    now = dt.datetime.now(dt.timezone.utc)
    safe_name = name.replace(" ", "_")
    return f"{safe_name}_{index}_{now:%Y%m%d_%H%M%S}"


def _count_lines(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8") as handle:
        return sum(1 for _ in handle)


def _count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8") as handle:
        rows = sum(1 for _ in handle)
    return max(rows - 1, 0)
