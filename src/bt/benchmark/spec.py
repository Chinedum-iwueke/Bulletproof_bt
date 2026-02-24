from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass(frozen=True)
class BenchmarkSpec:
    enabled: bool
    mode: str = "buy_hold"
    symbol: Optional[str] = None
    price_field: str = "close"
    initial_equity: Optional[float] = None
    fee_model: str = "none"
    baseline_strategy_name: Optional[str] = None
    baseline_strategy_params: dict[str, Any] | None = None


def parse_benchmark_spec(config: dict[str, Any]) -> BenchmarkSpec:
    benchmark_cfg = config.get("benchmark")
    if benchmark_cfg is None:
        return BenchmarkSpec(enabled=False)

    if not isinstance(benchmark_cfg, dict):
        raise ValueError(f"benchmark must be a mapping (got: {benchmark_cfg!r})")

    raw_enabled = benchmark_cfg.get("enabled", False)
    if not isinstance(raw_enabled, bool):
        raise ValueError(f"benchmark.enabled must be a bool (got: {raw_enabled!r})")

    raw_type = benchmark_cfg.get("type")
    if raw_enabled and raw_type is None:
        raw_type = "buy_hold"
    mode = str(raw_type) if raw_type is not None else "buy_hold"
    if mode not in {"buy_hold", "flat", "baseline_strategy"}:
        raise ValueError(
            "benchmark.type must be one of {'buy_hold','flat','baseline_strategy'} "
            f"(got: {raw_type!r})"
        )

    raw_price_field = benchmark_cfg.get("price_field", "close")
    if raw_price_field not in {"close", "open"}:
        raise ValueError(
            f"benchmark.price_field must be one of: close, open (got: {raw_price_field!r})"
        )

    raw_initial_equity = benchmark_cfg.get("initial_equity")
    if raw_initial_equity is not None:
        if isinstance(raw_initial_equity, bool) or not isinstance(raw_initial_equity, (int, float)):
            raise ValueError(
                f"benchmark.initial_equity must be > 0 (got: {raw_initial_equity!r})"
            )
        raw_initial_equity = float(raw_initial_equity)
        if raw_initial_equity <= 0:
            raise ValueError(
                f"benchmark.initial_equity must be > 0 (got: {benchmark_cfg.get('initial_equity')!r})"
            )

    raw_fee_model = benchmark_cfg.get("fee_model", "none")
    if raw_fee_model != "none":
        raise ValueError(f"benchmark.fee_model must be 'none' for V1 (got: {raw_fee_model!r})")

    raw_symbol = benchmark_cfg.get("symbol")
    symbol = raw_symbol if isinstance(raw_symbol, str) and raw_symbol.strip() else None

    if raw_enabled and mode == "buy_hold" and symbol is None:
        raise ValueError("benchmark.symbol is required when benchmark.enabled=true and benchmark.type=buy_hold")

    baseline_name: Optional[str] = None
    baseline_params: dict[str, Any] | None = None
    if raw_enabled and mode == "baseline_strategy":
        baseline_cfg = benchmark_cfg.get("baseline_strategy")
        if not isinstance(baseline_cfg, dict):
            raise ValueError(
                "benchmark.baseline_strategy is required when benchmark.type=baseline_strategy"
            )
        name = baseline_cfg.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ValueError(
                "benchmark.baseline_strategy.name is required when benchmark.type=baseline_strategy"
            )
        baseline_name = name.strip()
        params = baseline_cfg.get("params", {})
        if params is None:
            params = {}
        if not isinstance(params, dict):
            raise ValueError("benchmark.baseline_strategy.params must be a mapping when provided")
        baseline_params = dict(params)

    return BenchmarkSpec(
        enabled=raw_enabled,
        mode=mode,
        symbol=symbol,
        price_field=raw_price_field,
        initial_equity=raw_initial_equity,
        fee_model=raw_fee_model,
        baseline_strategy_name=baseline_name,
        baseline_strategy_params=baseline_params,
    )
