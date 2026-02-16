from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass(frozen=True)
class BenchmarkSpec:
    enabled: bool
    symbol: Optional[str] = None
    price_field: str = "close"
    initial_equity: Optional[float] = None
    fee_model: str = "none"


def parse_benchmark_spec(config: dict[str, Any]) -> BenchmarkSpec:
    """
    Parse and validate benchmark config.

    Expected config shape (all optional):
      benchmark:
        enabled: bool
        symbol: str              # required if enabled
        price_field: close|open  # optional; default close
        initial_equity: float|null
        fee_model: none          # V1 only

    Defaults:
      - If benchmark missing: enabled=False
      - If enabled=False: symbol may be None and is ignored

    Validation:
      - enabled must be bool if present
      - if enabled True: symbol must be non-empty string
      - price_field must be one of {"close","open"} (V1)
      - initial_equity if provided must be > 0
      - fee_model must be "none" for V1; anything else => ValueError
    """

    benchmark_cfg = config.get("benchmark")
    if benchmark_cfg is None:
        return BenchmarkSpec(enabled=False)

    if not isinstance(benchmark_cfg, dict):
        raise ValueError(f"benchmark must be a mapping (got: {benchmark_cfg!r})")

    raw_enabled = benchmark_cfg.get("enabled", False)
    if not isinstance(raw_enabled, bool):
        raise ValueError(f"benchmark.enabled must be a bool (got: {raw_enabled!r})")

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
    if raw_enabled:
        if not isinstance(raw_symbol, str) or not raw_symbol.strip():
            raise ValueError("benchmark.symbol is required when benchmark.enabled=true")
        symbol = raw_symbol
    else:
        symbol = raw_symbol if isinstance(raw_symbol, str) and raw_symbol.strip() else None

    return BenchmarkSpec(
        enabled=raw_enabled,
        symbol=symbol,
        price_field=raw_price_field,
        initial_equity=raw_initial_equity,
        fee_model=raw_fee_model,
    )
