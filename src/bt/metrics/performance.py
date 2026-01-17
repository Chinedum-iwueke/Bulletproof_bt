"""Performance metrics computation for backtest runs."""
from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd


@dataclass(frozen=True)
class PerformanceReport:
    run_id: str
    final_equity: float
    total_trades: int
    ev_net: float
    ev_gross: float
    win_rate: float
    max_drawdown_pct: float
    max_drawdown_duration_bars: int
    tail_loss_p95: float
    tail_loss_p99: float
    fee_total: float
    slippage_total: float
    fee_drag_pct_of_gross: Optional[float]
    slippage_drag_pct_of_gross: Optional[float]
    ev_by_bucket: Dict[str, float]
    trades_by_bucket: Dict[str, int]
    extra: Dict[str, Any]


def _coerce_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0)


def _sum_costs(df: pd.DataFrame, *, preferred: str, fallbacks: list[str]) -> pd.Series:
    if preferred in df.columns:
        return _coerce_numeric(df[preferred])
    available = [col for col in fallbacks if col in df.columns]
    if not available:
        return pd.Series(0.0, index=df.index)
    if len(available) == 1:
        return _coerce_numeric(df[available[0]])
    return _coerce_numeric(df[available]).sum(axis=1)


def _max_drawdown_duration(dd: pd.Series) -> int:
    if dd.empty:
        return 0
    underwater = dd < 0
    max_len = 0
    current = 0
    for flag in underwater:
        if flag:
            current += 1
            max_len = max(max_len, current)
        else:
            current = 0
    return max_len


def _bucket_metrics(
    pnl_net: pd.Series, bucket_series: Optional[pd.Series]
) -> tuple[Dict[str, float], Dict[str, int]]:
    if pnl_net.empty:
        return {"all": 0.0}, {"all": 0}
    if bucket_series is None:
        return {"all": float(pnl_net.mean())}, {"all": int(pnl_net.shape[0])}
    buckets = bucket_series.fillna("unknown").astype(str)
    grouped = pnl_net.groupby(buckets)
    ev = grouped.mean().sort_index()
    counts = grouped.size().sort_index()
    ev_by_bucket = {str(idx): float(val) for idx, val in ev.items()}
    trades_by_bucket = {str(idx): int(val) for idx, val in counts.items()}
    return ev_by_bucket, trades_by_bucket


def compute_performance(run_dir: str | Path) -> PerformanceReport:
    """
    Load run_dir/equity.csv and run_dir/trades.csv, compute required metrics,
    and return a PerformanceReport.
    """
    run_path = Path(run_dir)
    run_id = run_path.name

    equity_path = run_path / "equity.csv"
    trades_path = run_path / "trades.csv"

    equity_df = pd.read_csv(equity_path)
    trades_df = pd.read_csv(trades_path)

    equity_col = None
    for candidate in ["equity", "total_equity", "portfolio_equity"]:
        if candidate in equity_df.columns:
            equity_col = candidate
            break
    if equity_col is None:
        raise ValueError("Equity column not found in equity.csv")

    equity_series = _coerce_numeric(equity_df[equity_col])
    final_equity = float(equity_series.iloc[-1]) if not equity_series.empty else 0.0

    peak = equity_series.cummax()
    dd = equity_series / peak - 1.0 if not equity_series.empty else equity_series
    max_drawdown_pct = float(dd.min()) if not dd.empty else 0.0
    max_drawdown_duration_bars = _max_drawdown_duration(dd)

    total_trades = int(trades_df.shape[0])
    if total_trades == 0:
        return PerformanceReport(
            run_id=run_id,
            final_equity=final_equity,
            total_trades=0,
            ev_net=0.0,
            ev_gross=0.0,
            win_rate=0.0,
            max_drawdown_pct=max_drawdown_pct,
            max_drawdown_duration_bars=max_drawdown_duration_bars,
            tail_loss_p95=0.0,
            tail_loss_p99=0.0,
            fee_total=0.0,
            slippage_total=0.0,
            fee_drag_pct_of_gross=None,
            slippage_drag_pct_of_gross=None,
            ev_by_bucket={"all": 0.0},
            trades_by_bucket={"all": 0},
            extra={},
        )

    pnl_col = "pnl_net" if "pnl_net" in trades_df.columns else "pnl"
    if pnl_col not in trades_df.columns:
        raise ValueError("Trades must include pnl_net or pnl column")

    pnl_net = _coerce_numeric(trades_df[pnl_col])
    fees = _sum_costs(trades_df, preferred="fees_total", fallbacks=["fees", "fee"])
    slippage = _sum_costs(
        trades_df, preferred="slippage_total", fallbacks=["slippage", "slip"]
    )

    pnl_gross = pnl_net + fees + slippage
    ev_net = float(pnl_net.mean())
    ev_gross = float(pnl_gross.mean())
    win_rate = float((pnl_net > 0).mean())

    loss_values = -pnl_net[pnl_net < 0]
    if loss_values.empty:
        tail_loss_p95 = 0.0
        tail_loss_p99 = 0.0
    else:
        tail_loss_p95 = float(loss_values.quantile(0.95))
        tail_loss_p99 = float(loss_values.quantile(0.99))

    fee_total = float(fees.sum())
    slippage_total = float(slippage.sum())
    pnl_gross_total = float(pnl_gross.sum())

    if pnl_gross_total != 0:
        fee_drag_pct_of_gross = fee_total / pnl_gross_total
        slippage_drag_pct_of_gross = slippage_total / pnl_gross_total
    else:
        fee_drag_pct_of_gross = None
        slippage_drag_pct_of_gross = None

    bucket_series = None
    if "vol_bucket" in trades_df.columns:
        bucket_series = trades_df["vol_bucket"]
    elif "regime_bucket" in trades_df.columns:
        bucket_series = trades_df["regime_bucket"]

    ev_by_bucket, trades_by_bucket = _bucket_metrics(pnl_net, bucket_series)

    return PerformanceReport(
        run_id=run_id,
        final_equity=final_equity,
        total_trades=total_trades,
        ev_net=ev_net,
        ev_gross=ev_gross,
        win_rate=win_rate,
        max_drawdown_pct=max_drawdown_pct,
        max_drawdown_duration_bars=max_drawdown_duration_bars,
        tail_loss_p95=tail_loss_p95,
        tail_loss_p99=tail_loss_p99,
        fee_total=fee_total,
        slippage_total=slippage_total,
        fee_drag_pct_of_gross=fee_drag_pct_of_gross,
        slippage_drag_pct_of_gross=slippage_drag_pct_of_gross,
        ev_by_bucket=ev_by_bucket,
        trades_by_bucket=trades_by_bucket,
        extra={},
    )


def write_performance_artifacts(report: PerformanceReport, run_dir: str | Path) -> None:
    """
    Write:
      - run_dir/performance.json
      - run_dir/performance_by_bucket.csv  (bucket, trades, ev_net)
    Deterministic ordering.
    """
    run_path = Path(run_dir)
    performance_path = run_path / "performance.json"
    by_bucket_path = run_path / "performance_by_bucket.csv"

    with performance_path.open("w", encoding="utf-8") as handle:
        json.dump(asdict(report), handle, indent=2, sort_keys=True)
        handle.write("\n")

    rows = []
    for bucket in sorted(report.ev_by_bucket.keys()):
        rows.append(
            {
                "bucket": bucket,
                "n_trades": report.trades_by_bucket.get(bucket, 0),
                "ev_net": report.ev_by_bucket[bucket],
            }
        )
    pd.DataFrame(rows, columns=["bucket", "n_trades", "ev_net"]).to_csv(
        by_bucket_path, index=False
    )
