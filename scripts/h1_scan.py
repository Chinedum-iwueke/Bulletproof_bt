#!/usr/bin/env python3
"""
Hypothesis-1 Grid Scanner + Tail Diagnostics (Bulletproof_bt)

Scans run directories, extracts key artifacts, and produces:
  1) runs_extracted.csv (per-run summary + compact config identity)
  2) leaderboard_top.csv (quick triage)
  3) heatmap_cells.csv (pivot-ready)
  4) monotonicity_summary.csv (EV vs vol_floor)
  5) pivots/*.csv (ev_r_net pivot per group)

PLUS Tail Diagnostics to decide if Chandelier can help:
  6) runs_tail_stats.csv (quantiles, max, skew, etc. from trade_returns.csv)
  7) runs_chandelier_candidates.csv (heuristic shortlist + reasons)
  8) tail CCDF plot per run: OUTDIR/ccdf/<run_id>__tail_ccdf.png
  9) vol_floor_tail_strength.png (aggregate median p95/p99 vs vol_floor)

Expected artifacts in each run dir (some optional):
  benchmark_equity.csv, config_used.yaml, decisions.jsonl, fills.jsonl,
  performance.json, sanity.json, trades.csv, trade_returns.csv,
  cost_breakdown.json, run_status.json

Usage:
  python scripts/scan_h1_runs.py \
    --roots outputs/grid1.5/h1_volfloor_donchian/runs \
    --glob "run_*" \
    --outdir outputs/grid1.5/h1_volfloor_donchian/scan

Or scan explicit run dirs:
  python scripts/scan_h1_runs.py --runs <run1> <run2> --outdir outputs/scan_h1
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

try:
    import yaml  # PyYAML
except Exception:
    print("ERROR: PyYAML is required. Install with: pip install pyyaml", file=sys.stderr)
    raise

# Plotting: do not set explicit colors/styles (default matplotlib is fine).
try:
    import matplotlib.pyplot as plt
except Exception:
    print("ERROR: matplotlib is required. Install with: pip install matplotlib", file=sys.stderr)
    raise


# -----------------------------
# Helpers
# -----------------------------
def _safe_read_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def _safe_read_yaml(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        return yaml.safe_load(path.read_text())
    except Exception:
        return None


def _safe_read_csv(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    try:
        return pd.read_csv(path)
    except Exception:
        return None


def _deep_get(d: Any, keys: List[str], default: Any = None) -> Any:
    cur = d
    for k in keys:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return cur


def _flatten_dict(d: Dict[str, Any], prefix: str = "", out: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if out is None:
        out = {}
    for k, v in (d or {}).items():
        nk = f"{prefix}{k}" if prefix == "" else f"{prefix}.{k}"
        if isinstance(v, dict):
            _flatten_dict(v, nk, out)
        else:
            out[nk] = v
    return out


def _first_existing_key(d: Dict[str, Any], candidates: List[List[str]]) -> Any:
    for path in candidates:
        v = _deep_get(d, path, default=None)
        if v is not None:
            return v
    return None


def _coerce_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)) and not (isinstance(x, float) and math.isnan(x)):
        return float(x)
    try:
        return float(str(x))
    except Exception:
        return None


def _coerce_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    if isinstance(x, int):
        return x
    try:
        return int(float(str(x)))
    except Exception:
        return None


# -----------------------------
# Run summary extraction
# -----------------------------
@dataclass
class RunSummary:
    run_dir: Path
    run_id: str
    status: str

    dataset_name: str
    dataset_dir: str
    timeframe: str
    strategy_name: str
    exit_type: str
    tier_name: str
    vol_floor: Optional[int]
    adx_min: Optional[int]

    n_trades: Optional[int]
    win_rate: Optional[float]
    avg_trade: Optional[float]
    ev_r_net: Optional[float]
    ev_r_gross: Optional[float]
    sharpe: Optional[float]
    sortino: Optional[float]
    mar: Optional[float]
    max_dd: Optional[float]
    dd_duration: Optional[float]
    pnl_net: Optional[float]

    fees_paid: Optional[float]
    slippage_paid: Optional[float]
    spread_paid: Optional[float]

    config_flat: Dict[str, Any]


def _infer_run_id(run_dir: Path) -> str:
    return run_dir.name


def _infer_status(run_status: Optional[Dict[str, Any]]) -> str:
    if not run_status:
        return "UNKNOWN"
    for k in ("status", "state"):
        v = run_status.get(k)
        if isinstance(v, str):
            return v.upper()
    if run_status.get("ok") is True:
        return "SUCCESS"
    if run_status.get("ok") is False:
        return "FAILED"
    return "UNKNOWN"


def _extract_identity(config: Dict[str, Any]) -> Tuple[str, str, str, str, str, str, Optional[int], Optional[int], Dict[str, Any]]:
    flat = _flatten_dict(config or {})

    strategy_name = (
        _first_existing_key(
            config,
            [
                ["strategy", "name"],
                ["strategy", "id"],
                ["strategy_name"],
                ["strategy"],
            ],
        )
        or "UNKNOWN_STRATEGY"
    )
    if isinstance(strategy_name, dict):
        strategy_name = strategy_name.get("name") or strategy_name.get("id") or "UNKNOWN_STRATEGY"
    strategy_name = str(strategy_name)

    exit_type = (
        _first_existing_key(
            config,
            [
                ["strategy", "exit_type"],
                ["strategy", "params", "exit_type"],
                ["exit_type"],
            ],
        )
        or "DEFAULT"
    )
    exit_type = str(exit_type)

    timeframe = (
        _first_existing_key(
            config,
            [
                ["strategy", "timeframe"],
                ["strategy", "entry_timeframe"],
                ["data", "timeframe"],
                ["data", "entry_timeframe"],
                ["timeframe"],
                ["engine", "timeframe"],
                ["bars", "timeframe"],
            ],
        )
        or "UNKNOWN_TF"
    )
    timeframe = str(timeframe)

    tier_name = (
        _first_existing_key(
            config,
            [
                ["execution", "profile"],
                ["execution", "tier"],
                ["execution_tier"],
                ["engine", "execution_tier"],
                ["execution", "name"],
            ],
        )
        or "UNKNOWN_TIER"
    )
    tier_name = str(tier_name)

    dataset_dir = (
        _first_existing_key(
            config,
            [
                ["data", "dataset_dir"],
                ["data", "root_dir"],
                ["data", "dir"],
                ["dataset_dir"],
                ["dataset"],
            ],
        )
        or ""
    )
    dataset_dir = str(dataset_dir)

    dataset_name = (
        _first_existing_key(
            config,
            [
                ["data", "dataset_name"],
                ["data", "name"],
                ["dataset", "name"],
                ["dataset_name"],
            ],
        )
        or ""
    )
    dataset_name = str(dataset_name) if dataset_name else (Path(dataset_dir).name if dataset_dir else "UNKNOWN_DATASET")

    vol_floor = _coerce_int(
        _first_existing_key(
            config,
            [
                ["strategy", "vol_floor_pct"],
                ["strategy", "vol_floor"],
                ["strategy", "params", "vol_floor"],
                ["strategy", "params", "vol_floor_pct"],
                ["strategy", "params", "vol_pct_floor"],
                ["strategy", "params", "vol_floor_threshold"],
                ["vol_floor"],
                ["vol_floor_pct"],
            ],
        )
    )

    adx_min = _coerce_int(
        _first_existing_key(
            config,
            [
                ["strategy", "adx_min"],
                ["strategy", "params", "adx_min"],
                ["strategy", "params", "adx_threshold"],
                ["adx_min"],
                ["adx_threshold"],
            ],
        )
    )

    return dataset_name, dataset_dir, timeframe, strategy_name, exit_type, tier_name, vol_floor, adx_min, flat


def _extract_perf(perf: Optional[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    if not perf:
        return {
            "n_trades": None,
            "win_rate": None,
            "avg_trade": None,
            "ev_r_net": None,
            "ev_r_gross": None,
            "sharpe": None,
            "sortino": None,
            "mar": None,
            "max_dd": None,
            "dd_duration": None,
            "pnl_net": None,
        }

    n_trades = _coerce_int(_first_existing_key(perf, [["n_trades"], ["total_trades"], ["trades", "count"], ["summary", "n_trades"]]))
    win_rate = _coerce_float(_first_existing_key(perf, [["win_rate"], ["summary", "win_rate"], ["trades", "win_rate"]]))
    avg_trade = _coerce_float(_first_existing_key(perf, [["avg_trade_return"], ["avg_trade"], ["summary", "avg_trade_return"], ["summary", "avg_trade"]]))
    ev_r_net = _coerce_float(
        _first_existing_key(
            perf,
            [
                ["ev_r_net"],
                ["summary", "ev_r_net"],
                ["metrics", "ev_r_net"],
                ["ev_net"],
                ["expectancy_net"],
            ],
        )
    )
    ev_r_gross = _coerce_float(
        _first_existing_key(
            perf,
            [
                ["ev_r_gross"],
                ["summary", "ev_r_gross"],
                ["metrics", "ev_r_gross"],
                ["ev_gross"],
                ["expectancy_gross"],
            ],
        )
    )
    sharpe = _coerce_float(_first_existing_key(perf, [["sharpe_annualized"], ["sharpe"], ["summary", "sharpe"], ["metrics", "sharpe"]]))
    sortino = _coerce_float(_first_existing_key(perf, [["sortino_annualized"], ["sortino"], ["summary", "sortino"], ["metrics", "sortino"]]))
    mar = _coerce_float(_first_existing_key(perf, [["mar_ratio"], ["mar"], ["summary", "mar"], ["metrics", "mar"]]))
    max_dd = _coerce_float(_first_existing_key(perf, [["max_drawdown_pct"], ["max_drawdown"], ["max_dd"], ["summary", "max_drawdown"], ["summary", "max_dd"], ["drawdown", "max"]]))
    dd_duration = _coerce_float(_first_existing_key(perf, [["max_drawdown_duration_bars"], ["max_drawdown_duration"], ["dd_duration"], ["drawdown", "max_duration"]]))
    pnl_net = _coerce_float(_first_existing_key(perf, [["net_pnl"], ["pnl_net"], ["summary", "pnl_net"], ["summary", "net_pnl"]]))

    return {
        "n_trades": float(n_trades) if n_trades is not None else None,
        "win_rate": win_rate,
        "avg_trade": avg_trade,
        "ev_r_net": ev_r_net,
        "ev_r_gross": ev_r_gross,
        "sharpe": sharpe,
        "sortino": sortino,
        "mar": mar,
        "max_dd": max_dd,
        "dd_duration": dd_duration,
        "pnl_net": pnl_net,
    }


def _derive_perf_from_trade_returns(tr: Optional[pd.DataFrame]) -> Dict[str, Optional[float]]:
    if tr is None or tr.empty:
        return {"n_trades": None, "win_rate": None, "avg_trade": None, "ev_r_net": None}

    candidates = ["r", "r_multiple", "r_mult", "return", "ret", "net_return", "pnl_pct", "pnl_return"]
    col = None
    for c in candidates:
        if c in tr.columns:
            col = c
            break
    if col is None:
        num_cols = [c for c in tr.columns if pd.api.types.is_numeric_dtype(tr[c])]
        if num_cols:
            col = num_cols[0]
    if col is None:
        return {"n_trades": None, "win_rate": None, "avg_trade": None, "ev_r_net": None}

    s = pd.to_numeric(tr[col], errors="coerce").dropna()
    if s.empty:
        return {"n_trades": None, "win_rate": None, "avg_trade": None, "ev_r_net": None}

    n = int(s.shape[0])
    win_rate = float((s > 0).mean())
    avg_trade = float(s.mean())
    ev_r_net = avg_trade
    return {"n_trades": float(n), "win_rate": win_rate, "avg_trade": avg_trade, "ev_r_net": ev_r_net}


def _extract_costs(cost: Optional[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    if not cost:
        return {"fees_paid": None, "slippage_paid": None, "spread_paid": None}
    fees = _coerce_float(_first_existing_key(cost, [["fee_total"], ["fees"], ["fees_paid"], ["total_fees"], ["summary", "fees"]]))
    slip = _coerce_float(_first_existing_key(cost, [["slippage_total"], ["slippage"], ["slippage_paid"], ["total_slippage"], ["summary", "slippage"]]))
    sprd = _coerce_float(_first_existing_key(cost, [["spread_total"], ["spread"], ["spread_paid"], ["total_spread"], ["summary", "spread"]]))
    return {"fees_paid": fees, "slippage_paid": slip, "spread_paid": sprd}


def _sorted_or_empty(df: pd.DataFrame, sort_cols: List[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=sort_cols)
    missing = [c for c in sort_cols if c not in df.columns]
    for c in missing:
        df[c] = pd.NA
    return df.sort_values(sort_cols)


def _summarize_run(run_dir: Path) -> RunSummary:
    cfg = _safe_read_yaml(run_dir / "config_used.yaml") or {}
    perf = _safe_read_json(run_dir / "performance.json")
    cost = _safe_read_json(run_dir / "cost_breakdown.json")
    status = _safe_read_json(run_dir / "run_status.json")

    dataset_name, dataset_dir, timeframe, strategy_name, exit_type, tier_name, vol_floor, adx_min, flat_cfg = _extract_identity(cfg)

    perf_vals = _extract_perf(perf)
    tr = _safe_read_csv(run_dir / "trade_returns.csv")
    derived = _derive_perf_from_trade_returns(tr)

    def pick(primary: Optional[float], fallback: Optional[float]) -> Optional[float]:
        return primary if primary is not None else fallback

    n_trades = pick(perf_vals["n_trades"], derived["n_trades"])
    win_rate = pick(perf_vals["win_rate"], derived["win_rate"])
    avg_trade = pick(perf_vals["avg_trade"], derived["avg_trade"])
    ev_r_net = pick(perf_vals["ev_r_net"], derived["ev_r_net"])
    ev_r_gross = perf_vals["ev_r_gross"]

    costs = _extract_costs(cost)

    return RunSummary(
        run_dir=run_dir,
        run_id=_infer_run_id(run_dir),
        status=_infer_status(status),
        dataset_name=dataset_name,
        dataset_dir=dataset_dir,
        timeframe=timeframe,
        strategy_name=strategy_name,
        exit_type=exit_type,
        tier_name=tier_name,
        vol_floor=vol_floor,
        adx_min=adx_min,
        n_trades=_coerce_int(n_trades),
        win_rate=win_rate,
        avg_trade=avg_trade,
        ev_r_net=ev_r_net,
        ev_r_gross=ev_r_gross,
        sharpe=perf_vals["sharpe"],
        sortino=perf_vals["sortino"],
        mar=perf_vals["mar"],
        max_dd=perf_vals["max_dd"],
        dd_duration=perf_vals["dd_duration"],
        pnl_net=perf_vals["pnl_net"],
        fees_paid=costs["fees_paid"],
        slippage_paid=costs["slippage_paid"],
        spread_paid=costs["spread_paid"],
        config_flat=flat_cfg,
    )


# -----------------------------
# Scrap / keep heuristic (triage)
# -----------------------------
def _scrap_signal(ev_r_net: Optional[float], n_trades: Optional[int], max_dd: Optional[float]) -> str:
    if ev_r_net is None or (isinstance(ev_r_net, float) and math.isnan(ev_r_net)):
        return "UNKNOWN"
    n = n_trades or 0

    if n < 200:
        return "BORDERLINE_LOW_SAMPLE" if ev_r_net > 0 else "SCRAP_LOW_SAMPLE"
    if ev_r_net <= 0:
        return "SCRAP"
    # DD sanity (handles both negative or positive representations)
    if max_dd is not None and not (isinstance(max_dd, float) and math.isnan(max_dd)):
        if abs(float(max_dd)) > 0.35:
            return "BORDERLINE_DD"
    return "KEEP_CANDIDATE"


# -----------------------------
# Tail diagnostics
# -----------------------------
def _pick_trade_return_series(tr: Optional[pd.DataFrame]) -> Tuple[Optional[pd.Series], Optional[str]]:
    if tr is None or tr.empty:
        return None, None
    candidates = ["r", "r_multiple", "r_mult", "return", "ret", "net_return", "pnl_pct", "pnl_return"]
    for c in candidates:
        if c in tr.columns:
            s = pd.to_numeric(tr[c], errors="coerce").dropna()
            if not s.empty:
                return s, c
    num_cols = [c for c in tr.columns if pd.api.types.is_numeric_dtype(tr[c])]
    for c in num_cols:
        s = pd.to_numeric(tr[c], errors="coerce").dropna()
        if not s.empty:
            return s, c
    return None, None


def _tail_stats(s: pd.Series) -> Dict[str, Any]:
    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        return {}

    q = s.quantile([0.1, 0.5, 0.9, 0.95, 0.99]).to_dict()
    median = float(q.get(0.5, float("nan")))
    p95 = float(q.get(0.95, float("nan")))
    p99 = float(q.get(0.99, float("nan")))
    maxv = float(s.max())
    minv = float(s.min())

    # Robust tail ratio: p95 / |median| (avoid div by 0)
    denom = abs(median) if abs(median) > 1e-12 else float("nan")
    tail_ratio = (p95 / denom) if not math.isnan(denom) else float("nan")

    # Right-tail mass beyond 2R / 3R
    gt2 = float((s >= 2.0).mean())
    gt3 = float((s >= 3.0).mean())
    gt4 = float((s >= 4.0).mean())

    # Basic distribution diagnostics
    win_rate = float((s > 0).mean())
    mean = float(s.mean())
    std = float(s.std(ddof=1)) if s.shape[0] > 1 else float("nan")
    skew = float(s.skew()) if s.shape[0] > 2 else float("nan")
    kurt = float(s.kurtosis()) if s.shape[0] > 3 else float("nan")

    return {
        "n": int(s.shape[0]),
        "mean": mean,
        "std": std,
        "min": minv,
        "q10": float(q.get(0.1, float("nan"))),
        "median": median,
        "q90": float(q.get(0.9, float("nan"))),
        "p95": p95,
        "p99": p99,
        "max": maxv,
        "win_rate": win_rate,
        "skew": skew,
        "kurtosis": kurt,
        "frac_ge_2R": gt2,
        "frac_ge_3R": gt3,
        "frac_ge_4R": gt4,
        "tail_ratio_p95_over_abs_median": tail_ratio,
    }


def _is_chandelier_candidate(t: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Heuristic: chandelier can help if there are meaningful right tails already present.

    Rule of thumb (tweakable):
      - enough sample size
      - p95 reasonably large AND max large OR p99 very large
    """
    n = int(t.get("n", 0) or 0)
    p95 = float(t.get("p95", float("nan")))
    p99 = float(t.get("p99", float("nan")))
    mx = float(t.get("max", float("nan")))
    frac2 = float(t.get("frac_ge_2R", float("nan")))

    if n < 200:
        return False, "low_sample"

    # If there are no big winners at all, chandelier cannot create them
    if (math.isnan(mx) or mx < 2.0) and (math.isnan(p95) or p95 < 1.0):
        return False, "no_tail"

    # Strong tail candidate
    if (not math.isnan(p95) and p95 >= 2.0 and not math.isnan(mx) and mx >= 4.0):
        return True, "p95>=2_and_max>=4"
    if (not math.isnan(p99) and p99 >= 3.0 and not math.isnan(mx) and mx >= 5.0):
        return True, "p99>=3_and_max>=5"
    if (not math.isnan(frac2) and frac2 >= 0.03 and not math.isnan(mx) and mx >= 4.0):
        return True, ">=3%_trades_ge_2R_and_max>=4"

    return False, "weak_tail"


def _plot_ccdf(s: pd.Series, outpath: Path, title: str) -> None:
    """
    Empirical CCDF: P(R >= x) for x >= 0 (focus on right tail for chandelier viability).
    """
    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        return

    # Focus on non-negative returns (right tail)
    sp = s[s >= 0].sort_values()
    if sp.empty:
        return

    n = sp.shape[0]
    # For sorted values v_i, ccdf at v_i is (n - i) / n
    x = sp.to_numpy()
    ranks = pd.Series(range(1, n + 1))
    ccdf = (n - ranks + 1) / n

    plt.figure()
    plt.plot(x, ccdf.to_numpy())
    plt.yscale("log")  # tail visibility
    plt.xlabel("Trade return (R units or return column units)")
    plt.ylabel("CCDF: P(return >= x) (log scale)")
    plt.title(title)
    plt.grid(True, which="both", linestyle="--", linewidth=0.5)
    outpath.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()


def _plot_volfloor_tail_strength(tail_df: pd.DataFrame, outpath: Path) -> None:
    """
    Aggregate plot: median p95 and median p99 vs vol_floor across all runs.
    This is a quick “does tail strength grow with vol floor?” diagnostic.
    """
    d = tail_df.copy()
    d = d.dropna(subset=["vol_floor"])
    if d.empty:
        return

    d["vol_floor"] = pd.to_numeric(d["vol_floor"], errors="coerce")
    d["p95"] = pd.to_numeric(d["p95"], errors="coerce")
    d["p99"] = pd.to_numeric(d["p99"], errors="coerce")
    d = d.dropna(subset=["vol_floor"])

    agg = d.groupby("vol_floor", as_index=False).agg(
        p95_median=("p95", "median"),
        p99_median=("p99", "median"),
        n_runs=("run_id", "count"),
    ).sort_values("vol_floor")

    if agg.empty:
        return

    plt.figure()
    plt.plot(agg["vol_floor"], agg["p95_median"], marker="o", label="median p95")
    plt.plot(agg["vol_floor"], agg["p99_median"], marker="o", label="median p99")
    plt.xlabel("vol_floor threshold (percentile)")
    plt.ylabel("Tail strength (R units or return column units)")
    plt.title("Vol floor vs tail strength (median p95/p99 across runs)")
    plt.grid(True, linestyle="--", linewidth=0.5)
    plt.legend()
    outpath.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()


# -----------------------------
# Run discovery
# -----------------------------
def _discover_runs(roots: List[Path], glob_pat: str, contains: Optional[str]) -> List[Path]:
    run_dirs: List[Path] = []
    for r in roots:
        if not r.exists():
            continue
        for p in r.glob(glob_pat):
            if p.is_dir() and (p / "config_used.yaml").exists():
                if contains and contains not in p.name:
                    continue
                run_dirs.append(p)
    return sorted(set(run_dirs))


# -----------------------------
# Main
# -----------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--roots", nargs="*", default=[], help="Root dirs containing run folders")
    ap.add_argument("--glob", default="run_*", help="Glob pattern under each root to match run dirs")
    ap.add_argument("--contains", default=None, help="Only include run dirs whose name contains this substring")
    ap.add_argument("--runs", nargs="*", default=[], help="Explicit run dirs (overrides discovery if provided)")
    ap.add_argument("--outdir", default="outputs/scan_h1", help="Where to write outputs")
    ap.add_argument("--top", type=int, default=30, help="How many rows to write in top leaderboard")
    ap.add_argument("--min_status", default="SUCCESS", help="Filter by run_status (SUCCESS/FAILED/UNKNOWN); use ANY to keep all")
    ap.add_argument("--write_ccdf", action="store_true", help="Generate tail CCDF plot per run (uses trade_returns.csv)")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    if args.runs:
        run_dirs = [Path(x) for x in args.runs]
    else:
        roots = [Path(x) for x in args.roots] if args.roots else [Path("outputs/runs")]
        run_dirs = _discover_runs(roots, args.glob, args.contains)

    if not run_dirs:
        print("No run directories found. Use --roots/--glob or --runs.", file=sys.stderr)
        return 2

    # 1) Summarize runs
    rows: List[Dict[str, Any]] = []
    tail_rows: List[Dict[str, Any]] = []

    ccdf_dir = outdir / "ccdf"
    if args.write_ccdf:
        ccdf_dir.mkdir(parents=True, exist_ok=True)

    for rd in run_dirs:
        s = _summarize_run(rd)
        if args.min_status != "ANY" and s.status != args.min_status:
            continue

        # Compact config identity JSON
        cfg_keys_keep_prefixes = ("strategy.", "execution.", "data.", "engine.")
        cfg_compact = {k: v for k, v in s.config_flat.items() if k.startswith(cfg_keys_keep_prefixes)}

        row = {
            "run_id": s.run_id,
            "run_dir": str(s.run_dir),
            "status": s.status,
            "dataset_name": s.dataset_name,
            "dataset_dir": s.dataset_dir,
            "timeframe": s.timeframe,
            "tier": s.tier_name,
            "strategy": s.strategy_name,
            "exit_type": s.exit_type,
            "vol_floor": s.vol_floor,
            "adx_min": s.adx_min,
            "n_trades": s.n_trades,
            "win_rate": s.win_rate,
            "avg_trade": s.avg_trade,
            "ev_r_net": s.ev_r_net,
            "ev_r_gross": s.ev_r_gross,
            "cost_drag": (s.ev_r_gross - s.ev_r_net) if (s.ev_r_gross is not None and s.ev_r_net is not None) else None,
            "pnl_net": s.pnl_net,
            "sharpe": s.sharpe,
            "sortino": s.sortino,
            "mar": s.mar,
            "max_dd": s.max_dd,
            "dd_duration": s.dd_duration,
            "fees_paid": s.fees_paid,
            "slippage_paid": s.slippage_paid,
            "spread_paid": s.spread_paid,
        }

        row["config_id"] = (
            f"{row['strategy']}|exit={row['exit_type']}|tier={row['tier']}|tf={row['timeframe']}|"
            f"X={row['vol_floor']}|adx={row['adx_min']}|ds={row['dataset_name']}"
        )
        row["scrap_label"] = _scrap_signal(row.get("ev_r_net"), row.get("n_trades"), row.get("max_dd"))
        row["config_compact_json"] = json.dumps(cfg_compact, default=str, sort_keys=True)

        rows.append(row)

        # 2) Tail stats from trade_returns.csv
        tr = _safe_read_csv(rd / "trade_returns.csv")
        s_ret, used_col = _pick_trade_return_series(tr)
        if s_ret is not None:
            t = _tail_stats(s_ret)
            if t:
                is_cand, reason = _is_chandelier_candidate(t)
                trow = {
                    "run_id": s.run_id,
                    "run_dir": str(s.run_dir),
                    "dataset_name": s.dataset_name,
                    "timeframe": s.timeframe,
                    "tier": s.tier_name,
                    "strategy": s.strategy_name,
                    "exit_type": s.exit_type,
                    "vol_floor": s.vol_floor,
                    "adx_min": s.adx_min,
                    "trade_return_col": used_col,
                    "chandelier_candidate": bool(is_cand),
                    "candidate_reason": reason,
                    **t,
                }
                tail_rows.append(trow)

                # 3) CCDF plot per run
                if args.write_ccdf:
                    outpath = ccdf_dir / f"{s.run_id}__tail_ccdf.png"
                    title = f"{s.run_id} | {s.strategy_name} | X={s.vol_floor} adx={s.adx_min} | {s.dataset_name}"
                    _plot_ccdf(s_ret, outpath, title)

    df = pd.DataFrame(rows)
    if df.empty:
        print("No runs matched the status filter.", file=sys.stderr)
        return 3

    # Normalize numeric
    for c in ["win_rate", "avg_trade", "ev_r_net", "ev_r_gross", "cost_drag", "pnl_net", "sharpe", "sortino", "mar", "max_dd", "dd_duration"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Write extracted runs table
    df.sort_values(["dataset_name", "strategy", "exit_type", "tier", "vol_floor", "adx_min"]).to_csv(outdir / "runs_extracted.csv", index=False)

    # Leaderboard
    df_lb = df.copy()
    df_lb["dd_mag"] = pd.to_numeric(df_lb["max_dd"], errors="coerce").abs()
    df_lb = df_lb.sort_values(
        ["scrap_label", "ev_r_net", "sharpe", "dd_mag", "n_trades"],
        ascending=[True, False, False, True, False],
    )

    lb_cols = [
        "scrap_label",
        "ev_r_net",
        "sharpe",
        "max_dd",
        "n_trades",
        "win_rate",
        "pnl_net",
        "dataset_name",
        "tier",
        "strategy",
        "exit_type",
        "vol_floor",
        "adx_min",
        "run_id",
        "run_dir",
        "config_id",
    ]
    df_lb[lb_cols].head(args.top).to_csv(outdir / "leaderboard_top.csv", index=False)

    # Heatmap-friendly cells
    heat_cols = ["dataset_name", "strategy", "exit_type", "tier", "timeframe", "vol_floor", "adx_min", "ev_r_net", "ev_r_gross", "cost_drag", "sharpe", "max_dd", "n_trades"]
    df[heat_cols].to_csv(outdir / "heatmap_cells.csv", index=False)

    # Pivots
    pivots_dir = outdir / "pivots"
    pivots_dir.mkdir(parents=True, exist_ok=True)
    for (ds, strat, ex, tier, tf), g in df.groupby(["dataset_name", "strategy", "exit_type", "tier", "timeframe"], dropna=False):
        if g["vol_floor"].isna().all() or g["adx_min"].isna().all():
            continue
        pivot = g.pivot_table(index="vol_floor", columns="adx_min", values="ev_r_net", aggfunc="max").sort_index()
        fname = f"pivot_ev__ds={ds}__strat={strat}__exit={ex}__tier={tier}__tf={tf}.csv"
        fname = re.sub(r"[^A-Za-z0-9._=-]+", "_", fname)
        pivot.to_csv(pivots_dir / fname)

    # Monotonicity (best-of-adx per vol_floor, Spearman)
    mono_rows = []
    dmono = df.dropna(subset=["vol_floor"]).copy()
    dmono["vol_floor"] = pd.to_numeric(dmono["vol_floor"], errors="coerce")
    for keys, g in dmono.groupby(["dataset_name", "strategy", "exit_type", "tier", "timeframe"], dropna=False):
        g = g.dropna(subset=["vol_floor", "ev_r_net"])
        if g.empty:
            continue
        rep = (
            g.sort_values(["vol_floor", "ev_r_net"], ascending=[True, False])
             .groupby("vol_floor", as_index=False)
             .head(1)
             .sort_values("vol_floor")
        )
        if rep.shape[0] < 3:
            spearman = float("nan")
        else:
            spearman = rep["vol_floor"].corr(rep["ev_r_net"], method="spearman")

        low = rep[rep["vol_floor"].isin([40, 50])]
        high = rep[rep["vol_floor"].isin([70, 80])]
        low_mean = float(low["ev_r_net"].mean()) if not low.empty else float("nan")
        high_mean = float(high["ev_r_net"].mean()) if not high.empty else float("nan")
        delta = high_mean - low_mean if (not math.isnan(low_mean) and not math.isnan(high_mean)) else float("nan")

        mono_rows.append({
            "dataset_name": keys[0],
            "strategy": keys[1],
            "exit_type": keys[2],
            "tier": keys[3],
            "timeframe": keys[4],
            "n_cells": int(g.shape[0]),
            "n_floors_present": int(rep.shape[0]),
            "spearman_volfloor_ev_r_net": spearman,
            "ev_r_net_low_40_50": low_mean,
            "ev_r_net_high_70_80": high_mean,
            "delta_high_low": delta,
        })

    mono = _sorted_or_empty(pd.DataFrame(mono_rows), ["dataset_name", "strategy", "exit_type", "tier", "timeframe"])
    mono.to_csv(outdir / "monotonicity_summary.csv", index=False)

    # Tail outputs
    tail_df = pd.DataFrame(tail_rows)
    if not tail_df.empty:
        # Make numeric columns numeric
        num_cols = ["mean", "std", "min", "q10", "median", "q90", "p95", "p99", "max", "win_rate",
                    "skew", "kurtosis", "frac_ge_2R", "frac_ge_3R", "frac_ge_4R", "tail_ratio_p95_over_abs_median"]
        for c in num_cols:
            if c in tail_df.columns:
                tail_df[c] = pd.to_numeric(tail_df[c], errors="coerce")

        tail_df.to_csv(outdir / "runs_tail_stats.csv", index=False)

        cand_df = tail_df[tail_df["chandelier_candidate"] == True].copy()
        cand_df = cand_df.sort_values(["p99", "p95", "max"], ascending=[False, False, False])
        cand_df.to_csv(outdir / "runs_chandelier_candidates.csv", index=False)

        # Vol floor vs tail strength plot (aggregate)
        _plot_volfloor_tail_strength(tail_df, outdir / "vol_floor_tail_strength.png")
    else:
        # still create empty files for consistency
        pd.DataFrame([]).to_csv(outdir / "runs_tail_stats.csv", index=False)
        pd.DataFrame([]).to_csv(outdir / "runs_chandelier_candidates.csv", index=False)

    # Console summary
    print("\n=== H1 RUN SCAN SUMMARY ===")
    print(f"Runs discovered: {len(run_dirs)}")
    print(f"Rows extracted (after status filter): {df.shape[0]}")
    print(f"Outdir: {outdir}")
    print("Wrote:")
    print(f"  - {outdir/'runs_extracted.csv'}")
    print(f"  - {outdir/'leaderboard_top.csv'}")
    print(f"  - {outdir/'heatmap_cells.csv'}")
    print(f"  - {outdir/'monotonicity_summary.csv'}")
    print(f"  - {outdir/'runs_tail_stats.csv'}")
    print(f"  - {outdir/'runs_chandelier_candidates.csv'}")
    if args.write_ccdf:
        print(f"  - CCDF plots in {ccdf_dir}/")
    print(f"  - {outdir/'vol_floor_tail_strength.png'} (if tail stats present)")
    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
