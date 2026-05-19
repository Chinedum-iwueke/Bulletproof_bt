from __future__ import annotations

from math import isfinite, log1p
from typing import Any

import pandas as pd

SINGLE_STATE_COLUMNS = [
    "entry_state_csi_pctile",
    "entry_state_vol_pctile",
    "entry_state_atr_pct_pctile",
    "entry_state_spread_proxy_pctile",
    "entry_state_tr_over_atr",
    "entry_state_tr_over_atr_pctile",
    "entry_state_volume_pctile",
    "entry_state_volume_z",
    "entry_state_vol_of_vol_pctile",
    "entry_state_funding_raw",
    "entry_state_funding_pctile",
    "entry_state_funding_z",
    "entry_state_funding_regime",
    "entry_state_oi_level",
    "entry_state_oi_accel",
    "entry_state_oi_accel_pctile",
    "entry_state_oi_z",
    "entry_state_oi_regime",
    "entry_state_basis_pctile",
    "entry_state_premium_pctile",
    "entry_state_crowding_proxy_pctile",
    "entry_state_constraint_stress_pctile",
    "entry_state_csi_source",
    "entry_state_trend_state",
    "entry_state_vol_regime",
    "entry_state_liquidity_regime",
    "entry_state_displacement_regime",
    "entry_decision_setup_class",
    "label_structure_class",
    "label_market_regime_class",
]


def _pick_series(df: pd.DataFrame, candidates: list[str]) -> pd.Series:
    for col in candidates:
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce")
    return pd.Series([float("nan")] * len(df), index=df.index, dtype="float64")


def _has_values(series: pd.Series) -> bool:
    return pd.to_numeric(series, errors="coerce").notna().any()


def _bucket_numeric(col: pd.Series, name: str) -> pd.Series:
    if name in {"entry_state_tr_over_atr"}:
        bins = [-float("inf"), 1.0, 1.5, 2.0, float("inf")]
        labels = ["no_impulse", "mild_impulse", "strong_impulse", "extreme_impulse"]
        return pd.cut(pd.to_numeric(col, errors="coerce"), bins=bins, labels=labels, right=False).astype("object")

    vals = pd.to_numeric(col, errors="coerce")
    if "pctile" in name:
        bins = [0.0, 0.3, 0.7, 0.9, 1.01]
    elif name.endswith("_z"):
        bins = [-float("inf"), -1.0, 1.0, 2.5, float("inf")]
    else:
        bins = vals.quantile([0.0, 0.25, 0.5, 0.75, 1.0]).tolist()
        bins[0] = -float("inf")
        bins[-1] = float("inf")
    labels = [f"{name}_b{i}" for i in range(len(bins) - 1)]
    return pd.cut(vals, bins=bins, labels=labels, include_lowest=True, right=False).astype("object")


def compute_bucket_metrics(df: pd.DataFrame, *, state_variable: str, bucket_col: str, min_bucket_trades: int) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    grouped = df.groupby(bucket_col, dropna=True)
    for bucket, part in grouped:
        n = len(part)
        if n < min_bucket_trades:
            continue
        r = _pick_series(part, ["r_net", "realized_r_net", "r_multiple_net", "pnl_r"])
        rg = _pick_series(part, ["r_gross", "realized_r_gross", "r_multiple_gross", "gross_r", "gross_pnl_r"])
        mfe = _pick_series(part, ["path_mfe_r", "mfe_r"])
        mae = _pick_series(part, ["path_mae_r", "mae_r"])
        eff = _pick_series(part, ["counterfactual_exit_efficiency_realized_over_mfe"])
        cost_drag = _pick_series(part, ["cost_drag_r", "counterfactual_cost_drag_r"])
        fee_drag = _pick_series(part, ["fee_drag_r", "counterfactual_fee_drag_r"])
        slip_drag = _pick_series(part, ["slippage_drag_r", "counterfactual_slippage_drag_r"])
        spr_drag = _pick_series(part, ["spread_drag_r", "counterfactual_spread_drag_r"])

        ev = float(r.mean())
        avg_cost = float(cost_drag.mean()) if _has_values(cost_drag) else None
        if not _has_values(rg) and avg_cost is not None:
            rg = r + cost_drag
        cost_drag_to_ev_ratio = None
        if avg_cost is not None and isfinite(ev) and ev != 0.0:
            cost_drag_to_ev_ratio = avg_cost / ev
        rows.append(
            {
                "state_variable": state_variable,
                "bucket": str(bucket),
                "n_trades": n,
                "ev_r_net": ev,
                "ev_r_gross": float(rg.mean()) if _has_values(rg) else None,
                "median_r_net": float(r.median()),
                "win_rate": float((r > 0).mean()),
                "avg_r_win": float(r[r > 0].mean()) if (r > 0).any() else None,
                "avg_r_loss": float(r[r < 0].mean()) if (r < 0).any() else None,
                "payoff_ratio": float(abs(r[r > 0].mean() / r[r < 0].mean())) if (r > 0).any() and (r < 0).any() else None,
                "p25_r": float(r.quantile(0.25)),
                "p75_r": float(r.quantile(0.75)),
                "p95_r": float(r.quantile(0.95)),
                "p99_r": float(r.quantile(0.99)),
                "max_r": float(r.max()),
                "min_r": float(r.min()),
                "tail_2r_count": int((r >= 2).sum()),
                "tail_3r_count": int((r >= 3).sum()),
                "tail_5r_count": int((r >= 5).sum()),
                "tail_10r_count": int((r >= 10).sum()),
                "tail_2r_rate": float((r >= 2).mean()),
                "tail_3r_rate": float((r >= 3).mean()),
                "tail_5r_rate": float((r >= 5).mean()),
                "tail_10r_rate": float((r >= 10).mean()),
                "avg_mfe_r": float(mfe.mean()) if _has_values(mfe) else None,
                "avg_mae_r": float(mae.mean()) if _has_values(mae) else None,
                "median_mfe_r": float(mfe.median()) if _has_values(mfe) else None,
                "median_mae_r": float(mae.median()) if _has_values(mae) else None,
                "avg_exit_efficiency": float(eff.mean()) if _has_values(eff) else None,
                "avg_cost_drag_r": avg_cost,
                "avg_fee_drag_r": float(fee_drag.mean()) if _has_values(fee_drag) else None,
                "avg_slippage_drag_r": float(slip_drag.mean()) if _has_values(slip_drag) else None,
                "avg_spread_drag_r": float(spr_drag.mean()) if _has_values(spr_drag) else None,
                "cost_drag_to_ev_ratio": cost_drag_to_ev_ratio,
                "weak_sample": n < (2 * min_bucket_trades),
                "sample_score": log1p(n),
            }
        )
    return pd.DataFrame(rows)


def analyze_single_state_variables(trades: pd.DataFrame, *, min_bucket_trades: int) -> tuple[pd.DataFrame, list[str]]:
    out: list[pd.DataFrame] = []
    missing: list[str] = []
    for col in SINGLE_STATE_COLUMNS:
        if col not in trades.columns:
            missing.append(col)
            continue
        series = trades[col]
        bucket_col = f"__bucket__{col}"
        if pd.api.types.is_numeric_dtype(series):
            trades[bucket_col] = _bucket_numeric(series, col)
        else:
            trades[bucket_col] = series.astype("object")
        out.append(compute_bucket_metrics(trades, state_variable=col, bucket_col=bucket_col, min_bucket_trades=min_bucket_trades))

    if not out:
        return pd.DataFrame(), missing
    return pd.concat(out, ignore_index=True), missing
