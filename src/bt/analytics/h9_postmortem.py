"""L1-H9 family-specific post-run diagnostics."""
from __future__ import annotations

from dataclasses import dataclass
import math
from pathlib import Path

import pandas as pd
import yaml

from bt.analytics.segment_rollups import load_trades_with_entry_metadata


BREAKOUT_BUCKETS = [0.0, 0.5, 1.0, 1.5, math.inf]
BREAKOUT_LABELS = ["0-0.5_atr", "0.5-1.0_atr", "1.0-1.5_atr", ">1.5_atr"]


@dataclass(frozen=True)
class _Thresholds:
    weak_adx: float = 25.0
    weak_close_strength: float = 0.15
    failed_followthrough_mfe: float = 0.75
    exhaustion_range_atr: float = 2.5
    runner_capture_floor: float = 0.35


def _as_num(frame: pd.DataFrame, col: str) -> pd.Series:
    if col not in frame.columns:
        return pd.Series([float("nan")] * len(frame), index=frame.index, dtype="float64")
    return pd.to_numeric(frame[col], errors="coerce")


def _first_present(frame: pd.DataFrame, *cols: str) -> pd.Series:
    for col in cols:
        if col in frame.columns:
            return frame[col]
    return pd.Series([pd.NA] * len(frame), index=frame.index)


def _mean(series: pd.Series) -> float | None:
    s = pd.to_numeric(series, errors="coerce").dropna()
    return None if s.empty else float(s.mean())


def _safe_div(numer: pd.Series, denom: pd.Series) -> pd.Series:
    out = numer / denom
    return out.where(denom > 0)


def breakout_distance_bucket(distance_atr: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(distance_atr, errors="coerce")
    return pd.cut(numeric, bins=BREAKOUT_BUCKETS, labels=BREAKOUT_LABELS, include_lowest=True, right=True)


def classify_failure_mode(row: pd.Series, *, thresholds: _Thresholds = _Thresholds()) -> str:
    adx = pd.to_numeric(row.get("trend_strength_adx"), errors="coerce")
    gross = pd.to_numeric(row.get("realized_r_gross"), errors="coerce")
    net = pd.to_numeric(row.get("realized_r_net"), errors="coerce")
    mfe = pd.to_numeric(row.get("mfe_r"), errors="coerce")
    close_strength = pd.to_numeric(row.get("breakout_close_strength"), errors="coerce")
    range_atr = pd.to_numeric(row.get("breakout_bar_range_atr"), errors="coerce")
    failed_now = bool(row.get("failed_immediate_followthrough_flag", False))
    tp1_hit = bool(row.get("tp1_hit", False))
    capture = pd.to_numeric(row.get("capture_ratio"), errors="coerce")

    if pd.notna(adx) and adx < thresholds.weak_adx:
        return "trend_filter_weak"
    if pd.notna(gross) and pd.notna(net) and gross > 0 and net < 0:
        return "cost_killed"
    if pd.notna(range_atr) and range_atr >= thresholds.exhaustion_range_atr and pd.notna(close_strength) and close_strength <= thresholds.weak_close_strength:
        return "exhaustion_entry"
    if failed_now:
        return "false_breakout"
    if pd.notna(mfe) and mfe < thresholds.failed_followthrough_mfe:
        return "followthrough_failed"
    if tp1_hit and pd.notna(capture) and capture < thresholds.runner_capture_floor:
        return "runner_gave_back"
    if pd.notna(mfe) and mfe < 1.0:
        return "time_stop_no_extension"
    return "signal_noise"


def _aggregate_ev(frame: pd.DataFrame) -> dict[str, float | int | None]:
    gross = _as_num(frame, "realized_r_gross")
    net = _as_num(frame, "realized_r_net")
    mfe = _as_num(frame, "mfe_r")
    mae = _as_num(frame, "mae_r")
    wins = net[net > 0]
    losses = net[net < 0]
    avg_win = float(wins.mean()) if not wins.empty else None
    avg_loss = float(losses.mean()) if not losses.empty else None
    payoff = (avg_win / abs(avg_loss)) if (avg_win is not None and avg_loss not in (None, 0.0)) else None
    return {
        "n_trades": int(len(frame)),
        "EV_r_net": _mean(net),
        "EV_r_gross": _mean(gross),
        "win_rate": float((net > 0).mean()) if len(net) else None,
        "avg_r_win": avg_win,
        "avg_r_loss": avg_loss,
        "payoff_ratio": payoff,
        "avg_mfe_r": _mean(mfe),
        "avg_mae_r": _mean(mae),
    }


def _summary_by_group(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for key, segment in df.groupby(group_cols, dropna=False, observed=False):
        key_vals = key if isinstance(key, tuple) else (key,)
        payload = {group_cols[i]: key_vals[i] for i in range(len(group_cols))}
        payload.update(_aggregate_ev(segment))
        rows.append(payload)
    return pd.DataFrame(rows)


def build_h9_trade_diagnostic_rows(experiment_root: str | Path, *, run_dirs: list[Path] | None = None) -> pd.DataFrame:
    root = Path(experiment_root)
    paths = run_dirs if run_dirs is not None else sorted((root / "runs").glob("*"))
    rows: list[pd.DataFrame] = []

    for run_dir in paths:
        if not run_dir.is_dir():
            continue
        config_path = run_dir / "config_used.yaml"
        if not config_path.exists():
            continue
        cfg = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        strategy = cfg.get("strategy") if isinstance(cfg, dict) else {}
        if not isinstance(strategy, dict) or str(strategy.get("name", "")) != "l1_h9_momentum_breakout":
            continue

        trades = load_trades_with_entry_metadata(run_dir)
        if trades.empty:
            continue
        trades = trades.copy()
        trades["run_id"] = _first_present(trades, "run_id").fillna(run_dir.name)
        trades["run_dir"] = str(run_dir)
        trades["variant_id"] = _first_present(trades, "entry_meta__family_variant").replace("", pd.NA).fillna(str(strategy.get("family_variant", "L1-H9")))
        trades["hypothesis_id"] = trades["variant_id"]
        trades["signal_timeframe"] = _first_present(trades, "entry_meta__signal_timeframe", "entry_meta__timeframe").fillna(str(strategy.get("timeframe", "")))
        trades["breakout_level"] = _as_num(trades, "entry_meta__breakout_level")
        trades["breakout_level_type"] = _first_present(trades, "entry_meta__breakout_level_type")
        trades["breakout_distance_atr"] = _as_num(trades, "entry_meta__breakout_distance_atr")
        trades["breakout_close_strength"] = _as_num(trades, "entry_meta__breakout_close_strength")
        trades["breakout_bar_range_atr"] = _as_num(trades, "entry_meta__breakout_bar_range_atr")
        trades["trend_strength_adx"] = _as_num(trades, "entry_meta__trend_strength_adx").fillna(_as_num(trades, "entry_meta__adx_entry"))
        trades["ema_fast_entry"] = _as_num(trades, "entry_meta__ema_fast_entry")
        trades["ema_slow_entry"] = _as_num(trades, "entry_meta__ema_slow_entry")
        trades["ema_spread_pct"] = _as_num(trades, "entry_meta__ema_spread_pct")
        trades["bars_since_trend_alignment"] = _as_num(trades, "entry_meta__bars_since_trend_alignment")
        trades["exit_monitoring_timeframe"] = _first_present(trades, "entry_meta__exit_monitoring_timeframe")
        trades["stop_distance"] = _as_num(trades, "entry_meta__stop_distance")
        trades["stop_price"] = _as_num(trades, "entry_meta__stop_price")
        trades["entry_reference_price"] = _as_num(trades, "entry_meta__entry_reference_price")
        trades["tp1_at_r"] = _as_num(trades, "entry_meta__tp1_at_r")
        trades["post_tp1_lock_r"] = _as_num(trades, "entry_meta__post_tp1_lock_r")
        trades["trail_atr_mult"] = _as_num(trades, "entry_meta__trail_atr_mult")

        trades["mfe_r"] = _as_num(trades, "mfe_r")
        trades["mae_r"] = _as_num(trades, "mae_r")
        trades["realized_r_gross"] = _as_num(trades, "r_multiple_gross")
        trades["realized_r_net"] = _as_num(trades, "r_multiple_net")
        trades["capture_ratio"] = _safe_div(trades["realized_r_net"], trades["mfe_r"])
        trades["spread_cost"] = _as_num(trades, "spread_cost")
        trades["slippage_cost"] = _as_num(trades, "slippage")
        trades["fee_cost"] = _as_num(trades, "fees")
        trades["cost_drag_r"] = trades["realized_r_gross"] - trades["realized_r_net"]

        trades["tp1_hit"] = False
        if "max_unrealized_profit_r" in trades.columns:
            at_r = _as_num(trades, "entry_meta__tp1_at_r").fillna(2.0)
            trades["tp1_hit"] = (_as_num(trades, "max_unrealized_profit_r") >= at_r).fillna(False)

        trades["time_to_tp1_bars"] = _as_num(trades, "time_to_tp1_bars_signal")
        trades["time_to_tp1_bars"] = trades["time_to_tp1_bars"].fillna(_as_num(trades, "time_to_mfe_bars_signal").where(trades["tp1_hit"], pd.NA))
        trades["time_to_mfe_peak_bars"] = _as_num(trades, "time_to_mfe_bars_signal")
        trades["continuation_extension_atr"] = trades["mfe_r"]
        trades["failed_immediate_followthrough_flag"] = _first_present(trades, "failed_immediate_followthrough_flag").astype("boolean")
        trades["failed_immediate_followthrough_flag"] = trades["failed_immediate_followthrough_flag"].fillna((trades["mfe_r"] < 0.5).fillna(True))
        trades["runner_giveback_r"] = trades["mfe_r"] - trades["realized_r_net"]
        trades["failure_mode_label"] = trades.apply(classify_failure_mode, axis=1)
        rows.append(trades)

    if not rows:
        return pd.DataFrame()

    combined = pd.concat(rows, ignore_index=True)
    keep = [
        "run_id", "hypothesis_id", "variant_id", "symbol", "signal_timeframe", "side", "entry_ts", "exit_ts",
        "breakout_level", "breakout_level_type", "breakout_distance_atr", "breakout_close_strength", "breakout_bar_range_atr",
        "trend_strength_adx", "ema_fast_entry", "ema_slow_entry", "ema_spread_pct", "bars_since_trend_alignment",
        "exit_monitoring_timeframe", "stop_distance", "stop_price", "entry_reference_price", "tp1_at_r", "post_tp1_lock_r", "trail_atr_mult",
        "tp1_hit", "time_to_tp1_bars", "time_to_mfe_peak_bars", "continuation_extension_atr", "failed_immediate_followthrough_flag",
        "mfe_r", "mae_r", "realized_r_gross", "realized_r_net", "capture_ratio", "runner_giveback_r",
        "spread_cost", "slippage_cost", "fee_cost", "cost_drag_r", "failure_mode_label", "run_dir",
    ]
    for col in keep:
        if col not in combined.columns:
            combined[col] = pd.NA
    return combined[keep]


def run_h9_postmortem(experiment_root: str | Path, *, run_dirs: list[Path] | None = None, output_root: str | Path | None = None) -> dict[str, str]:
    root = Path(experiment_root)
    out_root = Path(output_root) if output_root else root / "summaries" / "diagnostics" / "l1_h9"
    out_root.mkdir(parents=True, exist_ok=True)

    rows = build_h9_trade_diagnostic_rows(root, run_dirs=run_dirs)
    outputs: dict[str, str] = {}
    if rows.empty:
        return outputs

    rows["breakout_distance_bucket"] = breakout_distance_bucket(rows["breakout_distance_atr"])
    trade_path = out_root / "h9_trade_diagnostics.csv"
    rows.to_csv(trade_path, index=False)
    outputs["h9_trade_diagnostics"] = str(trade_path)

    pd.DataFrame([_aggregate_ev(rows)]).to_csv(out_root / "breakout_quality_summary.csv", index=False)
    outputs["breakout_quality_summary"] = str(out_root / "breakout_quality_summary.csv")

    _summary_by_group(rows, ["breakout_distance_bucket"]).to_csv(out_root / "ev_by_breakout_distance_bucket.csv", index=False)
    outputs["ev_by_breakout_distance_bucket"] = str(out_root / "ev_by_breakout_distance_bucket.csv")

    _summary_by_group(rows, ["signal_timeframe"]).to_csv(out_root / "ev_by_signal_timeframe.csv", index=False)
    outputs["ev_by_signal_timeframe"] = str(out_root / "ev_by_signal_timeframe.csv")

    cont = pd.DataFrame([{
        "n_trades": int(len(rows)),
        "tp1_hit_rate": float(rows["tp1_hit"].mean()) if len(rows) else None,
        "avg_time_to_tp1_bars": _mean(rows["time_to_tp1_bars"]),
        "avg_time_to_mfe_peak_bars": _mean(rows["time_to_mfe_peak_bars"]),
        "avg_continuation_extension_atr": _mean(rows["continuation_extension_atr"]),
        "failed_immediate_followthrough_rate": float(rows["failed_immediate_followthrough_flag"].mean()),
    }])
    cont.to_csv(out_root / "continuation_strength_summary.csv", index=False)
    outputs["continuation_strength_summary"] = str(out_root / "continuation_strength_summary.csv")

    _summary_by_group(rows, ["failure_mode_label"]).to_csv(out_root / "failure_mode_summary.csv", index=False)
    outputs["failure_mode_summary"] = str(out_root / "failure_mode_summary.csv")

    _summary_by_group(rows, ["variant_id", "failure_mode_label"]).to_csv(out_root / "failure_mode_by_variant.csv", index=False)
    outputs["failure_mode_by_variant"] = str(out_root / "failure_mode_by_variant.csv")

    runner = pd.DataFrame([{
        "n_trades": int(len(rows)),
        "tp1_hit_rate": float(rows["tp1_hit"].mean()) if len(rows) else None,
        "runner_survival_rate": float((rows["tp1_hit"] & (rows["realized_r_net"] > 0)).mean()),
        "capture_after_tp1": _mean(rows.loc[rows["tp1_hit"], "capture_ratio"]),
        "runner_giveback_r": _mean(rows.loc[rows["tp1_hit"], "runner_giveback_r"]),
    }])
    runner.to_csv(out_root / "runner_capture_summary.csv", index=False)
    outputs["runner_capture_summary"] = str(out_root / "runner_capture_summary.csv")

    runner_var = []
    for variant, group in rows.groupby("variant_id", dropna=False, observed=False):
        runner_var.append({
            "variant_id": variant,
            "n_trades": int(len(group)),
            "tp1_hit_rate": float(group["tp1_hit"].mean()) if len(group) else None,
            "runner_survival_rate": float((group["tp1_hit"] & (group["realized_r_net"] > 0)).mean()),
            "capture_after_tp1": _mean(group.loc[group["tp1_hit"], "capture_ratio"]),
            "runner_giveback_r": _mean(group.loc[group["tp1_hit"], "runner_giveback_r"]),
        })
    pd.DataFrame(runner_var).to_csv(out_root / "runner_capture_by_variant.csv", index=False)
    outputs["runner_capture_by_variant"] = str(out_root / "runner_capture_by_variant.csv")

    cost = pd.DataFrame([{
        "n_trades": int(len(rows)),
        "avg_realized_r_gross": _mean(rows["realized_r_gross"]),
        "avg_realized_r_net": _mean(rows["realized_r_net"]),
        "avg_cost_drag": _mean(rows["cost_drag_r"]),
        "share_of_trades_gross_positive_but_net_negative": float(((rows["realized_r_gross"] > 0) & (rows["realized_r_net"] < 0)).mean()),
    }])
    cost.to_csv(out_root / "cost_kill_summary.csv", index=False)
    outputs["cost_kill_summary"] = str(out_root / "cost_kill_summary.csv")

    _summary_by_group(rows, ["breakout_distance_bucket"]).to_csv(out_root / "cost_kill_by_breakout_bucket.csv", index=False)
    outputs["cost_kill_by_breakout_bucket"] = str(out_root / "cost_kill_by_breakout_bucket.csv")

    _summary_by_group(rows, ["signal_timeframe"]).to_csv(out_root / "cost_kill_by_timeframe.csv", index=False)
    outputs["cost_kill_by_timeframe"] = str(out_root / "cost_kill_by_timeframe.csv")

    _summary_by_group(rows, ["symbol"]).to_csv(out_root / "cost_kill_by_symbol.csv", index=False)
    outputs["cost_kill_by_symbol"] = str(out_root / "cost_kill_by_symbol.csv")

    return outputs
