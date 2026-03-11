"""Reusable run-summary extraction for hypothesis experiments."""
from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from bt.experiments.status import detect_run_artifact_status

CORE_COLUMNS = [
    "run_id",
    "hypothesis_id",
    "hypothesis_title",
    "variant_id",
    "tier",
    "symbol",
    "signal_timeframe",
    "start_ts",
    "end_ts",
    "status",
    "ev_r_net",
    "ev_r_gross",
    "win_rate",
    "avg_r_win",
    "avg_r_loss",
    "payoff_ratio",
    "max_consecutive_losses",
    "num_trades",
    "max_drawdown_r",
    "drawdown_duration",
    "turnover",
    "tail_loss_p95",
    "tail_loss_p99",
    "mfe_mean_r",
    "mae_mean_r",
    "capture_ratio_mean",
    "output_dir",
]

HYPERPARAM_KEYS = [
    "theta_vol",
    "k_atr",
    "T_hold",
    "q_comp",
    "z0",
    "fit_window_days",
    "gate_quantile",
    "k",
]

_VOL_FIELD_CANDIDATES = [
    "entry_meta__vol_pct_t",
    "entry_meta__rvhat_pct_t",
    "entry_meta__vol_percentile",
    "entry_meta__rv_hat_pct",
]


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    return loaded if isinstance(loaded, dict) else {}


def _discover_run_dirs(experiment_root: Path, runs_glob: str = "runs/*") -> list[Path]:
    if (experiment_root / "performance.json").exists():
        return [experiment_root]
    run_dirs = [path for path in experiment_root.glob(runs_glob) if path.is_dir()]
    return sorted(run_dirs)


def _strategy_to_hypothesis_map() -> dict[str, dict[str, str]]:
    mapping: dict[str, dict[str, str]] = {}
    for path in sorted(Path("research/hypotheses").glob("*.yaml")):
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            continue
        entry = payload.get("entry") if isinstance(payload.get("entry"), dict) else {}
        strategy_name = entry.get("strategy")
        if not isinstance(strategy_name, str):
            continue
        mapping[strategy_name] = {
            "hypothesis_id": str(payload.get("hypothesis_id", strategy_name)),
            "hypothesis_title": str(payload.get("title", "")),
        }
    return mapping


def _infer_variant_id(run_dir: Path) -> str:
    m = re.search(r"(g\d{4,})", run_dir.name)
    if m:
        return m.group(1)
    return ""


def _infer_tier(config: dict[str, Any], run_dir: Path) -> str:
    execution = config.get("execution") if isinstance(config.get("execution"), dict) else {}
    profile = execution.get("profile")
    if isinstance(profile, str) and profile.startswith("tier"):
        return profile.capitalize()
    m = re.search(r"__(tier\d)", run_dir.name)
    if m:
        return m.group(1).capitalize()
    return ""


def _capture_ratio_mean_from_trades(trades_df: pd.DataFrame) -> float | None:
    if trades_df.empty or "r_multiple_net" not in trades_df.columns:
        return None
    if "mfe_price" not in trades_df.columns or "risk_amount" not in trades_df.columns:
        return None

    mfe_r = pd.to_numeric(trades_df["mfe_price"], errors="coerce") / pd.to_numeric(trades_df["risk_amount"], errors="coerce")
    realized_r = pd.to_numeric(trades_df["r_multiple_net"], errors="coerce")
    denom = mfe_r.where(mfe_r > 0)
    capture = realized_r / denom
    capture = capture.replace([pd.NA, float("inf"), float("-inf")], pd.NA).dropna()
    if capture.empty:
        return None
    return float(capture.mean())


def _turnover_from_trades(trades_df: pd.DataFrame) -> float | None:
    if trades_df.empty:
        return None
    if {"entry_price", "exit_price", "qty"}.issubset(trades_df.columns):
        qty = pd.to_numeric(trades_df["qty"], errors="coerce").abs()
        entry_notional = qty * pd.to_numeric(trades_df["entry_price"], errors="coerce")
        exit_notional = qty * pd.to_numeric(trades_df["exit_price"], errors="coerce")
        return float((entry_notional + exit_notional).sum())
    return None


def build_run_summary_row(run_dir: Path, *, completed_only: bool = True, hypothesis_catalog: dict[str, dict[str, str]] | None = None) -> tuple[dict[str, Any], list[str]]:
    warnings: list[str] = []
    status = detect_run_artifact_status(run_dir)
    if completed_only and status.state != "SUCCESS":
        return {}, []

    performance = _read_json(run_dir / "performance.json")
    config = _read_config(run_dir / "config_used.yaml")

    if not performance:
        warnings.append("missing_or_invalid_performance_json")

    strategy_cfg = config.get("strategy") if isinstance(config.get("strategy"), dict) else {}
    strategy_name = str(strategy_cfg.get("name", ""))

    hypo_id = strategy_name
    hypo_title = ""
    if hypothesis_catalog and strategy_name in hypothesis_catalog:
        hypo_id = hypothesis_catalog[strategy_name]["hypothesis_id"]
        hypo_title = hypothesis_catalog[strategy_name]["hypothesis_title"]

    data_cfg = config.get("data") if isinstance(config.get("data"), dict) else {}
    symbols = data_cfg.get("symbols_subset") if isinstance(data_cfg.get("symbols_subset"), list) else []
    date_range = data_cfg.get("date_range") if isinstance(data_cfg.get("date_range"), dict) else {}

    trades_path = run_dir / "trades.csv"
    try:
        trades_df = pd.read_csv(trades_path) if trades_path.exists() else pd.DataFrame()
    except pd.errors.EmptyDataError:
        trades_df = pd.DataFrame()

    row = {
        "run_id": run_dir.name,
        "hypothesis_id": hypo_id,
        "hypothesis_title": hypo_title,
        "variant_id": _infer_variant_id(run_dir),
        "tier": _infer_tier(config, run_dir),
        "symbol": "|".join(str(s) for s in symbols),
        "signal_timeframe": strategy_cfg.get("timeframe", strategy_cfg.get("signal_timeframe", "")),
        "start_ts": date_range.get("start", ""),
        "end_ts": date_range.get("end", ""),
        "status": status.state,
        "ev_r_net": performance.get("ev_r_net"),
        "ev_r_gross": performance.get("ev_r_gross"),
        "win_rate": performance.get("win_rate_r", performance.get("win_rate")),
        "avg_r_win": performance.get("avg_r_win"),
        "avg_r_loss": performance.get("avg_r_loss"),
        "payoff_ratio": performance.get("payoff_ratio_r"),
        "max_consecutive_losses": performance.get("max_consecutive_losses"),
        "num_trades": performance.get("total_trades", performance.get("trades")),
        "max_drawdown_r": performance.get("max_drawdown_pct", performance.get("max_drawdown")),
        "drawdown_duration": performance.get("max_drawdown_duration", performance.get("max_drawdown_duration_bars")),
        "turnover": performance.get("turnover", _turnover_from_trades(trades_df)),
        "tail_loss_p95": performance.get("tail_loss_p95"),
        "tail_loss_p99": performance.get("tail_loss_p99"),
        "mfe_mean_r": performance.get("mfe_mean_r"),
        "mae_mean_r": performance.get("mae_mean_r"),
        "capture_ratio_mean": _capture_ratio_mean_from_trades(trades_df),
        "output_dir": str(run_dir),
    }

    for key in HYPERPARAM_KEYS:
        row[key] = strategy_cfg.get(key)

    # evaluation-metric vocabulary availability snapshot
    row["eval_metric_tail_loss_max"] = performance.get("worst_streak_loss")
    row["eval_metric_avg_hold_bars"] = performance.get("avg_hold_bars")

    if not trades_path.exists():
        warnings.append("missing_trades_csv")

    return row, warnings


def summarize_experiment_runs(
    experiment_root: str | Path,
    *,
    output_csv: str | Path | None = None,
    completed_only: bool = True,
    runs_glob: str = "runs/*",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    root = Path(experiment_root)
    run_dirs = _discover_run_dirs(root, runs_glob=runs_glob)
    catalog = _strategy_to_hypothesis_map()

    rows: list[dict[str, Any]] = []
    warning_rows: list[dict[str, Any]] = []

    for run_dir in run_dirs:
        row, run_warnings = build_run_summary_row(
            run_dir,
            completed_only=completed_only,
            hypothesis_catalog=catalog,
        )
        if row:
            rows.append(row)
        for warning in run_warnings:
            warning_rows.append({"run_dir": str(run_dir), "warning": warning})

    summary_df = pd.DataFrame(rows)
    warning_df = pd.DataFrame(warning_rows)

    summaries_dir = root / "summaries"
    summaries_dir.mkdir(parents=True, exist_ok=True)
    out_path = Path(output_csv) if output_csv else summaries_dir / "run_summary.csv"
    if not summary_df.empty:
        ordered = [col for col in CORE_COLUMNS + HYPERPARAM_KEYS if col in summary_df.columns]
        ordered += sorted(c for c in summary_df.columns if c not in ordered)
        summary_df = summary_df[ordered]
    summary_df.to_csv(out_path, index=False)

    warnings_path = summaries_dir / "diagnostics_warnings.csv"
    warning_df.to_csv(warnings_path, index=False)

    return summary_df, warning_df


def write_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["key", "value"])
        for key in sorted(payload):
            writer.writerow([key, payload[key]])
