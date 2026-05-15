#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
import pandas as pd
import numpy as np

TOL = 1e-6

def _load_json(p: Path):
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding='utf-8'))
    except Exception:
        return None

def _load_df(p: Path):
    if not p.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(p) if p.suffix == '.parquet' else pd.read_csv(p)
    except Exception:
        return pd.DataFrame()

def _pick(df, names):
    for n in names:
        if n in df.columns:
            return pd.to_numeric(df[n], errors='coerce')
    return pd.Series(dtype=float)

def _as_float(value):
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if np.isfinite(out) else None

def _close(left, right, tol: float = TOL) -> bool:
    left_f = _as_float(left)
    right_f = _as_float(right)
    return left_f is not None and right_f is not None and abs(left_f - right_f) <= tol

def audit_run(run_dir: Path, absurd_r: float = 100.0) -> dict:
    perf = _load_json(run_dir / 'performance.json') or {}
    trades = _load_df(run_dir / 'trades.csv')
    _ = _load_df(run_dir / 'equity.csv')
    errors, suspicious = [], []

    init = float(perf.get('initial_equity', 0.0) or 0.0)
    final = float(perf.get('final_equity', 0.0) or 0.0)
    net = float(perf.get('net_pnl', 0.0) or 0.0)
    gross = float(perf.get('gross_pnl', 0.0) or 0.0)
    eq_delta = final - init
    if abs(eq_delta - net) > TOL:
        errors.append('equity_vs_net_pnl_mismatch')

    costs = perf.get('costs', {}) if isinstance(perf.get('costs'), dict) else {}
    fee = float(costs.get('fees_total', perf.get('fee_total', 0.0)) or 0.0)
    slip = float(costs.get('slippage_total', perf.get('slippage_total', 0.0)) or 0.0)
    spr = float(costs.get('spread_total', perf.get('spread_total', 0.0)) or 0.0)
    comm = float(costs.get('commission_total', 0.0) or 0.0)

    pnl_net = _pick(trades, ['pnl_net', 'net_pnl'])
    trade_net = float(pnl_net.sum()) if not pnl_net.empty else np.nan
    if np.isfinite(trade_net) and abs(trade_net - net) > TOL:
        errors.append('trade_net_pnl_mismatch')

    r = _pick(trades, ['r_net', 'r_multiple_net', 'realized_r_net'])
    valid = r.dropna()
    mean_r = float(valid.mean()) if not valid.empty else None
    wr = float((valid > 0).mean()) if not valid.empty else None

    if mean_r is not None and perf.get('ev_r_net') is not None and not _close(perf.get('ev_r_net'), mean_r):
        errors.append('ev_r_net_mismatch')
    if wr is not None and perf.get('win_rate_r') is not None and not _close(perf.get('win_rate_r'), wr):
        errors.append('win_rate_r_mismatch')

    evb = perf.get('ev_by_bucket', {}) if isinstance(perf.get('ev_by_bucket'), dict) else {}
    allv = evb.get('overall_all_trades', evb.get('all'))
    ev_net = perf.get('ev_net')
    bucket_ev_unit = None
    bucket_ev_diff = None
    bucket_ev_r_diff = (_as_float(allv) - mean_r) if (_as_float(allv) is not None and mean_r is not None) else None
    bucket_ev_pnl_diff = (_as_float(allv) - _as_float(ev_net)) if (_as_float(allv) is not None and _as_float(ev_net) is not None) else None
    if allv is not None:
        # performance.ev_by_bucket is legacy PnL-denominated EV, while structural
        # analysis writes ev_r_net separately. Accept either unit when it matches
        # the corresponding headline metric; only fail when it reconciles to neither.
        bucket_matches_pnl_ev = _close(allv, ev_net)
        bucket_matches_r_ev = mean_r is not None and _close(allv, mean_r)
        if bucket_matches_pnl_ev:
            bucket_ev_unit = 'pnl'
            bucket_ev_diff = bucket_ev_pnl_diff
        elif bucket_matches_r_ev:
            bucket_ev_unit = 'r'
            bucket_ev_diff = bucket_ev_r_diff
        else:
            bucket_ev_unit = 'unknown'
            bucket_ev_diff = bucket_ev_r_diff
            errors.append('ev_by_bucket_all_mismatch')

    risk = _pick(trades, ['risk_amount'])
    zero_risk = int(((risk <= 0) | risk.isna()).sum()) if not risk.empty else 0
    suspicious_r = int((valid.abs() > absurd_r).sum()) if not valid.empty else 0
    if zero_risk > 0 and not r[risk <= 0].dropna().empty:
        errors.append('r_present_with_zero_risk')
    if suspicious_r > 0:
        suspicious.append('absurd_r_values')

    status = 'PASSED' if not errors and not suspicious else ('FAILED' if errors else 'SUSPICIOUS')
    return {
        'run_id': run_dir.name,
        'status': status,
        'metrics_valid': len(errors) == 0,
        'errors': errors,
        'suspicious': suspicious,
        'reconciliation': {
            'equity_net_pnl_diff': eq_delta - net,
            'trade_net_pnl_diff': (trade_net - net) if np.isfinite(trade_net) else None,
            'mean_r_net_diff': (float(perf.get('ev_r_net')) - mean_r) if (mean_r is not None and perf.get('ev_r_net') is not None) else None,
            'win_rate_r_diff': (float(perf.get('win_rate_r')) - wr) if (wr is not None and perf.get('win_rate_r') is not None) else None,
            'bucket_ev_unit': bucket_ev_unit,
            'bucket_ev_diff': bucket_ev_diff,
            'bucket_ev_r_diff': bucket_ev_r_diff,
            'bucket_ev_pnl_diff': bucket_ev_pnl_diff,
            'suspicious_r_count': suspicious_r,
            'zero_risk_count': zero_risk,
            'gross_minus_costs': gross - (fee + slip + spr + comm),
            'net_pnl': net,
        },
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--run-dir', required=True)
    args = ap.parse_args()
    run = Path(args.run_dir)
    out = audit_run(run)
    rdir = Path('research/audits')
    rdir.mkdir(parents=True, exist_ok=True)
    (rdir / f'run_metric_audit_{run.name}.json').write_text(json.dumps(out, indent=2), encoding='utf-8')
    md = f"# Run Metric Audit {run.name}\n\nStatus: **{out['status']}**\n\n```json\n{json.dumps(out, indent=2)}\n```\n"
    (rdir / f'run_metric_audit_{run.name}.md').write_text(md, encoding='utf-8')
    print(json.dumps(out))

if __name__ == '__main__':
    main()
