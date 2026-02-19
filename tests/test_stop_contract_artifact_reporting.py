from __future__ import annotations

import json
from pathlib import Path

from bt.experiments.grid_runner import _write_run_status
from bt.logging.summary import write_summary_txt
from bt.risk.stop_contract_reporting import build_stop_contract_report


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_required_summary_artifacts(run_dir: Path) -> None:
    _write_json(
        run_dir / "performance.json",
        {
            "net_pnl": 1.0,
            "gross_pnl": 1.2,
            "total_return": 0.01,
            "max_drawdown_pct": 1.0,
            "sharpe_annualized": 1.1,
            "sortino_annualized": 1.2,
            "mar_ratio": 1.0,
            "win_rate": 0.5,
            "profit_factor": 1.1,
            "ev_r_net": 0.01,
            "ev_r_gross": 0.02,
            "fee_total": 0.0,
            "slippage_total": 0.0,
            "spread_total": 0.0,
            "fee_drag_pct": 0.0,
            "slippage_drag_pct": 0.0,
            "spread_drag_pct": 0.0,
            "longest_loss_streak": 1,
            "max_drawdown_duration": 2,
        },
    )
    (run_dir / "trades.csv").write_text(
        "entry_ts,exit_ts,symbol,side,pnl,r_multiple_net\n"
        "2024-01-01,2024-01-02,BTCUSDT,LONG,1.0,0.5\n",
        encoding="utf-8",
    )


def test_build_stop_contract_report_counts_deterministically(tmp_path: Path) -> None:
    decisions_path = tmp_path / "decisions.jsonl"
    lines = [
        {"order": {"metadata": {"stop_source": "explicit_stop_price", "used_legacy_stop_proxy": False}}},
        {"order": {"metadata": {"stop_source": "explicit_stop_price", "used_legacy_stop_proxy": False}}},
        {
            "order": {
                "metadata": {
                    "stop_source": "legacy_high_low_proxy",
                    "used_legacy_stop_proxy": True,
                    "stop_reason_code": "risk_fallback:legacy_proxy",
                }
            }
        },
        {"reason_code": "risk_reject:stop_missing"},
        {"reason": "AAA: StrategyContractError: missing stop for entry sizing in strict mode"},
    ]
    decisions_path.write_text("\n".join(json.dumps(line, sort_keys=True) for line in lines) + "\n", encoding="utf-8")

    report = build_stop_contract_report(
        config={"risk": {"stop_resolution": "strict", "allow_legacy_proxy": False}},
        decisions_path=decisions_path,
    )

    assert report is not None
    assert report.counts == {
        "resolved_explicit": 2,
        "resolved_structural": 0,
        "resolved_atr": 0,
        "resolved_hybrid": 0,
        "fallback_legacy_proxy": 1,
        "rejected_missing_stop": 2,
        "rejected_unresolvable_stop": 0,
        "rejected_invalid_stop_payload": 0,
    }
    assert report.notes == [
        "Strict mode requires entry signals to provide stop_price or metadata.stop_spec.",
        "Legacy proxy fallback was used for entries with unresolved stop inputs.",
        "Missing-stop entries were rejected; provide stop_price or metadata.stop_spec.",
    ]


def test_write_run_status_includes_stop_contract_schema(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    decisions_path = run_dir / "decisions.jsonl"
    decisions_path.write_text(json.dumps({"order": {"metadata": {"stop_source": "explicit_stop_price"}}}) + "\n", encoding="utf-8")

    _write_run_status(
        run_dir,
        {
            "status": "PASS",
            "error_type": "",
            "error_message": "",
            "traceback": "",
            "run_id": "run_001",
        },
        config={"risk": {"stop_resolution": "safe", "allow_legacy_proxy": True}},
    )

    payload = json.loads((run_dir / "run_status.json").read_text(encoding="utf-8"))
    assert payload["stop_contract"]["version"] == 1
    assert payload["stop_contract"]["mode"] == "safe"
    expected_count_keys = {
        "resolved_explicit",
        "resolved_structural",
        "resolved_atr",
        "resolved_hybrid",
        "fallback_legacy_proxy",
        "rejected_missing_stop",
        "rejected_unresolvable_stop",
        "rejected_invalid_stop_payload",
    }
    assert set(payload["stop_contract"]["counts"].keys()) == expected_count_keys


def test_summary_includes_stop_contract_block(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_summary"
    run_dir.mkdir()
    _write_required_summary_artifacts(run_dir)
    _write_json(
        run_dir / "run_status.json",
        {
            "status": "PASS",
            "stop_contract": {
                "version": 1,
                "mode": "strict",
                "allow_legacy_proxy": False,
                "counts": {
                    "resolved_explicit": 3,
                    "resolved_structural": 0,
                    "resolved_atr": 0,
                    "resolved_hybrid": 0,
                    "fallback_legacy_proxy": 0,
                    "rejected_missing_stop": 1,
                    "rejected_unresolvable_stop": 0,
                    "rejected_invalid_stop_payload": 0,
                },
                "notes": [],
            },
        },
    )

    write_summary_txt(run_dir)

    content = (run_dir / "summary.txt").read_text(encoding="utf-8")
    assert "Stop Contract (v1)" in content
    assert "  mode: strict" in content
    assert "  legacy proxy allowed: false" in content
    assert "  resolved: explicit=3 atr=0 structural=0 hybrid=0" in content
    assert "  fallback legacy proxy: 0" in content
    assert "  rejected: missing_stop=1 unresolvable=0 invalid_payload=0" in content
