from __future__ import annotations

import csv
import datetime as dt
import json
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from bt.api import run_backtest


def _ts(i: int) -> dt.datetime:
    return dt.datetime(2024, 1, 1, 0, 0, tzinfo=dt.timezone.utc) + dt.timedelta(minutes=i)


def _write_legacy_manifest(
    dataset_dir: Path,
    symbols: list[str],
    relpath_template: str = "symbols/{symbol}.parquet",
) -> None:
    dataset_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "format": "per_symbol_parquet",
        "symbols": symbols,
        "path": relpath_template,
    }
    (dataset_dir / "manifest.yaml").write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _write_symbol_parquet(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume", "symbol"])
    frame["ts"] = pd.to_datetime(frame["ts"], utc=True)
    table = pa.Table.from_pandas(frame, preserve_index=False)
    pq.write_table(table, path)


def _generate_rows(symbol: str, bars: int, base: float) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for i in range(bars):
        open_px = base + i * 0.5
        close_px = open_px + (0.2 if i % 2 == 0 else -0.1)
        high_px = max(open_px, close_px) + 0.3
        low_px = min(open_px, close_px) - 0.25
        rows.append(
            {
                "ts": _ts(i),
                "open": open_px,
                "high": high_px,
                "low": low_px,
                "close": close_px,
                "volume": float(i + 1),
                "symbol": symbol,
            }
        )
    return rows


def _write_dataset(dataset_dir: Path, bars: int = 80) -> None:
    symbols = ["BTCUSDT", "ETHUSDT"]
    _write_legacy_manifest(dataset_dir, symbols)
    _write_symbol_parquet(dataset_dir / "symbols" / "BTCUSDT.parquet", _generate_rows("BTCUSDT", bars, 30000.0))
    _write_symbol_parquet(dataset_dir / "symbols" / "ETHUSDT.parquet", _generate_rows("ETHUSDT", bars, 2000.0))


def _write_config(config_path: Path) -> None:
    config = {
        "initial_cash": 1000.0,
        "max_leverage": 2.0,
        "signal_delay_bars": 1,
        "risk": {"max_positions": 1, "risk_per_trade_pct": 0.001},
        "strategy": {
            "name": "coinflip",
            "seed": 11,
            "p_trade": 0.0,
            "cooldown_bars": 0,
        },
        "data": {
            "mode": "streaming",
            "symbols_subset": ["BTCUSDT", "ETHUSDT"],
        },
        "execution": {
            "profile": "tier2",
        },
        "benchmark": {
            "enabled": False,
        },
    }
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")


def _iso_utc(value: str) -> str:
    return pd.Timestamp(value).tz_convert("UTC").isoformat()


def _round_or_none(value: str, digits: int = 12) -> float | None:
    if value in ("", None):
        return None
    return round(float(value), digits)


def read_equity_csv(path: Path) -> list[tuple[str, float | None, float | None, float | None, float | None, float | None, float | None]]:
    selected = ["ts", "cash", "equity", "realized_pnl", "unrealized_pnl", "used_margin", "free_margin"]
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    normalized: list[tuple[str, float | None, float | None, float | None, float | None, float | None, float | None]] = []
    for row in rows:
        normalized.append(
            (
                _iso_utc(row["ts"]),
                *(_round_or_none(row.get(key)) for key in selected[1:]),
            )
        )
    return normalized


def read_trades_csv(path: Path) -> list[dict[str, Any]]:
    preferred = [
        "entry_ts",
        "exit_ts",
        "symbol",
        "side",
        "qty",
        "entry_qty",
        "exit_qty",
        "entry_price",
        "exit_price",
        "pnl",
        "pnl_price",
        "fees_paid",
        "pnl_net",
        "fees",
        "slippage",
    ]
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    normalized: list[dict[str, Any]] = []
    for row in rows:
        item: dict[str, Any] = {}
        for key in preferred:
            if key not in row:
                continue
            value = row[key]
            if key.endswith("_ts") and value:
                item[key] = _iso_utc(value)
            elif key in {"symbol", "side"}:
                item[key] = value
            else:
                item[key] = _round_or_none(value)
        normalized.append(item)
    return normalized


def _canonicalize_json(value: Any) -> Any:
    if isinstance(value, dict):
        ignored = {"run_id", "created_at", "created_at_utc", "generated_at", "timestamp"}
        return {k: _canonicalize_json(v) for k, v in sorted(value.items()) if k not in ignored}
    if isinstance(value, list):
        return [_canonicalize_json(item) for item in value]
    if isinstance(value, float):
        return round(value, 12)
    return value


def read_json(path: Path) -> str:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return json.dumps(_canonicalize_json(payload), sort_keys=True)


def read_text(path: Path) -> list[str]:
    stable_lines: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("Generated:"):
            continue
        if "seconds" in stripped.lower() or "elapsed" in stripped.lower():
            continue
        stable_lines.append(stripped)
    return stable_lines


def _run(config_path: Path, dataset_dir: Path, out_dir: Path, run_name: str) -> Path:
    return Path(
        run_backtest(
            config_path=str(config_path),
            data_path=str(dataset_dir),
            out_dir=str(out_dir),
            run_name=run_name,
        )
    )


def test_crypto_repeat_run_same_outputs(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    _write_dataset(dataset_dir, bars=80)

    config_path = tmp_path / "engine.yaml"
    _write_config(config_path)

    run_a = _run(config_path, dataset_dir, tmp_path / "out_a", "crypto-repeat-a")
    run_b = _run(config_path, dataset_dir, tmp_path / "out_b", "crypto-repeat-b")

    required = {
        "config_used.yaml",
        "equity.csv",
        "trades.csv",
        "performance.json",
        "fills.jsonl",
        "decisions.jsonl",
        "performance_by_bucket.csv",
    }
    for filename in required:
        assert (run_a / filename).exists(), filename
        assert (run_b / filename).exists(), filename

    run_status_a = run_a / "run_status.json"
    run_status_b = run_b / "run_status.json"
    if run_status_a.exists() and run_status_b.exists():
        assert json.loads(run_status_a.read_text(encoding="utf-8"))["status"] == "PASS"
        assert json.loads(run_status_b.read_text(encoding="utf-8"))["status"] == "PASS"

    assert not (run_a / "benchmark_equity.csv").exists()
    assert not (run_b / "benchmark_equity.csv").exists()

    assert read_equity_csv(run_a / "equity.csv") == read_equity_csv(run_b / "equity.csv")
    assert read_trades_csv(run_a / "trades.csv") == read_trades_csv(run_b / "trades.csv")
    assert read_json(run_a / "performance.json") == read_json(run_b / "performance.json")

    assert sum(1 for _ in (run_a / "decisions.jsonl").open("r", encoding="utf-8")) == sum(
        1 for _ in (run_b / "decisions.jsonl").open("r", encoding="utf-8")
    )
    assert sum(1 for _ in (run_a / "fills.jsonl").open("r", encoding="utf-8")) == sum(
        1 for _ in (run_b / "fills.jsonl").open("r", encoding="utf-8")
    )

    summary_a = run_a / "summary.txt"
    summary_b = run_b / "summary.txt"
    if summary_a.exists() and summary_b.exists():
        assert read_text(summary_a) == read_text(summary_b)


def test_crypto_config_used_is_resolved_and_stable(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    _write_dataset(dataset_dir, bars=60)

    config_path = tmp_path / "engine.yaml"
    _write_config(config_path)

    run_dir = _run(config_path, dataset_dir, tmp_path / "out", "crypto-config-used")
    config_used_path = run_dir / "config_used.yaml"

    assert config_used_path.exists()

    config_used = yaml.safe_load(config_used_path.read_text(encoding="utf-8"))
    assert isinstance(config_used, dict)

    data_section = config_used.get("data")
    assert isinstance(data_section, dict)
    assert "mode" in data_section
    assert any(scope_key in data_section for scope_key in ("symbols_subset", "symbols"))

    execution_section = config_used.get("execution")
    assert isinstance(execution_section, dict)
    assert "profile" in execution_section

    assert isinstance(config_used.get("risk"), dict)
