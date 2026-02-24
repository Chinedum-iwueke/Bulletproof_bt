from __future__ import annotations

import csv
import datetime as dt
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from bt.api import run_backtest
from bt.core.config_resolver import resolve_config
from bt.instruments.registry import resolve_instrument_spec
from bt.instruments.spec import InstrumentSpec


def _ts(i: int) -> dt.datetime:
    return dt.datetime(2024, 2, 1, 0, 0, tzinfo=dt.timezone.utc) + dt.timedelta(minutes=i)


def _write_legacy_manifest(dataset_dir: Path, symbols: list[str]) -> None:
    dataset_dir.mkdir(parents=True, exist_ok=True)
    (dataset_dir / "manifest.yaml").write_text(
        yaml.safe_dump(
            {
                "format": "per_symbol_parquet",
                "symbols": symbols,
                "path": "symbols/{symbol}.parquet",
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )


def _write_symbol_parquet(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume", "symbol"])
    frame["ts"] = pd.to_datetime(frame["ts"], utc=True)
    pq.write_table(pa.Table.from_pandas(frame, preserve_index=False), path)


def _write_dataset(dataset_dir: Path, symbol: str = "BTCUSDT", bars: int = 40) -> None:
    _write_legacy_manifest(dataset_dir, [symbol])
    rows: list[dict[str, object]] = []
    for i in range(bars):
        open_px = 100.0 + i * 0.25
        close_px = open_px + (0.1 if i % 2 == 0 else -0.05)
        rows.append(
            {
                "ts": _ts(i),
                "open": open_px,
                "high": max(open_px, close_px) + 0.2,
                "low": min(open_px, close_px) - 0.15,
                "close": close_px,
                "volume": float(i + 1),
                "symbol": symbol,
            }
        )
    _write_symbol_parquet(dataset_dir / "symbols" / f"{symbol}.parquet", rows)


def _write_config(path: Path, *, with_instrument: bool) -> None:
    cfg: dict[str, object] = {
        "initial_cash": 1000.0,
        "max_leverage": 2.0,
        "signal_delay_bars": 1,
        "risk": {"max_positions": 1, "risk_per_trade_pct": 0.001},
        "strategy": {"name": "coinflip", "seed": 3, "p_trade": 0.0, "cooldown_bars": 0},
        "data": {"mode": "streaming", "symbols_subset": ["BTCUSDT"]},
        "execution": {"profile": "tier2"},
        "benchmark": {"enabled": False},
    }
    if with_instrument:
        cfg["instrument"] = {"type": "crypto", "symbol": "BTCUSDT"}
    path.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")


def _final_equity(path: Path) -> float:
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows
    return float(rows[-1]["equity"])


def test_instrument_spec_validation() -> None:
    InstrumentSpec(type="crypto", symbol="BTCUSDT").validate()
    InstrumentSpec(type="forex", symbol="EURUSD", tick_size=0.00001, contract_size=100000).validate()

    with pytest.raises(ValueError, match="instrument.symbol"):
        InstrumentSpec(type="crypto", symbol="").validate()

    with pytest.raises(ValueError, match="instrument.tick_size.*0"):
        InstrumentSpec(type="crypto", symbol="BTCUSDT", tick_size=0).validate()

    with pytest.raises(ValueError, match="instrument.contract_size.*-1"):
        InstrumentSpec(type="forex", symbol="EURUSD", contract_size=-1).validate()


def test_resolve_instrument_spec_behavior() -> None:
    assert resolve_instrument_spec({"strategy": {"name": "coinflip"}}) is None

    resolved = resolve_instrument_spec({"instrument": {"type": "crypto"}}, symbol="BTCUSDT")
    assert resolved is not None
    assert resolved.symbol == "BTCUSDT"
    assert resolved.type == "crypto"

    with pytest.raises(ValueError, match="instrument.symbol.*ETHUSDT.*BTCUSDT"):
        resolve_instrument_spec({"instrument": {"type": "crypto", "symbol": "ETHUSDT"}}, symbol="BTCUSDT")


def test_resolve_config_preserves_instrument_mapping() -> None:
    resolved = resolve_config(
        {
            "strategy": {"name": "coinflip"},
            "risk": {"risk_per_trade_pct": 0.001},
            "instrument": {"symbol": "BTCUSDT"},
        }
    )
    instrument = resolved.get("instrument")
    assert isinstance(instrument, dict)
    assert instrument["type"] == "crypto"
    assert instrument["symbol"] == "BTCUSDT"

    resolved_without = resolve_config({"strategy": {"name": "coinflip"}, "risk": {"risk_per_trade_pct": 0.001}})
    assert "instrument" not in resolved_without
    assert resolved_without["data"]["mode"] == "streaming"


def test_crypto_no_behavior_change_with_optional_instrument(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    _write_dataset(dataset_dir)

    cfg_no_instrument = tmp_path / "engine_no_instrument.yaml"
    cfg_with_instrument = tmp_path / "engine_with_instrument.yaml"
    _write_config(cfg_no_instrument, with_instrument=False)
    _write_config(cfg_with_instrument, with_instrument=True)

    run_a = Path(
        run_backtest(
            config_path=str(cfg_no_instrument),
            data_path=str(dataset_dir),
            out_dir=str(tmp_path / "out_a"),
            run_name="no-instrument",
        )
    )
    run_b = Path(
        run_backtest(
            config_path=str(cfg_with_instrument),
            data_path=str(dataset_dir),
            out_dir=str(tmp_path / "out_b"),
            run_name="with-instrument",
        )
    )

    for artifact in ["equity.csv", "trades.csv", "performance.json", "config_used.yaml"]:
        assert (run_a / artifact).exists()
        assert (run_b / artifact).exists()

    assert _final_equity(run_a / "equity.csv") == pytest.approx(_final_equity(run_b / "equity.csv"), abs=1e-12)
