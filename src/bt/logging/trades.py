"""Trade lifecycle logging utilities."""
from __future__ import annotations

import csv
import datetime as dt
import json
from enum import Enum
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from bt.data.config_utils import parse_date_range
from bt.data.dataset import load_dataset_manifest
from bt.core.types import Trade
from bt.risk.r_multiple import compute_r_multiple


def make_run_id(prefix: str = "run") -> str:
    """Return e.g. run_20260117_130501 (UTC)."""
    now = dt.datetime.now(dt.timezone.utc)
    return f"{prefix}_{now:%Y%m%d_%H%M%S}"


def prepare_run_dir(base_dir: Path, run_id: str) -> Path:
    """Create outputs/runs/<run_id>/ and return path."""
    run_dir = base_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def write_config_used(run_dir: Path, config: dict[str, Any]) -> None:
    """Write config_used.yaml."""
    path = run_dir / "config_used.yaml"
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(config, handle, sort_keys=False)


def write_data_scope(run_dir: Path, *, config: dict, dataset_dir: str | None = None) -> None:
    """
    Write data_scope.json into run_dir if any scope-reducing knobs are active.
    This is metadata only: does not affect engine results.

    Knobs considered "scope-reducing":
      - data.symbols_subset
      - data.max_symbols
      - data.date_range
      - data.row_limit_per_symbol

    chunksize is NOT scope-reducing (perf-only) and should not trigger writing.
    """
    data_cfg = config.get("data", {}) if isinstance(config, dict) else {}
    if not isinstance(data_cfg, dict):
        data_cfg = {}

    requested_subset = data_cfg.get("symbols_subset")
    requested_max_symbols = data_cfg.get("max_symbols")
    requested_date_range = data_cfg.get("date_range")
    requested_row_limit = data_cfg.get("row_limit_per_symbol")

    has_scope_knob = any(
        (
            requested_subset not in (None, []),
            requested_max_symbols is not None,
            requested_date_range not in (None, {}),
            requested_row_limit is not None,
        )
    )
    if not has_scope_knob:
        return

    payload: dict[str, Any] = {}
    if "mode" in data_cfg:
        payload["mode"] = data_cfg.get("mode")
    if requested_subset is not None:
        payload["requested_symbols_subset"] = requested_subset
    if requested_max_symbols is not None:
        payload["requested_max_symbols"] = requested_max_symbols
    if requested_row_limit is not None:
        payload["row_limit_per_symbol"] = requested_row_limit

    parsed_range = parse_date_range(config)
    if parsed_range is not None:
        payload["date_range"] = {
            "start": parsed_range[0].isoformat(),
            "end": parsed_range[1].isoformat(),
        }

    if dataset_dir is not None:
        try:
            manifest = load_dataset_manifest(dataset_dir, config)
        except Exception as exc:
            raise ValueError(
                f"Failed to compute effective symbols for dataset_dir='{dataset_dir}': {exc}"
            ) from exc
        payload["effective_symbols"] = manifest.symbols
        payload["effective_symbol_count"] = len(manifest.symbols)

    path = run_dir / "data_scope.json"
    path.write_text(json.dumps(payload, sort_keys=True, indent=2), encoding="utf-8")


class TradesCsvWriter:
    _columns = [
        "entry_ts",
        "exit_ts",
        "symbol",
        "side",
        "qty",
        "entry_price",
        "exit_price",
        "pnl",
        "fees",
        "slippage",
        "mae_price",
        "mfe_price",
        "risk_amount",
        "stop_distance",
        "r_multiple_gross",
        "r_multiple_net",
    ]

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._path = path
        file_exists = path.exists()
        self._file = path.open("a", encoding="utf-8", newline="")
        self._writer = csv.writer(self._file)
        if not file_exists or path.stat().st_size == 0:
            self._writer.writerow(self._columns)
            self._file.flush()

    def _serialize_value(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        if isinstance(value, Enum):
            return value.name
        return str(value)

    def write_trade(self, trade: Trade) -> None:
        """Append one trade row."""
        metadata = trade.metadata if isinstance(trade.metadata, dict) else {}
        risk_amount = metadata.get("risk_amount")
        stop_distance = metadata.get("stop_distance")

        pnl_net = trade.pnl
        pnl_gross = trade.pnl + trade.fees + trade.slippage

        computed_values: dict[str, Any] = {
            "risk_amount": risk_amount,
            "stop_distance": stop_distance,
            "r_multiple_gross": compute_r_multiple(pnl_gross, risk_amount),
            "r_multiple_net": compute_r_multiple(pnl_net, risk_amount),
        }

        row: list[str] = []
        for column in self._columns:
            if column in computed_values:
                value = computed_values[column]
            else:
                value = getattr(trade, column, "")  # TODO: populate when Trade adds field.
            row.append(self._serialize_value(value))
        self._writer.writerow(row)
        self._file.flush()

    def close(self) -> None:
        if not self._file.closed:
            self._file.close()
