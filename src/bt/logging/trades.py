"""Trade lifecycle logging utilities."""
from __future__ import annotations

import csv
import datetime as dt
from enum import Enum
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from bt.data.config_utils import parse_date_range
from bt.logging.formatting import FLOAT_DECIMALS_CSV, write_json_deterministic
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




def _normalize_requested_symbols(value: Any, *, key_path: str) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"Invalid config: {key_path} must be a non-empty list of strings (got: {value!r})")

    normalized: list[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            raise ValueError(f"Invalid config: {key_path} must be a non-empty list of strings (got: {value!r})")
        symbol = item.strip()
        if not symbol:
            continue
        if symbol not in seen:
            seen.add(symbol)
            normalized.append(symbol)

    if not normalized:
        raise ValueError(f"Invalid config: {key_path} must be a non-empty list of strings (got: {value!r})")

    return normalized


def _resolve_requested_symbols(data_cfg: dict[str, Any]) -> tuple[list[str] | None, Any, Any]:
    requested_subset_raw = data_cfg.get("symbols_subset")
    requested_symbols_raw = data_cfg.get("symbols")

    normalized_subset = (
        None
        if requested_subset_raw is None
        else _normalize_requested_symbols(requested_subset_raw, key_path="data.symbols_subset")
    )
    normalized_symbols = (
        None
        if requested_symbols_raw is None
        else _normalize_requested_symbols(requested_symbols_raw, key_path="data.symbols")
    )

    if normalized_subset is None and normalized_symbols is None:
        return None, requested_subset_raw, requested_symbols_raw
    if normalized_subset is not None and normalized_symbols is not None and normalized_subset != normalized_symbols:
        raise ValueError(
            "Config conflict: data.symbols and data.symbols_subset both set but differ. "
            f"Use only one. data.symbols={requested_symbols_raw!r} data.symbols_subset={requested_subset_raw!r}"
        )
    return (normalized_subset if normalized_subset is not None else normalized_symbols), requested_subset_raw, requested_symbols_raw

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

    requested_symbols, requested_subset_raw, requested_symbols_raw = _resolve_requested_symbols(data_cfg)
    requested_max_symbols = data_cfg.get("max_symbols")
    requested_date_range = data_cfg.get("date_range")
    requested_row_limit = data_cfg.get("row_limit_per_symbol")

    has_scope_knob = any(
        (
            requested_symbols is not None,
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
    if requested_symbols is not None:
        payload["requested_symbols"] = requested_symbols
        if requested_subset_raw is not None:
            payload["requested_symbols_subset"] = requested_subset_raw
        elif requested_symbols_raw is not None:
            payload["requested_symbols_subset"] = requested_symbols_raw
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
    write_json_deterministic(path, payload)


class TradesCsvWriter:
    _columns = [
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
        "mae_price",
        "mfe_price",
        "risk_amount",
        "stop_distance",
        "entry_stop_distance",
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
        if isinstance(value, float):
            return f"{value:.{FLOAT_DECIMALS_CSV}f}"
        return str(value)

    def write_trade(self, trade: Trade) -> None:
        """Append one trade row."""
        metadata = trade.metadata if isinstance(trade.metadata, dict) else {}
        risk_amount = metadata.get("risk_amount")
        stop_distance = metadata.get("stop_distance")
        entry_qty = metadata.get("entry_qty", trade.qty)
        entry_stop_distance = metadata.get("entry_stop_distance", stop_distance)

        pnl_price = trade.pnl
        fees_paid = trade.fees
        pnl_net = pnl_price - fees_paid

        computed_values: dict[str, Any] = {
            "pnl_price": pnl_price,
            "fees_paid": fees_paid,
            "pnl_net": pnl_net,
            "risk_amount": risk_amount,
            "stop_distance": stop_distance,
            "entry_qty": entry_qty,
            "exit_qty": trade.qty,
            "entry_stop_distance": entry_stop_distance,
            "r_multiple_gross": compute_r_multiple(pnl_price, risk_amount),
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
