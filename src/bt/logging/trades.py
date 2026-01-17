"""Trade lifecycle logging utilities."""
from __future__ import annotations

import csv
import datetime as dt
from enum import Enum
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from bt.core.types import Trade


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
        row: list[str] = []
        for column in self._columns:
            value = getattr(trade, column, "")  # TODO: populate when Trade adds field.
            row.append(self._serialize_value(value))
        self._writer.writerow(row)
        self._file.flush()

    def close(self) -> None:
        if not self._file.closed:
            self._file.close()
