"""Feed-construction router for data inputs."""
from __future__ import annotations

import os
from typing import Any

from bt.data.feed import HistoricalDataFeed
from bt.data.loader import load_dataset


def _resolve_mode(config: dict[str, Any]) -> str | None:
    data_cfg = config.get("data", {})
    if not isinstance(data_cfg, dict):
        raise ValueError("config.data must be a mapping when provided")

    mode = data_cfg.get("mode", None)
    if mode is None:
        return None
    if mode not in {"streaming", "dataframe"}:
        raise ValueError(f"data.mode must be one of: streaming, dataframe (got: {mode})")
    return mode


def load_feed(data_path: str, config: dict[str, Any]):
    """Construct and return the engine feed for ``data_path``."""
    mode = _resolve_mode(config)

    if os.path.isdir(data_path):
        effective_mode = mode or "streaming"
        if effective_mode == "streaming":
            raise NotImplementedError(
                "Streaming dataset directory feed is not implemented yet (Stage B Task 4/5)."
            )
        raise NotImplementedError(
            "Dataset directories are not supported in dataframe mode. "
            "Use data.mode=streaming (Stage B) or pass a single .csv/.parquet file."
        )

    if os.path.isfile(data_path):
        _, extension = os.path.splitext(data_path)
        if extension not in {".csv", ".parquet"}:
            raise ValueError(f"Unsupported data file extension: {extension}")

        effective_mode = mode or "dataframe"
        if effective_mode == "streaming":
            raise NotImplementedError(
                "Streaming single-file feed not implemented in Task 1; use data.mode=dataframe for now."
            )

        bars_df = load_dataset(data_path)
        return HistoricalDataFeed(bars_df)

    raise ValueError(f"Data path not found: {data_path}")

