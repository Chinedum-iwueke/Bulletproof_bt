from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

FLOAT_DECIMALS_JSON = 12
FLOAT_DECIMALS_CSV = 12


def round_floats(obj: Any, *, decimals: int) -> Any:
    """
    Recursively round floats in dict/list structures to fixed decimals.
    - Must handle nested dict/list/tuple.
    - Must leave ints/strings/bools/None unchanged.
    - Must treat NaN/inf as ValueError (user-facing), because artifacts must be reconstructable.
    """
    if isinstance(obj, bool) or obj is None:
        return obj
    if isinstance(obj, float):
        if not math.isfinite(obj):
            raise ValueError(f"Non-finite float in artifact payload: {obj}")
        return round(obj, decimals)
    if isinstance(obj, int):
        return obj
    if isinstance(obj, dict):
        return {key: round_floats(value, decimals=decimals) for key, value in obj.items()}
    if isinstance(obj, list):
        return [round_floats(value, decimals=decimals) for value in obj]
    if isinstance(obj, tuple):
        return tuple(round_floats(value, decimals=decimals) for value in obj)
    return obj


def write_json_deterministic(path: Path, payload: dict[str, Any]) -> None:
    """
    Write JSON with:
      - sorted keys
      - indent=2
      - UTF-8
      - newline at EOF
      - floats rounded via round_floats(..., FLOAT_DECIMALS_JSON)
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        rounded_payload = round_floats(payload, decimals=FLOAT_DECIMALS_JSON)
    except ValueError as exc:
        message = str(exc)
        marker = "Non-finite float in artifact payload:"
        if message.startswith(marker):
            value = message[len(marker) :].strip()
            raise ValueError(f"Non-finite float in artifact payload for {path}: {value}") from exc
        raise

    with path.open("w", encoding="utf-8") as handle:
        json.dump(rounded_payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def write_text_deterministic(path: Path, text: str) -> None:
    """
    Write UTF-8 text with newline at EOF.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized = text.rstrip("\n") + "\n"
    path.write_text(normalized, encoding="utf-8")
