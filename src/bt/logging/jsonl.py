"""JSONL logging utilities."""
from __future__ import annotations

from dataclasses import asdict, is_dataclass
from enum import Enum
import json
from pathlib import Path
from typing import Any

import pandas as pd


def to_jsonable(obj: Any) -> Any:
    """Convert Python objects into JSON-serializable equivalents."""
    if is_dataclass(obj):
        return to_jsonable(asdict(obj))
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    if isinstance(obj, Enum):
        return obj.name
    if isinstance(obj, dict):
        return {key: to_jsonable(value) for key, value in obj.items()}
    if isinstance(obj, list):
        return [to_jsonable(value) for value in obj]
    if isinstance(obj, (str, float, int, bool)) or obj is None:
        return obj
    return str(obj)


class JsonlWriter:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._path = path
        self._file = path.open("a", encoding="utf-8")

    def write(self, record: dict[str, Any]) -> None:
        """Append one JSON line."""
        json_record = to_jsonable(record)
        json.dump(json_record, self._file, ensure_ascii=False)
        self._file.write("\n")
        self._file.flush()

    def close(self) -> None:
        if not self._file.closed:
            self._file.close()
