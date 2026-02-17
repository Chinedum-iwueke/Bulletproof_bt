from __future__ import annotations

import csv
import json
import math
from pathlib import Path
from typing import Any


def count_jsonl_lines(path: Path) -> int:
    if not path.exists():
        return 0
    count = 0
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                count += 1
    return count


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for idx, line in enumerate(handle, start=1):
            text = line.strip()
            if not text:
                continue
            payload = json.loads(text)
            if not isinstance(payload, dict):
                raise AssertionError(f"Expected JSON object at {path}:{idx}, got {type(payload).__name__}")
            rows.append(payload)
    return rows


def first_forbidden_token(run_dir: Path, tokens: list[str]) -> tuple[Path, int, str, str] | None:
    text_suffixes = {".json", ".jsonl", ".yaml", ".yml", ".txt", ".csv"}
    for file_path in sorted(p for p in run_dir.rglob("*") if p.is_file() and p.suffix.lower() in text_suffixes):
        with file_path.open("r", encoding="utf-8", errors="replace") as handle:
            for line_no, line in enumerate(handle, start=1):
                for token in tokens:
                    if token in line:
                        return file_path, line_no, token, line.rstrip("\n")
    return None


def parse_equity_values(equity_path: Path) -> list[float]:
    candidates = ["equity", "portfolio_equity", "equity_curve", "total_equity", "value"]
    with equity_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise AssertionError(f"{equity_path} has no header row")

        chosen: str | None = next((name for name in candidates if name in reader.fieldnames), None)
        if chosen is None:
            for field in reader.fieldnames:
                lowered = field.lower()
                if lowered in {"ts", "timestamp", "datetime", "date"}:
                    continue
                chosen = field
                break
        if chosen is None:
            raise AssertionError(
                f"Could not find an equity-like column in {equity_path}. "
                f"Available columns: {reader.fieldnames}"
            )

        values: list[float] = []
        for row_idx, row in enumerate(reader, start=2):
            raw = row.get(chosen)
            if raw is None or str(raw).strip() == "":
                raise AssertionError(f"Missing equity value at {equity_path}:{row_idx} column={chosen!r}")
            try:
                value = float(raw)
            except ValueError as exc:
                raise AssertionError(
                    f"Non-numeric equity value at {equity_path}:{row_idx} column={chosen!r} value={raw!r}"
                ) from exc
            if not math.isfinite(value):
                raise AssertionError(
                    f"Non-finite equity value at {equity_path}:{row_idx} column={chosen!r} value={value!r}"
                )
            values.append(value)
    return values


def format_sanity_headline(sanity: dict[str, Any]) -> str:
    keys = [
        "run_id",
        "signals_emitted",
        "signals_approved",
        "signals_rejected",
        "fills",
        "forced_liquidations",
    ]
    parts = [f"{key}={sanity.get(key)!r}" for key in keys]
    return ", ".join(parts)
