from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(8192)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def build_output_hashes(run_dir: Path) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for name in ["decisions.jsonl", "fills.jsonl", "trades.csv", "equity.csv"]:
        path = run_dir / name
        if path.exists():
            hashes[name] = file_sha256(path)
    return hashes


def compare_hashes(a: dict[str, str], b: dict[str, str]) -> dict[str, Any]:
    mismatches = {}
    for key in sorted(set(a) | set(b)):
        if a.get(key) != b.get(key):
            mismatches[key] = {"first": a.get(key), "second": b.get(key)}
    return {"passed": len(mismatches) == 0, "mismatches": mismatches}
