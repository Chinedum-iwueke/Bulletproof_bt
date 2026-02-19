from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

from bt.risk.reject_codes import RISK_FALLBACK_LEGACY_PROXY, RISK_REJECT_STOP_MISSING, RISK_REJECT_STOP_UNRESOLVABLE


_STOP_CONTRACT_VERSION = 1
_COUNT_KEYS: tuple[str, ...] = (
    "resolved_explicit",
    "resolved_structural",
    "resolved_atr",
    "resolved_hybrid",
    "fallback_legacy_proxy",
    "rejected_missing_stop",
    "rejected_unresolvable_stop",
    "rejected_invalid_stop_payload",
)


@dataclass(frozen=True)
class StopContractReport:
    version: int
    mode: str
    allow_legacy_proxy: bool
    counts: dict[str, int]
    notes: list[str]


def _initialize_counts() -> dict[str, int]:
    return {key: 0 for key in _COUNT_KEYS}


def _extract_reason_code(payload: dict[str, Any], metadata: dict[str, Any] | None) -> str:
    candidates: list[Any] = []
    if metadata is not None:
        candidates.extend(
            [
                metadata.get("stop_reason_code"),
                metadata.get("reason_code"),
            ]
        )
    candidates.extend([payload.get("reason_code"), payload.get("reason")])
    for candidate in candidates:
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return ""


def _count_resolved_source(stop_source: str, counts: dict[str, int]) -> None:
    if stop_source == "explicit_stop_price":
        counts["resolved_explicit"] += 1
    elif stop_source == "atr_multiple":
        counts["resolved_atr"] += 1
    elif stop_source == "legacy_high_low_proxy":
        # legacy proxy gets surfaced as fallback count only
        return
    elif "hybrid" in stop_source:
        counts["resolved_hybrid"] += 1
    elif stop_source and stop_source != "unresolved":
        counts["resolved_structural"] += 1


def _count_rejections(reason_code: str, counts: dict[str, int]) -> None:
    if not reason_code:
        return

    normalized = reason_code.lower()
    if RISK_REJECT_STOP_MISSING in normalized or "risk_reject:stop_missing" in normalized or "missing stop" in normalized:
        counts["rejected_missing_stop"] += 1
        return
    if RISK_REJECT_STOP_UNRESOLVABLE in normalized or "risk_reject:stop_unresolvable" in normalized or "stop_unresolvable" in normalized or "unresolvable stop" in normalized:
        counts["rejected_unresolvable_stop"] += 1
        return
    if "invalid_stop" in normalized or "invalid stop" in normalized:
        counts["rejected_invalid_stop_payload"] += 1


def _build_notes(mode: str, allow_legacy_proxy: bool, counts: dict[str, int]) -> list[str]:
    notes: list[str] = []
    if mode == "strict":
        notes.append("Strict mode requires entry signals to provide stop_price or metadata.stop_spec.")
    if counts["fallback_legacy_proxy"] > 0:
        notes.append("Legacy proxy fallback was used for entries with unresolved stop inputs.")
    if not allow_legacy_proxy and counts["rejected_missing_stop"] > 0:
        notes.append("Missing-stop entries were rejected; provide stop_price or metadata.stop_spec.")
    return notes


def build_stop_contract_report(
    *,
    config: dict[str, Any],
    decisions_path: Path | None,
) -> StopContractReport | None:
    """
    Returns a StopContractReport if stop-resolution keys exist in config (risk.*),
    else None. Must be deterministic and fast.
    """
    risk_cfg = config.get("risk") if isinstance(config, dict) else None
    if not isinstance(risk_cfg, dict):
        return None

    if "stop_resolution" not in risk_cfg and "allow_legacy_proxy" not in risk_cfg:
        return None

    mode = str(risk_cfg.get("stop_resolution", "safe"))
    allow_legacy_proxy = bool(risk_cfg.get("allow_legacy_proxy", False))
    counts = _initialize_counts()

    if decisions_path is not None and decisions_path.exists():
        try:
            with decisions_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    raw = line.strip()
                    if not raw:
                        continue
                    try:
                        payload = json.loads(raw)
                    except json.JSONDecodeError as exc:
                        raise ValueError(f"Invalid decisions JSONL at {decisions_path}: {exc}") from exc
                    if not isinstance(payload, dict):
                        continue

                    order = payload.get("order")
                    metadata = order.get("metadata") if isinstance(order, dict) else None
                    if not isinstance(metadata, dict):
                        metadata = None

                    stop_source = metadata.get("stop_source") if metadata is not None else None
                    if isinstance(stop_source, str):
                        _count_resolved_source(stop_source, counts)

                    used_proxy = False
                    if metadata is not None:
                        used_proxy = bool(metadata.get("used_legacy_stop_proxy"))
                        reason_code = _extract_reason_code(payload, metadata)
                    else:
                        reason_code = _extract_reason_code(payload, None)

                    if used_proxy or reason_code == RISK_FALLBACK_LEGACY_PROXY:
                        counts["fallback_legacy_proxy"] += 1

                    _count_rejections(reason_code, counts)
        except OSError as exc:
            raise ValueError(f"Unable to read decisions JSONL at {decisions_path}: {exc}") from exc

    notes = _build_notes(mode, allow_legacy_proxy, counts)
    return StopContractReport(
        version=_STOP_CONTRACT_VERSION,
        mode=mode,
        allow_legacy_proxy=allow_legacy_proxy,
        counts=counts,
        notes=notes,
    )
