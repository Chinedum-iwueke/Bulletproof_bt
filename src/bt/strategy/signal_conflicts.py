"""Deterministic per-(ts, symbol) signal conflict resolution."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from bt.core.enums import Side
from bt.core.types import Signal


_VALID_POLICIES = {"reject", "first_wins", "last_wins", "net_out"}


@dataclass(frozen=True)
class SignalConflictSummary:
    ts_iso: str
    symbol: str
    policy: str
    kept: Signal | None
    dropped_count: int
    reason: str


def _is_exit_like(signal: Signal) -> bool:
    metadata = signal.metadata if isinstance(signal.metadata, dict) else {}
    if bool(metadata.get("is_exit")) or bool(metadata.get("reduce_only")):
        return True
    return str(signal.signal_type).endswith("_exit")


def _entry_side(signal: Signal) -> Side | None:
    if _is_exit_like(signal):
        return None
    if signal.side in {Side.BUY, Side.SELL}:
        return signal.side
    return None


def _group_compact_summary(signals: Iterable[Signal]) -> str:
    return ", ".join(f"{signal.signal_type}:{signal.side}" for signal in signals)


def _build_summary(
    *,
    ts_iso: str,
    symbol: str,
    policy: str,
    kept: Signal | None,
    group_size: int,
    reason: str,
) -> SignalConflictSummary:
    dropped_count = group_size if kept is None else group_size - 1
    return SignalConflictSummary(
        ts_iso=ts_iso,
        symbol=symbol,
        policy=policy,
        kept=kept,
        dropped_count=dropped_count,
        reason=reason,
    )


def resolve_signal_conflicts(
    signals: list[Signal],
    *,
    policy: str,
) -> tuple[list[Signal], list[SignalConflictSummary]]:
    """
    Enforce per-(ts,symbol) conflict policy and return (resolved_signals, summaries).
    Must be deterministic and stable.
    """
    if policy not in _VALID_POLICIES:
        allowed = "|".join(sorted(_VALID_POLICIES))
        raise ValueError(f"Invalid strategy.signal_conflict_policy={policy!r}; expected one of {allowed}")

    grouped: dict[tuple[object, str], list[tuple[int, Signal]]] = {}
    for index, signal in enumerate(signals):
        grouped.setdefault((signal.ts, signal.symbol), []).append((index, signal))

    keep_indices: set[int] = set()
    summaries: list[SignalConflictSummary] = []

    for (ts, symbol), indexed_group in grouped.items():
        if len(indexed_group) == 1:
            keep_indices.add(indexed_group[0][0])
            continue

        group_signals = [signal for _, signal in indexed_group]
        ts_iso = ts.isoformat()
        if policy == "reject":
            involved = _group_compact_summary(group_signals)
            raise ValueError(
                f"Signal conflict at ts={ts_iso} symbol={symbol}: got {len(group_signals)} signals "
                f"[{involved}]. Set strategy.signal_conflict_policy to one of reject|first_wins|last_wins|net_out"
            )

        if policy == "first_wins":
            keep_idx, keep_signal = indexed_group[0]
            keep_indices.add(keep_idx)
            summaries.append(
                _build_summary(
                    ts_iso=ts_iso,
                    symbol=symbol,
                    policy=policy,
                    kept=keep_signal,
                    group_size=len(indexed_group),
                    reason="kept first emitted signal",
                )
            )
            continue

        if policy == "last_wins":
            keep_idx, keep_signal = indexed_group[-1]
            keep_indices.add(keep_idx)
            summaries.append(
                _build_summary(
                    ts_iso=ts_iso,
                    symbol=symbol,
                    policy=policy,
                    kept=keep_signal,
                    group_size=len(indexed_group),
                    reason="kept last emitted signal",
                )
            )
            continue

        exits = [(idx, signal) for idx, signal in indexed_group if _is_exit_like(signal)]
        entries = [(idx, signal) for idx, signal in indexed_group if not _is_exit_like(signal)]

        if exits:
            keep_idx, keep_signal = exits[-1]
            keep_indices.add(keep_idx)
            summaries.append(
                _build_summary(
                    ts_iso=ts_iso,
                    symbol=symbol,
                    policy=policy,
                    kept=keep_signal,
                    group_size=len(indexed_group),
                    reason="exit wins; kept last exit-like signal",
                )
            )
            continue

        entry_sides = {_entry_side(signal) for _, signal in entries}
        if Side.BUY in entry_sides and Side.SELL in entry_sides:
            summaries.append(
                _build_summary(
                    ts_iso=ts_iso,
                    symbol=symbol,
                    policy=policy,
                    kept=None,
                    group_size=len(indexed_group),
                    reason="opposite entry sides netted to no-op",
                )
            )
            continue

        keep_idx, keep_signal = entries[-1]
        keep_indices.add(keep_idx)
        summaries.append(
            _build_summary(
                ts_iso=ts_iso,
                symbol=symbol,
                policy=policy,
                kept=keep_signal,
                group_size=len(indexed_group),
                reason="kept last entry-like signal",
            )
        )

    resolved = [signal for index, signal in enumerate(signals) if index in keep_indices]
    return resolved, summaries

