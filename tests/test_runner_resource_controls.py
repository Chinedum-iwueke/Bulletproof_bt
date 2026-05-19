from __future__ import annotations

from bt.experiments.resource_controls import MemorySnapshot, resolve_auto_workers


def test_auto_workers_respects_ram_reservation_and_user_cap() -> None:
    workers = resolve_auto_workers(
        requested_max_workers=12,
        use_auto=True,
        reserve_ram_gb=8,
        estimated_ram_per_worker_gb=4,
        cpu_count=36,
        snapshot=MemorySnapshot(available_gb=28, total_gb=64, used_gb=36, source="test"),
    )
    assert workers == 5


def test_auto_workers_never_exceeds_requested_or_cpu_bound() -> None:
    workers = resolve_auto_workers(
        requested_max_workers=20,
        use_auto=True,
        reserve_ram_gb=0,
        estimated_ram_per_worker_gb=1,
        cpu_count=6,
        snapshot=MemorySnapshot(available_gb=64, total_gb=64, used_gb=0, source="test"),
    )
    assert workers == 4


def test_manual_workers_are_unchanged() -> None:
    assert resolve_auto_workers(requested_max_workers=8, use_auto=False) == 8
