"""Retry and rate-limit helpers for fetch orchestration."""
from __future__ import annotations

import random
import time
import urllib.error
from dataclasses import dataclass
from threading import Lock
from typing import Callable, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 5
    base_delay_seconds: float = 0.5
    max_delay_seconds: float = 30.0
    jitter_seconds: float = 0.25


@dataclass
class RetryResult:
    value: object
    retry_count: int


class ExchangeRateLimiter:
    """Simple process-local minimum-interval limiter."""

    def __init__(self, min_interval_seconds: float = 0.05) -> None:
        self.min_interval_seconds = min_interval_seconds
        self._last_call = 0.0
        self._lock = Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait_for = self.min_interval_seconds - (now - self._last_call)
            if wait_for > 0:
                time.sleep(wait_for)
            self._last_call = time.monotonic()


def is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code == 429 or 500 <= exc.code <= 599
    if isinstance(exc, (urllib.error.URLError, TimeoutError, ConnectionError)):
        return True
    message = str(exc).lower()
    return "429" in message or "too many requests" in message or "5xx" in message


def call_with_retry(fn: Callable[[], T], policy: RetryPolicy, limiter: ExchangeRateLimiter | None = None) -> tuple[T, int]:
    last_error: BaseException | None = None
    for attempt in range(policy.max_attempts):
        if limiter is not None:
            limiter.wait()
        try:
            return fn(), attempt
        except BaseException as exc:
            last_error = exc
            if attempt + 1 >= policy.max_attempts or not is_retryable(exc):
                raise
            delay = min(policy.max_delay_seconds, policy.base_delay_seconds * (2**attempt))
            delay += random.uniform(0.0, policy.jitter_seconds)
            time.sleep(delay)
    raise RuntimeError("retry loop exited unexpectedly") from last_error
