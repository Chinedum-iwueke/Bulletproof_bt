from __future__ import annotations

from bt.core.types import Bar
from bt.indicators._helpers import StreamingEMA
from bt.indicators.base import MultiValueIndicator, safe_div
from bt.indicators.registry import register


@register("ppo")
class PPO(MultiValueIndicator):
    def __init__(self, fast: int = 12, slow: int = 26, signal: int = 9) -> None:
        super().__init__(name="ppo", warmup_bars=slow + signal, primary_key="ppo")
        self._fast = StreamingEMA(fast)
        self._slow = StreamingEMA(slow)
        self._signal = StreamingEMA(signal)
        self._values = {"ppo": None, "signal": None, "histogram": None}

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        f = self._fast.update(bar.close)
        s = self._slow.update(bar.close)
        if f is None or s is None:
            return
        ppo = 100 * safe_div(f - s, s)
        sig = self._signal.update(ppo)
        self._values = {"ppo": ppo, "signal": sig, "histogram": (ppo - sig) if sig is not None else None}

    def reset(self) -> None:
        self._bars_seen = 0
        self._fast.reset(); self._slow.reset(); self._signal.reset()
        self._values = {"ppo": None, "signal": None, "histogram": None}
