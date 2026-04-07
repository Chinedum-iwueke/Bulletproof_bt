from __future__ import annotations

from bt.core.types import Bar
from bt.indicators.base import MultiValueIndicator
from bt.indicators.bollinger import BollingerBands
from bt.indicators.keltner import KeltnerChannel
from bt.indicators.registry import register


@register("bb_kc_squeeze")
class BBKCSqueeze(MultiValueIndicator):
    """Streaming BB/KC squeeze state.

    Squeeze is on when Bollinger Bands are fully inside Keltner Channel.
    """

    def __init__(
        self,
        *,
        bb_period: int = 20,
        bb_std_mult: float = 2.0,
        kc_period: int = 20,
        kc_atr_mult: float = 1.5,
    ) -> None:
        warmup = max(int(bb_period), int(kc_period) + 1)
        super().__init__(name="bb_kc_squeeze", warmup_bars=warmup, primary_key="squeeze_on")
        self._bb = BollingerBands(period=bb_period, num_std=bb_std_mult)
        self._kc = KeltnerChannel(period=kc_period, atr_mult=kc_atr_mult)
        self._duration = 0
        self._values = {
            "squeeze_on": None,
            "squeeze_duration": 0,
            "bb_upper": None,
            "bb_lower": None,
            "kc_upper": None,
            "kc_lower": None,
        }

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        self._bb.update(bar)
        self._kc.update(bar)
        bb_upper = self._bb.values.get("upper")
        bb_lower = self._bb.values.get("lower")
        kc_upper = self._kc.values.get("upper")
        kc_lower = self._kc.values.get("lower")
        if None in (bb_upper, bb_lower, kc_upper, kc_lower):
            return

        squeeze_on = bool(bb_upper <= kc_upper and bb_lower >= kc_lower)
        self._duration = self._duration + 1 if squeeze_on else 0
        self._values = {
            "squeeze_on": squeeze_on,
            "squeeze_duration": self._duration,
            "bb_upper": bb_upper,
            "bb_lower": bb_lower,
            "kc_upper": kc_upper,
            "kc_lower": kc_lower,
        }

    def reset(self) -> None:
        self._bars_seen = 0
        self._bb.reset()
        self._kc.reset()
        self._duration = 0
        self._values = {
            "squeeze_on": None,
            "squeeze_duration": 0,
            "bb_upper": None,
            "bb_lower": None,
            "kc_upper": None,
            "kc_lower": None,
        }
