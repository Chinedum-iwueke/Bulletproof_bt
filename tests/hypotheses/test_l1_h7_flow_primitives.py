from bt.core.types import Bar
from bt.hypotheses.l1_h7 import RollingZScore, flow_gate_passes, imbalance_proxy_from_bar, resolve_imbalance_threshold
import pandas as pd


def _bar(o: float, h: float, l: float, c: float) -> Bar:
    return Bar(ts=pd.Timestamp("2024-01-01", tz="UTC"), symbol="BTCUSDT", open=o, high=h, low=l, close=c, volume=1)


def test_rolling_zscore_warmup_then_value() -> None:
    z = RollingZScore(3)
    assert z.update(1.0) is None
    assert z.update(2.0) is None
    assert z.update(3.0) is None
    val = z.update(4.0)
    assert val is not None


def test_imbalance_proxy_bounds() -> None:
    v = imbalance_proxy_from_bar(_bar(1.0, 2.0, 0.0, 2.0))
    assert v is not None and -1.0 <= v <= 1.0


def test_flow_gate_off_and_strict() -> None:
    assert flow_gate_passes(gate_mode="off", sigma_z=None, sigma_z_threshold=1.0, spread_proxy=None, spread_ref=None, imbalance=None, imbalance_min=0.2)
    assert not flow_gate_passes(gate_mode="strict", sigma_z=1.0, sigma_z_threshold=1.0, spread_proxy=1.0, spread_ref=1.0, imbalance=0.1, imbalance_min=resolve_imbalance_threshold("strict"))
