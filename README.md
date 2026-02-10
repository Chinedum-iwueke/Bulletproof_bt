# Bulletproof BT

Scaffold for an event-driven, bar-by-bar backtesting engine.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
python scripts/run_backtest.py --help
```

## Streaming indicator library

All indicators are stateful and updated bar-by-bar (`update(bar)`), with explicit warmups (`warmup_bars`) and no lookahead.

### Trend / Moving averages
- EMA, SMA, WMA, DEMA, TEMA, HMA
- KAMA, RMA, VWMA, T3

### Momentum / Oscillators
- RSI, Stochastic, Stoch RSI
- CCI, ROC, Momentum, Williams %R
- TSI, Ultimate Oscillator, Fisher Transform

### Volatility / Bands / Channels
- True Range, ATR
- Bollinger Bands, Keltner Channel, Donchian Channel
- Choppiness Index, Ulcer Index, Historical Volatility

### Trend strength / directional movement
- DMI/ADX, Aroon
- MACD, PPO, TRIX, Vortex

### Volume / Money flow
- OBV, CMF, MFI
- VPT, ADL, Chaikin Oscillator, Force Index

### Range / price-action / stops
- Parabolic SAR, Supertrend
- Pivot Points (streaming daily UTC session pivots)
- Heikin Ashi

### Candle features
- Body/range/wicks/body ratio
- Gap, close position in range
- Rolling z-scores for returns/range/volume

### Usage

```python
from bt.indicators import make_indicator

ind = make_indicator("rsi", period=14)
for bar in bars:
    ind.update(bar)
    if ind.is_ready:
        print(ind.value)
```
