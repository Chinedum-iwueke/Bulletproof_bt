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


## Streaming acceleration knobs

When using dataset-directory streaming mode, you can cheaply reduce scope for smoke tests and debugging:

- `data.symbols_subset`: explicit symbol allow-list.
- `data.max_symbols`: cap the selected symbol list after subset filtering.
- `data.date_range`: UTC window filter applied per symbol (`start` inclusive, `end` exclusive).
- `data.row_limit_per_symbol`: maximum emitted rows per symbol.
- `data.chunksize`: parquet/csv chunk batch size used by each per-symbol source.

Quick smoke-test example:

```yaml
data:
  mode: streaming
  symbols_subset: [AAA, BBB]
  max_symbols: 2
  date_range:
    start: "2025-01-01T00:00:00Z"
    end: "2025-01-02T00:00:00Z"
  row_limit_per_symbol: 100
  chunksize: 1000
```
