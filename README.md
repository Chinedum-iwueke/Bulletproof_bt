# Bulletproof BT

Scaffold for an event-driven, bar-by-bar backtesting engine.

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest -q
```

## Run a backtest (CLI)

```bash
python scripts/run_backtest.py --data <PATH> --config configs/engine.yaml
```

## Run an experiment grid (CLI)

```bash
python scripts/run_experiment_grid.py --config configs/engine.yaml --experiment configs/experiments/h1_volfloor_donchian.yaml --data <PATH> --out <OUT_DIR>
```

## Overrides (recommended workflow)

- Add one or more overlays with `--override path/to/override.yaml` (flag is repeatable).
- For local-only edits, use `--local-config configs/local/engine.local.yaml`.
- Effective merge order is:
  1. base config (`--config`)
  2. `configs/fees.yaml`
  3. `configs/slippage.yaml`
  4. each `--override` in the order provided
  5. `--local-config` (if supplied)

## Public API

```python
from bt import run_backtest, run_grid

run_dir = run_backtest(
    config_path="configs/engine.yaml",
    data_path="data/curated/sample.csv",
    out_dir="outputs/runs",
)

experiment_dir = run_grid(
    config_path="configs/engine.yaml",
    experiment_path="configs/experiments/h1_volfloor_donchian.yaml",
    data_path="data/curated/sample.csv",
    out_dir="outputs/experiments",
)
```

## How to add a strategy

1. Copy `src/bt/strategy/templates/client_strategy_template.py`.
2. Rename class/file and place your strategy in `src/bt/strategy/`.
3. Register it with `register_strategy(...)`.
4. Strategy must emit `Signal` objects only.
5. `ctx` is read-only (`StrategyContextView`).

### DO NOT

- Do **not** edit `bt/core/engine.py`.
- Do **not** mutate `ctx`.
- Do **not** access portfolio/execution internals from a strategy.

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
