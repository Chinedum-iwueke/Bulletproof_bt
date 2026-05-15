# Research Orchestration Data Mode

The research daemon and parallel hypothesis grid now support the canonical
`research_data/` library directly. Legacy curated folders still work during the
transition.

## Before

```bash
python scripts/run_parallel_hypothesis_grid.py \
  --experiment-root outputs/tier2/l1_h7c_parallel_stable \
  --manifest outputs/tier2/l1_h7c_parallel_stable/manifests/grid.csv \
  --config configs/engine.yaml \
  --local-config configs/local/engine.lab.yaml \
  --data /home/omenka/research_data/bt/curated/stable_data_1m_canonical \
  --max-workers 6 \
  --skip-completed
```

## Stable

```bash
python scripts/run_parallel_hypothesis_grid.py \
  --experiment-root outputs/tier2/l1_h7c_parallel_stable \
  --manifest outputs/tier2/l1_h7c_parallel_stable/manifests/l1_h7c_high_selectivity_regime_tier2_grid.csv \
  --config configs/engine.yaml \
  --local-config configs/local/engine.lab.yaml \
  --data-root research_data \
  --data-kind research_panel \
  --exchange binance \
  --universe stable \
  --timeframe 1m \
  --max-workers 6 \
  --skip-completed
```

Stable mode reads:

```text
research_data/manifests/stable_universe.parquet
research_data/canonical/binance/<SYMBOL>/timeframe=1m/research_panel.parquet
```

## Volatile

```bash
python scripts/run_parallel_hypothesis_grid.py \
  --experiment-root outputs/tier2/l1_h7c_parallel_vol \
  --manifest outputs/tier2/l1_h7c_parallel_vol/manifests/l1_h7c_high_selectivity_regime_tier2_grid.csv \
  --config configs/engine.yaml \
  --local-config configs/local/engine.lab.yaml \
  --data-root research_data \
  --data-kind research_panel \
  --exchange binance \
  --universe volatile \
  --membership-path research_data/manifests/volatile_universe_membership.parquet \
  --timeframe 1m \
  --max-workers 6 \
  --skip-completed
```

Volatile mode reads membership from:

```text
research_data/manifests/volatile_universe_membership.parquet
```

The current engine consumes a static feed per run, so volatile membership is
applied as a safe materialized active-window filter before bars reach strategies.
At timestamp `t`, only rows for symbols active at `t` are emitted. Future
membership is used only by the loader to compute interval end boundaries and is
not exposed through `bar.extra` or any strategy-visible state.

## Preflight

Research-data mode fails before worker launch when:

- `research_data/` is missing
- the stable manifest or volatile membership manifest is missing
- required symbol panel files are missing
- panel timestamps are not UTC
- required OHLCV columns are absent
- `funding_source_ts` or `oi_source_ts` is later than the bar timestamp

Legacy `--data` remains supported and bypasses research-data profile preflight.
