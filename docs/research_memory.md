# Research Memory

Research memory ingests completed experiment trades, state bucket findings, verdicts, and alpha candidates into SQLite-backed long-term memory.

## Enriched Derivatives State Features

`research_memory_trades` now stores rich state fields when present:

- `funding_raw`, `funding_pctile`, `funding_z`
- `oi_level`, `oi_accel`, `oi_accel_pctile`, `oi_z`
- `mark_price`, `index_price`
- `basis_raw`, `basis_pct`, `basis_pctile`, `premium_pctile`
- `crowding_proxy_pctile`, `constraint_stress_pctile`
- `csi_source`, `csi_components_json`

`similar_state` queries can match on funding, OI acceleration, basis, crowding, and constraint stress in addition to OHLCV CSI/vol/liquidity/displacement.

Recommendations remain proposed-only and require human approval. Rich derivative recommendations include adding funding/OI/basis/constraint gates, avoiding funding extremes, and sizing down derivative fragility.
