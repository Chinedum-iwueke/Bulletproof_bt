# Research Trade Logging Schema (Phase 9)

Every trade row should preserve six layers: (1) structural state, (2) decision rationale, (3) execution reality, (4) realized path, (5) counterfactual outcomes, and (6) ML/meta labels.

Required prefixes:
- `identity_*`
- `entry_state_*`
- `entry_gate_*`
- `entry_decision_*`
- `execution_*`
- `risk_*`
- `path_*`
- `exit_*`
- `counterfactual_*`
- `label_*`

Strategies can attach `decision_trace` via signal metadata. The engine/trade writer flattens this into `entry_decision_*` and `entry_gate_*` columns. Missing fields remain `None` (additive, backward compatible).

## Enriched Derivatives State Features

The engine enriches signal metadata with the current causal state snapshot before orders are created. The trade writer preserves every exportable `entry_state_*` key dynamically, so strategies do not need to know about funding/OI/basis fields.

When enriched data is present, trade rows can include funding, OI, mark/index, basis/premium, crowding/stress, and CSI component fields. OHLCV-only datasets still produce the core `entry_state_*` OHLCV fields and `entry_state_csi_source = "ohlcv_proxy"`.

Post-run enrichment must not overwrite source `entry_state_*` fields. Extraction preserves all `entry_state_`, `entry_decision_`, `execution_`, `risk_`, `path_`, `counterfactual_`, `label_`, and `identity_` prefixed columns.
