# Structural EV Bucketing (Phase 9)

`E[R]` on all trades is not enough. We need `E[R | structure]`.

Implemented outputs include per-bucket EV for:
- CSI
- Volatility
- Liquidity
- Displacement
- Setup class
- Joint buckets (CSI×Vol, CSI×Liquidity, Vol×Liquidity)

If required fields are missing, we emit `summaries/ev_by_bucket_missing_fields.json` and skip only affected analyses (never silently collapse to only `all`, except baseline `overall_all_trades`).

## Enriched Derivatives Buckets

When rich state fields are present, structural EV analysis now writes additional files:

- `ev_by_bucket_funding.csv`
- `ev_by_bucket_oi_accel.csv`
- `ev_by_bucket_basis.csv`
- `ev_by_bucket_joint_csi_funding.csv`
- `ev_by_bucket_joint_csi_oi_accel.csv`
- `ev_by_bucket_joint_csi_basis.csv`
- `ev_by_bucket_joint_funding_oi_accel.csv`
- `ev_by_bucket_joint_vol_funding.csv`
- `ev_by_bucket_joint_displacement_oi_accel.csv`

Missing rich fields are reported in `ev_by_bucket_missing_fields.json`; affected analyses are skipped rather than collapsed into misleading all-trade buckets.
