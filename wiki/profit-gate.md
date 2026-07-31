---
created: 2026-07-30
updated: 2026-07-30
sources:
  - raw/README.md
---

# Profit Gate

`assess_profit` is the **execution gate** (shared basis with Brent/probe ranking).

## Formula (conceptual)

1. Simulate full route output (in-memory hop sim).
2. Apply full-route slippage haircut once.
3. Subtract selected flash premium ([[flash-loans]]).
4. Subtract `gas_units × (base_fee + charged priority)`; profit-derived tip only charges **incremental** uplift above that tip.
5. Require positive post-gas net, `max(MIN_PROFIT_MATIC_WEI, $0.01 in POL)`, optional ROI floor, gas safety cover.

## Fail closed

- Missing / invalid POL/USD → reject.
- Collapsed depth (`depth_bps >= 10000`) → reject.
- Unavailable +5% depth probe → 2500-bps haircut from shared depth helper.
- Low-decimal tokens: inputs need ≥1 whole token when decimals ≤ 8.

## Learned risk

`ROUTE_STATS_PATH` (default `.rpbot-route-stats.json`): unreliable routes need proportionally more expected net before preflight.

## Preflight stack (not interchangeable)

1. Local simulation  
2. `queryBatchSwap` + executor `eth_call` for Direct Balancer  
3. Final calldata `eth_call` / gas reassessment before submit  

## Related

- [[hf-pass]]
- [[oracle-pricing]]
- [[execution-path]]
