---
created: 2026-07-30
updated: 2026-07-30
sources:
  - raw/README.md
---

# Protocols

Multi-protocol local sim + calldata encoders under `src/core/math/*` and `src/services/execution/calldata/`.

## Families

| Family | Notes |
|--------|-------|
| Uniswap V2 / clones | Optional disable via env; high dust count on Polygon |
| Uniswap V3 / Sushi V3 | TickLens hydration; tickless stuck is common HF filter |
| Uniswap V4 | Hookless via `unlock` / `unlockCallback`; storage-slot state |
| QuickSwap Algebra V3 / Integral / V4 | Algebra tick paths |
| Balancer V2 | Weighted / stable / linear; Vault hub-spoke graph; Direct flash-swap |
| Curve | Stable + crypto; multi-coin hub |
| DODO | PMM; external flash disabled |
| WooFi | Quote + bases |

## Graph modeling

- Pair pools → direct directed edges.
- Multi-token / vault-order pools → virtual hubs + enter/exit legs (`GraphHopPhase`).
- Parallel edges per pair capped (`MAX_PARALLEL_EDGES_PER_PAIR`).

## Related

- [[routing-graph]]
- [[pool-discovery]]
- [[flash-loans]]
