---
created: 2026-07-30
updated: 2026-07-30
sources:
  - raw/README.md
  - raw/session-2026-07-30-graph-expansion.md
---

# State Refresh

Service: `src/services/state_refresh.rs`. Fills `StateCache` from multi-RPC multicall.

## Selection

- `fetch_missing_pool_states_indexed` rotates through discovered pools with a scan offset.
- Hot addresses prioritized when present.
- Batch size from [[lf-pass]] `lf_refresh_batch` (bootstrap vs hot vs full sweep).
- `MAX_MULTICALL_CALLS` chunks each batch (ops often 250).

## Warm policy

```
warm_cache_target = lf_bootstrap_batch * 6
full_sweep = pass==1 || routable < warm || pass % lf_full_sweep_interval == 0
batch = bootstrap if full_sweep else min(hot, bootstrap)
```

`routable_pool_count` = discovered pools with **tradable** cached state.

## RPC behavior

- Tries state URL candidates; deprioritizes on failure / rate limit ([[rpc-and-rate-limits]]).
- Logs `state refresh overrun` when `refresh_ms >= lf_interval_ms`.
- TickLens hydration for V3/V4 is separate and fragile under pressure.

## Arena sync

`sync_routable_arena_gated` can `freeze_append` while graph attach catch-up runs (attach/arena coupling with [[routing-graph]]).

## Related

- [[pool-discovery]]
- [[graph-poolset-expansion]]
