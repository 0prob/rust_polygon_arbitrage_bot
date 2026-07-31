---
created: 2026-07-30
updated: 2026-07-30
sources:
  - raw/README.md
  - raw/session-2026-07-30-graph-expansion.md
---

# Routing Graph

Core modules: `src/pipeline/graph.rs`, `graph_cache.rs`, `arena.rs`, `cycle_finder.rs`.

## Concepts

- **Arena** (`StateArena`): index-stable pool/token membership for routing.
- **RoutingGraph**: adjacency of spot-weighted edges (direct + virtual hub legs for multi-token pools).
- **GraphBuildGate**: filters edges using `token_to_matic_rates`, flash liquidity, and optional spoke connectivity.
- **Eligible / active / cycle_capable**: successive filters from tradable state → live edges → cycles that can close economically.

## Growth modes

| Mode | When | Notes |
|------|------|-------|
| Full rebuild | Interval, shrink/reorder, empty cache | Costly; uses [[lf-pass]] rebuild path |
| `attach_missing` | Eligibility growth | Capped by `ATTACH_BATCH_CAP` (768); catchup flag until drained |
| Dirty rescore | State generation change | Updates edge weights without full DFS |

`GraphCache` prefers attach on pure membership growth; full rebuild on interval or structural shrink.

## Cycle search

Default **Hybrid** (parallel DFS + Bellman-Ford). Env aliases include `dfs`, `bellman-ford` / `johnson`. Enumeration path cap: `ROUTING_ENUMERATION_MAX_PATHS`.

## Ops metrics (log lines)

- `lf tick: … arena_pools=N, graph_pools=M, …`
- `graph rebuild` / `graph patch_attach` / `graph cache_hit`
- `attach_missing catchup: attached=… hit_cap=… missing_after=…`

See [[graph-poolset-expansion]] for expansion campaign results.

## Related

- [[state-refresh]]
- [[protocols]]
- [[oracle-pricing]]
