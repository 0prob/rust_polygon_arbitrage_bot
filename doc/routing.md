# Protocol routing notes

Concise reference for graph construction, cycle search, and simulation. The graph is **directed** and **per-hop**: pool-level admission is not enough — each edge must be routable on its own legs.

Shared rules live in `PoolState::hop_token_funded` / `hop_pair_routable` and `MIN_HOP_TOKEN_BALANCE` (1e15 wei).

---

## Uniswap V2 (and V2-fork pairs)

- **Topology:** exactly 2 tokens → 2 directed edges (`zero_for_one` true/false).
- **Liquidity:** per-leg reserves; both sides must meet the dust threshold or the pool is not tradable and edges are dead.
- **Why:** pair pools are simple, but near-empty reserves create fake negative cycles and waste enumeration budget.

## Uniswap V3 / V4 / Algebra (concentrated liquidity)

- **Topology:** 2 tokens; direction encoded as `zero_for_one` (not `token_in_idx` alone).
- **Liquidity:** pool-scoped `liquidity` + `sqrt_price_x96` + `unlocked`; no per-token balance vector.
- **Graph vs sim:** graph admits the pool when CL state is live; **shallow tick depth** is rejected later in simulation (large trades that do not survive a tick walk).
- **V4:** hookless pools only (`hooks` must be zero).
- **Why:** marginal spot from `sqrt_price` is fine for ranking, but real output depends on tick bitmap — a separate fidelity layer, not graph topology.

## Balancer V2

- **Topology:** n tokens → up to n×(n−1) directed edges; **BPT leg excluded** via `bpt_index`.
- **Liquidity:** per-token vault balances; only pairs where **both** legs are funded get edges (pool can be tradable with ≥2 funded tokens, but must not emit hops through dust legs).
- **Math families:** weighted / stable / linear need different on-chain state; unknown `poolType` must not default to weighted (misquote).
- **Execution:** Balancer hops cost 2 executor calls; pure-Balancer routes can use vault flash liquidity; mixed routes have flash constraints.
- **Why:** multi-token expansion dominates search space if underfunded pairs are included; BPT is not a swap leg.

## Curve (stable & crypto)

- **Topology:** same full pairwise expansion as Balancer (2–8 coins).
- **Liquidity:** per-coin balances gated like Balancer; **all coin rates must be non-zero** for pool tradability.
- **Stable vs crypto:** different math (`CurveStable` vs `CurveCrypto`); indexer `poolType` disambiguates.
- **Why:** Curve pools often list many coins but only a subset is liquid; pairwise gating prevents phantom routes through empty slots.

## Dodo

- **Topology:** 2 tokens (base / quote); `zero_for_one` selects direction.
- **Liquidity:** `base_reserve` and `quote_reserve` both need dust threshold (not merely non-zero).
- **Why:** PMM reserves are asymmetric; a one-sided dust pool should not appear as a bidirectional edge pair.

## Woofi

- **Topology:** bases + one quote token (quote index = `base_states.len()`). Valid swaps: base↔quote and base↔base (via internal quote path). No quote↔quote.
- **Liquidity:** per-base `reserve` + `quote_reserve`; unfunded bases must not emit edges even if another base + quote are live.
- **Discovery vs state:** canonical token order comes from hydrated pool state, not stale indexer rows.
- **Why:** multi-base pools look like full meshes but most bases are often dust; without per-leg gating, hub tokens accumulate spurious out-degree.

---

## Cross-cutting graph / search concerns

| Concern | Why it matters |
|--------|----------------|
| **Per-hop vs pool tradability** | Multi-token pools can be “live” while most pairwise legs are not. |
| **Edge rescoring** | Topology is cached; `rescore_graph_edge` marks legs dead when funding drains without rebuilding adjacency. |
| **Start-token bias** | DFS prioritizes high out-degree hubs — inflated degree from bad edges skews which cycles fill `enumeration_max_paths`. |
| **One pool per cycle** | DFS forbids reusing the same `pool_index` in one route (no same-pool round trip). |
| **Route call budget** | `MAX_ROUTE_CALLS` (12); V3 = 1 call/hop, most others = 2. Long V4-heavy routes exceed executor limits before hop cap. |
| **Spot vs sim** | Graph weights use marginal spot; Curve/Balancer/Woofi spot uses `SPOT_PROBE` simulation. Ranking is approximate until atomic prefilter / Brent search. |

## Where to look in code

- Graph build: `src/pipeline/graph.rs`
- Per-leg gates: `src/core/types.rs` (`hop_token_funded`, `hop_pair_routable`, `is_tradable`)
- Simulation: `src/pipeline/local_sim.rs`
- Execution / calldata: `src/services/execution/calldata/encoders/`