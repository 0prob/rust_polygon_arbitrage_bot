//! Pipeline 2 (base-price): token → WPOL/MATIC rates from the same arena used for hop sim.
//!
//! Shortest live paths on the routing graph (Direct + resolved hub Enter/Exit legs);
//! amounts via `simulate_route_minimal`. Fail-closed below [`MIN_TOKEN_TO_MATIC_RATE`].
//! Configured oracle feeds win in enrich.

use alloy::primitives::{Address, U256};
use rustc_hash::FxHashMap;

use crate::core::constants::{
    MAX_SUPPORTED_TOKEN_DECIMALS, MIN_TOKEN_TO_MATIC_RATE, RATE_PRECISION, WMATIC,
};
use crate::core::types::{Edge, TokenIndex};
use crate::pipeline::arena::StateArena;
use crate::pipeline::cycle_finder::{index_pool_metas, is_live_graph_edge};
use crate::pipeline::graph::{
    PendingHubSwap, funded_token_indices, resolve_lazy_swap_edge, routing_token_at_leg,
};
use crate::pipeline::local_sim::simulate_route_minimal;
use crate::pipeline::spot_price::spot_probe_for_decimals;
use crate::pipeline::types::{GraphHopPhase, PoolMeta, RoutingGraph};
use crate::util::ten_pow_u256_cached;

#[derive(Debug, Clone, Copy)]
pub struct HubPathRateParams {
    pub enabled: bool,
    pub max_hops: u32,
}

impl Default for HubPathRateParams {
    fn default() -> Self {
        Self {
            enabled: true,
            max_hops: crate::core::constants::DEFAULT_HUB_PATH_MAX_HOPS,
        }
    }
}

/// MATIC wei per whole token unit from a marginal probe swap along `edges` into WMATIC.
#[must_use]
pub fn matic_rate_from_probe_sim(
    arena: &StateArena,
    token: TokenIndex,
    edges: &[Edge],
) -> Option<U256> {
    matic_rate_from_probe_sim_with_decimals(arena, token, edges, None)
}

#[must_use]
fn matic_rate_from_probe_sim_with_decimals(
    arena: &StateArena,
    token: TokenIndex,
    edges: &[Edge],
    token_decimals: Option<&FxHashMap<Address, u8>>,
) -> Option<U256> {
    if edges.is_empty() {
        return None;
    }
    let decimals = match token_decimals {
        Some(hints) => {
            crate::services::oracle::resolve_token_decimals_for_index(token, arena, hints)
        }
        None => arena.token_decimals(token),
    };
    if decimals > MAX_SUPPORTED_TOKEN_DECIMALS {
        return None;
    }
    // ponytail: micro then spot — thin V2/spoke paths exhaust at spot-only (HF rank ladder).
    let spot = spot_probe_for_decimals(decimals);
    let micro = if decimals >= 6 {
        ten_pow_u256_cached(decimals - 6)
    } else {
        U256::from(1u64)
    };
    let scale = ten_pow_u256_cached(decimals);
    for probe in [micro, spot] {
        if probe.is_zero() {
            continue;
        }
        let Some(sim) = simulate_route_minimal(arena, edges, probe) else {
            continue;
        };
        if sim.amount_out.is_zero() {
            continue;
        }
        let Some(rate) = sim
            .amount_out
            .checked_mul(scale)
            .and_then(|v| v.checked_mul(RATE_PRECISION))
            .map(|v| v / probe)
        else {
            continue;
        };
        if rate >= MIN_TOKEN_TO_MATIC_RATE {
            return Some(rate);
        }
    }
    None
}

#[inline]
fn resolve_wmatic_index(arena: &StateArena) -> Option<TokenIndex> {
    arena.address_to_token().get(&WMATIC).copied()
}

/// Reverse adjacency entry: forward edge `from → toward_wmatic`.
type RevHop = (TokenIndex, Edge);

/// Build reverse Direct + resolved hub-spoke edges for one reverse-BFS from WMATIC.
///
/// When `pool_metas` is provided, V4 Enter-only hubs resolve exits via funded legs
/// (same as cycle DFS); without metas those legs stay unreachable for hub rates.
fn build_reverse_hops(
    arena: &StateArena,
    graph: &RoutingGraph,
    pool_metas: Option<&[PoolMeta]>,
) -> Vec<Vec<RevHop>> {
    let token_slots = graph.token_count as usize;
    let meta_index = pool_metas.map(index_pool_metas);
    let mut rev = vec![Vec::new(); token_slots];
    for (src_idx, adj) in graph.adjacency.iter().enumerate().take(token_slots) {
        let src = TokenIndex(src_idx as u32);
        for ge in adj {
            match ge.phase {
                GraphHopPhase::Direct => {
                    if !is_live_graph_edge(ge) {
                        continue;
                    }
                    let dst = ge.edge.token_out.0 as usize;
                    if dst < token_slots {
                        rev[dst].push((src, ge.edge));
                    }
                }
                GraphHopPhase::EnterPool => {
                    // Pair Enter→Exit into one swap hop (Balancer/Curve/WooFi hubs).
                    // V4 is Enter-only; fall back to funded legs from pool_metas.
                    let hub = ge.target_node as usize;
                    let Some(hub_adj) = graph.adjacency.get(hub) else {
                        continue;
                    };
                    let pending = PendingHubSwap {
                        pool_index: ge.edge.pool_index,
                        token_in: src,
                        token_in_idx: ge.edge.token_in_idx,
                        protocol: ge.edge.protocol,
                        fee_bps: ge.edge.fee_bps,
                    };
                    let mut paired = false;
                    for exit in hub_adj {
                        if exit.phase != GraphHopPhase::ExitPool {
                            continue;
                        }
                        if exit.edge.pool_index != ge.edge.pool_index {
                            continue;
                        }
                        let out = TokenIndex(exit.target_node);
                        let out_idx = out.0 as usize;
                        if out_idx >= token_slots || out == src {
                            continue;
                        }
                        let Some((edge, _, _)) =
                            resolve_lazy_swap_edge(arena, pending, out, exit.edge.token_out_idx)
                        else {
                            continue;
                        };
                        rev[out_idx].push((src, edge));
                        paired = true;
                    }
                    if paired {
                        continue;
                    }
                    let Some(index) = meta_index.as_ref() else {
                        continue;
                    };
                    let Some(meta) = index.get(pending.pool_index.0 as usize).and_then(|m| *m)
                    else {
                        continue;
                    };
                    let Some(state) = arena.pool_state(pending.pool_index) else {
                        continue;
                    };
                    for out_leg in funded_token_indices(state, meta) {
                        if out_leg == pending.token_in_idx {
                            continue;
                        }
                        let Some(token_out) =
                            routing_token_at_leg(arena, state, meta, out_leg as usize)
                        else {
                            continue;
                        };
                        let out_idx = token_out.0 as usize;
                        if out_idx >= token_slots || token_out == src {
                            continue;
                        }
                        let Some((edge, _, _)) =
                            resolve_lazy_swap_edge(arena, pending, token_out, out_leg)
                        else {
                            continue;
                        };
                        rev[out_idx].push((src, edge));
                    }
                }
                GraphHopPhase::ExitPool => {}
            }
        }
    }
    rev
}

/// One reverse BFS from WMATIC: `parent[token] = (next_toward_wmatic, edge token→next)`.
/// `alt` keeps one second-choice first hop when the shortest path fails probe sim.
fn reverse_bfs_parents(
    rev: &[Vec<RevHop>],
    wmatic: TokenIndex,
    max_hops: u32,
) -> (Vec<Option<RevHop>>, Vec<Option<RevHop>>) {
    let token_slots = rev.len();
    let mut parent = vec![None; token_slots];
    let mut alt = vec![None; token_slots];
    let mut depth = vec![u32::MAX; token_slots];
    let w = wmatic.0 as usize;
    if w >= token_slots {
        return (parent, alt);
    }
    depth[w] = 0;
    let mut queue = std::collections::VecDeque::new();
    queue.push_back(wmatic);
    let max_hops = max_hops.max(1);

    while let Some(curr) = queue.pop_front() {
        let d = depth[curr.0 as usize];
        if d >= max_hops {
            continue;
        }
        for &(prev, edge) in &rev[curr.0 as usize] {
            let pi = prev.0 as usize;
            if pi >= token_slots {
                continue;
            }
            if depth[pi] != u32::MAX {
                // Second hop toward WMATIC — only when next already has a path.
                if alt[pi].is_none()
                    && parent[pi].is_some_and(|(n, _)| n != curr)
                    && depth[curr.0 as usize] < max_hops
                {
                    alt[pi] = Some((curr, edge));
                }
                continue;
            }
            depth[pi] = d + 1;
            parent[pi] = Some((curr, edge));
            queue.push_back(prev);
        }
    }
    (parent, alt)
}

fn reconstruct_path(
    parent: &[Option<RevHop>],
    from: TokenIndex,
    wmatic: TokenIndex,
) -> Option<Vec<Edge>> {
    reconstruct_path_with_first(parent, from, wmatic, None)
}

fn reconstruct_path_with_first(
    parent: &[Option<RevHop>],
    from: TokenIndex,
    wmatic: TokenIndex,
    first: Option<RevHop>,
) -> Option<Vec<Edge>> {
    if from == wmatic {
        return Some(Vec::new());
    }
    let mut path = Vec::new();
    let mut cur = from;
    let mut used_first = false;
    // ponytail: hop cap already enforced in BFS; bound walk to parent len.
    for _ in 0..parent.len() {
        if cur == wmatic {
            return Some(path);
        }
        let (next, edge) = if !used_first {
            used_first = true;
            first
                .or_else(|| parent.get(cur.0 as usize).copied().flatten())?
        } else {
            parent.get(cur.0 as usize).copied().flatten()?
        };
        path.push(edge);
        cur = next;
    }
    None
}

/// Hub-path base rates for cycle tokens (WMATIC = 1:1). Skips tokens without a simulable path.
#[must_use]
pub fn hub_path_matic_rates_batch(
    arena: &StateArena,
    graph: &RoutingGraph,
    tokens: &[TokenIndex],
    params: HubPathRateParams,
    token_decimals: Option<&FxHashMap<Address, u8>>,
    pool_metas: Option<&[PoolMeta]>,
) -> FxHashMap<TokenIndex, U256> {
    let mut out = FxHashMap::default();
    if !params.enabled {
        return out;
    }
    let Some(wmatic) = resolve_wmatic_index(arena) else {
        return out;
    };
    out.insert(wmatic, RATE_PRECISION);

    let need: Vec<TokenIndex> = tokens
        .iter()
        .copied()
        .filter(|t| *t != wmatic && !out.contains_key(t))
        .collect();
    if need.is_empty() {
        return out;
    }

    // One reverse BFS for the whole batch (was per-token forward BFS + path.clone).
    let started = crate::util::now_ms();
    let rev = build_reverse_hops(arena, graph, pool_metas);
    let (parent, alt) = reverse_bfs_parents(&rev, wmatic, params.max_hops);

    let need_n = need.len();
    let mut path_miss = 0u32;
    let mut sim_fail = 0u32;
    let mut alt_rescue = 0u32;
    let mut dual_reject = 0u32;
    let mut priced = 0u32;
    for token in need {
        if out.contains_key(&token) {
            continue;
        }
        let Some(path) = reconstruct_path(&parent, token, wmatic) else {
            path_miss += 1;
            continue;
        };
        if path.is_empty() {
            path_miss += 1;
            continue;
        }
        let primary =
            matic_rate_from_probe_sim_with_decimals(arena, token, &path, token_decimals);
        // Alternate first-hop path when present (rescue + dual-DEX sanity).
        let alt_rate = alt
            .get(token.0 as usize)
            .copied()
            .flatten()
            .and_then(|first| reconstruct_path_with_first(&parent, token, wmatic, Some(first)))
            .filter(|p| !p.is_empty())
            .and_then(|p| {
                matic_rate_from_probe_sim_with_decimals(arena, token, &p, token_decimals)
            });
        match (primary, alt_rate) {
            (Some(a), Some(b)) if rates_diverge_bps(a, b) > HUB_DUAL_PATH_MAX_DIVERGE_BPS => {
                // ponytail: >2% across two first hops → skip (flash / thin-pool risk).
                dual_reject += 1;
            }
            (Some(a), Some(b)) => {
                out.insert(token, a.min(b));
                priced += 1;
            }
            (Some(a), None) => {
                out.insert(token, a);
                priced += 1;
            }
            (None, Some(b)) => {
                out.insert(token, b);
                priced += 1;
                alt_rescue += 1;
            }
            (None, None) => {
                sim_fail += 1;
            }
        }
    }
    let ms = crate::util::now_ms().saturating_sub(started);
    if path_miss > 0 || sim_fail > 0 || alt_rescue > 0 || dual_reject > 0 || need_n > 32 {
        crate::info!(
            "hub_path batch: need={need_n} priced={priced} path_miss={path_miss} sim_fail={sim_fail} alt_rescue={alt_rescue} dual_reject={dual_reject} ms={ms} max_hops={}",
            params.max_hops
        );
    } else if need_n > 0 {
        crate::debug!(
            "hub_path batch: need={need_n} priced={priced} path_miss={path_miss} sim_fail={sim_fail} alt_rescue={alt_rescue} dual_reject={dual_reject} ms={ms} max_hops={}",
            params.max_hops
        );
    }
    out
}

/// Max relative divergence (bps) between shortest and alt first-hop rates.
const HUB_DUAL_PATH_MAX_DIVERGE_BPS: u64 = 200;

#[inline]
fn rates_diverge_bps(a: U256, b: U256) -> u64 {
    let (hi, lo) = if a >= b { (a, b) } else { (b, a) };
    if hi.is_zero() {
        return 0;
    }
    let delta = hi - lo;
    u64::try_from((delta * U256::from(10_000u64) / hi).min(U256::from(10_000u64))).unwrap_or(10_000)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{PoolIndex, PoolState, ProtocolType, V2PoolState};
    use crate::pipeline::graph::build_graph;
    use crate::pipeline::types::PoolMeta;
    use alloy::primitives::Address;
    use std::sync::Arc;

    fn pool_meta_from_pair(
        pool: PoolIndex,
        protocol: ProtocolType,
        a: TokenIndex,
        b: TokenIndex,
        fee_bps: u32,
    ) -> PoolMeta {
        PoolMeta {
            pool_index: pool,
            protocol,
            tokens: vec![a, b],
            fee_bps,
            bpt_index: None,
            pool_id: None,
            protocol_label: None,
            pool_type: None,
            hooks: None,
            tick_spacing: None,
        }
    }

    fn v2_pool() -> Arc<PoolState> {
        // Reserves must exceed the 18-decimal spot probe (~1e15) used by matic_rate_from_probe_sim.
        let funded = crate::pipeline::spot_price::spot_probe_for_decimals(18) * U256::from(100u64);
        Arc::new(PoolState::V2(V2PoolState {
            reserve0: funded,
            reserve1: funded * U256::from(2u64),
            fee: U256::from(30u8),
            fee_denominator: U256::from(10_000u64),
            block_timestamp_last: 1,
        }))
    }

    /// Thin enough that spot (~1e15) exhausts but micro (~1e12) still sims.
    fn thin_v2_pool() -> Arc<PoolState> {
        let funded = U256::from(10u128.pow(13)); // between micro and spot
        Arc::new(PoolState::V2(V2PoolState {
            reserve0: funded,
            reserve1: funded * U256::from(2u64),
            fee: U256::from(30u8),
            fee_denominator: U256::from(10_000u64),
            block_timestamp_last: 1,
        }))
    }

    #[test]
    fn hub_path_rate_reaches_wmatic_over_two_hops() {
        let mut arena = StateArena::default();
        let wmatic = arena.register_token(WMATIC);
        let mid = arena.register_token(Address::from([0x11u8; 20]));
        let tail = arena.register_token(Address::from([0x22u8; 20]));
        let p1 = arena.register_pool(Address::from([0xa1u8; 20]), v2_pool());
        let p2 = arena.register_pool(Address::from([0xa2u8; 20]), v2_pool());
        let metas = [
            pool_meta_from_pair(p1, ProtocolType::UniswapV2, wmatic, mid, 30),
            pool_meta_from_pair(p2, ProtocolType::UniswapV2, mid, tail, 30),
        ];
        let graph = build_graph(&arena, &metas);
        let rates = hub_path_matic_rates_batch(
            &arena,
            &graph,
            &[tail],
            HubPathRateParams {
                enabled: true,
                max_hops: 4,
            },
            None,
            Some(&metas),
        );
        let rate = rates.get(&tail).copied().expect("tail rate");
        assert!(rate >= MIN_TOKEN_TO_MATIC_RATE);
        assert_eq!(rates.get(&wmatic).copied(), Some(RATE_PRECISION));
    }

    #[test]
    fn wmatic_self_rate_without_graph_path() {
        let mut arena = StateArena::default();
        let wmatic = arena.register_token(WMATIC);
        let graph = RoutingGraph::new(1);
        let rates = hub_path_matic_rates_batch(
            &arena,
            &graph,
            &[wmatic],
            HubPathRateParams::default(),
            None,
            None,
        );
        assert_eq!(rates.get(&wmatic), Some(&RATE_PRECISION));
    }

    #[test]
    fn thin_v2_hub_path_prices_via_micro_probe() {
        let mut arena = StateArena::default();
        let wmatic = arena.register_token(WMATIC);
        let spoke = arena.register_token(Address::from([0x33u8; 20]));
        let p = arena.register_pool(Address::from([0xb1u8; 20]), thin_v2_pool());
        let metas = [pool_meta_from_pair(
            p,
            ProtocolType::UniswapV2,
            wmatic,
            spoke,
            30,
        )];
        let graph = build_graph(&arena, &metas);
        let rates = hub_path_matic_rates_batch(
            &arena,
            &graph,
            &[spoke],
            HubPathRateParams::default(),
            None,
            Some(&metas),
        );
        let rate = rates.get(&spoke).copied().expect("thin spoke must price via micro");
        assert!(rate >= MIN_TOKEN_TO_MATIC_RATE);
    }

    #[test]
    fn batch_rates_share_one_bfs_for_siblings() {
        let mut arena = StateArena::default();
        let wmatic = arena.register_token(WMATIC);
        let a = arena.register_token(Address::from([0x11u8; 20]));
        let b = arena.register_token(Address::from([0x22u8; 20]));
        let p1 = arena.register_pool(Address::from([0xa1u8; 20]), v2_pool());
        let p2 = arena.register_pool(Address::from([0xa2u8; 20]), v2_pool());
        let metas = [
            pool_meta_from_pair(p1, ProtocolType::UniswapV2, wmatic, a, 30),
            pool_meta_from_pair(p2, ProtocolType::UniswapV2, wmatic, b, 30),
        ];
        let graph = build_graph(&arena, &metas);
        let rates = hub_path_matic_rates_batch(
            &arena,
            &graph,
            &[a, b],
            HubPathRateParams::default(),
            None,
            Some(&metas),
        );
        assert!(rates.contains_key(&a));
        assert!(rates.contains_key(&b));
    }
}
