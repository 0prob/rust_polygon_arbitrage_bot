//! Pipeline 2 (base-price): token → WPOL/MATIC rates from the same arena used for hop sim.
//!
//! Shortest direct-edge paths on the routing graph; amounts via `simulate_route_minimal`.
//! Fail-closed below [`MIN_TOKEN_TO_MATIC_RATE`]. Configured oracle feeds win in enrich.

use alloy::primitives::U256;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::core::constants::{
    MAX_SUPPORTED_TOKEN_DECIMALS, MIN_TOKEN_TO_MATIC_RATE, RATE_PRECISION, WMATIC,
};
use crate::core::types::{Edge, TokenIndex};
use crate::pipeline::arena::StateArena;
use crate::pipeline::cycle_finder::is_live_graph_edge;
use crate::pipeline::local_sim::simulate_route_minimal;
use crate::pipeline::spot_price::spot_probe_for_token;
use crate::pipeline::types::{GraphHopPhase, RoutingGraph};
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
            max_hops: 4,
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
    if edges.is_empty() {
        return None;
    }
    let decimals = arena.token_decimals(token);
    if decimals > MAX_SUPPORTED_TOKEN_DECIMALS {
        return None;
    }
    let probe = spot_probe_for_token(arena, token);
    if probe.is_zero() {
        return None;
    }
    let sim = simulate_route_minimal(arena, edges, probe)?;
    if sim.amount_out.is_zero() {
        return None;
    }
    let scale = ten_pow_u256_cached(decimals);
    let rate = sim
        .amount_out
        .checked_mul(scale)?
        .checked_mul(RATE_PRECISION)?
        / probe;
    (rate >= MIN_TOKEN_TO_MATIC_RATE).then_some(rate)
}

fn resolve_wmatic_index(arena: &StateArena) -> Option<TokenIndex> {
    for i in 0..arena.token_count() {
        let idx = TokenIndex(i);
        if arena.token_address(idx) == Some(WMATIC) {
            return Some(idx);
        }
    }
    None
}

/// Unweighted shortest path using live **direct** graph edges only (no virtual hub legs).
fn shortest_direct_path_to_wmatic(
    graph: &RoutingGraph,
    from: TokenIndex,
    wmatic: TokenIndex,
    max_hops: u32,
) -> Option<Vec<Edge>> {
    if from == wmatic {
        return Some(Vec::new());
    }
    let token_slots = graph.token_count as usize;
    if from.0 as usize >= token_slots || wmatic.0 as usize >= token_slots {
        return None;
    }
    let max_hops = max_hops.max(1);
    let mut visited = FxHashSet::default();
    visited.insert(from);
    let mut queue = std::collections::VecDeque::new();
    queue.push_back((from, Vec::<Edge>::new()));

    while let Some((node, path)) = queue.pop_front() {
        if path.len() as u32 >= max_hops {
            continue;
        }
        let node_usize = node.0 as usize;
        if node_usize >= graph.adjacency.len() {
            continue;
        }
        for ge in &graph.adjacency[node_usize] {
            if ge.phase != GraphHopPhase::Direct || !is_live_graph_edge(ge) {
                continue;
            }
            let next = ge.edge.token_out;
            if next == wmatic {
                let mut full = path;
                full.push(ge.edge);
                return Some(full);
            }
            if next.0 as usize >= token_slots || !visited.insert(next) {
                continue;
            }
            let mut next_path = path.clone();
            next_path.push(ge.edge);
            queue.push_back((next, next_path));
        }
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
) -> FxHashMap<TokenIndex, U256> {
    let mut out = FxHashMap::default();
    if !params.enabled {
        return out;
    }
    let Some(wmatic) = resolve_wmatic_index(arena) else {
        return out;
    };
    out.insert(wmatic, RATE_PRECISION);
    for &token in tokens {
        if token == wmatic || out.contains_key(&token) {
            continue;
        }
        let Some(path) = shortest_direct_path_to_wmatic(graph, token, wmatic, params.max_hops)
        else {
            continue;
        };
        let Some(rate) = matic_rate_from_probe_sim(arena, token, &path) else {
            continue;
        };
        out.insert(token, rate);
    }
    out
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

    #[test]
    fn hub_path_rate_reaches_wmatic_over_two_hops() {
        let mut arena = StateArena::default();
        let wmatic = arena.register_token(WMATIC);
        let mid = arena.register_token(Address::from([0x11u8; 20]));
        let tail = arena.register_token(Address::from([0x22u8; 20]));
        let p1 = arena.register_pool(Address::from([0xa1u8; 20]), v2_pool());
        let p2 = arena.register_pool(Address::from([0xa2u8; 20]), v2_pool());
        let m1 = pool_meta_from_pair(p1, ProtocolType::UniswapV2, wmatic, mid, 30);
        let m2 = pool_meta_from_pair(p2, ProtocolType::UniswapV2, mid, tail, 30);
        let graph = build_graph(&arena, &[m1, m2]);
        let rates = hub_path_matic_rates_batch(
            &arena,
            &graph,
            &[tail],
            HubPathRateParams {
                enabled: true,
                max_hops: 4,
            },
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
        let rates =
            hub_path_matic_rates_batch(&arena, &graph, &[wmatic], HubPathRateParams::default());
        assert_eq!(rates.get(&wmatic), Some(&RATE_PRECISION));
    }
}
