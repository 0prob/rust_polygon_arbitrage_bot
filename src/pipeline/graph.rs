use crate::core::math::fixed_point::edge_log_weight_from_ratio;
use crate::core::types::{Edge, PoolIndex, PoolState, ProtocolType, TokenIndex};
use crate::pipeline::arena::StateArena;
use crate::pipeline::cycle_finder::DEAD_EDGE_LOG_WEIGHT;
use crate::pipeline::spot_price::{compute_edge_log_weight, compute_edge_ratio};
use crate::pipeline::types::{GraphEdge, PoolMeta, RoutingGraph};
use alloy::primitives::U256;
use rustc_hash::FxHashMap;

/// Max parallel directed edges per `(token_in, token_out, protocol)` after rescoring.
const MAX_PARALLEL_EDGES_PER_PAIR: usize = 2;

#[inline]
fn pair_zero_for_one(token_in_idx: u8) -> bool {
    token_in_idx == 0
}

#[inline]
fn multi_zero_for_one(token_in_idx: u8, token_out_idx: u8) -> bool {
    token_in_idx < token_out_idx
}

/// Build directed swap edges for a two-token pool (V2/V3/DODO).
/// When `state` is set, only emits hops that pass [`PoolState::hop_pair_routable`].
#[must_use]
pub fn edges_for_pair(
    pool_index: PoolIndex,
    protocol: ProtocolType,
    token0: TokenIndex,
    token1: TokenIndex,
    fee_bps: u32,
    state: Option<&PoolState>,
) -> Vec<Edge> {
    let mut out = Vec::with_capacity(2);
    if let Some(state) = state {
        if state.hop_pair_routable(0, 1) {
            out.push(Edge {
                pool_index,
                token_in: token0,
                token_out: token1,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol,
                fee_bps,
                zero_for_one: pair_zero_for_one(0),
            });
        }
        if state.hop_pair_routable(1, 0) {
            out.push(Edge {
                pool_index,
                token_in: token1,
                token_out: token0,
                token_in_idx: 1,
                token_out_idx: 0,
                protocol,
                fee_bps,
                zero_for_one: pair_zero_for_one(1),
            });
        }
    } else {
        out.push(Edge {
            pool_index,
            token_in: token0,
            token_out: token1,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol,
            fee_bps,
            zero_for_one: pair_zero_for_one(0),
        });
        out.push(Edge {
            pool_index,
            token_in: token1,
            token_out: token0,
            token_in_idx: 1,
            token_out_idx: 0,
            protocol,
            fee_bps,
            zero_for_one: pair_zero_for_one(1),
        });
    }
    out
}

/// Full multi-token edge expansion (Balancer/Curve/Woofi); skips `bpt_index`.
/// When `state` is set, only emits hops that pass [`PoolState::hop_pair_routable`].
#[must_use]
pub fn edges_for_multi_token(
    pool_index: PoolIndex,
    protocol: ProtocolType,
    tokens: &[TokenIndex],
    fee_bps: u32,
    bpt_index: Option<usize>,
    state: Option<&PoolState>,
) -> Vec<Edge> {
    let n = tokens.len();
    let mut out = Vec::with_capacity(n.saturating_mul(4).max(2));
    for (i, &tin) in tokens.iter().enumerate() {
        if bpt_index == Some(i) {
            continue;
        }
        for (j, &tout) in tokens.iter().enumerate() {
            if i == j || bpt_index == Some(j) {
                continue;
            }
            if let Some(state) = state
                && !state.hop_pair_routable(i, j)
            {
                continue;
            }
            out.push(Edge {
                pool_index,
                token_in: tin,
                token_out: tout,
                token_in_idx: i as u8,
                token_out_idx: j as u8,
                protocol,
                fee_bps,
                zero_for_one: multi_zero_for_one(i as u8, j as u8),
            });
        }
    }
    out
}

fn balancer_token_indices_to_expand(
    state: &PoolState,
    tokens: &[TokenIndex],
    bpt_index: Option<usize>,
) -> Vec<usize> {
    let PoolState::Balancer(balancer) = state else {
        return vec![];
    };
    let mut ranked: Vec<(usize, U256)> = tokens
        .iter()
        .enumerate()
        .filter(|(i, _)| bpt_index != Some(*i))
        .map(|(i, _)| (i, balancer.balances.get(i).copied().unwrap_or_default()))
        .collect();
    ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    let max_tokens = ranked.len().min(2);
    ranked.truncate(max_tokens);
    ranked.sort_by_key(|(i, _)| *i);
    ranked.into_iter().map(|(i, _)| i).collect()
}

fn edges_for_balancer_multi_token(
    pool_index: PoolIndex,
    tokens: &[TokenIndex],
    fee_bps: u32,
    bpt_index: Option<usize>,
    state: &PoolState,
    protocol: ProtocolType,
) -> Vec<Edge> {
    let keep = balancer_token_indices_to_expand(state, tokens, bpt_index);
    let mut out = Vec::with_capacity(keep.len().saturating_mul(keep.len().saturating_sub(1)));
    for &i in &keep {
        for &j in &keep {
            if i == j {
                continue;
            }
            if !state.hop_pair_routable(i, j) {
                continue;
            }
            out.push(Edge {
                pool_index,
                token_in: tokens[i],
                token_out: tokens[j],
                token_in_idx: i as u8,
                token_out_idx: j as u8,
                protocol,
                fee_bps,
                zero_for_one: multi_zero_for_one(i as u8, j as u8),
            });
        }
    }
    out
}

/// Pools that would receive at least one directed edge on the next graph build.
#[must_use]
pub fn count_graph_eligible_pools(arena: &StateArena, pools: &[PoolMeta]) -> usize {
    pools
        .iter()
        .filter(|meta| pool_has_admissible_edges(arena, meta))
        .count()
}

#[inline]
fn pool_has_admissible_edges(arena: &StateArena, meta: &PoolMeta) -> bool {
    let Some(state) = arena
        .pool_state(meta.pool_index)
        .filter(|s| s.is_tradable())
    else {
        return false;
    };
    match meta.tokens.len() {
        0 | 1 => false,
        2 => state.hop_pair_routable(0, 1) || state.hop_pair_routable(1, 0),
        n => {
            if meta.protocol == ProtocolType::BalancerV2 {
                let keep = balancer_token_indices_to_expand(state, &meta.tokens, meta.bpt_index);
                for &i in &keep {
                    for &j in &keep {
                        if i != j && state.hop_pair_routable(i, j) {
                            return true;
                        }
                    }
                }
                return false;
            }
            for i in 0..n {
                if meta.bpt_index == Some(i) {
                    continue;
                }
                for j in 0..n {
                    if i == j || meta.bpt_index == Some(j) {
                        continue;
                    }
                    if state.hop_pair_routable(i, j) {
                        return true;
                    }
                }
            }
            false
        }
    }
}

fn thin_parallel_edges_in_place(adj: &mut Vec<GraphEdge>, max_per_pair: usize) {
    if adj.len() <= max_per_pair || max_per_pair == 0 {
        return;
    }
    let mut by_pair: FxHashMap<(u32, u8), Vec<usize>> = FxHashMap::default();
    for (i, ge) in adj.iter().enumerate() {
        let key = (ge.edge.token_out.0, ge.edge.protocol as u8);
        by_pair.entry(key).or_default().push(i);
    }
    let mut keep = Vec::new();
    for mut indices in by_pair.into_values() {
        if indices.len() <= max_per_pair {
            keep.extend(indices);
            continue;
        }
        indices.sort_by(|&a, &b| {
            adj[b]
                .ratio
                .cmp(&adj[a].ratio)
                .then_with(|| adj[a].edge.pool_index.0.cmp(&adj[b].edge.pool_index.0))
        });
        keep.extend(indices.into_iter().take(max_per_pair));
    }
    keep.sort_unstable();
    let thinned: Vec<GraphEdge> = keep.into_iter().map(|i| adj[i]).collect();
    *adj = thinned;
}

fn thin_parallel_edges_in_graph(graph: &mut RoutingGraph) {
    for adj in &mut graph.adjacency {
        thin_parallel_edges_in_place(adj, MAX_PARALLEL_EDGES_PER_PAIR);
        sort_adjacency_edges(adj);
    }
}

#[inline]
fn push_graph_edge(graph: &mut RoutingGraph, edge: Edge) {
    if let Some(slot) = graph.adjacency.get_mut(edge.token_in.0 as usize) {
        slot.push(GraphEdge {
            edge,
            log_weight: 0.0,
            ratio: U256::ZERO,
        });
    }
}

pub fn build_graph(arena: &StateArena, pools: &[PoolMeta]) -> RoutingGraph {
    let mut graph = RoutingGraph::new(arena.token_count());

    for meta in pools {
        let Some(state) = arena
            .pool_state(meta.pool_index)
            .filter(|s| s.is_tradable())
        else {
            continue;
        };

        if meta.tokens.len() == 2 {
            for edge in edges_for_pair(
                meta.pool_index,
                meta.protocol,
                meta.tokens[0],
                meta.tokens[1],
                meta.fee_bps,
                Some(state),
            ) {
                push_graph_edge(&mut graph, edge);
            }
        } else if meta.tokens.len() > 2 {
            let edges = match state {
                PoolState::Balancer(_) => edges_for_balancer_multi_token(
                    meta.pool_index,
                    &meta.tokens,
                    meta.fee_bps,
                    meta.bpt_index,
                    state,
                    meta.protocol,
                ),
                _ => edges_for_multi_token(
                    meta.pool_index,
                    meta.protocol,
                    &meta.tokens,
                    meta.fee_bps,
                    meta.bpt_index,
                    Some(state),
                ),
            };
            for edge in edges {
                push_graph_edge(&mut graph, edge);
            }
        }
    }

    rescore_graph_in_place(arena, &mut graph);
    thin_parallel_edges_in_graph(&mut graph);
    rebuild_pool_edge_positions_full(&mut graph);
    graph.coverage = Some(std::sync::Arc::new(
        crate::pipeline::cycle_finder::cycle_capable_coverage(&graph),
    ));
    graph
}

/// Recompute edge log-weights from current pool states without rebuilding adjacency.
pub fn rescore_graph_in_place(arena: &StateArena, graph: &mut RoutingGraph) {
    rescore_adjacency(arena, &mut graph.adjacency);
}

/// Rescore only dirty pools when the touch set is small; otherwise rescore all edges.
pub fn rescore_dirty_pools_or_full(
    arena: &StateArena,
    graph: &mut RoutingGraph,
    dirty_pools: &[PoolIndex],
    arena_pool_count: usize,
) {
    if dirty_pools.is_empty() || dirty_pools.len() > arena_pool_count / 2 {
        rescore_graph_in_place(arena, graph);
    } else {
        rescore_pools_in_place(arena, graph, dirty_pools);
    }
}

/// Recompute log-weights only for edges touching the given pools (differential update).
pub fn rescore_pools_in_place(
    arena: &StateArena,
    graph: &mut RoutingGraph,
    pools: &[PoolIndex],
) -> usize {
    if pools.is_empty() {
        return 0;
    }
    let mut touched_pools = rustc_hash::FxHashSet::default();
    let mut touched = 0usize;
    let mut affected_adjacencies = rustc_hash::FxHashSet::default();

    for pool in pools {
        let pool_idx = pool.0 as usize;
        let Some(positions) = graph.pool_edge_positions.get(pool_idx) else {
            continue;
        };
        for &(adj_idx, edge_pos) in positions {
            let Some(adj) = graph.adjacency.get_mut(adj_idx) else {
                continue;
            };
            let Some(ge) = adj.get_mut(edge_pos) else {
                continue;
            };
            touched += rescore_graph_edge(arena, ge);
            affected_adjacencies.insert(adj_idx);
        }
        touched_pools.insert(pool_idx);
    }
    for adj_idx in &affected_adjacencies {
        if let Some(adj) = graph.adjacency.get_mut(*adj_idx) {
            sort_adjacency_edges(adj);
        }
    }
    rebuild_pool_edge_positions_for_pools(graph, &touched_pools);
    touched
}

fn rescore_adjacency(arena: &StateArena, adjacency: &mut [Vec<GraphEdge>]) {
    for adj in adjacency.iter_mut() {
        for ge in adj.iter_mut() {
            rescore_graph_edge(arena, ge);
        }
        sort_adjacency_edges(adj);
    }
}

fn sort_adjacency_edges(adj: &mut [GraphEdge]) {
    // ponytail: total_cmp is branchless for finite values (log_weights are never NaN).
    adj.sort_by(|a, b| a.log_weight.total_cmp(&b.log_weight));
}

fn rebuild_pool_edge_positions_full(graph: &mut RoutingGraph) {
    // Single-pass: find max pool index AND populate positions simultaneously.
    let mut max_pool = 0usize;
    // Pre-scan for empty graph early exit.
    let all_empty = graph.adjacency.iter().all(Vec::is_empty);
    if all_empty {
        graph.pool_edge_positions.clear();
        return;
    }
    // Single pass: max pool index + per-pool edge counts.
    let mut counts: Vec<usize> = Vec::new();
    for adj in &graph.adjacency {
        for ge in adj {
            let idx = ge.edge.pool_index.0 as usize;
            if idx >= counts.len() {
                counts.resize(idx + 1, 0);
            }
            counts[idx] += 1;
            if idx > max_pool {
                max_pool = idx;
            }
        }
    }
    let slot_count = max_pool + 1;
    if counts.len() < slot_count {
        counts.resize(slot_count, 0);
    }
    let mut slots: Vec<Vec<(usize, usize)>> = Vec::with_capacity(slot_count);
    for &c in &counts {
        slots.push(Vec::with_capacity(c));
    }
    for (adj_idx, adj) in graph.adjacency.iter().enumerate() {
        for (pos, ge) in adj.iter().enumerate() {
            let idx = ge.edge.pool_index.0 as usize;
            slots[idx].push((adj_idx, pos));
        }
    }
    graph.pool_edge_positions = slots;
}

fn rebuild_pool_edge_positions_for_pools(
    graph: &mut RoutingGraph,
    pools: &rustc_hash::FxHashSet<usize>,
) {
    if pools.is_empty() {
        return;
    }
    let max_pool = graph
        .pool_edge_positions
        .len()
        .max(pools.iter().copied().max().map_or(0, |idx| idx + 1));
    if graph.pool_edge_positions.len() < max_pool {
        graph.pool_edge_positions.resize(max_pool, Vec::new());
    }
    let mut adj_to_scan = rustc_hash::FxHashSet::default();
    for pool_idx in pools {
        if let Some(slot) = graph.pool_edge_positions.get(*pool_idx) {
            for &(adj_idx, _) in slot {
                adj_to_scan.insert(adj_idx);
            }
        }
        if let Some(slot) = graph.pool_edge_positions.get_mut(*pool_idx) {
            slot.clear();
        }
    }
    for adj_idx in adj_to_scan {
        let Some(adj) = graph.adjacency.get(adj_idx) else {
            continue;
        };
        for (pos, ge) in adj.iter().enumerate() {
            let pool_idx = ge.edge.pool_index.0 as usize;
            if pools.contains(&pool_idx) {
                graph.pool_edge_positions[pool_idx].push((adj_idx, pos));
            }
        }
    }
}

#[inline]
fn rescore_graph_edge(arena: &StateArena, ge: &mut GraphEdge) -> usize {
    let Some(state) = arena.pool_state(ge.edge.pool_index) else {
        ge.log_weight = DEAD_EDGE_LOG_WEIGHT;
        ge.ratio = U256::ZERO;
        return 1;
    };
    let tin = ge.edge.token_in_idx as usize;
    let tout = ge.edge.token_out_idx as usize;
    if !state.hop_pair_routable(tin, tout) {
        ge.log_weight = DEAD_EDGE_LOG_WEIGHT;
        ge.ratio = U256::ZERO;
        return 1;
    }
    // Compute U256 ratio first (once), then derive log_weight from it.
    // This avoids the double simulation that happened when both ensure_edge
    // (line 378) and compute_edge_ratio (line 384) independently called
    // simulate_hop_amount_out for complex pools (Balancer, Curve, Dodo).
    ge.ratio = compute_edge_ratio(arena, &ge.edge);
    if ge.ratio.is_zero() {
        ge.log_weight = DEAD_EDGE_LOG_WEIGHT;
        return 1;
    }
    ge.log_weight = edge_log_weight_from_ratio(ge.ratio);
    if !ge.log_weight.is_finite() {
        ge.log_weight = compute_edge_log_weight(ge.edge.fee_bps);
    }
    // ponytail: multi-token pools (Balancer, Curve) create n*(n-1) edges vs 2 for
    // 2-token pools, over-representing them in DFS enumeration. Apply a mild
    // per-extra-token penalty so their edge count doesn't bias cycle discovery.
    if let PoolState::Balancer(s) = state {
        let n = s.balances.len();
        if n > 2 {
            ge.log_weight += (n as f64 - 2.0) * 0.02;
        }
    }
    1
}

#[must_use]
pub fn pool_meta_from_pair(
    pool_index: PoolIndex,
    protocol: ProtocolType,
    token0: TokenIndex,
    token1: TokenIndex,
    fee_bps: u32,
) -> PoolMeta {
    PoolMeta {
        pool_index,
        protocol,
        tokens: vec![token0, token1],
        fee_bps,
        bpt_index: None,
        pool_id: None,
        protocol_label: None,
        pool_type: None,
        hooks: None,
        tick_spacing: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::constants::MIN_HOP_TOKEN_BALANCE;
    use crate::core::types::{
        BalancerPoolKind, BalancerPoolState, PoolState, V2PoolState, WoofiBaseTokenState,
        WoofiPoolState,
    };
    use alloy::primitives::{Address, U256};
    use std::sync::Arc;

    #[test]
    fn thin_parallel_edges_keeps_top_two_by_ratio() {
        fn ge(pool: u32, tout: u32, protocol: ProtocolType, ratio: u64) -> GraphEdge {
            GraphEdge {
                edge: Edge {
                    pool_index: PoolIndex(pool),
                    token_in: TokenIndex(4),
                    token_out: TokenIndex(tout),
                    token_in_idx: 0,
                    token_out_idx: 1,
                    protocol,
                    fee_bps: 30,
                    zero_for_one: true,
                },
                log_weight: -0.01,
                ratio: U256::from(ratio),
            }
        }

        let mut adj = vec![
            ge(1, 5, ProtocolType::BalancerV2, 1_010_000_000_000_000_000),
            ge(2, 5, ProtocolType::BalancerV2, 1_020_000_000_000_000_000),
            ge(3, 5, ProtocolType::BalancerV2, 1_015_000_000_000_000_000),
            ge(4, 5, ProtocolType::BalancerV2, 1_005_000_000_000_000_000),
            ge(10, 6, ProtocolType::UniswapV3, 1_010_000_000_000_000_000),
        ];
        thin_parallel_edges_in_place(&mut adj, 2);
        assert_eq!(adj.len(), 3);
        let pools: Vec<u32> = adj.iter().map(|e| e.edge.pool_index.0).collect();
        assert!(pools.contains(&2));
        assert!(pools.contains(&3));
        assert!(!pools.contains(&4));
        assert!(pools.contains(&10));
    }

    #[test]
    fn test_edges_for_pair() {
        let edges = edges_for_pair(
            PoolIndex(0),
            ProtocolType::UniswapV2,
            TokenIndex(0),
            TokenIndex(1),
            30,
            None,
        );
        assert_eq!(edges.len(), 2);
    }

    #[test]
    fn multi_token_edges_skip_underfunded_pairs() {
        let tokens = [
            TokenIndex(0),
            TokenIndex(1),
            TokenIndex(2),
            TokenIndex(3),
            TokenIndex(4),
        ];
        let funded = MIN_HOP_TOKEN_BALANCE;
        let dust = MIN_HOP_TOKEN_BALANCE - U256::from(1u64);
        let state = PoolState::Balancer(BalancerPoolState {
            pool_id: None,
            tokens: vec![],
            balances: vec![funded, funded, dust, dust, dust],
            weights: vec![funded; 5],
            scaling_factors: vec![funded; 5],
            amp: funded,
            amp_precision: U256::from(1u64),
            fee: U256::ZERO,
            pool_type: BalancerPoolKind::Weighted,
            linear: None,
            bpt_index: None,
            is_updating: false,
            last_change_block: 0,
        });

        let all_pairs = edges_for_multi_token(
            PoolIndex(0),
            ProtocolType::BalancerV2,
            &tokens,
            10,
            None,
            None,
        );
        assert_eq!(all_pairs.len(), 20);

        let gated = edges_for_multi_token(
            PoolIndex(0),
            ProtocolType::BalancerV2,
            &tokens,
            10,
            None,
            Some(&state),
        );
        assert_eq!(gated.len(), 2);
    }

    #[test]
    fn balancer_multi_token_edges_are_liquidity_capped() {
        let tokens = [
            TokenIndex(0),
            TokenIndex(1),
            TokenIndex(2),
            TokenIndex(3),
            TokenIndex(4),
            TokenIndex(5),
        ];
        let state = PoolState::Balancer(BalancerPoolState {
            pool_id: None,
            tokens: vec![
                Address::from([0u8; 20]),
                Address::from([1u8; 20]),
                Address::from([2u8; 20]),
                Address::from([3u8; 20]),
                Address::from([4u8; 20]),
                Address::from([5u8; 20]),
            ],
            balances: vec![
                MIN_HOP_TOKEN_BALANCE,
                MIN_HOP_TOKEN_BALANCE + U256::from(1u64),
                MIN_HOP_TOKEN_BALANCE + U256::from(2u64),
                MIN_HOP_TOKEN_BALANCE + U256::from(3u64),
                MIN_HOP_TOKEN_BALANCE - U256::from(1u64),
                MIN_HOP_TOKEN_BALANCE - U256::from(2u64),
            ],
            weights: vec![U256::from(1u64); 6],
            scaling_factors: vec![U256::from(1u64); 6],
            amp: U256::from(1u64),
            amp_precision: U256::from(1u64),
            fee: U256::ZERO,
            pool_type: BalancerPoolKind::Weighted,
            linear: None,
            bpt_index: None,
            is_updating: false,
            last_change_block: 0,
        });

        let edges = edges_for_balancer_multi_token(
            PoolIndex(0),
            &tokens,
            10,
            None,
            &state,
            ProtocolType::BalancerV2,
        );

        assert_eq!(edges.len(), 2);
        assert!(
            edges
                .iter()
                .all(|e| { matches!(e.token_in.0, 2 | 3) && matches!(e.token_out.0, 2 | 3) })
        );
    }

    #[test]
    fn woofi_multi_base_edges_skip_underfunded_bases() {
        let tokens = [TokenIndex(0), TokenIndex(1), TokenIndex(2), TokenIndex(3)];
        let funded = MIN_HOP_TOKEN_BALANCE;
        let dust = MIN_HOP_TOKEN_BALANCE - U256::from(1u64);
        let state = PoolState::Woofi(WoofiPoolState {
            tokens: vec![],
            quote_reserve: funded,
            base_states: vec![
                WoofiBaseTokenState {
                    price: U256::from(1u8),
                    spread: U256::ZERO,
                    coeff: U256::ZERO,
                    reserve: funded,
                    base_dec: U256::from(1u8),
                    quote_dec: U256::from(1u8),
                    price_dec: U256::from(1u8),
                    fee_rate: U256::ZERO,
                    max_gamma: U256::ZERO,
                    max_notional_swap: U256::ZERO,
                },
                WoofiBaseTokenState {
                    price: U256::from(1u8),
                    spread: U256::ZERO,
                    coeff: U256::ZERO,
                    reserve: dust,
                    base_dec: U256::from(1u8),
                    quote_dec: U256::from(1u8),
                    price_dec: U256::from(1u8),
                    fee_rate: U256::ZERO,
                    max_gamma: U256::ZERO,
                    max_notional_swap: U256::ZERO,
                },
                WoofiBaseTokenState {
                    price: U256::from(1u8),
                    spread: U256::ZERO,
                    coeff: U256::ZERO,
                    reserve: dust,
                    base_dec: U256::from(1u8),
                    quote_dec: U256::from(1u8),
                    price_dec: U256::from(1u8),
                    fee_rate: U256::ZERO,
                    max_gamma: U256::ZERO,
                    max_notional_swap: U256::ZERO,
                },
            ],
            fee: U256::ZERO,
        });

        let gated = edges_for_multi_token(
            PoolIndex(0),
            ProtocolType::Woofi,
            &tokens,
            0,
            None,
            Some(&state),
        );
        assert_eq!(gated.len(), 2);
    }

    #[test]
    fn rescore_preserves_pool_edge_positions_after_sort() {
        let mut arena = StateArena::default();
        let a = arena.register_token(Address::from([1u8; 20]));
        let b = arena.register_token(Address::from([2u8; 20]));
        let pool = arena.register_pool(
            Address::from([3u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: MIN_HOP_TOKEN_BALANCE,
                reserve1: MIN_HOP_TOKEN_BALANCE + U256::from(1u64),
                fee: U256::from(30u8),
                fee_denominator: U256::from(10_000u64),
                block_timestamp_last: 1,
            })),
        );
        let metas = [pool_meta_from_pair(pool, ProtocolType::UniswapV2, a, b, 30)];
        let mut graph = build_graph(&arena, &metas);
        let before = graph.pool_edge_positions[pool.0 as usize].clone();
        rescore_pools_in_place(&arena, &mut graph, &[pool]);
        let after = &graph.pool_edge_positions[pool.0 as usize];
        assert_eq!(after.len(), before.len());
        for &(adj_idx, edge_pos) in after {
            let ge = &graph.adjacency[adj_idx][edge_pos];
            assert_eq!(ge.edge.pool_index, pool);
        }
    }

    #[test]
    fn graph_keeps_long_tail_edge_connected_to_priced_token() {
        let mut arena = StateArena::default();
        let priced_addr = Address::from([1u8; 20]);
        let tail_addr = Address::from([2u8; 20]);
        let priced = arena.register_token(priced_addr);
        let tail = arena.register_token(tail_addr);
        let pool = arena.register_pool(
            Address::from([3u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: MIN_HOP_TOKEN_BALANCE,
                reserve1: MIN_HOP_TOKEN_BALANCE + U256::from(1u64),
                fee: U256::from(30u8),
                fee_denominator: U256::from(10_000u64),
                block_timestamp_last: 1,
            })),
        );
        let metas = [pool_meta_from_pair(
            pool,
            ProtocolType::UniswapV2,
            priced,
            tail,
            30,
        )];
        let graph = build_graph(&arena, &metas);
        assert_eq!(graph.adjacency[priced.0 as usize].len(), 1);
        assert_eq!(graph.adjacency[tail.0 as usize].len(), 1);
    }

    #[test]
    fn eligible_count_ignores_tradable_but_unroutable_pools() {
        let mut arena = StateArena::default();
        let a = arena.register_token(Address::from([10u8; 20]));
        let b = arena.register_token(Address::from([11u8; 20]));
        let c = arena.register_token(Address::from([12u8; 20]));
        let funded = MIN_HOP_TOKEN_BALANCE;
        let dust = MIN_HOP_TOKEN_BALANCE - U256::from(1u64);
        let routable_pool = arena.register_pool(
            Address::from([13u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: funded,
                reserve1: funded + U256::from(1u64),
                fee: U256::from(30u8),
                fee_denominator: U256::from(10_000u64),
                block_timestamp_last: 1,
            })),
        );
        let dust_pool = arena.register_pool(
            Address::from([14u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: dust,
                reserve1: dust,
                fee: U256::from(30u8),
                fee_denominator: U256::from(10_000u64),
                block_timestamp_last: 1,
            })),
        );
        let metas = [
            pool_meta_from_pair(routable_pool, ProtocolType::UniswapV2, a, b, 30),
            pool_meta_from_pair(dust_pool, ProtocolType::UniswapV2, b, c, 30),
        ];

        assert_eq!(count_graph_eligible_pools(&arena, &metas), 1);
        let graph = build_graph(&arena, &metas);
        assert_eq!(graph.active_pool_count(), 1);
        assert_eq!(metas.len(), 2);
    }

    #[test]
    fn pair_pools_use_same_edge_gating_as_multi_token() {
        let mut arena = StateArena::default();
        let a = arena.register_token(Address::from([20u8; 20]));
        let b = arena.register_token(Address::from([21u8; 20]));
        let funded = MIN_HOP_TOKEN_BALANCE;
        let dust = MIN_HOP_TOKEN_BALANCE - U256::from(1u64);
        let pool = arena.register_pool(
            Address::from([22u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: funded,
                reserve1: dust,
                fee: U256::from(30u8),
                fee_denominator: U256::from(10_000u64),
                block_timestamp_last: 1,
            })),
        );
        let metas = [pool_meta_from_pair(pool, ProtocolType::UniswapV2, a, b, 30)];
        let graph = build_graph(&arena, &metas);
        assert!(graph.adjacency[a.0 as usize].is_empty());
        assert!(graph.adjacency[b.0 as usize].is_empty());
    }

    #[test]
    fn active_pool_count_ignores_dead_only_pools() {
        let mut arena = StateArena::default();
        let a = arena.register_token(Address::from([30u8; 20]));
        let b = arena.register_token(Address::from([31u8; 20]));
        let funded = MIN_HOP_TOKEN_BALANCE;
        let live_pool = arena.register_pool(
            Address::from([32u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: funded,
                reserve1: funded + U256::from(1u64),
                fee: U256::from(30u8),
                fee_denominator: U256::from(10_000u64),
                block_timestamp_last: 1,
            })),
        );
        let dust_pool = arena.register_pool(
            Address::from([33u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: funded,
                reserve1: funded + U256::from(1u64),
                fee: U256::from(30u8),
                fee_denominator: U256::from(10_000u64),
                block_timestamp_last: 1,
            })),
        );
        let metas = [
            pool_meta_from_pair(live_pool, ProtocolType::UniswapV2, a, b, 30),
            pool_meta_from_pair(dust_pool, ProtocolType::UniswapV2, a, b, 30),
        ];
        let mut graph = build_graph(&arena, &metas);
        assert_eq!(graph.active_pool_count(), 2);
        let dust_idx = dust_pool.0 as usize;
        for &(adj_idx, edge_pos) in &graph.pool_edge_positions[dust_idx].clone() {
            let ge = &mut graph.adjacency[adj_idx][edge_pos];
            ge.log_weight = DEAD_EDGE_LOG_WEIGHT;
            ge.ratio = U256::ZERO;
        }
        assert_eq!(graph.active_pool_count(), 1);
        assert!(graph.pool_has_live_edges(live_pool));
        assert!(!graph.pool_has_live_edges(dust_pool));
    }

    #[test]
    fn partial_rescore_updates_only_dirty_pool_edges() {
        let mut arena = StateArena::default();
        let a = arena.register_token(Address::from([40u8; 20]));
        let b = arena.register_token(Address::from([41u8; 20]));
        let c = arena.register_token(Address::from([42u8; 20]));
        let funded = MIN_HOP_TOKEN_BALANCE;
        let pool0 = arena.register_pool(
            Address::from([43u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: funded,
                reserve1: funded + U256::from(1u64),
                fee: U256::from(30u8),
                fee_denominator: U256::from(10_000u64),
                block_timestamp_last: 1,
            })),
        );
        let pool1 = arena.register_pool(
            Address::from([44u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: funded,
                reserve1: funded + U256::from(100u64),
                fee: U256::from(30u8),
                fee_denominator: U256::from(10_000u64),
                block_timestamp_last: 1,
            })),
        );
        let metas = [
            pool_meta_from_pair(pool0, ProtocolType::UniswapV2, a, b, 30),
            pool_meta_from_pair(pool1, ProtocolType::UniswapV2, b, c, 30),
        ];
        let mut graph = build_graph(&arena, &metas);
        let before = graph.adjacency[a.0 as usize][0].log_weight;
        arena.register_pool(
            Address::from([43u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: funded,
                reserve1: funded + U256::from(50u64),
                fee: U256::from(30u8),
                fee_denominator: U256::from(10_000u64),
                block_timestamp_last: 2,
            })),
        );
        let untouched = graph.adjacency[b.0 as usize]
            .iter()
            .find(|ge| ge.edge.pool_index == pool1)
            .expect("pool1 edge")
            .log_weight;
        rescore_pools_in_place(&arena, &mut graph, &[pool0]);
        let after = graph.adjacency[a.0 as usize][0].log_weight;
        let still = graph.adjacency[b.0 as usize]
            .iter()
            .find(|ge| ge.edge.pool_index == pool1)
            .expect("pool1 edge")
            .log_weight;
        assert_ne!(before, after);
        assert_eq!(untouched, still);
    }

    #[test]
    fn graph_admits_tradable_pool_without_priced_tokens() {
        let mut arena = StateArena::default();
        let a = arena.register_token(Address::from([4u8; 20]));
        let b = arena.register_token(Address::from([5u8; 20]));
        let pool = arena.register_pool(
            Address::from([6u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: MIN_HOP_TOKEN_BALANCE,
                reserve1: MIN_HOP_TOKEN_BALANCE + U256::from(1u64),
                fee: U256::from(30u8),
                fee_denominator: U256::from(10_000u64),
                block_timestamp_last: 1,
            })),
        );
        let metas = [pool_meta_from_pair(pool, ProtocolType::UniswapV2, a, b, 30)];

        let graph = build_graph(&arena, &metas);

        assert_eq!(graph.pool_edge_positions[pool.0 as usize].len(), 2);
        assert_eq!(graph.adjacency[a.0 as usize].len(), 1);
        assert_eq!(graph.adjacency[b.0 as usize].len(), 1);
    }
}
