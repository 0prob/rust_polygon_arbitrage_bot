use crate::core::math::fixed_point::ONE;
use crate::core::math::fixed_point::edge_log_weight_from_ratio;
use crate::core::types::{Edge, PoolIndex, PoolState, ProtocolType, TokenIndex};
use crate::pipeline::arena::StateArena;
use crate::pipeline::cycle_finder::DEAD_EDGE_LOG_WEIGHT;
use crate::pipeline::spot_price::{compute_edge_log_weight, compute_edge_ratio};
use crate::pipeline::types::{
    GraphEdge, GraphHopPhase, PoolMeta, RoutingGraph, VirtualPoolHub,
};
use alloy::primitives::U256;
use rayon::prelude::*;
use smallvec::SmallVec;

/// Max parallel edges per `(token_in, token_out, protocol)` after rescoring.
const MAX_PARALLEL_EDGES_PER_PAIR: usize = 2;

/// Pending token-in context when traversing a virtual pool hub node.
#[derive(Debug, Clone, Copy)]
pub struct PendingHubSwap {
    pub pool_index: PoolIndex,
    pub token_in: TokenIndex,
    pub token_in_idx: u8,
    pub protocol: ProtocolType,
    pub fee_bps: u32,
}

#[inline]
fn pair_zero_for_one(token_in_idx: u8) -> bool {
    token_in_idx == 0
}

#[inline]
fn multi_zero_for_one(token_in_idx: u8, token_out_idx: u8) -> bool {
    token_in_idx < token_out_idx
}

#[inline]
fn uses_hub_spoke(meta: &PoolMeta) -> bool {
    meta.protocol == ProtocolType::UniswapV4 || meta.tokens.len() > 2
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

pub(crate) fn funded_token_indices(state: &PoolState, meta: &PoolMeta) -> SmallVec<[u8; 8]> {
    let mut out = SmallVec::new();
    for i in 0..meta.tokens.len() {
        if meta.bpt_index == Some(i) {
            continue;
        }
        if state.hop_token_funded(i) {
            out.push(i as u8);
        }
    }
    out
}

fn ensure_per_pool_hub(
    graph: &mut RoutingGraph,
    pool_index: PoolIndex,
    protocol: ProtocolType,
    exit_legs: SmallVec<[u8; 8]>,
) -> u32 {
    let node = graph.token_count + graph.virtual_hubs.len() as u32;
    graph.virtual_hubs.push(VirtualPoolHub {
        pool_index,
        protocol,
        exit_legs,
        v4_singleton: false,
    });
    graph.adjacency.push(Vec::new());
    node
}

fn ensure_v4_singleton_hub(graph: &mut RoutingGraph) -> u32 {
    if let Some(node) = graph.v4_singleton_hub {
        return node;
    }
    let node = graph.token_count + graph.virtual_hubs.len() as u32;
    graph.virtual_hubs.push(VirtualPoolHub {
        pool_index: PoolIndex(0),
        protocol: ProtocolType::UniswapV4,
        exit_legs: SmallVec::new(),
        v4_singleton: true,
    });
    graph.v4_singleton_hub = Some(node);
    graph.adjacency.push(Vec::new());
    node
}

fn push_enter_edge(
    graph: &mut RoutingGraph,
    from_token: TokenIndex,
    hub_node: u32,
    pool_index: PoolIndex,
    token_in_idx: u8,
    protocol: ProtocolType,
    fee_bps: u32,
) {
    graph.push_edge_at(
        from_token.0,
        GraphEdge {
            edge: Edge {
                pool_index,
                token_in: from_token,
                token_out: from_token,
                token_in_idx,
                token_out_idx: token_in_idx,
                protocol,
                fee_bps,
                zero_for_one: pair_zero_for_one(token_in_idx),
            },
            phase: GraphHopPhase::EnterPool,
            target_node: hub_node,
            log_weight: 0.0,
            ratio: ONE,
        },
    );
}

fn push_exit_edge(
    graph: &mut RoutingGraph,
    hub_node: u32,
    pool_index: PoolIndex,
    to_token: TokenIndex,
    token_out_idx: u8,
    protocol: ProtocolType,
    fee_bps: u32,
) {
    graph.push_edge_at(
        hub_node,
        GraphEdge {
            edge: Edge {
                pool_index,
                token_in: to_token,
                token_out: to_token,
                token_in_idx: token_out_idx,
                token_out_idx,
                protocol,
                fee_bps,
                zero_for_one: pair_zero_for_one(token_out_idx),
            },
            phase: GraphHopPhase::ExitPool,
            target_node: to_token.0,
            log_weight: 0.0,
            ratio: ONE,
        },
    );
}

fn attach_hub_spoke_pool(graph: &mut RoutingGraph, meta: &PoolMeta, state: &PoolState) {
    let funded = funded_token_indices(state, meta);
    if funded.len() < 2 {
        return;
    }

    let hub_node = if meta.protocol == ProtocolType::UniswapV4 {
        ensure_v4_singleton_hub(graph)
    } else {
        ensure_per_pool_hub(graph, meta.pool_index, meta.protocol, funded.clone())
    };

    for &leg in &funded {
        let token = meta.tokens[leg as usize];
        push_enter_edge(
            graph,
            token,
            hub_node,
            meta.pool_index,
            leg,
            meta.protocol,
            meta.fee_bps,
        );
    }

    if meta.protocol != ProtocolType::UniswapV4 {
        for &leg in &funded {
            let token = meta.tokens[leg as usize];
            push_exit_edge(
                graph,
                hub_node,
                meta.pool_index,
                token,
                leg,
                meta.protocol,
                meta.fee_bps,
            );
        }
    }
}

/// Resolve a hub enter+exit pair into a concrete swap edge with live weight.
#[must_use]
pub fn resolve_lazy_swap_edge(
    arena: &StateArena,
    pending: PendingHubSwap,
    token_out: TokenIndex,
    token_out_idx: u8,
) -> Option<(Edge, f64, U256)> {
    let state = arena.pool_state(pending.pool_index)?;
    if !state.hop_pair_routable(pending.token_in_idx as usize, token_out_idx as usize) {
        return None;
    }
    let edge = Edge {
        pool_index: pending.pool_index,
        token_in: pending.token_in,
        token_out,
        token_in_idx: pending.token_in_idx,
        token_out_idx,
        protocol: pending.protocol,
        fee_bps: pending.fee_bps,
        zero_for_one: multi_zero_for_one(pending.token_in_idx, token_out_idx),
    };
    let ratio = compute_edge_ratio(arena, &edge);
    if ratio.is_zero() {
        return None;
    }
    let mut log_weight = edge_log_weight_from_ratio(ratio);
    if !log_weight.is_finite() {
        log_weight = compute_edge_log_weight(edge.fee_bps);
    }
    Some((edge, log_weight, ratio))
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
        2 if !uses_hub_spoke(meta) => {
            state.hop_pair_routable(0, 1) || state.hop_pair_routable(1, 0)
        }
        _ => {
            let funded = funded_token_indices(state, meta);
            for (pos, &i) in funded.iter().enumerate() {
                for &j in &funded[pos + 1..] {
                    if state.hop_pair_routable(i as usize, j as usize)
                        || state.hop_pair_routable(j as usize, i as usize)
                    {
                        return true;
                    }
                }
            }
            false
        }
    }
}

fn thin_parallel_edges_in_place(adj: &mut Vec<GraphEdge>, max_per_pair: usize) {
    if max_per_pair == 0 {
        return;
    }
    use std::cmp::Reverse;

    let drained: Vec<GraphEdge> = adj.drain(..).collect();
    let (mut direct, mut hub_legs): (Vec<GraphEdge>, Vec<GraphEdge>) = drained
        .into_iter()
        .partition(|ge| ge.phase == GraphHopPhase::Direct);
    if direct.len() <= max_per_pair {
        direct.append(&mut hub_legs);
        *adj = direct;
        return;
    }

    direct.sort_by(|a, b| {
        (
            a.edge.token_out.0,
            a.edge.protocol as u8,
            Reverse(a.ratio),
            a.edge.pool_index.0,
        )
            .cmp(&(
                b.edge.token_out.0,
                b.edge.protocol as u8,
                Reverse(b.ratio),
                b.edge.pool_index.0,
            ))
    });
    let mut out_len = 0usize;
    let mut group_kept = 0usize;
    let mut cur_key = (u32::MAX, u8::MAX);
    for i in 0..direct.len() {
        let key = (direct[i].edge.token_out.0, direct[i].edge.protocol as u8);
        if key != cur_key {
            cur_key = key;
            group_kept = 0;
        }
        if group_kept < max_per_pair {
            if out_len != i {
                direct.swap(out_len, i);
            }
            out_len += 1;
            group_kept += 1;
        }
    }
    direct.truncate(out_len);
    direct.append(&mut hub_legs);
    *adj = direct;
}

fn thin_parallel_edges_in_graph(graph: &mut RoutingGraph) {
    let token_slots = graph.token_count as usize;
    for adj in graph.adjacency.iter_mut().take(token_slots) {
        thin_parallel_edges_in_place(adj, MAX_PARALLEL_EDGES_PER_PAIR);
        sort_adjacency_edges(adj);
    }
    for adj in graph.adjacency.iter_mut().skip(token_slots) {
        sort_adjacency_edges(adj);
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

        if uses_hub_spoke(meta) {
            attach_hub_spoke_pool(&mut graph, meta, state);
        } else if meta.tokens.len() == 2 {
            for edge in edges_for_pair(
                meta.pool_index,
                meta.protocol,
                meta.tokens[0],
                meta.tokens[1],
                meta.fee_bps,
                Some(state),
            ) {
                graph.push_edge_at(
                    edge.token_in.0,
                    GraphEdge {
                        edge,
                        phase: GraphHopPhase::Direct,
                        target_node: edge.token_out.0,
                        log_weight: 0.0,
                        ratio: U256::ZERO,
                    },
                );
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
    adjacency.par_iter_mut().for_each(|adj| {
        for ge in adj.iter_mut() {
            rescore_graph_edge(arena, ge);
        }
        sort_adjacency_edges(adj);
    });
}

fn sort_adjacency_edges(adj: &mut [GraphEdge]) {
    adj.sort_by(|a, b| a.log_weight.total_cmp(&b.log_weight));
}

fn rebuild_pool_edge_positions_full(graph: &mut RoutingGraph) {
    let mut max_pool = 0usize;
    let all_empty = graph.adjacency.iter().all(Vec::is_empty);
    if all_empty {
        graph.pool_edge_positions.clear();
        return;
    }
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
    match ge.phase {
        GraphHopPhase::EnterPool | GraphHopPhase::ExitPool => {
            ge.log_weight = 0.0;
            ge.ratio = ONE;
            return 1;
        }
        GraphHopPhase::Direct => {}
    }
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
    ge.ratio = compute_edge_ratio(arena, &ge.edge);
    if ge.ratio.is_zero() {
        ge.log_weight = DEAD_EDGE_LOG_WEIGHT;
        return 1;
    }
    ge.log_weight = edge_log_weight_from_ratio(ge.ratio);
    if !ge.log_weight.is_finite() {
        ge.log_weight = compute_edge_log_weight(ge.edge.fee_bps);
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
    use crate::core::types::{BalancerPoolKind, BalancerPoolState, PoolState, V2PoolState};
    use alloy::primitives::{Address, U256};
    use std::sync::Arc;

    fn direct_ge(pool: u32, tin: u32, tout: u32, protocol: ProtocolType, ratio: u64) -> GraphEdge {
        GraphEdge {
            edge: Edge {
                pool_index: PoolIndex(pool),
                token_in: TokenIndex(tin),
                token_out: TokenIndex(tout),
                token_in_idx: 0,
                token_out_idx: 1,
                protocol,
                fee_bps: 30,
                zero_for_one: true,
            },
            phase: GraphHopPhase::Direct,
            target_node: tout,
            log_weight: -0.01,
            ratio: U256::from(ratio),
        }
    }

    #[test]
    fn thin_parallel_edges_keeps_top_two_by_ratio() {
        let mut adj = vec![
            direct_ge(1, 4, 5, ProtocolType::BalancerV2, 1_010_000_000_000_000_000),
            direct_ge(2, 4, 5, ProtocolType::BalancerV2, 1_020_000_000_000_000_000),
            direct_ge(3, 4, 5, ProtocolType::BalancerV2, 1_015_000_000_000_000_000),
            direct_ge(4, 4, 5, ProtocolType::BalancerV2, 1_005_000_000_000_000_000),
            direct_ge(10, 4, 6, ProtocolType::UniswapV3, 1_010_000_000_000_000_000),
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
    fn hub_spoke_balancer_uses_linear_edge_count() {
        let mut arena = StateArena::default();
        let tokens: Vec<TokenIndex> = (0u8..8)
            .map(|i| arena.register_token(Address::from([i; 20])))
            .collect();
        let funded = MIN_HOP_TOKEN_BALANCE;
        let pool = arena.register_pool(
            Address::from([9u8; 20]),
            Arc::new(PoolState::Balancer(BalancerPoolState {
                pool_id: None,
                tokens: (0u8..8).map(|b| Address::from([b; 20])).collect(),
                balances: vec![funded; 8],
                weights: vec![funded; 8],
                scaling_factors: vec![funded; 8],
                amp: funded,
                amp_precision: U256::from(1u64),
                fee: U256::ZERO,
                pool_type: BalancerPoolKind::Weighted,
                linear: None,
                bpt_index: None,
                is_updating: false,
                last_change_block: 0,
            })),
        );
        let meta = PoolMeta {
            pool_index: pool,
            protocol: ProtocolType::BalancerV2,
            tokens,
            fee_bps: 10,
            bpt_index: None,
            pool_id: None,
            protocol_label: None,
            pool_type: None,
            hooks: None,
            tick_spacing: None,
        };
        let graph = build_graph(&arena, std::slice::from_ref(&meta));
        assert_eq!(graph.virtual_hubs.len(), 1);
        let hub = graph.token_count;
        let enter = graph
            .adjacency
            .iter()
            .take(graph.token_count as usize)
            .map(|adj| adj.iter().filter(|ge| ge.phase == GraphHopPhase::EnterPool).count())
            .sum::<usize>();
        let exit = graph.adjacency[hub as usize]
            .iter()
            .filter(|ge| ge.phase == GraphHopPhase::ExitPool)
            .count();
        assert_eq!(enter, 8);
        assert_eq!(exit, 8);
        assert_eq!(enter + exit, 16);
    }

    #[test]
    fn hub_spoke_skips_underfunded_legs() {
        let mut arena = StateArena::default();
        let tokens = [
            TokenIndex(0),
            TokenIndex(1),
            TokenIndex(2),
            TokenIndex(3),
            TokenIndex(4),
        ];
        for (i, token) in tokens.iter().enumerate() {
            arena.register_token(Address::from([i as u8; 20]));
            let _ = token;
        }
        let a = arena.register_token(Address::from([10u8; 20]));
        let b = arena.register_token(Address::from([11u8; 20]));
        let c = arena.register_token(Address::from([12u8; 20]));
        let d = arena.register_token(Address::from([13u8; 20]));
        let e = arena.register_token(Address::from([14u8; 20]));
        let funded = MIN_HOP_TOKEN_BALANCE;
        let dust = MIN_HOP_TOKEN_BALANCE - U256::from(1u64);
        let pool = arena.register_pool(
            Address::from([15u8; 20]),
            Arc::new(PoolState::Balancer(BalancerPoolState {
                pool_id: None,
                tokens: (0u8..5).map(|b| Address::from([b; 20])).collect(),
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
            })),
        );
        let meta = PoolMeta {
            pool_index: pool,
            protocol: ProtocolType::BalancerV2,
            tokens: vec![a, b, c, d, e],
            fee_bps: 10,
            bpt_index: None,
            pool_id: None,
            protocol_label: None,
            pool_type: None,
            hooks: None,
            tick_spacing: None,
        };
        let graph = build_graph(&arena, std::slice::from_ref(&meta));
        let enter = graph
            .adjacency
            .iter()
            .take(graph.token_count as usize)
            .flat_map(|adj| adj.iter())
            .filter(|ge| ge.phase == GraphHopPhase::EnterPool)
            .count();
        assert_eq!(enter, 2);
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
        let priced = arena.register_token(Address::from([1u8; 20]));
        let tail = arena.register_token(Address::from([2u8; 20]));
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