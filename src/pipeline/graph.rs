use std::sync::Arc;
use std::time::Duration;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::core::constants::MAX_POOL_TOKENS;
use crate::core::math::fixed_point::ONE;
use crate::core::math::fixed_point::edge_log_weight_from_ratio;
use crate::core::types::{Edge, PoolIndex, PoolState, ProtocolType, TokenIndex};
use crate::pipeline::arena::StateArena;
use crate::pipeline::cycle_finder::DEAD_EDGE_LOG_WEIGHT;
use crate::pipeline::local_sim::{protocol_matches_pool_state, sync_edge_fee_bps_from_state};
use crate::pipeline::spot_price::spot_price_from_state;
use crate::pipeline::spot_price::{compute_edge_log_weight, compute_edge_ratio};
use crate::pipeline::types::{GraphEdge, GraphHopPhase, PoolMeta, RoutingGraph, VirtualPoolHub};
use crate::services::execution::flash_liquidity::{
    FlashLiquiditySnapshot, token_eligible_for_flash_borrow_graph,
};
use crate::services::oracle::has_reliable_matic_rate;
use alloy::primitives::{Address, U256};
use rayon::prelude::*;
use smallvec::SmallVec;

/// Max parallel edges per `(token_in, token_out, protocol)` after rescoring.
const MAX_PARALLEL_EDGES_PER_PAIR: usize = 2;

#[derive(Clone)]
pub struct GraphBuildGate {
    pub token_to_matic_rates: Arc<FxHashMap<TokenIndex, U256>>,
    pub flash: Arc<FlashLiquiditySnapshot>,
    pub flash_ttl: Duration,
    /// Spoke tokens reachable from priced/hub tokens via shared pools (connectivity only).
    pub spoke_connectivity: Option<Arc<FxHashSet<Address>>>,
}

impl GraphBuildGate {
    #[must_use]
    pub fn active(&self) -> bool {
        !self.token_to_matic_rates.is_empty()
    }
}

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

/// Uniswap V3/V4: `zeroForOne` iff `token_in` is the lower pool address (on-chain token0).
#[inline]
fn cl_zero_for_one_from_addresses(
    arena: &StateArena,
    token_in: TokenIndex,
    token_out: TokenIndex,
) -> Option<bool> {
    match (
        arena.token_address(token_in),
        arena.token_address(token_out),
    ) {
        (Some(a_in), Some(a_out)) => Some(a_in < a_out),
        _ => None,
    }
}

/// V2/V3/V4: `zeroForOne` / reserve0-in iff `token_in` is the lower pool address.
#[inline]
fn apply_cl_zero_for_one(arena: &StateArena, edge: &mut Edge) {
    if matches!(
        edge.protocol,
        ProtocolType::UniswapV2 | ProtocolType::UniswapV3 | ProtocolType::UniswapV4
    ) && let Some(zfo) = cl_zero_for_one_from_addresses(arena, edge.token_in, edge.token_out)
    {
        edge.zero_for_one = zfo;
    }
}

#[inline]
fn multi_zero_for_one(token_in_idx: u8, token_out_idx: u8) -> bool {
    token_in_idx < token_out_idx
}

#[inline]
fn uses_hub_spoke(meta: &PoolMeta) -> bool {
    // Balancer/Woofi always hub-spoke: discovery meta often lists 2 tokens while
    // the vault has N>2 — Direct path only wires vault[0]↔[1] and can leave
    // phantom idxs on cached cycles (live: multi realign foreign tout).
    matches!(
        meta.protocol,
        ProtocolType::UniswapV4 | ProtocolType::BalancerV2 | ProtocolType::Woofi
    ) || meta.tokens.len() > 2
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

pub(crate) fn funded_token_indices(
    state: &PoolState,
    meta: &PoolMeta,
) -> SmallVec<[u8; MAX_POOL_TOKENS]> {
    let mut out = SmallVec::new();
    // Balancer/Woofi indices must follow vault/oracle token order (state.tokens), not
    // discovery meta order — a mismatch yields phantom local sim and BAL#521 on-chain.
    let (n, bpt) = match state {
        PoolState::Balancer(b) if !b.tokens.is_empty() => {
            (b.tokens.len(), b.bpt_index.or(meta.bpt_index))
        }
        PoolState::Woofi(w) if !w.tokens.is_empty() => (w.tokens.len(), None),
        _ => (meta.tokens.len(), meta.bpt_index),
    };
    for i in 0..n {
        if bpt == Some(i) {
            continue;
        }
        if state.hop_token_funded(i) {
            out.push(i as u8);
        }
    }
    out
}

/// Resolve the graph `TokenIndex` for a pool-local leg.
/// Prefer live state token addresses (vault order) when present.
#[must_use]
pub(crate) fn routing_token_at_leg(
    arena: &StateArena,
    state: &PoolState,
    meta: &PoolMeta,
    leg: usize,
) -> Option<TokenIndex> {
    match state {
        PoolState::Balancer(b) if !b.tokens.is_empty() => {
            let addr = *b.tokens.get(leg)?;
            arena.address_to_token().get(&addr).copied()
        }
        PoolState::Woofi(w) if !w.tokens.is_empty() => {
            let addr = *w.tokens.get(leg)?;
            arena.address_to_token().get(&addr).copied()
        }
        _ => meta.tokens.get(leg).copied(),
    }
}

fn ensure_per_pool_hub(
    graph: &mut RoutingGraph,
    pool_index: PoolIndex,
    protocol: ProtocolType,
    exit_legs: SmallVec<[u8; MAX_POOL_TOKENS]>,
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

fn attach_hub_spoke_pool(
    graph: &mut RoutingGraph,
    arena: &StateArena,
    meta: &PoolMeta,
    state: &PoolState,
) {
    let funded: SmallVec<[(u8, TokenIndex); MAX_POOL_TOKENS]> = funded_token_indices(state, meta)
        .into_iter()
        .filter_map(|leg| {
            routing_token_at_leg(arena, state, meta, leg as usize).map(|token| (leg, token))
        })
        .collect();
    if funded.len() < 2 {
        return;
    }

    let exit_legs: SmallVec<[u8; MAX_POOL_TOKENS]> = funded.iter().map(|(leg, _)| *leg).collect();
    let hub_node = if meta.protocol == ProtocolType::UniswapV4 {
        ensure_v4_singleton_hub(graph)
    } else {
        ensure_per_pool_hub(graph, meta.pool_index, meta.protocol, exit_legs)
    };

    for &(leg, token) in &funded {
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
        for &(leg, token) in &funded {
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
    // Prefer pending idxs when they already match vault addresses; otherwise remap
    // by address (hot-cache can reshuffle getPoolTokens vs discovery meta).
    let (resolved_in_idx, resolved_out_idx) = match state {
        PoolState::Balancer(b) if !b.tokens.is_empty() => {
            let tin = arena.token_address(pending.token_in)?;
            let tout = arena.token_address(token_out)?;
            if b.tokens.get(pending.token_in_idx as usize).copied() == Some(tin)
                && b.tokens.get(token_out_idx as usize).copied() == Some(tout)
            {
                (pending.token_in_idx, token_out_idx)
            } else {
                crate::pipeline::local_sim::resolve_multi_token_vault_indices(&b.tokens, tin, tout)?
            }
        }
        PoolState::Woofi(w) if !w.tokens.is_empty() => {
            let tin = arena.token_address(pending.token_in)?;
            let tout = arena.token_address(token_out)?;
            if w.tokens.get(pending.token_in_idx as usize).copied() == Some(tin)
                && w.tokens.get(token_out_idx as usize).copied() == Some(tout)
            {
                (pending.token_in_idx, token_out_idx)
            } else {
                crate::pipeline::local_sim::resolve_multi_token_vault_indices(&w.tokens, tin, tout)?
            }
        }
        _ => (pending.token_in_idx, token_out_idx),
    };
    if !state.hop_pair_routable(resolved_in_idx as usize, resolved_out_idx as usize) {
        return None;
    }
    let zero_for_one = match pending.protocol {
        ProtocolType::UniswapV3 | ProtocolType::UniswapV4 => {
            cl_zero_for_one_from_addresses(arena, pending.token_in, token_out)?
        }
        _ => multi_zero_for_one(resolved_in_idx, resolved_out_idx),
    };
    let edge = Edge {
        pool_index: pending.pool_index,
        token_in: pending.token_in,
        token_out,
        token_in_idx: resolved_in_idx,
        token_out_idx: resolved_out_idx,
        protocol: pending.protocol,
        fee_bps: pending.fee_bps,
        zero_for_one,
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
    count_graph_eligible_pools_with_gate(arena, pools, None)
}

#[must_use]
pub fn count_graph_eligible_pools_with_gate(
    arena: &StateArena,
    pools: &[PoolMeta],
    gate: Option<&GraphBuildGate>,
) -> usize {
    pools
        .iter()
        .filter(|meta| pool_has_admissible_edges(arena, meta, gate))
        .count()
}

#[must_use]
pub fn count_graph_eligible_unpriced_pools(
    arena: &StateArena,
    pools: &[PoolMeta],
    gate: &GraphBuildGate,
) -> usize {
    if !gate.active() {
        return 0;
    }
    pools
        .iter()
        .filter(|meta| {
            let bpt_index = meta.bpt_index;
            let has_priced_token = meta.tokens.iter().enumerate().any(|(i, &token)| {
                bpt_index != Some(i)
                    && has_reliable_matic_rate(token, gate.token_to_matic_rates.as_ref())
            });
            !has_priced_token && pool_has_admissible_edges(arena, meta, Some(gate))
        })
        .count()
}

fn direct_pair_has_marginal_spot(
    state: &PoolState,
    protocol: ProtocolType,
    fee_bps: u32,
    input_decimals: (u8, u8),
) -> bool {
    let (dec0, dec1) = input_decimals;
    for (t_in_idx, t_out_idx, zfo, tin_dec) in [(0u8, 1u8, true, dec0), (1u8, 0u8, false, dec1)] {
        if !state.hop_pair_routable(t_in_idx as usize, t_out_idx as usize) {
            continue;
        }
        let edge = Edge {
            pool_index: PoolIndex(0),
            token_in: TokenIndex(u32::from(t_in_idx)),
            token_out: TokenIndex(u32::from(t_out_idx)),
            token_in_idx: t_in_idx,
            token_out_idx: t_out_idx,
            protocol,
            fee_bps,
            zero_for_one: zfo,
        };
        if spot_price_from_state(state, &edge, tin_dec) > 0.0 {
            return true;
        }
    }
    false
}

/// Tradable pool would emit at least one routable graph edge (arena sync gate).
#[must_use]
pub fn pool_state_graph_eligible(
    arena: Option<&StateArena>,
    state: &PoolState,
    protocol: ProtocolType,
    token_count: usize,
    bpt_index: Option<usize>,
    fee_bps: u32,
    pair_input_decimals: Option<(u8, u8)>,
) -> bool {
    if !state.is_tradable() || token_count < 2 {
        return false;
    }
    let hub_spoke = protocol == ProtocolType::UniswapV4 || token_count > 2;
    if token_count == 2 && !hub_spoke {
        let Some(decimals) = pair_input_decimals else {
            return false;
        };
        return direct_pair_has_marginal_spot(state, protocol, fee_bps, decimals);
    }
    let _ = arena;
    let mut funded = SmallVec::<[u8; MAX_POOL_TOKENS]>::new();
    for i in 0..token_count {
        if bpt_index == Some(i) {
            continue;
        }
        if state.hop_token_funded(i) {
            funded.push(i as u8);
        }
    }
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

#[inline]
fn pool_has_admissible_edges(
    arena: &StateArena,
    meta: &PoolMeta,
    gate: Option<&GraphBuildGate>,
) -> bool {
    let Some(state) = arena.pool_state(meta.pool_index) else {
        return false;
    };
    // Meta protocol from discovery can disagree with the fetched arena variant.
    // Prefer arena family when it yields a matching tag (live: attach_fail=all
    // on V2-meta/V3-state skew after strip).
    let protocol = if protocol_matches_pool_state(meta.protocol, state) {
        meta.protocol
    } else {
        let healed = crate::pipeline::local_sim::protocol_from_pool_state(state, meta.protocol);
        if !protocol_matches_pool_state(healed, state) {
            return false;
        }
        healed
    };
    let token_count = match state {
        PoolState::Balancer(b) if !b.tokens.is_empty() => b.tokens.len(),
        PoolState::Woofi(w) if !w.tokens.is_empty() => w.tokens.len(),
        _ => meta.tokens.len(),
    };
    let bpt_index = meta.bpt_index.or(match state {
        PoolState::Balancer(b) => b.bpt_index,
        _ => None,
    });
    // Vault/oracle order for Balancer/Woofi — discovery meta order can be reversed.
    let pair_input_decimals = routing_token_at_leg(arena, state, meta, 0)
        .zip(routing_token_at_leg(arena, state, meta, 1))
        .map(|(t0, t1)| (arena.token_decimals(t0), arena.token_decimals(t1)));
    if !pool_state_graph_eligible(
        Some(arena),
        state,
        protocol,
        token_count,
        bpt_index,
        meta.fee_bps,
        pair_input_decimals,
    ) {
        return false;
    }
    // Pricing/flash gate: keep pools with a priced token, or an unpriced flash/hub borrow leg.
    let Some(gate) = gate.filter(|g| g.active()) else {
        return true;
    };
    let has_priced_token = meta.tokens.iter().enumerate().any(|(i, &token)| {
        bpt_index != Some(i) && has_reliable_matic_rate(token, gate.token_to_matic_rates.as_ref())
    });
    if has_priced_token {
        return true;
    }
    if meta.tokens.iter().enumerate().any(|(i, &token)| {
        bpt_index != Some(i)
            && arena.token_address(token).is_some_and(|addr| {
                token_eligible_for_flash_borrow_graph(addr, gate.flash.as_ref(), gate.flash_ttl)
            })
    }) {
        return true;
    }
    // Hub-spoke connectivity (no fake rates): admit pools that touch the expanded set.
    gate.spoke_connectivity.as_ref().is_some_and(|conn| {
        meta.tokens.iter().enumerate().any(|(i, &token)| {
            bpt_index != Some(i)
                && arena
                    .token_address(token)
                    .is_some_and(|a| conn.contains(&a))
        })
    })
}

fn pool_worth_capped_attach(
    arena: &StateArena,
    meta: &PoolMeta,
    gate: Option<&GraphBuildGate>,
) -> bool {
    let Some(gate) = gate.filter(|g| g.active()) else {
        return true;
    };
    let bpt_index = meta.bpt_index.or_else(|| {
        arena.pool_state(meta.pool_index).and_then(|s| match s {
            PoolState::Balancer(b) => b.bpt_index,
            _ => None,
        })
    });
    if meta.tokens.iter().enumerate().any(|(i, &token)| {
        bpt_index != Some(i) && has_reliable_matic_rate(token, gate.token_to_matic_rates.as_ref())
    }) {
        return true;
    }
    if meta.tokens.iter().enumerate().any(|(i, &token)| {
        bpt_index != Some(i)
            && arena.token_address(token).is_some_and(|addr| {
                token_eligible_for_flash_borrow_graph(addr, gate.flash.as_ref(), gate.flash_ttl)
            })
    }) {
        return true;
    }
    gate.spoke_connectivity.as_ref().is_some_and(|conn| {
        meta.tokens.iter().enumerate().any(|(i, &token)| {
            bpt_index != Some(i)
                && arena
                    .token_address(token)
                    .is_some_and(|addr| conn.contains(&addr))
        })
    })
}

/// Returns true when parallel thinning removed at least one edge.
fn thin_parallel_edges_in_place(adj: &mut Vec<GraphEdge>, max_per_pair: usize) -> bool {
    use std::cmp::Reverse;

    let before_len = adj.len();
    let drained = std::mem::take(adj);
    let (direct, mut hub_legs): (Vec<GraphEdge>, Vec<GraphEdge>) = drained
        .into_iter()
        .partition(|ge| ge.phase == GraphHopPhase::Direct);
    let (mut live_direct, mut stubs): (Vec<GraphEdge>, Vec<GraphEdge>) = direct
        .into_iter()
        .partition(crate::pipeline::cycle_finder::is_live_graph_edge);
    if max_per_pair == 0 {
        live_direct.append(&mut stubs);
        live_direct.append(&mut hub_legs);
        *adj = live_direct;
        return before_len != adj.len();
    }
    if live_direct.len() <= max_per_pair {
        live_direct.append(&mut stubs);
        live_direct.append(&mut hub_legs);
        *adj = live_direct;
        return before_len != adj.len();
    }

    live_direct.sort_by(|a, b| {
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
    let mut retained = Vec::with_capacity(live_direct.len());
    let mut group_kept = 0usize;
    let mut cur_key = (u32::MAX, u8::MAX);
    for mut edge in live_direct {
        let key = (edge.edge.token_out.0, edge.edge.protocol as u8);
        if key != cur_key {
            cur_key = key;
            group_kept = 0;
        }
        if group_kept < max_per_pair {
            retained.push(edge);
            group_kept += 1;
        } else {
            edge.ratio = U256::ZERO;
            edge.log_weight = DEAD_EDGE_LOG_WEIGHT;
            stubs.push(edge);
        }
    }
    retained.append(&mut stubs);
    retained.append(&mut hub_legs);
    *adj = retained;
    before_len != adj.len()
}

/// Drop dead/unprofitable direct edges, re-thin parallel routes, and refresh coverage.
///
/// `token_slots`: when `Some`, only those token adjacencies are thinned/sorted
/// (partial dirty rescore). Hub adjacencies are only fully re-sorted on a full
/// compact — partial path used to re-sort every hub every tick (live: 500+ hubs).
fn compact_token_adjacency(graph: &mut RoutingGraph, token_slots: Option<&[usize]>) {
    let token_count = graph.token_count as usize;
    let touch_all = token_slots.is_none();
    let mut topology_changed = graph.coverage.is_none();
    let mut reindex_pools = rustc_hash::FxHashSet::default();
    for adj_idx in 0..token_count {
        if !touch_all && !token_slots.is_some_and(|slots| slots.binary_search(&adj_idx).is_ok()) {
            continue;
        }
        if let Some(adj) = graph.adjacency.get_mut(adj_idx) {
            // Capture pool ids *before* thin so fully-dropped parallels still reindex
            // (live: stale pool_edge_positions pointed at thinned-away Directs).
            let pools_before: smallvec::SmallVec<[usize; 16]> =
                adj.iter().map(|ge| ge.edge.pool_index.0 as usize).collect();
            if thin_parallel_edges_in_place(adj, MAX_PARALLEL_EDGES_PER_PAIR) {
                topology_changed = true;
                for p in pools_before {
                    reindex_pools.insert(p);
                }
            }
            sort_adjacency_edges(adj);
            for ge in adj.iter() {
                reindex_pools.insert(ge.edge.pool_index.0 as usize);
            }
        }
    }
    if touch_all {
        for adj in graph.adjacency.iter_mut().skip(token_count) {
            sort_adjacency_edges(adj);
            for ge in adj.iter() {
                reindex_pools.insert(ge.edge.pool_index.0 as usize);
            }
        }
    }
    if !reindex_pools.is_empty() {
        rebuild_pool_edge_positions_for_pools(graph, reindex_pools);
    }
    if topology_changed {
        graph.coverage = Some(std::sync::Arc::new(
            crate::pipeline::cycle_finder::cycle_capable_coverage(graph),
        ));
    }
}

/// True when every Direct edge for `meta.pool_index` still matches meta token legs
/// (address-aware). Stale edges survive `pool_has_live_edges` after discovery meta
/// refresh (live: V3 tin=WMATIC tout=foreign vs meta=[WMATIC, other]).
fn pool_direct_edges_match_meta(graph: &RoutingGraph, arena: &StateArena, meta: &PoolMeta) -> bool {
    if meta.tokens.len() < 2 {
        return true;
    }
    let pool_idx = meta.pool_index.0 as usize;
    let Some(positions) = graph.pool_edge_positions.get(pool_idx) else {
        return true;
    };
    // Discovery meta order can disagree with vault/oracle routing legs used at attach.
    let mut ok: smallvec::SmallVec<[TokenIndex; 8]> = smallvec::SmallVec::from_slice(&meta.tokens);
    if let Some(state) = arena.pool_state(meta.pool_index) {
        for leg in 0..2 {
            if let Some(t) = routing_token_at_leg(arena, state, meta, leg) {
                if !ok.contains(&t) {
                    ok.push(t);
                }
            }
        }
    }
    let on_ok = |tok: TokenIndex| {
        ok.iter().any(|&t| {
            t == tok
                || (arena.token_address(t).is_some()
                    && arena.token_address(t) == arena.token_address(tok))
        })
    };
    for &(adj_idx, edge_pos) in positions {
        let Some(ge) = graph.adjacency.get(adj_idx).and_then(|a| a.get(edge_pos)) else {
            continue;
        };
        if ge.phase != GraphHopPhase::Direct {
            continue;
        }
        if !on_ok(ge.edge.token_in) || !on_ok(ge.edge.token_out) {
            return false;
        }
    }
    true
}

pub(crate) fn remove_pool_edges_from_graph(graph: &mut RoutingGraph, pool: PoolIndex) {
    let pool_idx = pool.0 as usize;
    // Use reverse index — full adjacency scan was O(all edges) per stale-meta strip
    // (live: attach_missing strip+rebuild thrash on large graphs).
    let positions = graph
        .pool_edge_positions
        .get(pool_idx)
        .cloned()
        .unwrap_or_default();
    if positions.is_empty() {
        return;
    }
    let mut reindex = rustc_hash::FxHashSet::default();
    reindex.insert(pool_idx);
    let mut adj_touched = rustc_hash::FxHashSet::default();
    for &(adj_idx, _) in &positions {
        adj_touched.insert(adj_idx);
    }
    let mut changed = false;
    for adj_idx in adj_touched {
        let Some(adj) = graph.adjacency.get_mut(adj_idx) else {
            continue;
        };
        let before = adj.len();
        adj.retain(|ge| ge.edge.pool_index != pool);
        if adj.len() != before {
            changed = true;
            for ge in adj.iter() {
                reindex.insert(ge.edge.pool_index.0 as usize);
            }
        }
    }
    if !changed {
        if let Some(slot) = graph.pool_edge_positions.get_mut(pool_idx) {
            slot.clear();
        }
        return;
    }
    // ponytail: leave hub slot; clear exit_legs so DFS won't fan out.
    if let Some(hub) = graph.virtual_hubs.iter_mut().find(|h| h.pool_index == pool) {
        hub.exit_legs.clear();
    }
    rebuild_pool_edge_positions_for_pools(graph, reindex);
    graph.coverage = None;
}

pub(crate) fn attach_pool_to_graph(
    graph: &mut RoutingGraph,
    arena: &StateArena,
    meta: &PoolMeta,
    gate: Option<&GraphBuildGate>,
) -> bool {
    if graph.pool_has_live_edges(meta.pool_index) {
        // ponytail: meta token refresh leaves phantom Direct edges; strip+rebuild.
        if pool_direct_edges_match_meta(graph, arena, meta) {
            return false;
        }
        remove_pool_edges_from_graph(graph, meta.pool_index);
    } else if graph.pool_has_edges(meta.pool_index) {
        // Dead stubs already present — dirty/force rescore prices them; strip+reattach
        // every LF was thrashing thousands of Directs (live: attached=2000+/admit).
        return false;
    }
    let Some(state) = arena
        .pool_state(meta.pool_index)
        .filter(|s| s.is_tradable())
    else {
        return false;
    };
    if !pool_has_admissible_edges(arena, meta, gate) {
        return false;
    }

    // Arena tokens are append-only; cached graphs may predate newer TokenIndex ids.
    graph.ensure_token_capacity(arena.token_count());

    if uses_hub_spoke(meta) {
        attach_hub_spoke_pool(graph, arena, meta, state);
    } else if meta.tokens.len() == 2 {
        // Prefer vault/oracle token order (Balancer/Woofi) over discovery meta order.
        let Some(token0) = routing_token_at_leg(arena, state, meta, 0) else {
            return false;
        };
        let Some(token1) = routing_token_at_leg(arena, state, meta, 1) else {
            return false;
        };
        if token0 == token1 {
            return false;
        }
        let edge_proto = if protocol_matches_pool_state(meta.protocol, state) {
            meta.protocol
        } else {
            crate::pipeline::local_sim::protocol_from_pool_state(state, meta.protocol)
        };
        for mut edge in edges_for_pair(
            meta.pool_index,
            edge_proto,
            token0,
            token1,
            meta.fee_bps,
            Some(state),
        ) {
            apply_cl_zero_for_one(arena, &mut edge);
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
    } else {
        return false;
    }
    // Direct edges stay ratio=0 until rescore; callers must rescore then check live.
    // Returning live-only made attach_missing/force_attach skip rescore (always false).
    graph
        .pool_edge_positions
        .get(meta.pool_index.0 as usize)
        .is_some_and(|p| !p.is_empty())
}

pub(crate) fn refresh_graph_cycle_coverage(graph: &mut RoutingGraph) {
    graph.coverage = Some(std::sync::Arc::new(
        crate::pipeline::cycle_finder::cycle_capable_coverage(graph),
    ));
}

fn finalize_graph_topology(arena: &StateArena, graph: &mut RoutingGraph) {
    rescore_graph_in_place(arena, graph);
    refresh_graph_cycle_coverage(graph);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphRescoreMode {
    Full,
    Partial,
}

#[derive(Debug, Clone, Default)]
pub struct GraphRescoreReport {
    pub mode: Option<GraphRescoreMode>,
    pub edges_touched: usize,
    pub dirty_pools: usize,
}

#[derive(Debug, Clone, Default)]
pub struct GraphTopologyStats {
    pub token_slots: usize,
    pub virtual_hubs: usize,
    pub live_direct_edges: usize,
    pub dead_direct_edges: usize,
    pub hub_leg_edges: usize,
    pub active_pools: usize,
    pub cycle_capable_pools: usize,
}

impl GraphTopologyStats {
    pub fn log_summary(&self, action: &str) {
        crate::info!(
            "graph {action}: tokens={} virtual_hubs={} live_direct={} dead_direct={} hub_legs={} active_pools={} cycle_capable={}",
            self.token_slots,
            self.virtual_hubs,
            self.live_direct_edges,
            self.dead_direct_edges,
            self.hub_leg_edges,
            self.active_pools,
            self.cycle_capable_pools,
        );
    }
}

/// Per-LF-tick attach budget. Remainder continues on later ticks via
/// `attach_catchup_pending`. Priced/flash-only (spoke waits for rebuild).
/// Live: 256/tick lost to arena growth (eligible 1.7k→7.4k/150s) — never drained.
/// Alias of [`crate::core::constants::ATTACH_BATCH_CAP`] for graph attach catch-up.
pub const ATTACH_MISSING_CAP: usize = crate::core::constants::ATTACH_BATCH_CAP;

#[derive(Debug, Clone, Default)]
pub struct GraphAttachReport {
    pub attached_pools: usize,
    /// Attached pools that have live edges after the batch rescore.
    pub live_after: usize,
    /// True when the per-tick cap stopped the pass with more missing pools left.
    pub hit_cap: bool,
    /// Priced/flash missing pools still absent after this pass (diag backlog).
    pub missing_after: usize,
    pub missing_sample: Option<PoolIndex>,
}

/// Single-pass adjacency scan for LF/HF diagnostics.
#[must_use]
pub fn topology_stats(graph: &RoutingGraph) -> GraphTopologyStats {
    use crate::pipeline::cycle_finder::is_live_graph_edge;

    let mut stats = GraphTopologyStats {
        token_slots: graph.token_count as usize,
        virtual_hubs: graph.virtual_hubs.len(),
        ..GraphTopologyStats::default()
    };
    for (adj_idx, adj) in graph.adjacency.iter().enumerate() {
        for ge in adj {
            match ge.phase {
                GraphHopPhase::Direct => {
                    if is_live_graph_edge(ge) {
                        stats.live_direct_edges += 1;
                    } else {
                        stats.dead_direct_edges += 1;
                    }
                }
                GraphHopPhase::EnterPool | GraphHopPhase::ExitPool => {
                    if adj_idx < stats.token_slots && is_live_graph_edge(ge) {
                        stats.hub_leg_edges += 1;
                    }
                }
            }
        }
    }
    stats.active_pools = graph.active_pool_count();
    stats.cycle_capable_pools = graph
        .coverage
        .as_ref()
        .map(|c| c.pool_indices.len())
        .unwrap_or(0);
    stats
}

/// Eligible arena pools that have no live edges in the cached graph yet.
#[must_use]
pub fn count_eligible_pools_missing_from_graph_with_gate(
    arena: &StateArena,
    pools: &[PoolMeta],
    graph: &RoutingGraph,
    gate: Option<&GraphBuildGate>,
) -> usize {
    pools
        .iter()
        .filter(|meta| {
            pool_has_admissible_edges(arena, meta, gate)
                && pool_worth_capped_attach(arena, meta, gate)
                && !graph.pool_has_live_edges(meta.pool_index)
                && !graph.pool_has_edges(meta.pool_index)
        })
        .count()
}

pub fn count_eligible_pools_missing_from_graph(
    arena: &StateArena,
    pools: &[PoolMeta],
    graph: &RoutingGraph,
) -> usize {
    count_eligible_pools_missing_from_graph_with_gate(arena, pools, graph, None)
}

#[must_use]
pub fn has_missing_eligible_pools(
    arena: &StateArena,
    pools: &[PoolMeta],
    graph: &RoutingGraph,
) -> bool {
    has_missing_eligible_pools_with_gate(arena, pools, graph, None)
}

#[must_use]
pub fn has_missing_eligible_pools_with_gate(
    arena: &StateArena,
    pools: &[PoolMeta],
    graph: &RoutingGraph,
    gate: Option<&GraphBuildGate>,
) -> bool {
    pools.iter().any(|meta| {
        if !pool_has_admissible_edges(arena, meta, gate) {
            return false;
        }
        if !pool_worth_capped_attach(arena, meta, gate) {
            return false;
        }
        // Stale Direct edges after meta refresh still count as "live" and
        // skipped the attach pass entirely (live: 0 graph rebuilds / 120s).
        if graph.pool_has_live_edges(meta.pool_index) {
            return !pool_direct_edges_match_meta(graph, arena, meta);
        }
        // Dead stubs are not "missing" — dirty/force rescore prices them.
        !graph.pool_has_edges(meta.pool_index)
    })
}

/// Attach pools that became tradable since the last connectivity build.
///
/// Rescoring alone never adds adjacency for new arena members; without this pass
/// V4 (and other) pools can sit in the arena with `no_graph` until a full rebuild.
#[must_use]
pub fn attach_missing_eligible_pools(
    arena: &StateArena,
    graph: &mut RoutingGraph,
    pools: &[PoolMeta],
) -> GraphAttachReport {
    attach_missing_eligible_pools_with_gate(arena, graph, pools, None)
}

#[must_use]
pub fn attach_missing_eligible_pools_with_gate(
    arena: &StateArena,
    graph: &mut RoutingGraph,
    pools: &[PoolMeta],
    gate: Option<&GraphBuildGate>,
) -> GraphAttachReport {
    // Grow once for the whole batch (attach_pool also ensures; this avoids
    // repeated layout shifts when many late tokens arrive in one tick).
    graph.ensure_token_capacity(arena.token_count());
    let mut attached_pools: Vec<PoolIndex> = Vec::new();
    // ponytail: uncapped growth attaches were 2k–3k/LF and stalled enum; remainder
    // lands on later ticks via LF catch-up (cached_eligible held back on hit_cap).
    for meta in pools {
        if attached_pools.len() >= ATTACH_MISSING_CAP {
            break;
        }
        if !pool_worth_capped_attach(arena, meta, gate) {
            continue;
        }
        // Stale Direct edges after meta refresh still look "live" — strip so
        // attach_pool can rebuild (live: uni pin tout ∉ meta → uni_both).
        if graph.pool_has_live_edges(meta.pool_index)
            && !pool_direct_edges_match_meta(graph, arena, meta)
        {
            static STALE_META_EDGES: std::sync::atomic::AtomicU32 =
                std::sync::atomic::AtomicU32::new(0);
            if STALE_META_EDGES.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % 16 == 0 {
                crate::info!(
                    "graph rebuild stale meta edges: pool_index={} tokens={}",
                    meta.pool_index.0,
                    meta.tokens.len()
                );
            }
            remove_pool_edges_from_graph(graph, meta.pool_index);
        } else if graph.pool_has_live_edges(meta.pool_index) {
            continue;
        } else if graph.pool_has_edges(meta.pool_index) {
            // Already stubbed; leave for dirty/force rescore (not a missing attach).
            continue;
        }
        if attach_pool_to_graph(graph, arena, meta, gate) {
            attached_pools.push(meta.pool_index);
        } else if !graph.pool_has_edges(meta.pool_index)
            && pool_has_admissible_edges(arena, meta, gate)
        {
            let state = arena.pool_state(meta.pool_index);
            let (state_protocol, state_tokens, leg0, leg1) = state.map_or(
                (meta.protocol, 0, None, None),
                |state| {
                    (
                        crate::pipeline::local_sim::protocol_from_pool_state(state, meta.protocol),
                        match state {
                            PoolState::Balancer(pool) => pool.tokens.len(),
                            PoolState::Woofi(pool) => pool.tokens.len(),
                            _ => meta.tokens.len(),
                        },
                        routing_token_at_leg(arena, state, meta, 0),
                        routing_token_at_leg(arena, state, meta, 1),
                    )
                },
            );
            crate::debug!(
                "graph attach no adjacency: pool_index={} address={:?} meta_protocol={:?} state_protocol={:?} meta_tokens={} state_tokens={} legs={:?}/{:?}",
                meta.pool_index.0,
                arena.pool_address(meta.pool_index),
                meta.protocol,
                state_protocol,
                meta.tokens.len(),
                state_tokens,
                leg0,
                leg1,
            );
        }
    }
    if !attached_pools.is_empty() {
        // Patch path: rescore only new edges — full-graph rescore was ~O(all edges) per attach.
        let _ = rescore_pools_in_place(arena, graph, &attached_pools);
        refresh_graph_cycle_coverage(graph);
    }
    let live_after = attached_pools
        .iter()
        .filter(|&&p| graph.pool_has_live_edges(p))
        .count();
    let (missing_after, missing_sample) = pools.iter().fold((0usize, None), |acc, meta| {
        let missing = pool_has_admissible_edges(arena, meta, gate)
            && pool_worth_capped_attach(arena, meta, gate)
            && !graph.pool_has_live_edges(meta.pool_index)
            && !graph.pool_has_edges(meta.pool_index);
        if missing {
            (acc.0 + 1, acc.1.or(Some(meta.pool_index)))
        } else {
            acc
        }
    });
    if let Some(pool_index) = missing_sample
        && let Some(meta) = pools.iter().find(|meta| meta.pool_index == pool_index)
    {
        let state = arena.pool_state(pool_index);
        let (state_protocol, state_tokens, leg0, leg1) = state.map_or(
            (meta.protocol, 0, None, None),
            |state| {
                (
                    crate::pipeline::local_sim::protocol_from_pool_state(state, meta.protocol),
                    match state {
                        PoolState::Balancer(pool) => pool.tokens.len(),
                        PoolState::Woofi(pool) => pool.tokens.len(),
                        _ => meta.tokens.len(),
                    },
                    routing_token_at_leg(arena, state, meta, 0),
                    routing_token_at_leg(arena, state, meta, 1),
                )
            },
        );
        crate::debug!(
            "graph missing adjacency: pool_index={} address={:?} meta_protocol={:?} state_protocol={:?} meta_tokens={} state_tokens={} hub_spoke={} edges={} live={} legs={:?}/{:?}",
            pool_index.0,
            arena.pool_address(pool_index),
            meta.protocol,
            state_protocol,
            meta.tokens.len(),
            state_tokens,
            uses_hub_spoke(meta),
            graph.pool_has_edges(pool_index),
            graph.pool_has_live_edges(pool_index),
            leg0,
            leg1,
        );
    }
    let hit_cap = attached_pools.len() >= ATTACH_MISSING_CAP && missing_after > 0;
    GraphAttachReport {
        attached_pools: attached_pools.len(),
        live_after,
        hit_cap,
        missing_after,
        missing_sample,
    }
}

pub fn build_graph(arena: &StateArena, pools: &[PoolMeta]) -> RoutingGraph {
    build_graph_with_gate(arena, pools, None)
}

#[must_use]
pub fn build_graph_with_gate(
    arena: &StateArena,
    pools: &[PoolMeta],
    gate: Option<&GraphBuildGate>,
) -> RoutingGraph {
    let mut graph = RoutingGraph::new(arena.token_count());

    for meta in pools {
        attach_pool_to_graph(&mut graph, arena, meta, gate);
    }

    finalize_graph_topology(arena, &mut graph);
    graph
}

/// Recompute edge log-weights from current pool states without rebuilding adjacency.
pub fn rescore_graph_in_place(arena: &StateArena, graph: &mut RoutingGraph) {
    rescore_adjacency(arena, &mut graph.adjacency);
    compact_token_adjacency(graph, None);
}

/// Rescore only dirty pools when the touch set is small; otherwise rescore all edges.
pub fn rescore_dirty_pools_or_full(
    arena: &StateArena,
    graph: &mut RoutingGraph,
    dirty_pools: &[PoolIndex],
    arena_pool_count: usize,
) -> GraphRescoreReport {
    if dirty_pools.is_empty() {
        return GraphRescoreReport::default();
    }
    if dirty_pools.len() > arena_pool_count / 2 {
        rescore_graph_in_place(arena, graph);
        GraphRescoreReport {
            mode: Some(GraphRescoreMode::Full),
            edges_touched: graph.adjacency.iter().map(|adj| adj.len()).sum(),
            dirty_pools: dirty_pools.len(),
        }
    } else {
        let edges_touched = rescore_pools_in_place(arena, graph, dirty_pools);
        GraphRescoreReport {
            mode: Some(GraphRescoreMode::Partial),
            edges_touched,
            dirty_pools: dirty_pools.len(),
        }
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
    let mut touched_pools: Vec<usize> = Vec::with_capacity(pools.len());
    let mut touched = 0usize;
    let mut affected_adjacencies: Vec<usize> = Vec::with_capacity(pools.len().saturating_mul(2));

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
            affected_adjacencies.push(adj_idx);
        }
        touched_pools.push(pool_idx);
    }
    touched_pools.sort_unstable();
    touched_pools.dedup();
    affected_adjacencies.sort_unstable();
    affected_adjacencies.dedup();
    let token_count = graph.token_count as usize;
    let compact_slots: Vec<usize> = affected_adjacencies
        .iter()
        .copied()
        .filter(|idx| *idx < token_count)
        .collect();
    if compact_slots.is_empty() {
        // Hub-only dirty set: sort those hubs and reindex (no full-hub compact).
        for adj_idx in &affected_adjacencies {
            if let Some(adj) = graph.adjacency.get_mut(*adj_idx) {
                sort_adjacency_edges(adj);
            }
        }
        rebuild_pool_edge_positions_for_pools(graph, touched_pools);
    } else {
        // Also sort any affected hub adjacencies without walking every hub.
        let token_count = graph.token_count as usize;
        for &adj_idx in &affected_adjacencies {
            if adj_idx >= token_count
                && let Some(adj) = graph.adjacency.get_mut(adj_idx)
            {
                sort_adjacency_edges(adj);
            }
        }
        compact_token_adjacency(graph, Some(&compact_slots));
        // Hub rescores need positions refreshed for dirty pools (compact skipped hubs).
        if affected_adjacencies.iter().any(|&i| i >= token_count) {
            rebuild_pool_edge_positions_for_pools(graph, touched_pools.iter().copied());
        }
    }
    touched
}

fn rescore_adjacency(arena: &StateArena, adjacency: &mut [Vec<GraphEdge>]) {
    if crate::util::should_use_rayon(adjacency.len()) {
        adjacency.par_iter_mut().for_each(|adj| {
            for ge in adj.iter_mut() {
                rescore_graph_edge(arena, ge);
            }
        });
    } else {
        for adj in adjacency.iter_mut() {
            for ge in adj.iter_mut() {
                rescore_graph_edge(arena, ge);
            }
        }
    }
}

fn sort_adjacency_edges(adj: &mut [GraphEdge]) {
    adj.sort_by(|a, b| a.log_weight.total_cmp(&b.log_weight));
}

fn rebuild_pool_edge_positions_for_pools(
    graph: &mut RoutingGraph,
    pools: impl IntoIterator<Item = usize>,
) {
    let mut pools: Vec<usize> = pools.into_iter().collect();
    if pools.is_empty() {
        return;
    }
    pools.sort_unstable();
    pools.dedup();
    let max_pool = graph
        .pool_edge_positions
        .len()
        .max(pools.last().copied().map_or(0, |idx| idx + 1));
    if graph.pool_edge_positions.len() < max_pool {
        graph.pool_edge_positions.resize(max_pool, Vec::new());
    }
    let mut adj_to_scan: Vec<usize> = Vec::new();
    for &pool_idx in &pools {
        if let Some(slot) = graph.pool_edge_positions.get(pool_idx) {
            for &(adj_idx, _) in slot {
                adj_to_scan.push(adj_idx);
            }
        }
        if let Some(slot) = graph.pool_edge_positions.get_mut(pool_idx) {
            slot.clear();
        }
    }
    adj_to_scan.sort_unstable();
    adj_to_scan.dedup();
    for adj_idx in adj_to_scan {
        let Some(adj) = graph.adjacency.get(adj_idx) else {
            continue;
        };
        for (pos, ge) in adj.iter().enumerate() {
            let pool_idx = ge.edge.pool_index.0 as usize;
            if pools.binary_search(&pool_idx).is_ok() {
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
    // Heal stale edge tags (discovery V2 on arena V3) instead of killing until rebuild.
    if !protocol_matches_pool_state(ge.edge.protocol, state) {
        let corrected =
            crate::pipeline::local_sim::protocol_from_pool_state(state, ge.edge.protocol);
        if corrected == ge.edge.protocol || !protocol_matches_pool_state(corrected, state) {
            ge.log_weight = DEAD_EDGE_LOG_WEIGHT;
            ge.ratio = U256::ZERO;
            return 1;
        }
        ge.edge.protocol = corrected;
    }
    // Keep edge fee_bps aligned with live pool fee (discovery lag / V2→V3 heal).
    sync_edge_fee_bps_from_state(&mut ge.edge, state);
    // Stale zfo flips V2/CL spot ratio — heal before compute_edge_ratio.
    apply_cl_zero_for_one(arena, &mut ge.edge);
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
    use crate::core::types::{
        BalancerPoolKind, BalancerPoolState, PoolState, V2PoolState, V3PoolState, V3Tick,
    };
    use alloy::primitives::{Address, U256};
    use std::sync::Arc;

    const TEST_FUNDED_RESERVE: U256 = U256::from_limbs([1_000_000_000_000_000, 0, 0, 0]);

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
    fn two_token_balancer_hub_spoke_follows_vault_order() {
        // Balancer is always hub-spoke (not Direct) — meta may reverse tokens
        // vs vault order; Enter legs must use vault indices 0/1.
        let mut arena = StateArena::default();
        let addr_a = Address::from([10u8; 20]);
        let addr_b = Address::from([20u8; 20]);
        let token_a = arena.register_token(addr_a);
        let token_b = arena.register_token(addr_b);
        let one = crate::core::math::fixed_point::ONE;
        let bal = U256::from(100u64) * one;
        let pool = arena.register_pool(
            Address::from([30u8; 20]),
            Arc::new(PoolState::Balancer(BalancerPoolState {
                pool_id: None,
                tokens: vec![addr_a, addr_b],
                balances: vec![bal, bal + U256::from(1u64)],
                weights: vec![one / U256::from(2u64), one / U256::from(2u64)],
                scaling_factors: vec![one, one],
                amp: U256::ZERO,
                amp_precision: U256::from(1_000u64),
                fee: U256::from(1_000_000_000_000_000u64), // 0.1%
                pool_type: BalancerPoolKind::Weighted,
                linear: None,
                bpt_index: None,
                is_updating: false,
                last_change_block: 0,
            })),
        );
        // Discovery meta reversed vs vault order.
        let meta = pool_meta_from_pair(pool, ProtocolType::BalancerV2, token_b, token_a, 10);
        let graph = build_graph(&arena, std::slice::from_ref(&meta));
        assert_eq!(graph.virtual_hubs.len(), 1);
        let enter_a: Vec<&GraphEdge> = graph.adjacency[token_a.0 as usize]
            .iter()
            .filter(|ge| ge.phase == GraphHopPhase::EnterPool && ge.edge.pool_index == pool)
            .collect();
        assert_eq!(enter_a.len(), 1);
        assert_eq!(enter_a[0].edge.token_in, token_a);
        assert_eq!(enter_a[0].edge.token_in_idx, 0); // vault leg 0 = addr_a
        let enter_b: Vec<&GraphEdge> = graph.adjacency[token_b.0 as usize]
            .iter()
            .filter(|ge| ge.phase == GraphHopPhase::EnterPool && ge.edge.pool_index == pool)
            .collect();
        assert_eq!(enter_b.len(), 1);
        assert_eq!(enter_b[0].edge.token_in_idx, 1);
    }

    #[test]
    fn v2_zero_for_one_follows_on_chain_token_order_not_meta_index() {
        let mut arena = StateArena::default();
        let low = arena.register_token(Address::from([1u8; 20]));
        let high = arena.register_token(Address::from([2u8; 20]));
        let mut sell_low = Edge {
            pool_index: PoolIndex(0),
            token_in: low,
            token_out: high,
            token_in_idx: 1,
            token_out_idx: 0,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: false,
        };
        apply_cl_zero_for_one(&arena, &mut sell_low);
        assert!(sell_low.zero_for_one);
    }

    #[test]
    fn v3_zero_for_one_follows_on_chain_token_order_not_meta_index() {
        let mut arena = StateArena::default();
        let low = arena.register_token(Address::from([1u8; 20]));
        let high = arena.register_token(Address::from([2u8; 20]));
        let mut sell_low = Edge {
            pool_index: PoolIndex(0),
            token_in: low,
            token_out: high,
            token_in_idx: 1,
            token_out_idx: 0,
            protocol: ProtocolType::UniswapV3,
            fee_bps: 30,
            zero_for_one: true,
        };
        let mut sell_high = Edge {
            pool_index: PoolIndex(0),
            token_in: high,
            token_out: low,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV3,
            fee_bps: 30,
            zero_for_one: true,
        };
        apply_cl_zero_for_one(&arena, &mut sell_low);
        apply_cl_zero_for_one(&arena, &mut sell_high);
        assert!(sell_low.zero_for_one);
        assert!(!sell_high.zero_for_one);
    }

    #[test]
    fn v4_lazy_swap_zero_for_one_matches_currency_order() {
        let mut arena = StateArena::default();
        let low = arena.register_token(Address::from([1u8; 20]));
        let high = arena.register_token(Address::from([2u8; 20]));
        let pool = arena.register_pool(Address::from([3u8; 20]), v4_pool_state());
        let pending = PendingHubSwap {
            pool_index: pool,
            token_in: high,
            token_in_idx: 0,
            protocol: ProtocolType::UniswapV4,
            fee_bps: 30,
        };
        let (edge, log_w, ratio) =
            resolve_lazy_swap_edge(&arena, pending, low, 1).expect("lazy v4 swap");
        assert!(!edge.zero_for_one);
        assert!(log_w.is_finite());
        assert!(!ratio.is_zero());
    }

    #[test]
    fn hub_spoke_balancer_uses_linear_edge_count() {
        let mut arena = StateArena::default();
        let tokens: Vec<TokenIndex> = (0u8..8)
            .map(|i| arena.register_token(Address::from([i; 20])))
            .collect();
        let funded = TEST_FUNDED_RESERVE;
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
            .map(|adj| {
                adj.iter()
                    .filter(|ge| ge.phase == GraphHopPhase::EnterPool)
                    .count()
            })
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
        let funded = TEST_FUNDED_RESERVE;
        let dust = U256::ZERO;
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
        // Hub legs must bind vault addresses (0x00.., 0x01..), not discovery meta (0x0a..).
        let enter_tokens: Vec<_> = graph
            .adjacency
            .iter()
            .take(graph.token_count as usize)
            .flat_map(|adj| adj.iter())
            .filter(|ge| ge.phase == GraphHopPhase::EnterPool)
            .map(|ge| ge.edge.token_in)
            .collect();
        assert!(enter_tokens.contains(&TokenIndex(0)));
        assert!(enter_tokens.contains(&TokenIndex(1)));
        assert!(!enter_tokens.contains(&a));
        assert!(!enter_tokens.contains(&b));
    }

    #[test]
    fn resolve_lazy_swap_rejects_meta_vault_token_mismatch() {
        use crate::core::math::fixed_point::ONE;
        let mut arena = StateArena::default();
        let vault0 = arena.register_token(Address::from([1u8; 20]));
        let vault1 = arena.register_token(Address::from([2u8; 20]));
        let meta_wrong = arena.register_token(Address::from([9u8; 20]));
        let bal = U256::from(5u64) * ONE;
        let w = ONE / U256::from(2u64);
        let pool = arena.register_pool(
            Address::from([15u8; 20]),
            Arc::new(PoolState::Balancer(BalancerPoolState {
                pool_id: None,
                tokens: vec![Address::from([1u8; 20]), Address::from([2u8; 20])],
                balances: vec![bal, bal],
                weights: vec![w, w],
                scaling_factors: vec![ONE, ONE],
                amp: U256::ZERO,
                amp_precision: U256::ZERO,
                fee: U256::ZERO,
                pool_type: BalancerPoolKind::Weighted,
                linear: None,
                bpt_index: None,
                is_updating: false,
                last_change_block: 0,
            })),
        );
        let pending = PendingHubSwap {
            pool_index: pool,
            token_in: meta_wrong,
            token_in_idx: 0,
            protocol: ProtocolType::BalancerV2,
            fee_bps: 10,
        };
        assert!(
            resolve_lazy_swap_edge(&arena, pending, vault1, 1).is_none(),
            "wrong token_in address must not resolve"
        );
        let pending_ok = PendingHubSwap {
            token_in: vault0,
            ..pending
        };
        assert!(
            resolve_lazy_swap_edge(&arena, pending_ok, vault1, 1).is_some(),
            "vault-aligned legs must still resolve"
        );
    }

    #[test]
    fn rescore_preserves_pool_edge_positions_after_sort() {
        let mut arena = StateArena::default();
        let a = arena.register_token(Address::from([1u8; 20]));
        let b = arena.register_token(Address::from([2u8; 20]));
        let pool = arena.register_pool(
            Address::from([3u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: TEST_FUNDED_RESERVE,
                reserve1: TEST_FUNDED_RESERVE + U256::from(1u64),
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
    fn cached_graph_has_no_missing_eligible_pools() {
        let mut arena = StateArena::default();
        let a = arena.register_token(Address::from([1u8; 20]));
        let b = arena.register_token(Address::from([2u8; 20]));
        let pool = arena.register_pool(
            Address::from([3u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: TEST_FUNDED_RESERVE,
                reserve1: TEST_FUNDED_RESERVE,
                fee: U256::from(30u8),
                fee_denominator: U256::from(10_000u64),
                block_timestamp_last: 1,
            })),
        );
        let metas = [pool_meta_from_pair(pool, ProtocolType::UniswapV2, a, b, 30)];
        let graph = build_graph(&arena, &metas);

        assert!(!has_missing_eligible_pools(&arena, &metas, &graph));
    }

    #[test]
    fn graph_keeps_long_tail_edge_connected_to_priced_token() {
        let mut arena = StateArena::default();
        let priced = arena.register_token(Address::from([1u8; 20]));
        let tail = arena.register_token(Address::from([2u8; 20]));
        let pool = arena.register_pool(
            Address::from([3u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: TEST_FUNDED_RESERVE,
                reserve1: TEST_FUNDED_RESERVE + U256::from(1u64),
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
        let funded = TEST_FUNDED_RESERVE;
        let dust = U256::ZERO;
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
        let funded = TEST_FUNDED_RESERVE;
        let dust = U256::ZERO;
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
        let funded = TEST_FUNDED_RESERVE;
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
    fn pool_state_graph_eligible_requires_routable_pair() {
        let funded = TEST_FUNDED_RESERVE;
        let dust = U256::ZERO;
        let one_sided = PoolState::V2(V2PoolState {
            reserve0: funded,
            reserve1: dust,
            fee: U256::from(997u64),
            fee_denominator: U256::from(1_000u64),
            block_timestamp_last: 0,
        });
        assert!(!pool_state_graph_eligible(
            None,
            &one_sided,
            ProtocolType::UniswapV2,
            2,
            None,
            30,
            Some((18, 18)),
        ));
        let two_sided = PoolState::V2(V2PoolState {
            reserve0: funded,
            reserve1: funded + U256::from(1u64),
            fee: U256::from(997u64),
            fee_denominator: U256::from(1_000u64),
            block_timestamp_last: 0,
        });
        assert!(pool_state_graph_eligible(
            None,
            &two_sided,
            ProtocolType::UniswapV2,
            2,
            None,
            30,
            Some((18, 18)),
        ));
    }

    #[test]
    fn rescore_prunes_dead_parallel_edges() {
        let mut arena = StateArena::default();
        let hub = arena.register_token(Address::from([50u8; 20]));
        let leaf = arena.register_token(Address::from([51u8; 20]));
        let funded = TEST_FUNDED_RESERVE;
        let dust = U256::ZERO;
        let live_pool = arena.register_pool(
            Address::from([52u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: funded + U256::from(10u64),
                reserve1: funded + U256::from(100u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 0,
            })),
        );
        let dying_pool = arena.register_pool(
            Address::from([53u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: funded + U256::from(5u64),
                reserve1: funded + U256::from(100u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 0,
            })),
        );
        let metas = [
            pool_meta_from_pair(live_pool, ProtocolType::UniswapV2, hub, leaf, 30),
            pool_meta_from_pair(dying_pool, ProtocolType::UniswapV2, hub, leaf, 30),
        ];
        let mut graph = build_graph(&arena, &metas);
        assert_eq!(graph.adjacency[hub.0 as usize].len(), 2);

        arena.register_pool(
            Address::from([53u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: dust,
                reserve1: dust,
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 1,
            })),
        );
        rescore_graph_in_place(&arena, &mut graph);
        // Dead Direct stubs stay in adjacency (attach-thrash guard); only live count.
        let live_pools: Vec<u32> = graph.adjacency[hub.0 as usize]
            .iter()
            .filter(|ge| {
                ge.phase == GraphHopPhase::Direct
                    && crate::pipeline::cycle_finder::is_live_graph_edge(ge)
            })
            .map(|ge| ge.edge.pool_index.0)
            .collect();
        assert_eq!(live_pools, vec![live_pool.0]);
    }

    #[test]
    fn weight_only_rescore_reuses_cycle_coverage() {
        let mut arena = StateArena::default();
        let a = arena.register_token(Address::from([60u8; 20]));
        let b = arena.register_token(Address::from([61u8; 20]));
        let funded = TEST_FUNDED_RESERVE;
        let pool = arena.register_pool(
            Address::from([62u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: funded + U256::from(100u64),
                reserve1: funded + U256::from(200u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 0,
            })),
        );
        let metas = [pool_meta_from_pair(pool, ProtocolType::UniswapV2, a, b, 30)];
        let mut graph = build_graph(&arena, &metas);
        let coverage_ptr = std::sync::Arc::as_ptr(graph.coverage.as_ref().expect("coverage"));
        arena.register_pool(
            Address::from([62u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: funded + U256::from(110u64),
                reserve1: funded + U256::from(190u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 1,
            })),
        );
        rescore_pools_in_place(&arena, &mut graph, &[pool]);
        let coverage_after = graph.coverage.as_ref().expect("coverage");
        assert_eq!(std::sync::Arc::as_ptr(coverage_after), coverage_ptr);
    }

    #[test]
    fn rescore_heals_stale_v2_edge_on_v3_state() {
        let mut arena = StateArena::default();
        let a = arena.register_token(Address::from([60u8; 20]));
        let b = arena.register_token(Address::from([61u8; 20]));
        let funded = TEST_FUNDED_RESERVE;
        let pool = arena.register_pool(
            Address::from([62u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: funded,
                reserve1: funded + U256::from(1u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 0,
            })),
        );
        let metas = [pool_meta_from_pair(pool, ProtocolType::UniswapV2, a, b, 30)];
        let mut graph = build_graph(&arena, &metas);
        assert_eq!(
            graph.adjacency[a.0 as usize][0].edge.protocol,
            ProtocolType::UniswapV2
        );
        // Arena flips to V3 (discovery mislabel) — rescore must heal, not DEAD.
        arena.register_pool(
            Address::from([62u8; 20]),
            Arc::new(PoolState::V3(V3PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                liquidity: 1_000_000_000_000_000_000u128,
                tick: 0,
                fee: U256::from(5000u32), // 50 bps — distinct from discovery 30
                tick_spacing: 60,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
                ticks: Arc::from(vec![V3Tick {
                    tick: -60_000,
                    liquidity_gross: 1,
                    liquidity_net: 0,
                }]),
            })),
        );
        rescore_pools_in_place(&arena, &mut graph, &[pool]);
        let ge = &graph.adjacency[a.0 as usize][0];
        assert_eq!(ge.edge.protocol, ProtocolType::UniswapV3);
        assert_eq!(ge.edge.fee_bps, 50); // synced from 5000 pips
        assert!(
            ge.log_weight < DEAD_EDGE_LOG_WEIGHT && !ge.ratio.is_zero(),
            "healed edge must stay live"
        );
    }

    #[test]
    fn partial_rescore_updates_only_dirty_pool_edges() {
        let mut arena = StateArena::default();
        let a = arena.register_token(Address::from([40u8; 20]));
        let b = arena.register_token(Address::from([41u8; 20]));
        let c = arena.register_token(Address::from([42u8; 20]));
        // Modest reserves so ratio moves are visible in f64 log-weight.
        let r0 = crate::core::constants::V2_MIN_RESERVE * U256::from(10u64);
        let pool0 = arena.register_pool(
            Address::from([43u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: r0,
                reserve1: r0,
                fee: U256::from(30u8),
                fee_denominator: U256::from(10_000u64),
                block_timestamp_last: 1,
            })),
        );
        let pool1 = arena.register_pool(
            Address::from([44u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: r0,
                reserve1: r0 * U256::from(2u64),
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
        // Skew pool0 heavily so log-weight must move.
        arena.register_pool(
            Address::from([43u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: r0,
                reserve1: r0 * U256::from(50u64),
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
        assert_ne!(before, after, "dirty pool0 weight must update");
        assert_eq!(untouched, still, "untouched pool1 weight must stay");
    }

    #[test]
    fn thin_parallel_retains_stub_for_dropped_pool() {
        let mut arena = StateArena::default();
        let a = arena.register_token(Address::from([1u8; 20]));
        let b = arena.register_token(Address::from([2u8; 20]));
        let r = crate::core::constants::V2_MIN_RESERVE * U256::from(100u64);
        let best = arena.register_pool(
            Address::from([6u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: r,
                reserve1: r * U256::from(5u64),
                fee: U256::from(30u8),
                fee_denominator: U256::from(10_000u64),
                block_timestamp_last: 1,
            })),
        );
        let higher = arena.register_pool(
            Address::from([7u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: r,
                reserve1: r * U256::from(4u64),
                fee: U256::from(30u8),
                fee_denominator: U256::from(10_000u64),
                block_timestamp_last: 1,
            })),
        );
        let better = arena.register_pool(
            Address::from([3u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: r,
                reserve1: r * U256::from(3u64),
                fee: U256::from(30u8),
                fee_denominator: U256::from(10_000u64),
                block_timestamp_last: 1,
            })),
        );
        let worse = arena.register_pool(
            Address::from([4u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: r,
                reserve1: r, // flatter ratio → lower priority
                fee: U256::from(30u8),
                fee_denominator: U256::from(10_000u64),
                block_timestamp_last: 1,
            })),
        );
        let mid = arena.register_pool(
            Address::from([5u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: r,
                reserve1: r * U256::from(2u64),
                fee: U256::from(30u8),
                fee_denominator: U256::from(10_000u64),
                block_timestamp_last: 1,
            })),
        );
        let metas = [
            pool_meta_from_pair(best, ProtocolType::UniswapV2, a, b, 30),
            pool_meta_from_pair(higher, ProtocolType::UniswapV2, a, b, 30),
            pool_meta_from_pair(better, ProtocolType::UniswapV2, a, b, 30),
            pool_meta_from_pair(mid, ProtocolType::UniswapV2, a, b, 30),
            pool_meta_from_pair(worse, ProtocolType::UniswapV2, a, b, 30),
        ];
        let mut graph = build_graph(&arena, &metas);
        // Force thin via full compact (MAX_PARALLEL_EDGES_PER_PAIR = 2).
        rescore_graph_in_place(&arena, &mut graph);
        let from_a_live = graph.adjacency[a.0 as usize]
            .iter()
            .filter(|ge| crate::pipeline::cycle_finder::is_live_graph_edge(ge))
            .count();
        assert!(
            from_a_live <= 2,
            "thin keeps at most 2 live a→b parallels, got {from_a_live}"
        );
        if let Some(pos) = graph.pool_edge_positions.get(better.0 as usize) {
            for &(adj_idx, edge_pos) in pos {
                let ge = &graph.adjacency[adj_idx][edge_pos];
                assert_eq!(
                    ge.edge.pool_index, better,
                    "stale reverse index: pos points at pool {}",
                    ge.edge.pool_index.0
                );
            }
        }
        assert!(graph.pool_has_edges(better));
        assert!(!graph.pool_has_live_edges(better));
        assert_eq!(
            attach_missing_eligible_pools(&arena, &mut graph, &metas).attached_pools,
            0
        );
    }

    fn v4_pool_state() -> Arc<PoolState> {
        Arc::new(PoolState::V4(crate::core::types::V4PoolState {
            sqrt_price_x96: U256::from(1u128 << 96),
            tick: 0,
            liquidity: 1_000_000,
            fee: U256::from(3000u32),
            tick_spacing: 60,
            ticks: Arc::from([] as [crate::core::types::V3Tick; 0]),
            unlocked: true,
            fee_protocol: 0,
            observation_cardinality: 1,
        }))
    }

    #[test]
    fn v4_pools_use_singleton_hub_with_enter_edges() {
        let mut arena = StateArena::default();
        let a = arena.register_token(Address::from([1u8; 20]));
        let b = arena.register_token(Address::from([2u8; 20]));
        let pool = arena.register_pool(Address::from([3u8; 20]), v4_pool_state());
        let mut meta = pool_meta_from_pair(pool, ProtocolType::UniswapV4, a, b, 30);
        meta.pool_id = Some(alloy::primitives::FixedBytes::ZERO);
        let graph = build_graph(&arena, std::slice::from_ref(&meta));
        assert!(graph.v4_singleton_hub.is_some());
        assert!(graph.pool_has_live_edges(pool));
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
    fn attach_missing_eligible_pools_adds_late_v4_members() {
        let mut arena = StateArena::default();
        let a = arena.register_token(Address::from([1u8; 20]));
        let b = arena.register_token(Address::from([2u8; 20]));
        let pool0 = arena.register_pool(Address::from([3u8; 20]), v4_pool_state());
        let mut meta0 = pool_meta_from_pair(pool0, ProtocolType::UniswapV4, a, b, 30);
        meta0.pool_id = Some(alloy::primitives::FixedBytes::ZERO);
        let mut graph = RoutingGraph::new(arena.token_count());

        let pool1 = arena.register_pool(Address::from([4u8; 20]), v4_pool_state());
        let c = arena.register_token(Address::from([5u8; 20]));
        let d = arena.register_token(Address::from([6u8; 20]));
        let mut meta1 = pool_meta_from_pair(pool1, ProtocolType::UniswapV4, c, d, 30);
        meta1.pool_id = Some(alloy::primitives::FixedBytes::repeat_byte(1));

        attach_pool_to_graph(&mut graph, &arena, &meta0, None);
        finalize_graph_topology(&arena, &mut graph);
        assert!(graph.pool_has_live_edges(pool0));
        assert!(!graph.pool_has_live_edges(pool1));

        let attached =
            attach_missing_eligible_pools(&arena, &mut graph, &[meta0, meta1]).attached_pools;
        assert_eq!(attached, 1);
        assert!(graph.pool_has_live_edges(pool1));
        // New tokens must sit in the token region, not collide with the V4 hub slot.
        assert_eq!(graph.token_count, arena.token_count());
        assert!(
            graph
                .v4_singleton_hub
                .is_some_and(|h| h >= graph.token_count)
        );
        assert!(c.0 < graph.token_count && d.0 < graph.token_count);
        assert!(
            graph
                .adjacency
                .get(c.0 as usize)
                .is_some_and(|adj| adj.iter().any(|ge| ge.phase == GraphHopPhase::EnterPool))
        );
    }

    #[test]
    fn ensure_token_capacity_shifts_hubs_and_accepts_new_token_edges() {
        let mut graph = RoutingGraph::new(2);
        // Hub lives at node 2 while token_count is 2.
        graph.virtual_hubs.push(VirtualPoolHub {
            pool_index: PoolIndex(9),
            protocol: ProtocolType::BalancerV2,
            exit_legs: smallvec::smallvec![0, 1],
            v4_singleton: false,
        });
        graph.adjacency.push(vec![direct_ge(
            9,
            0,
            0,
            ProtocolType::BalancerV2,
            1_000_000_000_000_000_000,
        )]);
        graph.adjacency[2][0].phase = GraphHopPhase::ExitPool;
        graph.adjacency[2][0].target_node = 1;
        graph.v4_singleton_hub = None;
        graph.pool_edge_positions.resize(10, Vec::new());
        graph.pool_edge_positions[9].push((2, 0));

        graph.ensure_token_capacity(4);
        assert_eq!(graph.token_count, 4);
        assert_eq!(graph.adjacency.len(), 5); // 4 tokens + 1 hub
        assert!(graph.adjacency[2].is_empty());
        assert!(graph.adjacency[3].is_empty());
        assert_eq!(graph.adjacency[4].len(), 1);
        assert_eq!(graph.adjacency[4][0].target_node, 1); // token target unchanged
        assert_eq!(graph.pool_edge_positions[9], vec![(4, 0)]);

        // New direct edge token 0 → token 3 must stay inside the token region.
        graph.push_edge_at(
            0,
            GraphEdge {
                edge: Edge {
                    pool_index: PoolIndex(1),
                    token_in: TokenIndex(0),
                    token_out: TokenIndex(3),
                    token_in_idx: 0,
                    token_out_idx: 1,
                    protocol: ProtocolType::UniswapV2,
                    fee_bps: 30,
                    zero_for_one: true,
                },
                phase: GraphHopPhase::Direct,
                target_node: 3,
                log_weight: -0.01,
                ratio: U256::from(1_100_000_000_000_000_000u64),
            },
        );
        let adj = crate::pipeline::weighted_graph::build_weighted_adjacency(&graph);
        assert_eq!(adj.len(), 4);
        assert_eq!(adj[0].len(), 1);
        assert_eq!(adj[0][0].edge.token_out.0, 3);
        let _ = crate::pipeline::bellman_ford::find_cycles_bellman_ford_multi_pass_with_adj(
            &adj,
            &[crate::pipeline::types::CycleSearchPass {
                max_hops: 3,
                max_cycles: 8,
            }],
        );
    }

    #[test]
    fn graph_admits_tradable_pool_without_priced_tokens() {
        let mut arena = StateArena::default();
        let a = arena.register_token(Address::from([4u8; 20]));
        let b = arena.register_token(Address::from([5u8; 20]));
        let pool = arena.register_pool(
            Address::from([6u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: TEST_FUNDED_RESERVE,
                reserve1: TEST_FUNDED_RESERVE + U256::from(1u64),
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

    #[test]
    fn graph_gate_keeps_non_flash_intermediate_pool_in_closed_cycle() {
        use crate::core::constants::{MIN_TOKEN_TO_MATIC_RATE, WMATIC};
        use crate::services::execution::flash_liquidity::FlashLiquiditySnapshot;

        let mut arena = StateArena::default();
        let hub = arena.register_token(WMATIC);
        let tail_a = arena.register_token(Address::from([0xaau8; 20]));
        let tail_b = arena.register_token(Address::from([0xbbu8; 20]));
        let funded = TEST_FUNDED_RESERVE;
        let hub_pool = arena.register_pool(
            Address::from([0x01u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: funded,
                reserve1: funded + U256::ONE,
                fee: U256::from(30u8),
                fee_denominator: U256::from(10_000u64),
                block_timestamp_last: 1,
            })),
        );
        let tail_pool = arena.register_pool(
            Address::from([0x02u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: funded,
                reserve1: funded + U256::ONE,
                fee: U256::from(30u8),
                fee_denominator: U256::from(10_000u64),
                block_timestamp_last: 1,
            })),
        );
        let close_pool = arena.register_pool(
            Address::from([0x03u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: funded,
                reserve1: funded + U256::ONE,
                fee: U256::from(30u8),
                fee_denominator: U256::from(10_000u64),
                block_timestamp_last: 1,
            })),
        );
        let hub_meta = pool_meta_from_pair(hub_pool, ProtocolType::UniswapV2, hub, tail_a, 30);
        let tail_meta = pool_meta_from_pair(tail_pool, ProtocolType::UniswapV2, tail_a, tail_b, 30);
        let close_meta = pool_meta_from_pair(close_pool, ProtocolType::UniswapV2, tail_b, hub, 30);

        let mut rates = FxHashMap::default();
        rates.insert(hub, MIN_TOKEN_TO_MATIC_RATE);
        rates.insert(tail_a, MIN_TOKEN_TO_MATIC_RATE);
        rates.insert(tail_b, MIN_TOKEN_TO_MATIC_RATE);

        let gate = GraphBuildGate {
            token_to_matic_rates: Arc::new(rates),
            flash: Arc::new(FlashLiquiditySnapshot::default()),
            flash_ttl: Duration::from_secs(60),
            spoke_connectivity: None,
        };

        let gated = build_graph_with_gate(&arena, &[hub_meta, tail_meta, close_meta], Some(&gate));
        assert!(gated.pool_has_live_edges(hub_pool));
        assert!(gated.pool_has_live_edges(tail_pool));
        assert!(gated.pool_has_live_edges(close_pool));
    }

    #[test]
    fn graph_gate_keeps_flash_eligible_unpriced_intermediate_pool() {
        use crate::core::constants::{MIN_TOKEN_TO_MATIC_RATE, WMATIC};
        use crate::services::execution::flash_liquidity::FlashLiquiditySnapshot;

        let mut arena = StateArena::default();
        let priced_start = arena.register_token(Address::from([0x11u8; 20]));
        let intermediate = arena.register_token(WMATIC);
        let unpriced = arena.register_token(Address::from([0x22u8; 20]));
        let funded = TEST_FUNDED_RESERVE;
        let pool = arena.register_pool(
            Address::from([0x33u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: funded,
                reserve1: funded + U256::ONE,
                fee: U256::from(30u8),
                fee_denominator: U256::from(10_000u64),
                block_timestamp_last: 1,
            })),
        );
        let meta = pool_meta_from_pair(pool, ProtocolType::UniswapV2, intermediate, unpriced, 30);

        let mut rates = FxHashMap::default();
        rates.insert(priced_start, MIN_TOKEN_TO_MATIC_RATE);
        let gate = GraphBuildGate {
            token_to_matic_rates: Arc::new(rates),
            flash: Arc::new(FlashLiquiditySnapshot::default()),
            flash_ttl: Duration::from_secs(60),
            spoke_connectivity: None,
        };

        let metas = [meta];
        let gated = build_graph_with_gate(&arena, &metas, Some(&gate));
        assert!(gated.pool_has_live_edges(pool));
        assert_eq!(
            count_graph_eligible_unpriced_pools(&arena, &metas, &gate),
            1
        );
    }

    #[test]
    fn graph_gate_rejects_unpriced_non_flash_pool() {
        use crate::core::constants::MIN_TOKEN_TO_MATIC_RATE;
        use crate::services::execution::flash_liquidity::FlashLiquiditySnapshot;

        let mut arena = StateArena::default();
        let priced = arena.register_token(Address::from([0x11u8; 20]));
        let a = arena.register_token(Address::from([0xaau8; 20]));
        let b = arena.register_token(Address::from([0xbbu8; 20]));
        let funded = TEST_FUNDED_RESERVE;
        let pool = arena.register_pool(
            Address::from([0x33u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: funded,
                reserve1: funded + U256::ONE,
                fee: U256::from(30u8),
                fee_denominator: U256::from(10_000u64),
                block_timestamp_last: 1,
            })),
        );
        let meta = pool_meta_from_pair(pool, ProtocolType::UniswapV2, a, b, 30);

        let mut rates = FxHashMap::default();
        rates.insert(priced, MIN_TOKEN_TO_MATIC_RATE);
        let gate = GraphBuildGate {
            token_to_matic_rates: Arc::new(rates),
            flash: Arc::new(FlashLiquiditySnapshot::default()),
            flash_ttl: Duration::from_secs(60),
            spoke_connectivity: None,
        };

        let metas = [meta];
        let gated = build_graph_with_gate(&arena, &metas, Some(&gate));
        assert!(!gated.pool_has_live_edges(pool));
        assert_eq!(
            count_graph_eligible_unpriced_pools(&arena, &metas, &gate),
            0
        );
        // Ungated build still admits the pool.
        let ungated = build_graph(&arena, &metas);
        assert!(ungated.pool_has_live_edges(pool));
    }

    #[test]
    fn capped_attach_keeps_spoke_connected_pool() {
        use crate::core::constants::MIN_TOKEN_TO_MATIC_RATE;
        use crate::services::execution::flash_liquidity::FlashLiquiditySnapshot;

        let mut arena = StateArena::default();
        let spoke_addr = Address::from([0x41u8; 20]);
        let spoke = arena.register_token(spoke_addr);
        let tail = arena.register_token(Address::from([0x42u8; 20]));
        let pool = arena.register_pool(
            Address::from([0x43u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: TEST_FUNDED_RESERVE,
                reserve1: TEST_FUNDED_RESERVE + U256::ONE,
                fee: U256::from(30u8),
                fee_denominator: U256::from(10_000u64),
                block_timestamp_last: 1,
            })),
        );
        let meta = pool_meta_from_pair(pool, ProtocolType::UniswapV2, spoke, tail, 30);
        let mut rates = FxHashMap::default();
        rates.insert(
            arena.register_token(Address::from([0x44u8; 20])),
            MIN_TOKEN_TO_MATIC_RATE,
        );
        let gate = GraphBuildGate {
            token_to_matic_rates: Arc::new(rates),
            flash: Arc::new(FlashLiquiditySnapshot::default()),
            flash_ttl: Duration::from_secs(60),
            spoke_connectivity: Some(Arc::new(FxHashSet::from_iter([spoke_addr]))),
        };
        let mut graph = RoutingGraph::new(arena.token_count());

        assert_eq!(
            count_eligible_pools_missing_from_graph_with_gate(
                &arena,
                std::slice::from_ref(&meta),
                &graph,
                Some(&gate),
            ),
            1
        );
        let report = attach_missing_eligible_pools_with_gate(
            &arena,
            &mut graph,
            std::slice::from_ref(&meta),
            Some(&gate),
        );
        assert_eq!(report.attached_pools, 1);
        assert!(graph.pool_has_live_edges(pool));
    }
}
