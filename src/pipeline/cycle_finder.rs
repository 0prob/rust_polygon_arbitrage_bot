use std::time::Duration;

use rayon::prelude::*;

use alloy::primitives::U256;

use crate::core::constants::HOP_CAP;
use crate::core::math::fixed_point::ONE;
use crate::core::types::{CycleEdges, Edge, FoundCycle, ProtocolType, TokenIndex};
use crate::pipeline::cycle_filter::{cycle_key, dedupe_cycles_by_edges};
use crate::pipeline::deadline::SharedDeadlineGuard;
use crate::pipeline::route_calls::{MAX_ROUTE_CALLS, estimate_hop_calls};
use crate::pipeline::spot_price::{min_profitable_cycle_ratio, mul_ratio_saturating};
use crate::pipeline::types::{CycleSearchPass, GraphEdge, RoutingGraph, compare_cycle_score};

pub use crate::pipeline::spot_price::hop_penalty;

const MAX_CYCLES_PER_PASS: usize = 50_000;
const CYCLE_ENUM_TIME_BUDGET: Duration = Duration::from_millis(500);
/// Amortize elapsed-time checks during DFS enumeration.
/// Prune DFS branches once log-weight exceeds this (spot-weighted graphs only).
const LOG_WEIGHT_PRUNE_THRESHOLD: f64 = 0.0;
/// Edges rescored to this weight are non-tradable — skip during enumeration.
pub(crate) const DEAD_EDGE_LOG_WEIGHT: f64 = 15.0;

#[inline]
#[must_use]
pub fn is_live_graph_edge(ge: &GraphEdge) -> bool {
    !ge.ratio.is_zero()
}

/// Structural cycle coverage for the current live routing graph.
///
/// Pools are represented as nodes in a token-pool bipartite graph. A pool can
/// participate in a route cycle only when at least two of its token incidences
/// are non-bridges. This correctly excludes an isolated bidirectional AMM pool:
/// returning through the same pool is forbidden by route enumeration.
#[derive(Debug, Default, Clone)]
pub struct CycleCapableCoverage {
    token_mask: Vec<bool>,
    pub pool_indices: rustc_hash::FxHashSet<u32>,
}

#[must_use]
pub fn cycle_capable_coverage(graph: &RoutingGraph) -> CycleCapableCoverage {
    let token_count = graph.adjacency.len();
    let pool_count = graph.pool_edge_positions.len();
    let node_count = token_count.saturating_add(pool_count);
    let mut incidences = rustc_hash::FxHashSet::default();
    for edges in &graph.adjacency {
        for ge in edges {
            if is_live_graph_edge(ge) {
                incidences.insert((ge.edge.pool_index.0 as usize, ge.edge.token_in.0 as usize));
                incidences.insert((ge.edge.pool_index.0 as usize, ge.edge.token_out.0 as usize));
            }
        }
    }

    let mut adjacency = vec![Vec::<(usize, usize)>::new(); node_count];
    let mut incidence_edges = Vec::with_capacity(incidences.len());
    for (pool, token) in incidences {
        if pool >= pool_count || token >= token_count {
            continue;
        }
        let pool_node = token_count + pool;
        let edge_id = incidence_edges.len();
        incidence_edges.push((pool, token));
        adjacency[token].push((pool_node, edge_id));
        adjacency[pool_node].push((token, edge_id));
    }

    // Iterative Tarjan bridge search avoids recursion depth risk on large
    // long-tail graphs and handles parallel pools as distinct bipartite paths.
    let mut discovered = vec![0usize; node_count];
    let mut low = vec![0usize; node_count];
    let mut parent_edge = vec![usize::MAX; node_count];
    let mut bridges = vec![false; incidence_edges.len()];
    let mut clock = 0usize;
    for root in 0..node_count {
        if discovered[root] != 0 || adjacency[root].is_empty() {
            continue;
        }
        clock += 1;
        discovered[root] = clock;
        low[root] = clock;
        let mut stack = vec![(root, 0usize)];
        while let Some((node, next_index)) = stack.last_mut() {
            if *next_index < adjacency[*node].len() {
                let (next, edge_id) = adjacency[*node][*next_index];
                *next_index += 1;
                if edge_id == parent_edge[*node] {
                    continue;
                }
                if discovered[next] == 0 {
                    parent_edge[next] = edge_id;
                    clock += 1;
                    discovered[next] = clock;
                    low[next] = clock;
                    stack.push((next, 0));
                } else {
                    low[*node] = low[*node].min(discovered[next]);
                }
            } else {
                let Some((finished, _)) = stack.pop() else {
                    break;
                };
                let edge_id = parent_edge[finished];
                if edge_id != usize::MAX {
                    let (pool, token) = incidence_edges[edge_id];
                    let pool_node = token_count + pool;
                    let parent = if finished == pool_node {
                        token
                    } else {
                        pool_node
                    };
                    bridges[edge_id] = low[finished] > discovered[parent];
                    low[parent] = low[parent].min(low[finished]);
                }
            }
        }
    }

    let mut non_bridge_incidence_count = vec![0usize; pool_count];
    for (edge_id, (pool, _)) in incidence_edges.iter().enumerate() {
        if !bridges[edge_id] {
            non_bridge_incidence_count[*pool] += 1;
        }
    }
    let mut participating = vec![false; pool_count];
    for (pool, count) in non_bridge_incidence_count.into_iter().enumerate() {
        if count >= 2 {
            participating[pool] = true;
        }
    }
    let mut pool_indices = rustc_hash::FxHashSet::default();
    let mut token_mask = vec![false; token_count];
    for (pool, token) in incidence_edges {
        if participating[pool] {
            pool_indices.insert(pool as u32);
            token_mask[token] = true;
        }
    }
    CycleCapableCoverage {
        token_mask,
        pool_indices,
    }
}
#[must_use]
pub fn clamp_fee_bps(fee_bps: u32) -> u32 {
    fee_bps.min(9_999)
}

/// Major-token-first + high live out-degree hubs for DFS start order.
/// Boosts tokens whose live outgoing edges span more protocols (helps surface
/// cross-protocol cycles instead of pure Balancer-dense hubs).
#[cfg(test)]
pub fn prioritize_cycle_start_tokens(graph: &RoutingGraph) -> Vec<TokenIndex> {
    let mut scored: Vec<(TokenIndex, usize, usize)> = Vec::with_capacity(graph.adjacency.len()); // (token, proto_diversity, degree)
    for (ti, edges) in graph.adjacency.iter().enumerate() {
        let live: Vec<_> = edges.iter().filter(|ge| is_live_graph_edge(ge)).collect();
        if live.is_empty() {
            continue;
        }
        let degree = live.len();
        let mut protos: rustc_hash::FxHashSet<ProtocolType> = rustc_hash::FxHashSet::default();
        for ge in &live {
            protos.insert(ge.edge.protocol);
        }
        let diversity = protos.len();
        scored.push((TokenIndex(ti as u32), diversity, degree));
    }

    scored.sort_by(|a, b| {
        // primary: higher diversity first
        b.1.cmp(&a.1)
            // secondary: higher degree
            .then_with(|| b.2.cmp(&a.2))
            .then_with(|| a.0.0.cmp(&b.0.0))
    });
    scored.into_iter().map(|(t, _, _)| t).collect()
}

pub(crate) fn prioritize_cycle_start_tokens_from_out_degrees(
    out_degrees: impl ExactSizeIterator<Item = usize>,
) -> Vec<TokenIndex> {
    let mut scored: Vec<(TokenIndex, usize)> = out_degrees
        .enumerate()
        .filter(|(_, degree)| *degree > 0)
        .map(|(i, degree)| (TokenIndex(i as u32), degree))
        .collect();
    scored.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.0.cmp(&b.0.0)));
    scored.into_iter().map(|(t, _)| t).collect()
}

struct ActiveGraph {
    /// Live edges only — DFS never walks dead rescored legs.
    adjacency: Vec<Vec<GraphEdge>>,
    start_tokens: Vec<TokenIndex>,
    /// Best live edge leaving each token (used for the next hop in the bound).
    /// Only valid for token indices where adjacency has live edges;
    /// for dead tokens the caller falls through to global_min.
    min_outgoing_weight: Vec<f64>,
    /// Graph-wide minimum live edge (optimistic bound for hops after the first).
    global_min_live_edge_weight: f64,
    /// Best (highest) U256 ratio leaving each token for optimistic profitability bounds.
    max_outgoing_ratio: Vec<U256>,
    /// Graph-wide best live edge ratio.
    global_max_live_edge_ratio: U256,
}

fn prepare_active_graph(graph: &RoutingGraph) -> ActiveGraph {
    let coverage = graph
        .coverage
        .as_ref()
        .map(std::sync::Arc::clone)
        .unwrap_or_else(|| std::sync::Arc::new(cycle_capable_coverage(graph)));
    let token_count = graph.adjacency.len();
    let mut compact = Vec::with_capacity(token_count);
    // min_outgoing lazily populated: only tokens with live edges get entries.
    // Use a sparse representation: None for dead/unreachable tokens.
    let mut min_outgoing: Vec<Option<f64>> = vec![None; token_count];
    let mut max_outgoing_ratio: Vec<U256> = vec![ONE; token_count];
    let mut global_min = f64::INFINITY;
    let mut global_max_ratio = ONE;
    // ponytail: single pass for live edges + diversity scoring (was double iteration).
    let mut scored_div: Vec<(TokenIndex, usize, usize)> = Vec::new();

    for (index, edges) in graph.adjacency.iter().enumerate() {
        if !coverage.token_mask.get(index).copied().unwrap_or(false) {
            compact.push(Vec::new());
            continue;
        }
        let mut live: Vec<GraphEdge> = Vec::with_capacity(edges.len());
        let mut protos: u8 = 0;
        let mut proto_bits = 0u16; // bitmask: ProtocolType has ≤9 variants
        for ge in edges {
            if !is_live_graph_edge(ge) {
                continue;
            }
            let bit = 1u16 << (ge.edge.protocol as u8);
            if proto_bits & bit == 0 {
                protos += 1;
                proto_bits |= bit;
            }
            live.push(*ge);
            let w = ge.log_weight;
            match min_outgoing[index] {
                Some(ref mut best) if w < *best => *best = w,
                None => min_outgoing[index] = Some(w),
                _ => {}
            }
            if w < global_min {
                global_min = w;
            }
            if ge.ratio >= ONE && ge.ratio > max_outgoing_ratio[index] {
                max_outgoing_ratio[index] = ge.ratio;
            }
            if ge.ratio > global_max_ratio {
                global_max_ratio = ge.ratio;
            }
        }
        let len = live.len();
        compact.push(live);
        if len > 0 {
            scored_div.push((TokenIndex(index as u32), protos as usize, len));
        }
    }

    scored_div.sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then_with(|| b.2.cmp(&a.2))
            .then_with(|| a.0.0.cmp(&b.0.0))
    });
    let start_tokens = scored_div.into_iter().map(|(t, _, _)| t).collect();
    // Convert sparse Option<f64> to dense f64 with INFINITY sentinel
    // Only needed for tokens that reached min_outgoing — others get INFINITY
    // which makes can_still_be_negative return false immediately.
    let min_outgoing_dense: Vec<f64> = min_outgoing
        .into_iter()
        .map(|opt| opt.unwrap_or(f64::INFINITY))
        .collect();
    ActiveGraph {
        adjacency: compact,
        start_tokens,
        min_outgoing_weight: min_outgoing_dense,
        global_min_live_edge_weight: if global_min == f64::INFINITY {
            0.0
        } else {
            global_min
        },
        max_outgoing_ratio,
        global_max_live_edge_ratio: global_max_ratio,
    }
}

#[inline]
fn can_still_be_negative(
    log_weight: f64,
    hops: u32,
    hop_cap: u32,
    curr: TokenIndex,
    min_outgoing: &[f64],
    global_min: f64,
) -> bool {
    let remaining = hop_cap.saturating_sub(hops);
    if remaining == 0 {
        return log_weight <= LOG_WEIGHT_PRUNE_THRESHOLD;
    }
    let first = min_outgoing
        .get(curr.0 as usize)
        .copied()
        .unwrap_or(f64::INFINITY);
    if !first.is_finite() {
        return false;
    }
    let tail = match remaining {
        1 => 0.0,
        _ => f64::from(remaining - 1) * global_min,
    };
    log_weight + first + tail <= LOG_WEIGHT_PRUNE_THRESHOLD
}

#[inline]
fn can_still_be_profitable_u256(
    product_ratio: U256,
    hops: u32,
    hop_cap: u32,
    curr: TokenIndex,
    max_outgoing_ratio: &[U256],
    global_max_ratio: U256,
) -> bool {
    if product_ratio > ONE && product_ratio >= min_profitable_cycle_ratio(hops) {
        return true;
    }
    let remaining = hop_cap.saturating_sub(hops);
    if remaining == 0 {
        return false;
    }
    let first = max_outgoing_ratio
        .get(curr.0 as usize)
        .copied()
        .unwrap_or(ONE);
    if first < ONE {
        return false;
    }
    let mut optimistic = mul_ratio_saturating(product_ratio, first);
    if optimistic > ONE {
        return true;
    }
    if remaining > 1 && global_max_ratio >= ONE {
        for _ in 1..remaining {
            optimistic = mul_ratio_saturating(optimistic, global_max_ratio);
            if optimistic > ONE {
                return true;
            }
        }
    }
    false
}

#[inline]
fn can_still_find_profitable_cycle(
    log_weight: f64,
    product_ratio: U256,
    hops: u32,
    hop_cap: u32,
    curr: TokenIndex,
    prep: &ActiveGraph,
) -> bool {
    if product_ratio > ONE {
        return true;
    }
    if !can_still_be_negative(
        log_weight,
        hops,
        hop_cap,
        curr,
        &prep.min_outgoing_weight,
        prep.global_min_live_edge_weight,
    ) {
        return false;
    }
    can_still_be_profitable_u256(
        product_ratio,
        hops,
        hop_cap,
        curr,
        &prep.max_outgoing_ratio,
        prep.global_max_live_edge_ratio,
    )
}

fn collect_cycles_dfs_single_start(
    prep: &ActiveGraph,
    start: TokenIndex,
    hop_limit: u32,
    max_cycles: usize,
    budget: &SharedDeadlineGuard,
) -> Vec<FoundCycle> {
    let hop_cap = hop_limit.min(HOP_CAP);
    // Track visited pools via FxHashSet instead of vec![0u8; pool_slot_count].
    // At most HOP_CAP=8 pools are in-use simultaneously — a hash set with ≤8
    // entries is cheaper to insert/check/clear than zeroing a Vec of thousands.
    let mut used_pools: rustc_hash::FxHashSet<u32> = rustc_hash::FxHashSet::default();
    let mut used_tokens = vec![false; prep.adjacency.len()];
    let mut path = Vec::with_capacity(hop_cap as usize);
    let mut cycles = Vec::new();
    let mut seen = rustc_hash::FxHashSet::default();

    fn pool_mark(used: &mut rustc_hash::FxHashSet<u32>, pool_id: u32) -> bool {
        used.insert(pool_id)
    }

    fn pool_unmark(used: &mut rustc_hash::FxHashSet<u32>, pool_id: u32) {
        used.remove(&pool_id);
    }

    #[allow(clippy::too_many_arguments)]
    fn dfs(
        prep: &ActiveGraph,
        start: TokenIndex,
        curr: TokenIndex,
        path: &mut Vec<Edge>,
        used_pools: &mut rustc_hash::FxHashSet<u32>,
        used_tokens: &mut [bool],
        hops: u32,
        log_w: f64,
        product_ratio: U256,
        cum_fee: u32,
        route_calls: usize,
        hop_cap: u32,
        max_cycles: usize,
        budget: &SharedDeadlineGuard,
        cycles: &mut Vec<FoundCycle>,
        seen: &mut rustc_hash::FxHashSet<u64>,
    ) {
        if budget.tick() || cycles.len() >= max_cycles {
            return;
        }

        if hops >= 2 && curr == start {
            if route_calls > MAX_ROUTE_CALLS
                || product_ratio <= ONE
                || product_ratio < min_profitable_cycle_ratio(hops)
            {
                return;
            }
            let penalty = hop_penalty(hops);
            let score = log_w + penalty;
            if score > LOG_WEIGHT_PRUNE_THRESHOLD {
                return;
            }
            let fp = cycle_key(path);
            if seen.contains(&fp) {
                return;
            }
            seen.insert(fp);
            // ponytail: SmallVec::from(&[T]) copies inline (≤HOP_CAP=8) without
            // iterator adaptor overhead — avoids .iter().copied().collect() dispatch.
            let edges: CycleEdges = CycleEdges::from(path.as_slice());
            cycles.push(FoundCycle {
                start_token: start,
                edges,
                hop_count: hops,
                log_weight: score,
                cumulative_fee_bps: cum_fee,
                score,
                cycle_ratio: product_ratio,
            });
            return;
        }

        if used_tokens[curr.0 as usize] || hops >= hop_cap {
            return;
        }
        if !can_still_find_profitable_cycle(log_w, product_ratio, hops, hop_cap, curr, prep) {
            return;
        }
        let next_edges = match prep.adjacency.get(curr.0 as usize) {
            Some(e) if !e.is_empty() => e,
            _ => return,
        };

        used_tokens[curr.0 as usize] = true;

        for ge in next_edges {
            if budget.tick() || cycles.len() >= max_cycles {
                break;
            }
            if ge.ratio.is_zero() || ge.ratio < ONE {
                continue;
            }
            let pool_id = ge.edge.pool_index.0;
            if !pool_mark(used_pools, pool_id) {
                continue;
            }
            let next_log_w = log_w + ge.log_weight;
            let next_ratio = mul_ratio_saturating(product_ratio, ge.ratio);
            if !can_still_find_profitable_cycle(
                next_log_w,
                next_ratio,
                hops + 1,
                hop_cap,
                ge.edge.token_out,
                prep,
            ) {
                pool_unmark(used_pools, pool_id);
                continue;
            }
            let hop_calls = estimate_hop_calls(ge.edge.protocol);
            if route_calls + hop_calls > MAX_ROUTE_CALLS {
                pool_unmark(used_pools, pool_id);
                continue;
            }

            path.push(ge.edge);
            dfs(
                prep,
                start,
                ge.edge.token_out,
                path,
                used_pools,
                used_tokens,
                hops + 1,
                next_log_w,
                next_ratio,
                cum_fee + clamp_fee_bps(ge.edge.fee_bps),
                route_calls + hop_calls,
                hop_cap,
                max_cycles,
                budget,
                cycles,
                seen,
            );
            path.pop();
            pool_unmark(used_pools, pool_id);
        }

        used_tokens[curr.0 as usize] = false;
    }

    let first_edges = match prep.adjacency.get(start.0 as usize) {
        Some(e) if !e.is_empty() => e,
        _ => return cycles,
    };

    used_tokens[start.0 as usize] = true;
    for ge in first_edges {
        if budget.tick() || cycles.len() >= max_cycles {
            break;
        }
        if ge.ratio.is_zero() || ge.ratio < ONE {
            continue;
        }
        let pool_id = ge.edge.pool_index.0;
        let next_ratio_preview = mul_ratio_saturating(ONE, ge.ratio);
        if !can_still_find_profitable_cycle(
            ge.log_weight,
            next_ratio_preview,
            1,
            hop_cap,
            ge.edge.token_out,
            prep,
        ) {
            continue;
        }
        pool_mark(&mut used_pools, pool_id);
        let hop_calls = estimate_hop_calls(ge.edge.protocol);
        path.push(ge.edge);
        // Start product_ratio = edge.ratio (ONE * edge.ratio / ONE = edge.ratio)
        let initial_product = ge.ratio;
        dfs(
            prep,
            start,
            ge.edge.token_out,
            &mut path,
            &mut used_pools,
            &mut used_tokens,
            1,
            ge.log_weight,
            initial_product,
            clamp_fee_bps(ge.edge.fee_bps),
            hop_calls,
            hop_cap,
            max_cycles,
            budget,
            &mut cycles,
            &mut seen,
        );
        path.pop();
        pool_unmark(&mut used_pools, pool_id);
    }
    used_tokens[start.0 as usize] = false;
    cycles
}

fn collect_cycles_dfs_parallel(
    prep: &ActiveGraph,
    hop_limit: u32,
    max_cycles: usize,
) -> Vec<FoundCycle> {
    let start_tokens = &prep.start_tokens;
    if start_tokens.is_empty() || max_cycles == 0 {
        return Vec::new();
    }
    let budget = SharedDeadlineGuard::new(CYCLE_ENUM_TIME_BUDGET);
    let per_shard = max_cycles.div_ceil(start_tokens.len()).max(1);
    let mut shard_caps = vec![per_shard; start_tokens.len()];
    let mut shard_cycles: Vec<Vec<FoundCycle>> = start_tokens
        .par_iter()
        .zip(shard_caps.par_iter())
        .map(|(start, cap)| {
            collect_cycles_dfs_single_start(prep, *start, hop_limit, *cap, budget.as_ref())
        })
        .collect();

    let mut merged = merge_shard_cycles(&shard_cycles);

    // A flat per-start quota strands most of the global budget when many start
    // tokens are sparse. Reallocate unused capacity to starts that saturated
    // their quota. Two bounded retries recover productive hubs without letting
    // one hub monopolize the first parallel pass.
    for _ in 0..2 {
        if merged.len() >= max_cycles || budget.tick() {
            break;
        }
        let saturated: Vec<usize> = shard_cycles
            .iter()
            .zip(&shard_caps)
            .enumerate()
            .filter_map(|(i, (cycles, cap))| (cycles.len() >= *cap).then_some(i))
            .collect();
        if saturated.is_empty() {
            break;
        }
        let extra = (max_cycles - merged.len()).div_ceil(saturated.len());
        let rerun: Vec<(usize, Vec<FoundCycle>)> = saturated
            .par_iter()
            .map(|&i| {
                let cap = shard_caps[i].saturating_add(extra).min(max_cycles);
                (
                    i,
                    collect_cycles_dfs_single_start(
                        prep,
                        start_tokens[i],
                        hop_limit,
                        cap,
                        budget.as_ref(),
                    ),
                )
            })
            .collect();
        let previous_len = merged.len();
        for (i, cycles) in rerun {
            shard_caps[i] = shard_caps[i].saturating_add(extra).min(max_cycles);
            shard_cycles[i] = cycles;
        }
        merged = merge_shard_cycles(&shard_cycles);
        if merged.len() == previous_len {
            break;
        }
    }

    if merged.len() > max_cycles {
        merged.truncate(max_cycles);
    }
    merged
}

fn merge_shard_cycles(shard_cycles: &[Vec<FoundCycle>]) -> Vec<FoundCycle> {
    use std::collections::hash_map::Entry;

    use crate::pipeline::cycle_filter::cycle_key;
    use crate::pipeline::types::compare_cycle_score;

    let mut best: rustc_hash::FxHashMap<u64, FoundCycle> = rustc_hash::FxHashMap::default();
    for cycle in shard_cycles.iter().flat_map(|s| s.iter()) {
        let key = cycle_key(&cycle.edges);
        match best.entry(key) {
            Entry::Occupied(mut e) => {
                if cycle.score < e.get().score {
                    e.insert(cycle.clone());
                }
            }
            Entry::Vacant(e) => {
                e.insert(cycle.clone());
            }
        }
    }
    let mut out: Vec<FoundCycle> = best.into_values().collect();
    out.sort_unstable_by(compare_cycle_score);
    out
}

#[must_use]
pub fn find_cycles_multi_pass(graph: &RoutingGraph, passes: &[CycleSearchPass]) -> Vec<FoundCycle> {
    if passes.is_empty() {
        return Vec::new();
    }

    let prep = prepare_active_graph(graph);
    if prep.start_tokens.is_empty() {
        return Vec::new();
    }
    let mut all = Vec::new();
    for pass in passes {
        let mut shard = collect_cycles_dfs_parallel(
            &prep,
            pass.max_hops,
            pass.max_cycles.min(MAX_CYCLES_PER_PASS),
        );
        all.append(&mut shard);
    }
    dedupe_cycles_by_edges(all)
}

/// Returns the most frequently used protocol in the cycle (primary protocol for diversity).
#[must_use]
pub fn primary_protocol(edges: &[Edge]) -> ProtocolType {
    // ponytail: fixed-size [u32; 9] stack array avoids both O(n²) nested loop
    // and HashMap alloc. ProtocolType has ≤ 8 variants so this is O(n) with
    // a single pass and zero heap allocation.
    let mut counts = [0u32; 9]; // ProtocolType has 8 variants + sentinel
    for e in edges {
        match e.protocol {
            ProtocolType::UniswapV2 => counts[0] += 1,
            ProtocolType::UniswapV3 => counts[1] += 1,
            ProtocolType::UniswapV4 => counts[2] += 1,
            ProtocolType::BalancerV2 => counts[3] += 1,
            ProtocolType::CurveStable => counts[4] += 1,
            ProtocolType::CurveCrypto => counts[5] += 1,
            ProtocolType::Dodo => counts[6] += 1,
            ProtocolType::Woofi => counts[7] += 1,
        }
    }
    let mut best_idx = 0usize;
    for i in 1..8 {
        if counts[i] > counts[best_idx] {
            best_idx = i;
        }
    }
    match best_idx {
        0 => ProtocolType::UniswapV2,
        1 => ProtocolType::UniswapV3,
        2 => ProtocolType::UniswapV4,
        3 => ProtocolType::BalancerV2,
        4 => ProtocolType::CurveStable,
        5 => ProtocolType::CurveCrypto,
        6 => ProtocolType::Dodo,
        7 => ProtocolType::Woofi,
        _ => unreachable!(),
    }
}

/// Selects up to `max_cycles` opportunities with better protocol distribution.
/// For each protocol that appears, takes its best-scoring cycles in round-robin
/// fashion so that if good opportunities exist in V2/V3/Curve/Dodo/Woofi/etc.
/// they are not crowded out by high-degree Balancer subgraphs (common for 3-hop).
/// Always applies a per-protocol ceiling so that no single protocol dominates even
/// when total cycles are below max_cycles.
#[must_use]
pub fn apply_protocol_diverse_selection(
    cycles: Vec<FoundCycle>,
    max_cycles: usize,
) -> Vec<FoundCycle> {
    if max_cycles == 0 || cycles.is_empty() {
        return vec![];
    }

    let mut groups: rustc_hash::FxHashMap<ProtocolType, Vec<FoundCycle>> =
        rustc_hash::FxHashMap::default();
    groups.reserve(8);
    for c in cycles {
        let p = primary_protocol(&c.edges);
        groups.entry(p).or_default().push(c);
    }

    for g in groups.values_mut() {
        g.sort_by(compare_cycle_score);
    }

    let protos: Vec<ProtocolType> = groups.keys().copied().collect();

    let total: usize = protos.iter().map(|p| groups[p].len()).sum();
    let cap = max_cycles.min(total);
    // ponytail: per-protocol hard ceiling prevents Balancer multi-token pools
    // (n tokens → n*(n-1) edges) from dominating the cycle set. When 2+ protocols
    // have live candidates, no single protocol exceeds 40% of cap. When only one
    // protocol exists, no ceiling applies (no alternative to diversify with).
    let hard_ceiling = if protos.len() <= 1 {
        cap
    } else {
        cap.saturating_mul(40) / 100
    };

    // ponytail: flat Vec<(cursor, count)> indexed by proto position replaces
    // two HashMaps — eliminates hashing overhead on the hot round-robin path.
    let mut proto_state: Vec<(usize, usize)> = vec![(0, 0); protos.len()];
    let mut selected: Vec<FoundCycle> = Vec::with_capacity(cap);
    let mut seen: rustc_hash::FxHashSet<u64> = rustc_hash::FxHashSet::default();

    while selected.len() < cap {
        let mut progressed = false;
        for (i, &p) in protos.iter().enumerate() {
            if selected.len() >= cap {
                break;
            }
            if proto_state[i].1 >= hard_ceiling {
                continue;
            }
            if let Some(g) = groups.get_mut(&p) {
                while proto_state[i].0 < g.len() {
                    let key = crate::pipeline::cycle_filter::cycle_key(&g[proto_state[i].0].edges);
                    if !seen.insert(key) {
                        g.swap_remove(proto_state[i].0);
                        continue;
                    }
                    proto_state[i].1 += 1;
                    selected.push(g.swap_remove(proto_state[i].0));
                    progressed = true;
                    break;
                }
            }
        }
        if !progressed {
            break;
        }
    }

    selected.sort_by(compare_cycle_score);
    selected
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{PoolIndex, ProtocolType};
    use crate::pipeline::types::GraphEdge;

    fn graph_edge(pool: u32, from: TokenIndex, to: TokenIndex) -> GraphEdge {
        GraphEdge {
            edge: Edge {
                pool_index: PoolIndex(pool),
                token_in: from,
                token_out: to,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
            log_weight: -0.01,
            ratio: U256::from(1_000_000_000_000_000_000u64), // ONE
        }
    }

    #[test]
    fn start_token_priority_ignores_dead_out_edges() {
        let hub = TokenIndex(0);
        let tail = TokenIndex(1);
        let mut graph = RoutingGraph::new(2);
        graph.add_edge(hub, graph_edge(0, hub, tail));
        let mut dead = graph_edge(1, tail, hub);
        dead.log_weight = DEAD_EDGE_LOG_WEIGHT;
        dead.ratio = U256::ZERO;
        graph.add_edge(tail, dead);
        graph.add_edge(tail, graph_edge(2, tail, hub));

        let order = prioritize_cycle_start_tokens(&graph);
        assert_eq!(order, vec![hub, tail]);
    }

    #[test]
    fn test_empty_degrees_returns_empty() {
        let degrees: [usize; 0] = [];
        let r = prioritize_cycle_start_tokens_from_out_degrees(degrees.into_iter());
        assert!(r.is_empty());
    }

    #[test]
    fn cycle_coverage_excludes_one_way_spokes() {
        let a = TokenIndex(0);
        let b = TokenIndex(1);
        let dead_end = TokenIndex(2);
        let mut graph = RoutingGraph::new(3);
        graph.add_edge(a, graph_edge(0, a, b));
        graph.add_edge(b, graph_edge(1, b, a));
        graph.add_edge(b, graph_edge(2, b, dead_end));

        let coverage = cycle_capable_coverage(&graph);
        assert!(coverage.pool_indices.contains(&0));
        assert!(coverage.pool_indices.contains(&1));
        assert!(!coverage.pool_indices.contains(&2));

        let active = prepare_active_graph(&graph);
        assert_eq!(active.start_tokens.len(), 2);
        assert!(active.start_tokens.contains(&a));
        assert!(active.start_tokens.contains(&b));
        assert!(!active.start_tokens.contains(&dead_end));
    }

    #[test]
    fn dead_edges_do_not_make_a_component_cycle_capable() {
        let a = TokenIndex(0);
        let b = TokenIndex(1);
        let mut graph = RoutingGraph::new(2);
        graph.add_edge(a, graph_edge(0, a, b));
        let mut dead_return = graph_edge(1, b, a);
        dead_return.log_weight = DEAD_EDGE_LOG_WEIGHT;
        dead_return.ratio = U256::ZERO;
        graph.add_edge(b, dead_return);

        let coverage = cycle_capable_coverage(&graph);
        assert!(coverage.pool_indices.is_empty());
        assert!(prepare_active_graph(&graph).start_tokens.is_empty());
    }

    #[test]
    fn isolated_bidirectional_pool_cannot_cycle_through_itself() {
        let a = TokenIndex(0);
        let b = TokenIndex(1);
        let mut graph = RoutingGraph::new(2);
        graph.add_edge(a, graph_edge(0, a, b));
        graph.add_edge(b, graph_edge(0, b, a));

        let coverage = cycle_capable_coverage(&graph);
        assert!(coverage.pool_indices.is_empty());
        assert!(prepare_active_graph(&graph).start_tokens.is_empty());
    }

    #[test]
    fn dfs_keeps_profitable_cycle_with_positive_prefix() {
        let a = TokenIndex(0);
        let b = TokenIndex(1);
        let mut graph = RoutingGraph::new(2);
        graph.add_edge(
            a,
            GraphEdge {
                edge: Edge {
                    pool_index: PoolIndex(0),
                    token_in: a,
                    token_out: b,
                    token_in_idx: 0,
                    token_out_idx: 1,
                    protocol: ProtocolType::UniswapV2,
                    fee_bps: 30,
                    zero_for_one: true,
                },
                log_weight: 0.10,
                ratio: U256::from(1_000_000_000_000_000_000u64),
            },
        );
        graph.add_edge(
            b,
            GraphEdge {
                edge: Edge {
                    pool_index: PoolIndex(1),
                    token_in: b,
                    token_out: a,
                    token_in_idx: 0,
                    token_out_idx: 1,
                    protocol: ProtocolType::UniswapV2,
                    fee_bps: 30,
                    zero_for_one: true,
                },
                log_weight: -1.0,
                ratio: U256::from(1_000_000_000_000_000_001u64),
            },
        );

        let mut prep = prepare_active_graph(&graph);
        prep.start_tokens = vec![a];
        let cycles = collect_cycles_dfs_parallel(&prep, 2, 10);
        assert_eq!(cycles.len(), 1);
        assert_eq!(cycles[0].hop_count, 2);
        assert!(cycles[0].score < 0.0);
    }

    #[test]
    fn u256_bound_keeps_prefix_when_ratio_can_recover() {
        let max_out = [ONE + U256::from(2u64), ONE + U256::from(1u64)];
        assert!(can_still_be_profitable_u256(
            ONE,
            1,
            2,
            TokenIndex(0),
            &max_out,
            ONE + U256::from(1u64),
        ));
        assert!(!can_still_be_profitable_u256(
            ONE,
            1,
            2,
            TokenIndex(0),
            &[ONE, ONE],
            ONE,
        ));
    }

    #[test]
    fn optimistic_bound_only_prunes_unrecoverable_prefixes() {
        let min_out = [-1.0, 0.5];
        assert!(can_still_be_negative(
            0.10,
            1,
            2,
            TokenIndex(0),
            &min_out,
            -1.0
        ));
        assert!(!can_still_be_negative(
            1.10,
            1,
            2,
            TokenIndex(0),
            &min_out,
            -1.0
        ));
    }

    #[test]
    fn per_token_min_tightens_pruning_vs_global_min() {
        let min_out = [0.2, -1.0];
        // Global min (-1.0) would keep this branch; local min at token 0 is 0.2.
        assert!(!can_still_be_negative(
            0.10,
            1,
            2,
            TokenIndex(0),
            &min_out,
            -1.0
        ));
        assert!(can_still_be_negative(
            0.10,
            1,
            2,
            TokenIndex(1),
            &min_out,
            -1.0
        ));
    }

    #[test]
    fn parallel_dfs_reallocates_unused_start_budget() {
        fn edge(pool: u32, token_in: TokenIndex, token_out: TokenIndex) -> GraphEdge {
            GraphEdge {
                edge: Edge {
                    pool_index: PoolIndex(pool),
                    token_in,
                    token_out,
                    token_in_idx: 0,
                    token_out_idx: 1,
                    protocol: ProtocolType::UniswapV2,
                    fee_bps: 30,
                    zero_for_one: true,
                },
                log_weight: -1.0,
                ratio: U256::from(1_000_000_000_000_000_001u64),
            }
        }

        let dead = TokenIndex(0);
        let hub = TokenIndex(1);
        let mut graph = RoutingGraph::new(5);
        graph.add_edge(dead, edge(0, dead, TokenIndex(4)));
        for (branch, token) in [TokenIndex(2), TokenIndex(3), TokenIndex(4)]
            .into_iter()
            .enumerate()
        {
            let pool = 1 + (branch as u32 * 2);
            graph.add_edge(hub, edge(pool, hub, token));
            graph.add_edge(token, edge(pool + 1, token, hub));
        }

        let mut prep = prepare_active_graph(&graph);
        prep.start_tokens = vec![dead, hub];
        let cycles = collect_cycles_dfs_parallel(&prep, 2, 4);
        assert_eq!(cycles.len(), 3);
    }
}
