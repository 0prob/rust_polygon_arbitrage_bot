use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::time::Duration;

use rayon::prelude::*;

use alloy::primitives::U256;

use crate::core::constants::{HOP_CAP, MAX_POOL_TOKENS};
use crate::core::math::fixed_point::ONE;
use crate::core::types::{
    CycleEdges, Edge, FoundCycle, PoolIndex, PoolState, ProtocolType, TokenIndex,
};
use crate::pipeline::arena::StateArena;
use crate::pipeline::cycle_filter::{cycle_key, dedupe_cycles_by_edges};
use crate::pipeline::deadline::SharedDeadlineGuard;
use crate::pipeline::graph::{PendingHubSwap, resolve_lazy_swap_edge};
use crate::pipeline::route_calls::{estimate_hop_calls, packed_calls_fit_executor};
use crate::pipeline::spot_price::{min_profitable_cycle_ratio, mul_ratio_saturating};
use crate::pipeline::types::{
    CycleSearchPass, GraphEdge, GraphHopPhase, PoolMeta, RoutingGraph, compare_cycle_execution,
    compare_cycle_score,
};

pub use crate::pipeline::spot_price::hop_penalty;

const MAX_CYCLES_PER_PASS: usize = 50_000;
pub const CYCLE_ENUM_TIME_BUDGET: Duration = Duration::from_millis(1000);
/// Budget for patch-only refinds that merge into an existing cycle cache.
pub const CYCLE_ENUM_PATCH_BUDGET: Duration = Duration::from_millis(300);
/// Cap parallel DFS shards — unbounded hub enumeration burns the shared deadline.
const DFS_MAX_START_SOURCES: usize = 32;
/// Amortize elapsed-time checks during DFS enumeration.
/// Prune DFS branches once log-weight exceeds this (spot-weighted graphs only).
const LOG_WEIGHT_PRUNE_THRESHOLD: f64 = 0.0;
/// Edges rescored to this weight are non-tradable — skip during enumeration.
pub(crate) const DEAD_EDGE_LOG_WEIGHT: f64 = 15.0;

/// Shared across parallel DFS start shards so the global `max_cycles` cap
/// stops all workers once enough cycles are collected (not just per-shard).
struct SharedCycleCap {
    max: usize,
    found: AtomicUsize,
}

impl SharedCycleCap {
    fn new(max: usize) -> Arc<Self> {
        Arc::new(Self {
            max,
            found: AtomicUsize::new(0),
        })
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.found.load(AtomicOrdering::Relaxed) >= self.max
    }

    /// True when claimed count is at least `num/den` of max (pin-search headroom).
    #[inline]
    fn is_past_fraction(&self, num: usize, den: usize) -> bool {
        if den == 0 || self.max == 0 {
            return self.is_full();
        }
        self.found.load(AtomicOrdering::Relaxed).saturating_mul(den) >= self.max.saturating_mul(num)
    }

    /// Reserve one slot. Returns false when the global cap is already filled.
    #[inline]
    fn try_claim(&self) -> bool {
        let mut cur = self.found.load(AtomicOrdering::Relaxed);
        loop {
            if cur >= self.max {
                return false;
            }
            match self.found.compare_exchange_weak(
                cur,
                cur + 1,
                AtomicOrdering::Relaxed,
                AtomicOrdering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(v) => cur = v,
            }
        }
    }

    /// Reset the claimed count before a reallocation pass (deduped merge may
    /// free headroom; re-runs must be allowed to re-claim up to `max`).
    fn reset_to(&self, count: usize) {
        self.found
            .store(count.min(self.max), AtomicOrdering::Relaxed);
    }
}

type PoolMetaIndex<'a> = Vec<Option<&'a PoolMeta>>;

pub(crate) fn index_pool_metas(pool_metas: &[PoolMeta]) -> PoolMetaIndex<'_> {
    let mut index = vec![
        None;
        pool_metas
            .iter()
            .map(|meta| meta.pool_index.0 as usize)
            .max()
            .map_or(0, |max| max + 1)
    ];
    for meta in pool_metas {
        index[meta.pool_index.0 as usize] = Some(meta);
    }
    index
}

#[inline]
#[must_use]
pub fn is_live_graph_edge(ge: &GraphEdge) -> bool {
    match ge.phase {
        GraphHopPhase::EnterPool | GraphHopPhase::ExitPool => true,
        GraphHopPhase::Direct => !ge.ratio.is_zero(),
    }
}

/// Structural cycle coverage for the current live routing graph.
///
/// Pools are represented as nodes in a token-pool bipartite graph. A pool can
/// participate in a route cycle only when at least two of its token incidences
/// are non-bridges. This correctly excludes an isolated bidirectional AMM pool:
/// returning through the same pool is forbidden by route enumeration.
/// Dense bitmask over token indices for cycle-capable coverage.
/// 64 tokens per `u64` word — set and test are single bitwise instructions,
/// and 2 000 tokens consume just 32 cache lines vs. 2 000 with `Vec<bool>`.
#[derive(Debug, Default, Clone)]
pub struct CycleCapableCoverage {
    /// Packed token membership: bit `i % 64` of word `i / 64`.
    token_mask: Vec<u64>,
    /// Number of tokens the mask was built for (needed by is_token_capable).
    token_count: usize,
    pub pool_indices: rustc_hash::FxHashSet<u32>,
}

impl CycleCapableCoverage {
    #[inline]
    fn mask_set(words: &mut [u64], idx: usize) {
        let word = idx / 64;
        let bit = idx % 64;
        if word < words.len() {
            words[word] |= 1u64 << bit;
        }
    }

    #[inline]
    #[must_use]
    pub fn is_token_capable(&self, token_idx: usize) -> bool {
        if token_idx >= self.token_count {
            return false;
        }
        let word = token_idx / 64;
        let bit = token_idx % 64;
        word < self.token_mask.len() && (self.token_mask[word] >> bit) & 1 == 1
    }
}

#[must_use]
pub fn cycle_capable_coverage(graph: &RoutingGraph) -> CycleCapableCoverage {
    let token_count = graph.token_count as usize;
    let pool_count = graph.pool_edge_positions.len();
    let node_count = token_count.saturating_add(pool_count);
    let mut incidences = rustc_hash::FxHashSet::default();
    for edges in graph.adjacency.iter().take(token_count) {
        for ge in edges {
            if !is_live_graph_edge(ge) {
                continue;
            }
            match ge.phase {
                GraphHopPhase::Direct => {
                    incidences.insert((ge.edge.pool_index.0 as usize, ge.edge.token_in.0 as usize));
                    incidences
                        .insert((ge.edge.pool_index.0 as usize, ge.edge.token_out.0 as usize));
                }
                GraphHopPhase::EnterPool => {
                    incidences.insert((ge.edge.pool_index.0 as usize, ge.edge.token_in.0 as usize));
                }
                GraphHopPhase::ExitPool => {}
            }
        }
    }
    for edges in graph.adjacency.iter().skip(token_count) {
        for ge in edges {
            if ge.phase == GraphHopPhase::ExitPool && is_live_graph_edge(ge) {
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
    // Dense bitmask: token_count bits packed into ceil(token_count/64) u64 words.
    let words = token_count.div_ceil(64);
    let mut token_mask = vec![0u64; words];
    for (pool, token) in incidence_edges {
        if participating[pool] {
            pool_indices.insert(pool as u32);
            CycleCapableCoverage::mask_set(&mut token_mask, token);
        }
    }
    CycleCapableCoverage {
        token_mask,
        token_count,
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

/// DFS enumeration view of a routing graph (live edges + pruning bounds).
pub struct ActiveGraph {
    /// Live edges only — DFS never walks dead rescored legs.
    adjacency: Vec<Vec<GraphEdge>>,
    start_tokens: Vec<TokenIndex>,
    /// True when hub Enter legs outnumber Direct legs (hybrid BF budget shrink).
    pub hub_heavy: bool,
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
    /// When set, the first hop from each start token must use one of these pools
    /// (observed-admit exclusive DFS — otherwise SharedCycleCap fills with
    /// unrelated spokes from the same endpoints).
    first_hop_pools: Option<rustc_hash::FxHashSet<PoolIndex>>,
}

impl ActiveGraph {
    #[must_use]
    pub fn start_token_count(&self) -> usize {
        self.start_tokens.len()
    }

    /// Prepend DFS start tokens (e.g. endpoints of WSS-observed pools) so patch
    /// enumeration is not hub-blind for freshly admitted venues.
    ///
    /// `exclusive`: only seed `extra` (no hub fill). Use for observed-admit
    /// refinds so SharedCycleCap is not filled by hub routes before peripherals.
    pub fn prefer_start_tokens(&mut self, extra: &[TokenIndex], exclusive: bool) {
        if extra.is_empty() {
            return;
        }
        let extra_cap = extra.len().min(16);
        let mut seen = rustc_hash::FxHashSet::default();
        let mut out = Vec::with_capacity(if exclusive {
            extra_cap
        } else {
            DFS_MAX_START_SOURCES + extra_cap
        });
        for &t in extra.iter().take(extra_cap) {
            if seen.insert(t) {
                out.push(t);
            }
        }
        if !exclusive {
            let hub_cap = DFS_MAX_START_SOURCES;
            for &t in &self.start_tokens {
                if out.len() >= hub_cap + extra_cap {
                    break;
                }
                if seen.insert(t) {
                    out.push(t);
                }
            }
        }
        self.start_tokens = out;
    }

    /// Prefer opening hops through `pools` (observed venue pin). When a start has
    /// any pin edge, DFS opening hard-requires it. Drop starts that cannot open
    /// through a pin — sibling non-pin starts filled SharedCycleCap (live:
    /// first_hop_pin>0 raw=100+ enum_touch=0).
    pub fn prioritize_first_hop_pools(&mut self, pools: &[PoolIndex]) {
        if pools.is_empty() {
            return;
        }
        let pin: rustc_hash::FxHashSet<PoolIndex> = pools.iter().copied().collect();
        self.first_hop_pools = Some(pin.clone());
        let pin_starts: Vec<TokenIndex> = self
            .start_tokens
            .iter()
            .copied()
            .filter(|&t| {
                self.adjacency
                    .get(t.0 as usize)
                    .is_some_and(|edges| edges.iter().any(|ge| pin.contains(&ge.edge.pool_index)))
            })
            .collect();
        if !pin_starts.is_empty() {
            self.start_tokens = pin_starts;
        }
        for &t in &self.start_tokens {
            let Some(edges) = self.adjacency.get_mut(t.0 as usize) else {
                continue;
            };
            edges.sort_by_key(|ge| !pin.contains(&ge.edge.pool_index));
        }
    }

    /// Drop Balancer/Woofi/DODO sidehops that aren't the observed pin — exclusive
    /// obs DFS otherwise fills with multi-fail pins (live: uni_only=0 enum_touch=11).
    pub fn retain_uni_or_pin_edges(&mut self, pools: &[PoolIndex]) {
        let pin: rustc_hash::FxHashSet<PoolIndex> = pools.iter().copied().collect();
        for edges in &mut self.adjacency {
            edges.retain(|ge| {
                pin.contains(&ge.edge.pool_index)
                    || matches!(
                        ge.edge.protocol,
                        ProtocolType::UniswapV2 | ProtocolType::UniswapV3 | ProtocolType::UniswapV4
                    )
            });
        }
    }

    /// Ensure exclusive DFS can open through observed pin pools.
    ///
    /// - Fresh attach leaves Direct pins at ratio=0 (not live in ActiveGraph).
    /// - Coverage prep may strip tokens whose only pin edge is a bridge-side
    ///   incidence even when the RoutingGraph still has the live Direct.
    ///
    /// Inject any missing pin Direct (zero-ratio → ONE filler; live ratios kept).
    pub fn inject_unpriced_pin_directs(&mut self, graph: &RoutingGraph, pools: &[PoolIndex]) {
        if pools.is_empty() {
            return;
        }
        let pin: rustc_hash::FxHashSet<PoolIndex> = pools.iter().copied().collect();
        let token_n = graph.token_count as usize;
        for (ti, edges) in graph.adjacency.iter().enumerate().take(token_n) {
            while self.adjacency.len() <= ti {
                self.adjacency.push(Vec::new());
            }
            for ge in edges {
                if ge.phase != GraphHopPhase::Direct || !pin.contains(&ge.edge.pool_index) {
                    continue;
                }
                let already = self.adjacency[ti].iter().any(|e| {
                    e.edge.pool_index == ge.edge.pool_index
                        && e.edge.token_out == ge.edge.token_out
                        && e.phase == GraphHopPhase::Direct
                });
                if already {
                    continue;
                }
                let mut injected = *ge;
                if injected.ratio.is_zero() {
                    injected.ratio = ONE;
                    injected.log_weight = 0.0;
                }
                self.adjacency[ti].push(injected);
            }
        }
    }
}

struct NodePrep {
    live: Vec<GraphEdge>,
    protos: u8,
    min_outgoing: Option<f64>,
    max_outgoing_ratio: U256,
    enter_legs: u16,
    direct_legs: u16,
}

fn compare_cycle_start_source(
    a: &(TokenIndex, usize, usize),
    b: &(TokenIndex, usize, usize),
) -> std::cmp::Ordering {
    b.1.cmp(&a.1)
        .then_with(|| b.2.cmp(&a.2))
        .then_with(|| a.0.0.cmp(&b.0.0))
}

fn select_cycle_start_tokens(mut scored: Vec<(TokenIndex, usize, usize)>) -> Vec<TokenIndex> {
    let take = scored.len().min(DFS_MAX_START_SOURCES);
    if take == 0 {
        return Vec::new();
    }
    if take < scored.len() {
        scored.select_nth_unstable_by(take, compare_cycle_start_source);
    }
    scored[..take].sort_unstable_by(compare_cycle_start_source);
    scored
        .into_iter()
        .take(take)
        .map(|(token, _, _)| token)
        .collect()
}

fn prep_node(
    index: usize,
    token_count: usize,
    graph: &RoutingGraph,
    coverage: &CycleCapableCoverage,
) -> NodePrep {
    let edges = graph.adjacency.get(index).map(Vec::as_slice).unwrap_or(&[]);
    if index < token_count && !coverage.is_token_capable(index) {
        return NodePrep {
            live: Vec::new(),
            protos: 0,
            min_outgoing: None,
            max_outgoing_ratio: ONE,
            enter_legs: 0,
            direct_legs: 0,
        };
    }
    let mut live: Vec<GraphEdge> = Vec::with_capacity(edges.len());
    let mut protos: u8 = 0;
    let mut proto_bits = 0u16;
    let mut min_outgoing: Option<f64> = None;
    let mut max_outgoing_ratio = ONE;
    let mut enter_legs = 0u16;
    let mut direct_legs = 0u16;
    for ge in edges {
        if !is_live_graph_edge(ge) {
            continue;
        }
        if index < token_count {
            match ge.phase {
                GraphHopPhase::EnterPool => enter_legs = enter_legs.saturating_add(1),
                GraphHopPhase::Direct => direct_legs = direct_legs.saturating_add(1),
                GraphHopPhase::ExitPool => {}
            }
            let bit = 1u16 << (ge.edge.protocol as u8);
            if proto_bits & bit == 0 {
                protos += 1;
                proto_bits |= bit;
            }
            if ge.phase == GraphHopPhase::Direct {
                let w = ge.log_weight;
                match min_outgoing {
                    Some(ref mut best) if w < *best => *best = w,
                    None => min_outgoing = Some(w),
                    _ => {}
                }
                if ge.ratio >= ONE && ge.ratio > max_outgoing_ratio {
                    max_outgoing_ratio = ge.ratio;
                }
            }
        }
        live.push(*ge);
    }
    // Best (most negative log-weight) edges first so DFS finds
    // profitable cycles earlier and can hit max_cycles before deadline.
    live.sort_unstable_by(|a, b| {
        a.log_weight
            .partial_cmp(&b.log_weight)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    NodePrep {
        live,
        protos,
        min_outgoing,
        max_outgoing_ratio,
        enter_legs,
        direct_legs,
    }
}

/// Build the live-edge DFS view and hub/direct stats (single graph scan).
#[must_use]
pub fn prepare_active_graph(graph: &RoutingGraph) -> ActiveGraph {
    let coverage = graph
        .coverage
        .as_ref()
        .map(std::sync::Arc::clone)
        .unwrap_or_else(|| std::sync::Arc::new(cycle_capable_coverage(graph)));
    let token_count = graph.token_count as usize;
    let node_count = graph.adjacency.len();

    let per_node: Vec<NodePrep> = if crate::util::should_use_rayon(node_count) {
        (0..node_count)
            .into_par_iter()
            .map(|index| prep_node(index, token_count, graph, coverage.as_ref()))
            .collect()
    } else {
        (0..node_count)
            .map(|index| prep_node(index, token_count, graph, coverage.as_ref()))
            .collect()
    };

    let mut hub_enter = 0usize;
    let mut hub_direct = 0usize;
    let mut compact = Vec::with_capacity(node_count);
    let mut min_outgoing: Vec<Option<f64>> = vec![None; token_count];
    let mut max_outgoing_ratio: Vec<U256> = vec![ONE; token_count];
    let mut global_min = f64::INFINITY;
    let mut global_max_ratio = ONE;
    let mut scored_div: Vec<(TokenIndex, usize, usize)> = Vec::with_capacity(token_count);

    for (index, node) in per_node.into_iter().enumerate() {
        let len = node.live.len();
        if index < token_count {
            hub_enter += node.enter_legs as usize;
            hub_direct += node.direct_legs as usize;
            if let Some(w) = node.min_outgoing {
                min_outgoing[index] = Some(w);
                if w < global_min {
                    global_min = w;
                }
            }
            if node.max_outgoing_ratio > max_outgoing_ratio[index] {
                max_outgoing_ratio[index] = node.max_outgoing_ratio;
            }
            if node.max_outgoing_ratio > global_max_ratio {
                global_max_ratio = node.max_outgoing_ratio;
            }
            if len > 0 {
                scored_div.push((TokenIndex(index as u32), node.protos as usize, len));
            }
        }
        compact.push(node.live);
    }

    let start_tokens = select_cycle_start_tokens(scored_div);
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
        hub_heavy: hub_enter > hub_direct,
        min_outgoing_weight: min_outgoing_dense,
        global_min_live_edge_weight: if global_min == f64::INFINITY {
            0.0
        } else {
            global_min
        },
        max_outgoing_ratio,
        global_max_live_edge_ratio: global_max_ratio,
        first_hop_pools: None,
    }
}

#[inline]
fn route_hop_budget_exceeded(hop_call_sum: u16) -> bool {
    !packed_calls_fit_executor(hop_call_sum as usize)
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
    // DFS stores hop_penalty in score only at cycle close; reserve worst-case depth penalty.
    let close_penalty = hop_penalty(hop_cap.max(2));
    let remaining = hop_cap.saturating_sub(hops);
    if remaining == 0 {
        return log_weight + close_penalty <= LOG_WEIGHT_PRUNE_THRESHOLD;
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
    log_weight + first + tail + close_penalty <= LOG_WEIGHT_PRUNE_THRESHOLD
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
fn edge_is_pin(prep: &ActiveGraph, pool: PoolIndex) -> bool {
    prep.first_hop_pools
        .as_ref()
        .is_some_and(|pins| pins.contains(&pool))
}

#[inline]
fn can_still_find_profitable_cycle(
    log_weight: f64,
    product_ratio: U256,
    hops: u32,
    hop_cap: u32,
    curr: TokenIndex,
    prep: &ActiveGraph,
    pin_touched: bool,
) -> bool {
    // Once a pin pool is on the path (or pending hub enter), keep exploring so
    // pin closes survive underwater intermediate scores (live: first_hop_pin>0
    // enum_touch=0 under profit prune). Disabling prune for *all* branches while
    // first_hop_pools is set burned the full 1s enum budget on non-pin Uni filler.
    if pin_touched {
        return true;
    }
    let hop_floor = hops.max(2);
    if product_ratio > ONE && product_ratio >= min_profitable_cycle_ratio(hop_floor) {
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

fn hub_exit_legs(
    graph: &RoutingGraph,
    hub_node: u32,
    meta: &PoolMeta,
    state: &PoolState,
) -> smallvec::SmallVec<[u8; MAX_POOL_TOKENS]> {
    // Prefer live funded legs so newly funded Balancer/Woofi tokens appear mid-cache.
    let live = crate::pipeline::graph::funded_token_indices(state, meta);
    if !live.is_empty() {
        return live;
    }
    let Some(idx) = graph.virtual_hub_index(hub_node) else {
        return smallvec::SmallVec::new();
    };
    graph
        .virtual_hubs
        .get(idx)
        .map(|hub| hub.exit_legs.clone())
        .unwrap_or_default()
}

#[allow(clippy::too_many_arguments)]
/// O(1) pool-reuse bitset for DFS. Sized to arena/graph slots and grown if a
/// stale edge carries a higher `pool_index` (never treat OOB as "already used").
#[inline]
fn pool_mark(used: &mut Vec<bool>, pool_id: u32) -> bool {
    let idx = pool_id as usize;
    if idx >= used.len() {
        used.resize(idx + 1, false);
    }
    if used[idx] {
        false
    } else {
        used[idx] = true;
        true
    }
}

#[inline]
fn pool_unmark(used: &mut [bool], pool_id: u32) {
    if let Some(slot) = used.get_mut(pool_id as usize) {
        *slot = false;
    }
}

#[derive(Default)]
pub struct DfsScratch {
    pub used_pools: Vec<bool>,
    pub used_tokens: Vec<bool>,
    pub path: Vec<Edge>,
}

impl DfsScratch {
    pub fn prepare(&mut self, pool_slots: usize, token_count: usize, hop_cap: usize) {
        if self.used_pools.len() < pool_slots {
            self.used_pools.resize(pool_slots, false);
        } else {
            self.used_pools[..pool_slots].fill(false);
        }
        if self.used_tokens.len() < token_count {
            self.used_tokens.resize(token_count, false);
        } else {
            self.used_tokens[..token_count].fill(false);
        }
        self.path.clear();
        if self.path.capacity() < hop_cap {
            self.path
                .reserve(hop_cap.saturating_sub(self.path.capacity()));
        }
    }
}

thread_local! {
    static DFS_SCRATCH: std::cell::RefCell<DfsScratch> = std::cell::RefCell::new(DfsScratch::default());
}

#[allow(clippy::too_many_arguments)]
fn collect_cycles_dfs_single_start(
    graph: &RoutingGraph,
    arena: &StateArena,
    pool_metas: &PoolMetaIndex<'_>,
    prep: &ActiveGraph,
    start: TokenIndex,
    hop_limit: u32,
    max_cycles: usize,
    budget: &SharedDeadlineGuard,
    global_cap: &SharedCycleCap,
) -> Vec<FoundCycle> {
    let hop_cap = hop_limit.min(HOP_CAP);
    let token_count = graph.token_count as usize;
    // Dense arena indices are usually the tight upper bound; positions may lag
    // after partial attach, so take the max and grow on demand in `pool_mark`.
    let pool_slots = graph
        .pool_edge_positions
        .len()
        .max(arena.pool_count())
        .max(1);

    DFS_SCRATCH.with(|scratch_cell| {
        let mut scratch = scratch_cell.borrow_mut();
        scratch.prepare(pool_slots, token_count, hop_cap as usize);
        let DfsScratch {
            used_pools,
            used_tokens,
            path,
        } = &mut *scratch;

        let mut cycles = Vec::new();
        let mut seen = rustc_hash::FxHashSet::default();

        #[allow(clippy::too_many_arguments)]
        fn dfs(
            graph: &RoutingGraph,
            arena: &StateArena,
            pool_metas: &PoolMetaIndex<'_>,
            prep: &ActiveGraph,
            start: TokenIndex,
            curr_node: u32,
            pending: Option<PendingHubSwap>,
            path: &mut Vec<Edge>,
            path_hop_calls: u16,
            used_pools: &mut Vec<bool>,
            used_tokens: &mut [bool],
            hops: u32,
            log_w: f64,
            product_ratio: U256,
            cum_fee: u32,
            hop_cap: u32,
            max_cycles: usize,
            budget: &SharedDeadlineGuard,
            global_cap: &SharedCycleCap,
            cycles: &mut Vec<FoundCycle>,
            seen: &mut rustc_hash::FxHashSet<u64>,
            pin_touched: bool,
        ) {
            if budget.tick() || cycles.len() >= max_cycles || global_cap.is_full() {
                return;
            }

            if graph.is_virtual_node(curr_node) {
                let Some(pending) = pending else {
                    return;
                };
                let pool_id = pending.pool_index.0;
                if !pool_mark(used_pools, pool_id) {
                    return;
                }
                // Hub enter already marks pin_touched when the pending pool is a pin.
                let hub_pin = pin_touched || edge_is_pin(prep, pending.pool_index);
                // Hoist meta/state once — exit-leg loop used to re-fetch every iteration.
                let Some(meta) = pool_metas
                    .get(pending.pool_index.0 as usize)
                    .and_then(Option::as_ref)
                else {
                    pool_unmark(used_pools, pool_id);
                    return;
                };
                let Some(state) = arena.pool_state(pending.pool_index) else {
                    pool_unmark(used_pools, pool_id);
                    return;
                };
                let exit_legs = hub_exit_legs(graph, curr_node, meta, state);
                for out_leg in exit_legs {
                    if out_leg == pending.token_in_idx {
                        continue;
                    }
                    if budget.tick() || cycles.len() >= max_cycles || global_cap.is_full() {
                        break;
                    }
                    let out_idx = out_leg as usize;
                    let Some(token_out) =
                        crate::pipeline::graph::routing_token_at_leg(arena, state, meta, out_idx)
                    else {
                        continue;
                    };
                    let Some((edge, edge_log_w, ratio)) =
                        resolve_lazy_swap_edge(arena, pending, token_out, out_leg)
                    else {
                        continue;
                    };
                    let next_log_w = log_w + edge_log_w;
                    let next_ratio = mul_ratio_saturating(product_ratio, ratio);
                    if !can_still_find_profitable_cycle(
                        next_log_w,
                        next_ratio,
                        hops + 1,
                        hop_cap,
                        token_out,
                        prep,
                        hub_pin,
                    ) {
                        continue;
                    }
                    let hop_calls = estimate_hop_calls(edge.protocol) as u16;
                    if route_hop_budget_exceeded(path_hop_calls.saturating_add(hop_calls)) {
                        continue;
                    }
                    path.push(edge);
                    dfs(
                        graph,
                        arena,
                        pool_metas,
                        prep,
                        start,
                        token_out.0,
                        None,
                        path,
                        path_hop_calls.saturating_add(hop_calls),
                        used_pools,
                        used_tokens,
                        hops + 1,
                        next_log_w,
                        next_ratio,
                        cum_fee + clamp_fee_bps(edge.fee_bps),
                        hop_cap,
                        max_cycles,
                        budget,
                        global_cap,
                        cycles,
                        seen,
                        hub_pin,
                    );
                    path.pop();
                }
                pool_unmark(used_pools, pool_id);
                return;
            }

            let curr_idx = curr_node as usize;
            // Stale graphs can carry target_node / token ids past token_count.
            if curr_idx >= used_tokens.len() {
                return;
            }
            let curr = TokenIndex(curr_node);
            if hops >= 2 && curr == start {
                if route_hop_budget_exceeded(path_hop_calls) {
                    return;
                }
                // Observed pin search: relax close profit/score for pin-touching
                // paths (live: first_hop_pin>0 enum_touch=0 under min_ratio).
                // Hard pin-only recording zeroed exclusive DFS (raw=0); pin paths
                // rarely close before budget while non-pin Uni fills the cap —
                // prefer pin via prioritize_first_hop + pin_cycles_touching_pools.
                let pin_touch = pin_touched
                    || prep
                        .first_hop_pools
                        .as_ref()
                        .is_some_and(|pins| path.iter().any(|e| pins.contains(&e.pool_index)));
                if !pin_touch
                    && (product_ratio <= ONE || product_ratio < min_profitable_cycle_ratio(hops))
                {
                    return;
                }
                let penalty = hop_penalty(hops);
                let score = log_w + penalty;
                if !pin_touch && score > LOG_WEIGHT_PRUNE_THRESHOLD {
                    return;
                }
                let fp = cycle_key(path);
                if seen.contains(&fp) {
                    return;
                }
                // Reserve half the shared cap for pin-touch closes during obs search
                // (live: non-pin Uni filled cap before pin paths returned).
                if prep.first_hop_pools.is_some() && !pin_touch && global_cap.is_past_fraction(1, 2)
                {
                    return;
                }
                if !global_cap.try_claim() {
                    return;
                }
                seen.insert(fp);
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

            if used_tokens[curr_idx] || hops >= hop_cap {
                return;
            }
            if !can_still_find_profitable_cycle(
                log_w,
                product_ratio,
                hops,
                hop_cap,
                curr,
                prep,
                pin_touched,
            ) {
                return;
            }
            let next_edges = match prep.adjacency.get(curr_idx) {
                Some(e) if !e.is_empty() => e,
                _ => return,
            };

            used_tokens[curr_idx] = true;

            for ge in next_edges {
                if budget.tick() || cycles.len() >= max_cycles || global_cap.is_full() {
                    break;
                }
                match ge.phase {
                    GraphHopPhase::EnterPool => {
                        let next_pin = pin_touched || edge_is_pin(prep, ge.edge.pool_index);
                        let pending = PendingHubSwap {
                            pool_index: ge.edge.pool_index,
                            token_in: ge.edge.token_in,
                            token_in_idx: ge.edge.token_in_idx,
                            protocol: ge.edge.protocol,
                            fee_bps: ge.edge.fee_bps,
                        };
                        dfs(
                            graph,
                            arena,
                            pool_metas,
                            prep,
                            start,
                            ge.target_node,
                            Some(pending),
                            path,
                            path_hop_calls,
                            used_pools,
                            used_tokens,
                            hops,
                            log_w,
                            product_ratio,
                            cum_fee,
                            hop_cap,
                            max_cycles,
                            budget,
                            global_cap,
                            cycles,
                            seen,
                            next_pin,
                        );
                    }
                    GraphHopPhase::Direct => {
                        if ge.ratio.is_zero() {
                            continue;
                        }
                        let pool_id = ge.edge.pool_index.0;
                        if !pool_mark(used_pools, pool_id) {
                            continue;
                        }
                        let next_log_w = log_w + ge.log_weight;
                        let next_ratio = mul_ratio_saturating(product_ratio, ge.ratio);
                        let next_pin = pin_touched || edge_is_pin(prep, ge.edge.pool_index);
                        if !can_still_find_profitable_cycle(
                            next_log_w,
                            next_ratio,
                            hops + 1,
                            hop_cap,
                            ge.edge.token_out,
                            prep,
                            next_pin,
                        ) {
                            pool_unmark(used_pools, pool_id);
                            continue;
                        }
                        let hop_calls = estimate_hop_calls(ge.edge.protocol) as u16;
                        if route_hop_budget_exceeded(path_hop_calls.saturating_add(hop_calls)) {
                            pool_unmark(used_pools, pool_id);
                            continue;
                        }
                        path.push(ge.edge);
                        dfs(
                            graph,
                            arena,
                            pool_metas,
                            prep,
                            start,
                            ge.target_node,
                            None,
                            path,
                            path_hop_calls.saturating_add(hop_calls),
                            used_pools,
                            used_tokens,
                            hops + 1,
                            next_log_w,
                            next_ratio,
                            cum_fee + clamp_fee_bps(ge.edge.fee_bps),
                            hop_cap,
                            max_cycles,
                            budget,
                            global_cap,
                            cycles,
                            seen,
                            next_pin,
                        );
                        path.pop();
                        pool_unmark(used_pools, pool_id);
                    }
                    GraphHopPhase::ExitPool => {}
                }
            }

            used_tokens[curr_idx] = false;
        }

        if start.0 as usize >= used_tokens.len() {
            return cycles;
        }
        let first_edges = match prep.adjacency.get(start.0 as usize) {
            Some(e) if !e.is_empty() => e,
            _ => return cycles,
        };
        // Exclusive obs: if this start has a pin opening edge, only take those.
        let pin_open = prep.first_hop_pools.as_ref().is_some_and(|pins| {
            first_edges
                .iter()
                .any(|ge| pins.contains(&ge.edge.pool_index))
        });

        used_tokens[start.0 as usize] = true;
        for ge in first_edges {
            if budget.tick() || cycles.len() >= max_cycles || global_cap.is_full() {
                break;
            }
            if pin_open
                && prep
                    .first_hop_pools
                    .as_ref()
                    .is_some_and(|pins| !pins.contains(&ge.edge.pool_index))
            {
                continue;
            }
            match ge.phase {
                GraphHopPhase::EnterPool => {
                    let next_pin = edge_is_pin(prep, ge.edge.pool_index);
                    let pending = PendingHubSwap {
                        pool_index: ge.edge.pool_index,
                        token_in: ge.edge.token_in,
                        token_in_idx: ge.edge.token_in_idx,
                        protocol: ge.edge.protocol,
                        fee_bps: ge.edge.fee_bps,
                    };
                    dfs(
                        graph,
                        arena,
                        pool_metas,
                        prep,
                        start,
                        ge.target_node,
                        Some(pending),
                        path,
                        0,
                        used_pools,
                        used_tokens,
                        0,
                        0.0,
                        ONE,
                        0,
                        hop_cap,
                        max_cycles,
                        budget,
                        global_cap,
                        &mut cycles,
                        &mut seen,
                        next_pin,
                    );
                }
                GraphHopPhase::Direct => {
                    if ge.ratio.is_zero() {
                        continue;
                    }
                    let pool_id = ge.edge.pool_index.0;
                    let next_pin = edge_is_pin(prep, ge.edge.pool_index);
                    if !can_still_find_profitable_cycle(
                        ge.log_weight,
                        ge.ratio,
                        1,
                        hop_cap,
                        ge.edge.token_out,
                        prep,
                        next_pin,
                    ) {
                        continue;
                    }
                    let hop_calls = estimate_hop_calls(ge.edge.protocol) as u16;
                    if route_hop_budget_exceeded(hop_calls) {
                        continue;
                    }
                    // Same mark semantics as mid-path Direct — never walk unmarked OOB.
                    if !pool_mark(used_pools, pool_id) {
                        continue;
                    }
                    path.push(ge.edge);
                    dfs(
                        graph,
                        arena,
                        pool_metas,
                        prep,
                        start,
                        ge.target_node,
                        None,
                        path,
                        hop_calls,
                        used_pools,
                        used_tokens,
                        1,
                        ge.log_weight,
                        ge.ratio,
                        clamp_fee_bps(ge.edge.fee_bps),
                        hop_cap,
                        max_cycles,
                        budget,
                        global_cap,
                        &mut cycles,
                        &mut seen,
                        next_pin,
                    );
                    path.pop();
                    pool_unmark(used_pools, pool_id);
                }
                GraphHopPhase::ExitPool => {}
            }
        }
        used_tokens[start.0 as usize] = false;
        cycles
    })
}

fn collect_cycles_dfs_parallel(
    graph: &RoutingGraph,
    arena: &StateArena,
    pool_metas: &PoolMetaIndex<'_>,
    prep: &ActiveGraph,
    hop_limit: u32,
    max_cycles: usize,
    budget: &std::sync::Arc<SharedDeadlineGuard>,
) -> Vec<FoundCycle> {
    let start_tokens = &prep.start_tokens;
    if start_tokens.is_empty() || max_cycles == 0 || budget.tick() {
        return Vec::new();
    }
    let global_cap = SharedCycleCap::new(max_cycles);
    let per_shard = max_cycles.div_ceil(start_tokens.len()).max(1);
    let mut shard_caps = vec![per_shard; start_tokens.len()];
    let mut shard_cycles: Vec<Vec<FoundCycle>> =
        if crate::util::should_use_rayon(start_tokens.len()) {
            start_tokens
                .par_iter()
                .zip(shard_caps.par_iter())
                .map(|(start, cap)| {
                    collect_cycles_dfs_single_start(
                        graph,
                        arena,
                        pool_metas,
                        prep,
                        *start,
                        hop_limit,
                        *cap,
                        budget.as_ref(),
                        global_cap.as_ref(),
                    )
                })
                .collect()
        } else {
            start_tokens
                .iter()
                .zip(shard_caps.iter())
                .map(|(start, cap)| {
                    collect_cycles_dfs_single_start(
                        graph,
                        arena,
                        pool_metas,
                        prep,
                        *start,
                        hop_limit,
                        *cap,
                        budget.as_ref(),
                        global_cap.as_ref(),
                    )
                })
                .collect()
        };

    // A flat per-start quota strands most of the global budget when many start
    // tokens are sparse. Reallocate unused capacity to starts that saturated
    // their quota. Two bounded retries recover productive hubs without letting
    // one hub monopolize the first parallel pass.
    let any_saturated = shard_cycles
        .iter()
        .zip(&shard_caps)
        .any(|(cycles, cap)| cycles.len() >= *cap);
    // Common path: no hub saturated its quota — one move-merge, no retry clones.
    if !any_saturated || budget.tick() {
        let mut merged = merge_shard_cycles_owned(shard_cycles);
        if merged.len() > max_cycles {
            merged.truncate(max_cycles);
        }
        return merged;
    }

    let mut merged = merge_shard_cycles(&shard_cycles);
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
        // Re-runs replace shard vectors from scratch; free all claim slots so a
        // saturated hub can re-collect its full quota (not only remaining headroom).
        global_cap.reset_to(0);
        let extra = (max_cycles - merged.len()).div_ceil(saturated.len());
        let rerun: Vec<(usize, Vec<FoundCycle>)> = if crate::util::should_use_rayon(saturated.len())
        {
            saturated
                .par_iter()
                .map(|&i| {
                    let cap = shard_caps[i].saturating_add(extra).min(max_cycles);
                    (
                        i,
                        collect_cycles_dfs_single_start(
                            graph,
                            arena,
                            pool_metas,
                            prep,
                            start_tokens[i],
                            hop_limit,
                            cap,
                            budget.as_ref(),
                            global_cap.as_ref(),
                        ),
                    )
                })
                .collect()
        } else {
            saturated
                .iter()
                .map(|&i| {
                    let cap = shard_caps[i].saturating_add(extra).min(max_cycles);
                    (
                        i,
                        collect_cycles_dfs_single_start(
                            graph,
                            arena,
                            pool_metas,
                            prep,
                            start_tokens[i],
                            hop_limit,
                            cap,
                            budget.as_ref(),
                            global_cap.as_ref(),
                        ),
                    )
                })
                .collect()
        };
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

/// Merge parallel DFS shards, keeping the best score per cycle key.
fn merge_shard_cycles(shard_cycles: &[Vec<FoundCycle>]) -> Vec<FoundCycle> {
    use std::collections::hash_map::Entry;

    use crate::pipeline::types::{compare_cycle_score, cycle_prefers_candidate};

    let total: usize = shard_cycles.iter().map(Vec::len).sum();
    let mut best: rustc_hash::FxHashMap<u64, FoundCycle> = rustc_hash::FxHashMap::default();
    best.reserve(total);
    for cycle in shard_cycles.iter().flat_map(|s| s.iter()) {
        let key = cycle_key(&cycle.edges);
        match best.entry(key) {
            Entry::Occupied(mut e) => {
                if cycle_prefers_candidate(cycle, e.get()) {
                    *e.get_mut() = cycle.clone();
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

/// Final merge that consumes shards — vacant inserts move, no clone on first win.
fn merge_shard_cycles_owned(shard_cycles: Vec<Vec<FoundCycle>>) -> Vec<FoundCycle> {
    use std::collections::hash_map::Entry;

    use crate::pipeline::types::{compare_cycle_score, cycle_prefers_candidate};

    let total: usize = shard_cycles.iter().map(Vec::len).sum();
    let mut best: rustc_hash::FxHashMap<u64, FoundCycle> = rustc_hash::FxHashMap::default();
    best.reserve(total);
    for cycle in shard_cycles.into_iter().flatten() {
        let key = cycle_key(&cycle.edges);
        match best.entry(key) {
            Entry::Occupied(mut e) => {
                if cycle_prefers_candidate(&cycle, e.get()) {
                    *e.get_mut() = cycle;
                }
            }
            Entry::Vacant(e) => {
                e.insert(cycle);
            }
        }
    }
    let mut out: Vec<FoundCycle> = best.into_values().collect();
    out.sort_unstable_by(compare_cycle_score);
    out
}

#[must_use]
pub fn find_cycles_multi_pass_with_prep_budget(
    graph: &RoutingGraph,
    arena: &StateArena,
    pool_metas: &PoolMetaIndex<'_>,
    prep: &ActiveGraph,
    passes: &[CycleSearchPass],
    enum_budget: Duration,
) -> Vec<FoundCycle> {
    if passes.is_empty() || prep.start_tokens.is_empty() {
        return Vec::new();
    }
    // One deadline for all hop passes — each pass used to allocate a full budget
    // sequentially (~2s wall time for the default two-pass schedule).
    let budget = SharedDeadlineGuard::new(enum_budget);
    let mut all = Vec::new();
    let pass_cap = passes.iter().map(|p| p.max_cycles).max().unwrap_or(0);
    let collect_bound = pass_cap
        .saturating_mul(3)
        .min(MAX_CYCLES_PER_PASS.saturating_mul(passes.len().max(1)));
    for pass in passes {
        if budget.tick() {
            break;
        }
        let mut shard = collect_cycles_dfs_parallel(
            graph,
            arena,
            pool_metas,
            prep,
            pass.max_hops,
            pass.max_cycles.min(MAX_CYCLES_PER_PASS),
            &budget,
        );
        all.append(&mut shard);
        if all.len() > collect_bound {
            all = dedupe_cycles_by_edges(all);
            if all.len() > collect_bound {
                all.sort_unstable_by(compare_cycle_score);
                all.truncate(collect_bound);
            }
        }
    }
    // Final dedupe happens in `finalize_cycles` after hybrid DFS+BF merge.
    all
}

#[inline]
fn protocol_fetch_slot(protocol: ProtocolType) -> usize {
    protocol.fetch_slot().unwrap_or(0)
}

#[inline]
fn protocol_from_slot(slot: usize) -> ProtocolType {
    match slot {
        0 => ProtocolType::UniswapV2,
        1 => ProtocolType::UniswapV3,
        2 => ProtocolType::UniswapV4,
        3 => ProtocolType::BalancerV2,
        4 => ProtocolType::CurveStable,
        5 => ProtocolType::CurveCrypto,
        6 => ProtocolType::Dodo,
        _ => ProtocolType::Woofi,
    }
}

/// Returns the most frequently used protocol in the cycle (primary protocol for diversity).
/// Ties break toward the first hop's protocol (not a fixed V2 bias).
#[must_use]
pub fn primary_protocol(edges: &[Edge]) -> ProtocolType {
    // Fixed [u32; 8] counts — O(hops), no heap.
    let mut counts = [0u32; 8];
    for e in edges {
        counts[protocol_fetch_slot(e.protocol)] += 1;
    }
    let mut best_idx = edges
        .first()
        .map(|e| protocol_fetch_slot(e.protocol))
        .unwrap_or(0);
    let mut best_count = counts[best_idx];
    for (i, &count) in counts.iter().enumerate() {
        if count > best_count {
            best_count = count;
            best_idx = i;
        }
    }
    protocol_from_slot(best_idx)
}

/// Selects up to `max_cycles` opportunities with better protocol distribution.
///
/// For each protocol that appears, takes its best cycles (via
/// [`compare_cycle_execution`]) in round-robin so V2/V3/Curve/etc. are not
/// crowded out by high-degree Balancer subgraphs. Prefer gas-aware score among
/// profitable routes after LF rescore; safe at enum-time too (score = log-weight).
#[must_use]
pub fn apply_protocol_diverse_selection(
    cycles: Vec<FoundCycle>,
    max_cycles: usize,
) -> Vec<FoundCycle> {
    if max_cycles == 0 || cycles.is_empty() {
        return vec![];
    }

    // Fixed 8 protocol buckets — no HashMap. Slot order = FETCHABLE_PROTOCOLS walk order.
    let mut groups: [Vec<FoundCycle>; 8] = std::array::from_fn(|_| Vec::new());
    for c in cycles {
        let slot = protocol_fetch_slot(primary_protocol(&c.edges));
        groups[slot].push(c);
    }

    // Best-first sort (gas-aware when rescored), reverse so `pop()` is O(1) best.
    for g in &mut groups {
        if g.len() > 1 {
            g.sort_by(compare_cycle_execution);
            g.reverse();
        }
    }

    let total: usize = groups.iter().map(Vec::len).sum();
    let cap = max_cycles.min(total);

    // Compact active slot list so exhausted protocols are not scanned every pass.
    let mut active = [0usize; 8];
    let mut active_len = 0usize;
    for (slot, g) in groups.iter().enumerate() {
        if !g.is_empty() {
            active[active_len] = slot;
            active_len += 1;
        }
    }

    let mut selected: Vec<FoundCycle> = Vec::with_capacity(cap);
    let mut seen: rustc_hash::FxHashSet<u64> =
        rustc_hash::FxHashSet::with_capacity_and_hasher(cap, rustc_hash::FxBuildHasher);

    while selected.len() < cap && active_len > 0 {
        let mut i = 0;
        let mut progressed = false;
        while i < active_len {
            if selected.len() >= cap {
                break;
            }
            let slot = active[i];
            let g = &mut groups[slot];
            let mut took = false;
            while let Some(cycle) = g.pop() {
                let key = cycle_key(&cycle.edges);
                if !seen.insert(key) {
                    continue; // duplicate edge set — drop, keep popping for a unique
                }
                selected.push(cycle);
                took = true;
                progressed = true;
                break;
            }
            if g.is_empty() {
                // Compact active list (order among remaining slots is stable enough).
                active_len -= 1;
                active[i] = active[active_len];
            } else {
                debug_assert!(took, "non-empty group must yield a unique cycle or empty");
                i += 1;
            }
        }
        if !progressed {
            break;
        }
    }

    selected.sort_by(compare_cycle_execution);
    selected
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{PoolIndex, ProtocolType};
    use crate::pipeline::types::{GraphEdge, GraphHopPhase};

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
            phase: GraphHopPhase::Direct,
            target_node: to.0,
            log_weight: -0.01,
            ratio: U256::from(1_000_000_000_000_000_000u64), // ONE
        }
    }

    #[test]
    fn prefer_start_tokens_prepends_observed_endpoints() {
        let hub = TokenIndex(0);
        let spoke = TokenIndex(1);
        let mut graph = RoutingGraph::new(2);
        graph.add_direct_edge(hub, graph_edge(0, hub, spoke));
        graph.add_direct_edge(spoke, graph_edge(1, spoke, hub));
        let mut prep = prepare_active_graph(&graph);
        assert!(prep.start_tokens.contains(&hub));
        prep.prefer_start_tokens(&[spoke], false);
        assert_eq!(prep.start_tokens.first().copied(), Some(spoke));
    }

    #[test]
    fn start_token_priority_ignores_dead_out_edges() {
        let hub = TokenIndex(0);
        let tail = TokenIndex(1);
        let mut graph = RoutingGraph::new(2);
        graph.add_direct_edge(hub, graph_edge(0, hub, tail));
        let mut dead = graph_edge(1, tail, hub);
        dead.log_weight = DEAD_EDGE_LOG_WEIGHT;
        dead.ratio = U256::ZERO;
        graph.add_direct_edge(tail, dead);
        graph.add_direct_edge(tail, graph_edge(2, tail, hub));

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
    fn select_cycle_start_tokens_keeps_the_best_ranked_sources() {
        let starts = select_cycle_start_tokens(
            (0..(DFS_MAX_START_SOURCES as u32 + 8))
                .map(|index| (TokenIndex(index), 1, index as usize))
                .collect(),
        );

        assert_eq!(starts.len(), DFS_MAX_START_SOURCES);
        assert_eq!(starts.first(), Some(&TokenIndex(39)));
        assert_eq!(starts.last(), Some(&TokenIndex(8)));
    }

    #[test]
    fn cycle_coverage_excludes_one_way_spokes() {
        let a = TokenIndex(0);
        let b = TokenIndex(1);
        let dead_end = TokenIndex(2);
        let mut graph = RoutingGraph::new(3);
        graph.add_direct_edge(a, graph_edge(0, a, b));
        graph.add_direct_edge(b, graph_edge(1, b, a));
        graph.add_direct_edge(b, graph_edge(2, b, dead_end));

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
        graph.add_direct_edge(a, graph_edge(0, a, b));
        let mut dead_return = graph_edge(1, b, a);
        dead_return.log_weight = DEAD_EDGE_LOG_WEIGHT;
        dead_return.ratio = U256::ZERO;
        graph.add_direct_edge(b, dead_return);

        let coverage = cycle_capable_coverage(&graph);
        assert!(coverage.pool_indices.is_empty());
        assert!(prepare_active_graph(&graph).start_tokens.is_empty());
    }

    #[test]
    fn isolated_bidirectional_pool_cannot_cycle_through_itself() {
        let a = TokenIndex(0);
        let b = TokenIndex(1);
        let mut graph = RoutingGraph::new(2);
        graph.add_direct_edge(a, graph_edge(0, a, b));
        graph.add_direct_edge(b, graph_edge(0, b, a));

        let coverage = cycle_capable_coverage(&graph);
        assert!(coverage.pool_indices.is_empty());
        assert!(prepare_active_graph(&graph).start_tokens.is_empty());
    }

    #[test]
    fn dfs_keeps_profitable_cycle_with_positive_prefix() {
        let a = TokenIndex(0);
        let b = TokenIndex(1);
        let mut graph = RoutingGraph::new(2);
        graph.add_direct_edge(
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
                phase: GraphHopPhase::Direct,
                target_node: b.0,
                log_weight: 0.10,
                ratio: U256::from(1_000_000_000_000_000_000u64),
            },
        );
        graph.add_direct_edge(
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
                phase: GraphHopPhase::Direct,
                target_node: a.0,
                log_weight: -1.0,
                ratio: U256::from(1_000_000_000_000_000_001u64),
            },
        );

        let arena = StateArena::default();
        let metas = index_pool_metas(&[]);
        let mut prep = prepare_active_graph(&graph);
        prep.start_tokens = vec![a];
        let budget = SharedDeadlineGuard::new(CYCLE_ENUM_TIME_BUDGET);
        let cycles = collect_cycles_dfs_parallel(&graph, &arena, &metas, &prep, 2, 10, &budget);
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
    fn pin_touched_bypasses_profit_prune_not_mere_pin_presence() {
        // Hopeless prefix: log_w far above any live edge recovery.
        let a = TokenIndex(0);
        let b = TokenIndex(1);
        let mut graph = RoutingGraph::new(2);
        graph.add_direct_edge(a, graph_edge(0, a, b));
        graph.add_direct_edge(b, graph_edge(1, b, a));
        let mut prep = prepare_active_graph(&graph);
        prep.prioritize_first_hop_pools(&[PoolIndex(0)]);
        let dead_log = 50.0;

        // Pins configured but path has not touched one → still prune.
        assert!(!can_still_find_profitable_cycle(
            dead_log, ONE, 1, 3, a, &prep, false,
        ));
        // Path already on a pin → keep exploring for the close.
        assert!(can_still_find_profitable_cycle(
            dead_log, ONE, 1, 3, a, &prep, true,
        ));
    }

    #[test]
    fn pool_mark_grows_for_high_pool_index_instead_of_false_reuse() {
        // Recent O(1) bitset sized only to pool_edge_positions.len(); high pool
        // indices used to look "already used" and silently drop Direct edges.
        let mut used = vec![false; 1];
        assert!(pool_mark(&mut used, 9));
        assert_eq!(used.len(), 10);
        assert!(!pool_mark(&mut used, 9));
        pool_unmark(&mut used, 9);
        assert!(pool_mark(&mut used, 9));
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
                phase: GraphHopPhase::Direct,
                target_node: token_out.0,
                log_weight: -1.0,
                ratio: U256::from(1_000_000_000_000_000_001u64),
            }
        }

        let dead = TokenIndex(0);
        let hub = TokenIndex(1);
        let mut graph = RoutingGraph::new(5);
        graph.add_direct_edge(dead, edge(0, dead, TokenIndex(4)));
        for (branch, token) in [TokenIndex(2), TokenIndex(3), TokenIndex(4)]
            .into_iter()
            .enumerate()
        {
            let pool = 1 + (branch as u32 * 2);
            graph.add_direct_edge(hub, edge(pool, hub, token));
            graph.add_direct_edge(token, edge(pool + 1, token, hub));
        }

        let arena = StateArena::default();
        let metas = index_pool_metas(&[]);
        let mut prep = prepare_active_graph(&graph);
        prep.start_tokens = vec![dead, hub];
        let budget = SharedDeadlineGuard::new(CYCLE_ENUM_TIME_BUDGET);
        let cycles = collect_cycles_dfs_parallel(&graph, &arena, &metas, &prep, 2, 4, &budget);
        assert_eq!(cycles.len(), 3);
    }

    fn ranked_cycle(pool: u32, protocol: ProtocolType, ratio: u64, score: f64) -> FoundCycle {
        FoundCycle {
            start_token: TokenIndex(0),
            edges: CycleEdges::from(
                [Edge {
                    pool_index: PoolIndex(pool),
                    token_in: TokenIndex(0),
                    token_out: TokenIndex(1),
                    token_in_idx: 0,
                    token_out_idx: 1,
                    protocol,
                    fee_bps: 30,
                    zero_for_one: true,
                }]
                .as_slice(),
            ),
            hop_count: 1,
            log_weight: score,
            cumulative_fee_bps: 30,
            score,
            // Distinct non-zero ratios so compare_cycle_score ranks by ratio.
            cycle_ratio: U256::from(1_000_000_000_000_000_000u64) + U256::from(ratio),
        }
    }

    #[test]
    fn protocol_diverse_selection_takes_best_per_protocol_not_worst() {
        // Five V2 cycles ranked best→worst by cycle_ratio, two Balancer.
        // Pure RR best-first: V2 best, Balancer best, V2 second-best — never V2 worst.
        let cycles = vec![
            ranked_cycle(1, ProtocolType::UniswapV2, 500, -5.0),
            ranked_cycle(2, ProtocolType::UniswapV2, 400, -4.0),
            ranked_cycle(3, ProtocolType::UniswapV2, 300, -3.0),
            ranked_cycle(4, ProtocolType::UniswapV2, 200, -2.0),
            ranked_cycle(5, ProtocolType::UniswapV2, 100, -1.0),
            ranked_cycle(10, ProtocolType::BalancerV2, 450, -4.5),
            ranked_cycle(11, ProtocolType::BalancerV2, 150, -1.5),
        ];
        let selected = apply_protocol_diverse_selection(cycles, 3);
        assert_eq!(selected.len(), 3);
        let pools: Vec<u32> = selected.iter().map(|c| c.edges[0].pool_index.0).collect();
        assert!(pools.contains(&1), "missing best V2: {pools:?}");
        assert!(pools.contains(&10), "missing best Balancer: {pools:?}");
        assert!(
            pools.contains(&2),
            "second V2 pick must be second-best, not worst: {pools:?}"
        );
        assert!(
            !pools.contains(&5),
            "worst V2 must not displace second-best: {pools:?}"
        );
    }

    #[test]
    fn primary_protocol_tie_breaks_to_first_hop() {
        let v2_then_bal = [
            Edge {
                pool_index: PoolIndex(0),
                token_in: TokenIndex(0),
                token_out: TokenIndex(1),
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::BalancerV2,
                fee_bps: 10,
                zero_for_one: true,
            },
            Edge {
                pool_index: PoolIndex(1),
                token_in: TokenIndex(1),
                token_out: TokenIndex(0),
                token_in_idx: 1,
                token_out_idx: 0,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: false,
            },
        ];
        // 1-1 hop tie: first hop is Balancer → Balancer primary (not fixed V2 bias).
        assert_eq!(primary_protocol(&v2_then_bal), ProtocolType::BalancerV2);

        let two_v2_one_bal = [
            v2_then_bal[0],
            Edge {
                pool_index: PoolIndex(2),
                token_in: TokenIndex(0),
                token_out: TokenIndex(1),
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
            Edge {
                pool_index: PoolIndex(3),
                token_in: TokenIndex(1),
                token_out: TokenIndex(0),
                token_in_idx: 1,
                token_out_idx: 0,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: false,
            },
        ];
        assert_eq!(primary_protocol(&two_v2_one_bal), ProtocolType::UniswapV2);
    }

    #[test]
    fn protocol_robin_fills_cap_when_one_protocol_exhausts_early() {
        // One V3 cycle, many V2 — RR should not stall after V3 empties.
        let mut cycles = vec![ranked_cycle(99, ProtocolType::UniswapV3, 900, -9.0)];
        for i in 0..5u32 {
            cycles.push(ranked_cycle(
                i,
                ProtocolType::UniswapV2,
                500 - i as u64,
                -5.0,
            ));
        }
        let selected = apply_protocol_diverse_selection(cycles, 4);
        assert_eq!(selected.len(), 4);
        assert!(
            selected
                .iter()
                .any(|c| c.edges[0].protocol == ProtocolType::UniswapV3),
            "V3 must appear once"
        );
        let v2_count = selected
            .iter()
            .filter(|c| c.edges[0].protocol == ProtocolType::UniswapV2)
            .count();
        assert_eq!(v2_count, 3);
    }
}
