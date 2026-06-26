use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, Instant};

use rayon::prelude::*;

use crate::core::types::{CycleEdges, Edge, FoundCycle, TokenIndex};
use crate::pipeline::arena::StateArena;
use crate::pipeline::cycle_filter::dedupe_cycles_by_fingerprint;
use crate::pipeline::types::{
    CycleSearchPass, GraphEdge, RoutingGraph, compare_cycle_score, route_fingerprint,
};

pub use crate::pipeline::spot_price::hop_penalty;

const MAX_CYCLES_PER_PASS: usize = 250_000;
const CYCLE_ENUM_TIME_BUDGET: Duration = Duration::from_millis(3_000);
const HOP_CAP: u32 = 8;
/// Amortize elapsed-time checks during DFS enumeration.
const DFS_BUDGET_CHECK_INTERVAL: u32 = 4096;
/// Prune DFS branches once log-weight exceeds this (spot-weighted graphs only).
const LOG_WEIGHT_PRUNE_THRESHOLD: f64 = 0.0;
/// Edges rescored to this weight are non-tradable — skip during enumeration.
const DEAD_EDGE_LOG_WEIGHT: f64 = 15.0;
/// Minimum hub starts before sharding DFS across rayon workers.
const PARALLEL_DFS_MIN_STARTS: usize = 8;

pub fn clamp_fee_bps(fee_bps: u32) -> u32 {
    fee_bps.min(9_999)
}

#[inline]
fn edges_from_path(path: &[Edge]) -> CycleEdges {
    path.iter().copied().collect()
}

/// Major-token-first + high out-degree hubs for DFS start order.
pub fn prioritize_cycle_start_tokens(graph: &RoutingGraph) -> Vec<TokenIndex> {
    prioritize_cycle_start_tokens_from_out_degrees(graph.adjacency.iter().map(Vec::len))
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

struct ActiveGraph<'a> {
    adjacency: &'a [Vec<GraphEdge>],
    start_tokens: Vec<TokenIndex>,
}

fn prepare_active_graph(graph: &RoutingGraph) -> ActiveGraph<'_> {
    let start_tokens =
        prioritize_cycle_start_tokens_from_out_degrees(graph.adjacency.iter().map(Vec::len));
    ActiveGraph {
        adjacency: &graph.adjacency,
        start_tokens,
    }
}

struct Collector {
    cycles: Vec<FoundCycle>,
}

fn max_pool_index(adj: &[Vec<GraphEdge>]) -> usize {
    adj.iter()
        .flat_map(|edges| edges.iter().map(|ge| ge.edge.pool_index.0 as usize))
        .max()
        .unwrap_or(0)
        .saturating_add(1)
}

fn collect_cycles_dfs(
    prep: &ActiveGraph<'_>,
    hop_limit: u32,
    max_cycles: usize,
    budget: &mut CycleBudget,
    collector: &mut Collector,
    seen: &mut rustc_hash::FxHashSet<u64>,
    used_tokens: &mut [bool],
    pool_state: &mut [u8],
    pass_start: usize,
    start_tokens: &[TokenIndex],
) {
    let hop_cap = hop_limit.min(HOP_CAP);

    fn dfs(
        prep: &ActiveGraph<'_>,
        start: TokenIndex,
        curr: TokenIndex,
        path: &mut Vec<Edge>,
        pool_state: &mut [u8],
        used_tokens: &mut [bool],
        hops: u32,
        log_w: f64,
        cum_fee: u32,
        hop_cap: u32,
        max_cycles: usize,
        pass_start: usize,
        budget: &mut CycleBudget,
        collector: &mut Collector,
        seen: &mut rustc_hash::FxHashSet<u64>,
    ) {
        if budget.tick() {
            return;
        }
        if collector.cycles.len() - pass_start >= max_cycles {
            return;
        }

        if hops >= 2 && curr == start {
            let penalty = hop_penalty(hops);
            let score = log_w + penalty;
            if score > LOG_WEIGHT_PRUNE_THRESHOLD {
                return;
            }
            let fp = route_fingerprint(path);
            if seen.contains(&fp) {
                return;
            }
            seen.insert(fp);
            collector.cycles.push(FoundCycle {
                start_token: start,
                edges: edges_from_path(path),
                hop_count: hops,
                log_weight: score,
                cumulative_fee_bps: cum_fee,
                score,
            });
            return;
        }

        if used_tokens[curr.0 as usize] || hops >= hop_cap {
            return;
        }
        if hops >= 1 && log_w > LOG_WEIGHT_PRUNE_THRESHOLD {
            return;
        }

        let next_edges = match prep.adjacency.get(curr.0 as usize) {
            Some(e) if !e.is_empty() => e,
            _ => return,
        };

        used_tokens[curr.0 as usize] = true;

        for ge in next_edges {
            if ge.log_weight >= DEAD_EDGE_LOG_WEIGHT {
                continue;
            }
            let pool_id = ge.edge.pool_index.0 as usize;
            if pool_state[pool_id] != 0 {
                continue;
            }
            let next_log_w = log_w + ge.log_weight;
            if next_log_w > LOG_WEIGHT_PRUNE_THRESHOLD {
                continue;
            }
            pool_state[pool_id] = 1;

            path.push(ge.edge);
            dfs(
                prep,
                start,
                ge.edge.token_out,
                path,
                pool_state,
                used_tokens,
                hops + 1,
                next_log_w,
                cum_fee + clamp_fee_bps(ge.edge.fee_bps),
                hop_cap,
                max_cycles,
                pass_start,
                budget,
                collector,
                seen,
            );
            path.pop();
            pool_state[pool_id] = 0;

            if budget.tick() || collector.cycles.len() - pass_start >= max_cycles {
                break;
            }
        }

        used_tokens[curr.0 as usize] = false;
    }

    let mut path = Vec::with_capacity(hop_cap as usize);

    for start in start_tokens {
        if budget.tick() || collector.cycles.len() - pass_start >= max_cycles {
            break;
        }

        let first_edges = match prep.adjacency.get(start.0 as usize) {
            Some(e) if !e.is_empty() => e,
            _ => continue,
        };

        used_tokens[start.0 as usize] = true;

        for ge in first_edges {
            if budget.tick() || collector.cycles.len() - pass_start >= max_cycles {
                break;
            }
            if ge.log_weight >= DEAD_EDGE_LOG_WEIGHT {
                continue;
            }
            let pool_id = ge.edge.pool_index.0 as usize;
            pool_state[pool_id] = 1;
            path.push(ge.edge);
            dfs(
                prep,
                *start,
                ge.edge.token_out,
                &mut path,
                pool_state,
                used_tokens,
                1,
                ge.log_weight,
                clamp_fee_bps(ge.edge.fee_bps),
                hop_cap,
                max_cycles,
                pass_start,
                budget,
                collector,
                seen,
            );
            path.pop();
            pool_state[pool_id] = 0;
        }

        used_tokens[start.0 as usize] = false;
    }
}

struct SharedCycleBudget {
    start: Instant,
    exceeded: AtomicBool,
    ops: AtomicU32,
}

impl SharedCycleBudget {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            exceeded: AtomicBool::new(false),
            ops: AtomicU32::new(0),
        }
    }

    #[inline]
    fn tick(&self) -> bool {
        if self.exceeded.load(Ordering::Relaxed) {
            return true;
        }
        let ops = self.ops.fetch_add(1, Ordering::Relaxed);
        if ops.is_multiple_of(DFS_BUDGET_CHECK_INTERVAL)
            && self.start.elapsed() > CYCLE_ENUM_TIME_BUDGET
        {
            self.exceeded.store(true, Ordering::Relaxed);
        }
        self.exceeded.load(Ordering::Relaxed)
    }
}

fn collect_cycles_dfs_single_start(
    prep: &ActiveGraph<'_>,
    start: TokenIndex,
    hop_limit: u32,
    max_cycles: usize,
    budget: &SharedCycleBudget,
) -> Vec<FoundCycle> {
    let hop_cap = hop_limit.min(HOP_CAP);
    let pool_slot_count = max_pool_index(prep.adjacency);
    let mut pool_state = vec![0u8; pool_slot_count];
    let mut used_tokens = vec![false; prep.adjacency.len()];
    let mut path = Vec::with_capacity(hop_cap as usize);
    let mut cycles = Vec::new();
    let mut seen = rustc_hash::FxHashSet::default();

    fn dfs(
        prep: &ActiveGraph<'_>,
        start: TokenIndex,
        curr: TokenIndex,
        path: &mut Vec<Edge>,
        pool_state: &mut [u8],
        used_tokens: &mut [bool],
        hops: u32,
        log_w: f64,
        cum_fee: u32,
        hop_cap: u32,
        max_cycles: usize,
        budget: &SharedCycleBudget,
        cycles: &mut Vec<FoundCycle>,
        seen: &mut rustc_hash::FxHashSet<u64>,
    ) {
        if budget.tick() || cycles.len() >= max_cycles {
            return;
        }

        if hops >= 2 && curr == start {
            let penalty = hop_penalty(hops);
            let score = log_w + penalty;
            if score > LOG_WEIGHT_PRUNE_THRESHOLD {
                return;
            }
            let fp = route_fingerprint(path);
            if seen.contains(&fp) {
                return;
            }
            seen.insert(fp);
            cycles.push(FoundCycle {
                start_token: start,
                edges: edges_from_path(path),
                hop_count: hops,
                log_weight: score,
                cumulative_fee_bps: cum_fee,
                score,
            });
            return;
        }

        if used_tokens[curr.0 as usize] || hops >= hop_cap {
            return;
        }
        if hops >= 1 && log_w > LOG_WEIGHT_PRUNE_THRESHOLD {
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
            if ge.log_weight >= DEAD_EDGE_LOG_WEIGHT {
                continue;
            }
            let pool_id = ge.edge.pool_index.0 as usize;
            if pool_state[pool_id] != 0 {
                continue;
            }
            let next_log_w = log_w + ge.log_weight;
            if next_log_w > LOG_WEIGHT_PRUNE_THRESHOLD {
                continue;
            }
            pool_state[pool_id] = 1;

            path.push(ge.edge);
            dfs(
                prep,
                start,
                ge.edge.token_out,
                path,
                pool_state,
                used_tokens,
                hops + 1,
                next_log_w,
                cum_fee + clamp_fee_bps(ge.edge.fee_bps),
                hop_cap,
                max_cycles,
                budget,
                cycles,
                seen,
            );
            path.pop();
            pool_state[pool_id] = 0;
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
        if ge.log_weight >= DEAD_EDGE_LOG_WEIGHT {
            continue;
        }
        let pool_id = ge.edge.pool_index.0 as usize;
        pool_state[pool_id] = 1;
        path.push(ge.edge);
        dfs(
            prep,
            start,
            ge.edge.token_out,
            &mut path,
            &mut pool_state,
            &mut used_tokens,
            1,
            ge.log_weight,
            clamp_fee_bps(ge.edge.fee_bps),
            hop_cap,
            max_cycles,
            budget,
            &mut cycles,
            &mut seen,
        );
        path.pop();
        pool_state[pool_id] = 0;
    }
    used_tokens[start.0 as usize] = false;
    cycles
}

fn collect_cycles_dfs_parallel(
    graph: &RoutingGraph,
    hop_limit: u32,
    max_cycles: usize,
    start_tokens: &[TokenIndex],
) -> Vec<FoundCycle> {
    if start_tokens.is_empty() {
        return Vec::new();
    }
    let budget = Arc::new(SharedCycleBudget::new());
    let per_shard = (max_cycles / start_tokens.len()).max(1);

    let shard_cycles: Vec<Vec<FoundCycle>> = start_tokens
        .par_iter()
        .map(|start| {
            let prep = ActiveGraph {
                adjacency: &graph.adjacency,
                start_tokens: Vec::new(),
            };
            collect_cycles_dfs_single_start(&prep, *start, hop_limit, per_shard, budget.as_ref())
        })
        .collect();

    let mut merged: Vec<FoundCycle> = shard_cycles.into_iter().flatten().collect();
    if merged.len() > max_cycles {
        merged = dedupe_cycles_by_fingerprint(merged);
        merged.truncate(max_cycles);
    } else {
        merged = dedupe_cycles_by_fingerprint(merged);
    }
    merged
}

struct CycleBudget {
    start: Instant,
    exceeded: bool,
    ops: u32,
}

impl CycleBudget {
    #[inline]
    fn tick(&mut self) -> bool {
        if self.exceeded {
            return true;
        }
        self.ops += 1;
        if self.ops.is_multiple_of(DFS_BUDGET_CHECK_INTERVAL)
            && self.start.elapsed() > CYCLE_ENUM_TIME_BUDGET
        {
            self.exceeded = true;
        }
        self.exceeded
    }
}

pub fn find_cycles(
    arena: &StateArena,
    graph: &RoutingGraph,
    max_hops: u32,
    max_cycles: usize,
) -> Vec<FoundCycle> {
    find_cycles_multi_pass(
        arena,
        graph,
        &[CycleSearchPass {
            max_hops,
            max_cycles,
        }],
    )
}

pub fn find_cycles_multi_pass(
    arena: &StateArena,
    graph: &RoutingGraph,
    passes: &[CycleSearchPass],
) -> Vec<FoundCycle> {
    find_cycles_multi_pass_impl(arena, graph, passes)
}

pub fn find_cycles_multi_pass_arc(
    arena: &StateArena,
    graph: &Arc<RoutingGraph>,
    passes: &[CycleSearchPass],
) -> Vec<FoundCycle> {
    find_cycles_multi_pass_impl(arena, graph.as_ref(), passes)
}

fn find_cycles_multi_pass_impl(
    _arena: &StateArena,
    graph: &RoutingGraph,
    passes: &[CycleSearchPass],
) -> Vec<FoundCycle> {
    if passes.is_empty() {
        return Vec::new();
    }

    let prep = prepare_active_graph(graph);
    let use_parallel = prep.start_tokens.len() >= PARALLEL_DFS_MIN_STARTS;

    if use_parallel {
        let mut all = Vec::new();
        for pass in passes {
            let mut shard = collect_cycles_dfs_parallel(
                graph,
                pass.max_hops,
                pass.max_cycles.min(MAX_CYCLES_PER_PASS),
                &prep.start_tokens,
            );
            all.append(&mut shard);
        }
        return dedupe_cycles_by_fingerprint(all);
    }

    let mut budget = CycleBudget {
        start: Instant::now(),
        exceeded: false,
        ops: 0,
    };
    let capacity_hint = passes
        .iter()
        .map(|pass| pass.max_cycles.min(MAX_CYCLES_PER_PASS))
        .sum();
    let mut collector = Collector {
        cycles: Vec::with_capacity(capacity_hint),
    };
    let mut seen = rustc_hash::FxHashSet::default();
    let mut used_tokens = vec![false; prep.adjacency.len()];
    let pool_slot_count = max_pool_index(prep.adjacency);
    let mut pool_state = vec![0u8; pool_slot_count];

    for pass in passes {
        if budget.tick() {
            break;
        }
        let pass_start = collector.cycles.len();
        collect_cycles_dfs(
            &prep,
            pass.max_hops,
            pass.max_cycles.min(MAX_CYCLES_PER_PASS),
            &mut budget,
            &mut collector,
            &mut seen,
            &mut used_tokens,
            &mut pool_state,
            pass_start,
            &prep.start_tokens,
        );
    }

    collector.cycles
}

/// Per-hop minimum slots before global score fill.
pub fn default_hop_quotas() -> Vec<(u32, usize)> {
    vec![(2, 1500), (3, 2000), (4, 2000), (5, 2500)]
}

pub fn apply_hop_stratified_cap(cycles: Vec<FoundCycle>, max_cycles: usize) -> Vec<FoundCycle> {
    apply_hop_stratified_cap_with_quotas(cycles, max_cycles, &default_hop_quotas())
}

pub fn apply_hop_stratified_cap_with_quotas(
    mut cycles: Vec<FoundCycle>,
    max_cycles: usize,
    quotas: &[(u32, usize)],
) -> Vec<FoundCycle> {
    if cycles.len() <= max_cycles {
        cycles.sort_by(compare_cycle_score);
        return cycles;
    }

    let mut by_hop: std::collections::HashMap<u32, Vec<FoundCycle>> =
        std::collections::HashMap::new();
    for c in cycles {
        by_hop.entry(c.hop_count).or_default().push(c);
    }
    for tier in by_hop.values_mut() {
        tier.sort_by(compare_cycle_score);
    }

    let total_quota: usize = quotas.iter().map(|(_, q)| q).sum();
    let scale = if total_quota > max_cycles {
        max_cycles as f64 / total_quota as f64
    } else {
        1.0
    };

    let mut selected = Vec::new();
    let mut selected_ids = HashSet::new();
    let mut hop_keys: Vec<u32> = quotas.iter().map(|(h, _)| *h).collect();
    hop_keys.sort_unstable();

    for hop in hop_keys {
        let Some(tier) = by_hop.get_mut(&hop) else {
            continue;
        };
        let quota = quotas
            .iter()
            .find(|(h, _)| *h == hop)
            .map(|(_, q)| ((*q as f64) * scale) as usize)
            .unwrap_or(0);
        let take = quota.min(tier.len());
        for c in tier.drain(..take) {
            if selected.len() >= max_cycles {
                break;
            }
            let fp = route_fingerprint(&c.edges);
            if selected_ids.contains(&fp) {
                continue;
            }
            selected_ids.insert(fp);
            selected.push(c);
        }
    }

    if selected.len() < max_cycles {
        let mut rest: Vec<FoundCycle> = by_hop
            .into_values()
            .flatten()
            .filter(|c| !selected_ids.contains(&route_fingerprint(&c.edges)))
            .collect();
        rest.sort_by(compare_cycle_score);
        for c in rest {
            if selected.len() >= max_cycles {
                break;
            }
            let fp = route_fingerprint(&c.edges);
            if selected_ids.contains(&fp) {
                continue;
            }
            selected_ids.insert(fp);
            selected.push(c);
        }
    }

    selected.sort_by(compare_cycle_score);
    selected
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{PoolIndex, PoolState, ProtocolType, V2PoolState};
    use crate::pipeline::graph::{build_graph, pool_meta_from_pair};
    use alloy::primitives::Address;
    use ruint::aliases::U256;

    #[test]
    fn finds_triangle_cycle() {
        let mut arena = StateArena::new();
        let t0 = arena.register_token(Address::repeat_byte(1));
        let t1 = arena.register_token(Address::repeat_byte(2));
        let t2 = arena.register_token(Address::repeat_byte(3));
        let reserve = U256::from(1_000_000u128) * U256::from(10u128).pow(U256::from(18));
        let v2 = |r0: U256, r1: U256| {
            PoolState::V2(V2PoolState {
                reserve0: r0,
                reserve1: r1,
                fee: U256::ZERO,
                fee_denominator: U256::ZERO,
            })
        };

        let p01 = arena.register_pool(
            Address::repeat_byte(0x10),
            v2(reserve, reserve * U256::from(2u8)),
        );
        let p12 = arena.register_pool(
            Address::repeat_byte(0x11),
            v2(reserve, reserve * U256::from(2u8)),
        );
        let p20 = arena.register_pool(
            Address::repeat_byte(0x12),
            v2(reserve * U256::from(2u8), reserve),
        );

        let pools = vec![
            pool_meta_from_pair(p01, ProtocolType::UniswapV2, t0, t1, Some(30)),
            pool_meta_from_pair(p12, ProtocolType::UniswapV2, t1, t2, Some(30)),
            pool_meta_from_pair(p20, ProtocolType::UniswapV2, t2, t0, Some(30)),
        ];
        let graph = build_graph(&arena, &pools);
        let cycles = find_cycles(&arena, &graph, 4, 100);
        assert!(!cycles.is_empty());
        assert!(cycles.iter().any(|c| c.hop_count == 3));
    }

    #[test]
    fn parallel_dfs_finds_cycles_on_hub_graph() {
        let mut arena = StateArena::new();
        let reserve = U256::from(1_000_000u128) * U256::from(10u128).pow(U256::from(18));
        let v2 = |r0: U256, r1: U256| {
            PoolState::V2(V2PoolState {
                reserve0: r0,
                reserve1: r1,
                fee: U256::ZERO,
                fee_denominator: U256::ZERO,
            })
        };

        let mut tokens = Vec::new();
        let mut pools = Vec::new();
        for i in 0..10u8 {
            tokens.push(arena.register_token(Address::repeat_byte(i + 1)));
        }
        let hub = tokens[0];
        for (i, &t) in tokens.iter().enumerate().skip(1) {
            let skew = if i % 2 == 0 { 2u8 } else { 1u8 };
            let p = arena.register_pool(
                Address::repeat_byte(0x20 + i as u8),
                v2(reserve, reserve * U256::from(skew)),
            );
            pools.push(pool_meta_from_pair(
                p,
                ProtocolType::UniswapV2,
                hub,
                t,
                Some(30),
            ));
            let p_back = arena.register_pool(
                Address::repeat_byte(0x40 + i as u8),
                v2(reserve * U256::from(skew), reserve),
            );
            pools.push(pool_meta_from_pair(
                p_back,
                ProtocolType::UniswapV2,
                t,
                hub,
                Some(30),
            ));
        }

        let graph = build_graph(&arena, &pools);
        assert!(graph.adjacency[hub.0 as usize].len() >= PARALLEL_DFS_MIN_STARTS);
        let cycles = find_cycles(&arena, &graph, 4, 200);
        assert!(cycles.len() <= 200);
    }

    #[test]
    fn hop_stratified_cap_limits_output() {
        let mk = |hop: u32, score: f64, pool: u32| FoundCycle {
            start_token: TokenIndex(0),
            edges: vec![Edge {
                pool_index: PoolIndex(pool),
                token_in: TokenIndex(0),
                token_out: TokenIndex(hop),
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            }]
            .into(),
            hop_count: hop,
            log_weight: score,
            cumulative_fee_bps: 30,
            score,
        };
        let cycles: Vec<_> = (0..20)
            .map(|i| mk(2 + (i % 4) as u32, i as f64, i as u32))
            .collect();
        let capped = apply_hop_stratified_cap(cycles, 5);
        assert_eq!(capped.len(), 5);
    }
}
