use alloy::primitives::{Address, FixedBytes};

use crate::core::types::{Edge, FoundCycle, PoolIndex, ProtocolType, TokenIndex};

#[derive(Debug, Clone, Copy)]
pub struct GraphEdge {
    pub edge: Edge,
    pub log_weight: f64,
}

#[derive(Debug, Clone)]
pub struct PoolMeta {
    pub pool_index: PoolIndex,
    pub protocol: ProtocolType,
    pub tokens: Vec<TokenIndex>,
    pub fee_bps: u32,
    pub token0: TokenIndex,
    pub token1: TokenIndex,
    pub bpt_index: Option<usize>,
    pub pool_id: Option<FixedBytes<32>>,
    pub protocol_label: Option<String>,
    pub router: Option<Address>,
    pub hooks: Option<Address>,
    pub tick_spacing: Option<i32>,
}

#[derive(Debug, Clone, Default)]
pub struct RoutingGraph {
    /// `adjacency[token.0]` = outgoing edges from that token.
    pub adjacency: Vec<Vec<GraphEdge>>,
    pub token_count: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct CycleSearchPass {
    pub max_hops: u32,
    pub max_cycles: usize,
}

#[derive(Debug, Clone)]
pub struct MinimalSimResult {
    pub profit: ruint::aliases::U256,
    pub amount_out: ruint::aliases::U256,
    pub total_gas: u32,
}

#[derive(Debug, Clone)]
pub struct OptimizationResult {
    pub optimal_input: ruint::aliases::U256,
    pub expected_gross: ruint::aliases::U256,
    /// Gross token profit at `optimal_input` (before gas/fees).
    pub net_profit: ruint::aliases::U256,
    pub total_gas: u32,
    /// Brent search lower bound used for sanity pinning checks.
    pub search_low: ruint::aliases::U256,
}

impl RoutingGraph {
    pub fn new(token_count: u32) -> Self {
        Self {
            adjacency: vec![Vec::new(); token_count as usize],
            token_count,
        }
    }

    pub fn add_edge(&mut self, from: TokenIndex, graph_edge: GraphEdge) {
        if let Some(slot) = self.adjacency.get_mut(from.0 as usize) {
            slot.push(graph_edge);
        }
    }
}

pub fn route_fingerprint(edges: &[Edge]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = rustc_hash::FxHasher::default();
    for e in edges {
        e.pool_index.0.hash(&mut h);
        e.token_in.0.hash(&mut h);
        e.token_out.0.hash(&mut h);
        e.zero_for_one.hash(&mut h);
    }
    h.finish()
}

/// Sorted pool-index set for cache invalidation when route pool membership changes.
pub fn pool_set_fingerprint(edges: &[Edge]) -> u64 {
    hash_sorted_pool_indices(edges.iter().map(|e| e.pool_index))
}

pub fn union_pool_set_fingerprint(cycles: &[FoundCycle]) -> u64 {
    hash_sorted_pool_indices(
        cycles
            .iter()
            .flat_map(|c| c.edges.iter().map(|e| e.pool_index)),
    )
}

fn hash_sorted_pool_indices(indices: impl Iterator<Item = PoolIndex>) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut pools: Vec<PoolIndex> = indices.collect();
    pools.sort_by_key(|p| p.0);
    pools.dedup_by_key(|p| p.0);
    let mut h = rustc_hash::FxHasher::default();
    for p in pools {
        p.0.hash(&mut h);
    }
    h.finish()
}

pub fn compare_cycle_score(a: &FoundCycle, b: &FoundCycle) -> std::cmp::Ordering {
    a.score
        .partial_cmp(&b.score)
        .unwrap_or(std::cmp::Ordering::Equal)
}
