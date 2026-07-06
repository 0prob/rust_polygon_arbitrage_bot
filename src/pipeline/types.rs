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
    pub bpt_index: Option<usize>,
    pub pool_id: Option<FixedBytes<32>>,
    pub protocol_label: Option<String>,
    pub pool_type: Option<String>,
    pub hooks: Option<Address>,
    pub tick_spacing: Option<i32>,
}

#[derive(Debug, Clone, Default)]
pub struct RoutingGraph {
    /// `adjacency[token.0]` = outgoing edges from that token.
    pub adjacency: Vec<Vec<GraphEdge>>,
    pub token_count: u32,
    /// Reverse index: pool_index.0 → list of (adjacency_token_index, edge_index_in_list)
    pub pool_edge_positions: Vec<Vec<(usize, usize)>>,
    /// Cached cycle-capable coverage from Tarjan bridge search.
    /// Only recomputed when graph topology changes (not on rescore).
    pub coverage: Option<crate::pipeline::cycle_finder::CycleCapableCoverage>,
}

#[derive(Debug, Clone, Copy)]
pub struct CycleSearchPass {
    pub max_hops: u32,
    pub max_cycles: usize,
}

#[derive(Debug, Clone)]
pub struct MinimalSimResult {
    pub profit: alloy::primitives::U256,
    pub amount_out: alloy::primitives::U256,
    pub total_gas: u32,
}

#[derive(Debug, Clone)]
pub struct OptimizationResult {
    pub optimal_input: alloy::primitives::U256,
    pub expected_gross: alloy::primitives::U256,
    /// Gross token profit at `optimal_input` (before gas/fees).
    pub net_profit: alloy::primitives::U256,
    pub total_gas: u32,
    /// Brent search lower bound used for sanity pinning checks.
    pub search_low: alloy::primitives::U256,
}

impl RoutingGraph {
    #[must_use]
    pub fn new(token_count: u32) -> Self {
        Self {
            adjacency: vec![Vec::new(); token_count as usize],
            token_count,
            pool_edge_positions: Vec::new(),
            coverage: None,
        }
    }

    pub fn add_edge(&mut self, from: TokenIndex, graph_edge: GraphEdge) {
        if let Some(slot) = self.adjacency.get_mut(from.0 as usize) {
            let pos = slot.len();
            let pool_idx = graph_edge.edge.pool_index.0 as usize;
            slot.push(graph_edge);
            if pool_idx >= self.pool_edge_positions.len() {
                self.pool_edge_positions.resize(pool_idx + 1, Vec::new());
            }
            self.pool_edge_positions[pool_idx].push((from.0 as usize, pos));
        }
    }

    /// Pools with at least one live (tradable) directed edge in adjacency.
    #[must_use]
    pub fn active_pool_count(&self) -> usize {
        let mut live = rustc_hash::FxHashSet::default();
        for adj in &self.adjacency {
            for ge in adj {
                if crate::pipeline::cycle_finder::is_live_graph_edge(ge) {
                    live.insert(ge.edge.pool_index.0);
                }
            }
        }
        live.len()
    }

    #[must_use]
    pub fn pool_has_live_edges(&self, pool_index: PoolIndex) -> bool {
        let idx = pool_index.0 as usize;
        self.pool_edge_positions.get(idx).is_some_and(|positions| {
            positions.iter().any(|&(adj_idx, edge_pos)| {
                self.adjacency
                    .get(adj_idx)
                    .and_then(|adj| adj.get(edge_pos))
                    .is_some_and(crate::pipeline::cycle_finder::is_live_graph_edge)
            })
        })
    }
}

#[must_use]
pub fn pool_metas_by_index(pool_metas: &[PoolMeta]) -> rustc_hash::FxHashMap<PoolIndex, &PoolMeta> {
    pool_metas
        .iter()
        .map(|meta| (meta.pool_index, meta))
        .collect()
}

#[inline]
#[must_use]
pub fn pool_meta_at(pool_metas: &[PoolMeta], index: PoolIndex) -> Option<&PoolMeta> {
    pool_metas
        .get(index.0 as usize)
        .filter(|meta| meta.pool_index == index)
        .or_else(|| pool_metas.iter().find(|meta| meta.pool_index == index))
}

#[must_use]
pub fn compare_cycle_score(a: &FoundCycle, b: &FoundCycle) -> std::cmp::Ordering {
    a.score
        .total_cmp(&b.score)
        .then_with(|| a.hop_count.cmp(&b.hop_count))
        .then_with(|| a.start_token.0.cmp(&b.start_token.0))
        .then_with(|| {
            a.edges
                .iter()
                .zip(&b.edges)
                .find_map(|(left, right)| {
                    let order = left
                        .pool_index
                        .0
                        .cmp(&right.pool_index.0)
                        .then_with(|| left.token_in.0.cmp(&right.token_in.0))
                        .then_with(|| left.token_out.0.cmp(&right.token_out.0))
                        .then_with(|| left.token_in_idx.cmp(&right.token_in_idx))
                        .then_with(|| left.token_out_idx.cmp(&right.token_out_idx));
                    order.is_ne().then_some(order)
                })
                .unwrap_or_else(|| a.edges.len().cmp(&b.edges.len()))
        })
}
