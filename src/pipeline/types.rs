use alloy::primitives::{Address, FixedBytes, U256};
use smallvec::SmallVec;

use crate::core::constants::MAX_POOL_TOKENS;
use crate::core::types::{Edge, FoundCycle, PoolIndex, ProtocolType, TokenIndex};

/// Graph traversal phase for hub-and-spoke pool abstraction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GraphHopPhase {
    /// Token-to-token swap on a 2-token pool (classic edge).
    #[default]
    Direct,
    /// Token deposits into a virtual pool hub node.
    EnterPool,
    /// Virtual pool hub exits to a token (weight resolved lazily on traversal).
    ExitPool,
}

#[derive(Debug, Clone, Copy)]
pub struct GraphEdge {
    pub edge: Edge,
    pub phase: GraphHopPhase,
    /// Destination graph node index (token id or virtual hub id).
    pub target_node: u32,
    pub log_weight: f64,
    /// U256 fixed-point ratio: spot_price * (1 - fee_bps/10000) scaled to ONE (1e18).
    /// ratio > ONE means the edge is profitable (more output than input).
    /// U256::ZERO = dead/unroutable edge.
    /// Enter/Exit hub legs use ONE as a neutral placeholder until paired.
    pub ratio: U256,
}

/// Virtual pool hub node in the routing graph (pool-as-a-node abstraction).
#[derive(Debug, Clone)]
pub struct VirtualPoolHub {
    pub pool_index: PoolIndex,
    pub protocol: ProtocolType,
    /// Funded token leg indices for exit fan-out (pool-local indices).
    pub exit_legs: SmallVec<[u8; MAX_POOL_TOKENS]>,
    /// When true, `pool_index` on enter edges selects the active V4 pool context.
    pub v4_singleton: bool,
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
    /// `adjacency[node]` = outgoing edges from that graph node (tokens + virtual hubs).
    pub adjacency: Vec<Vec<GraphEdge>>,
    pub token_count: u32,
    /// Virtual pool hub metadata; hub node id = `token_count + hub_index`.
    pub virtual_hubs: Vec<VirtualPoolHub>,
    /// Shared singleton hub node for all Uniswap V4 pools (PoolManager).
    pub v4_singleton_hub: Option<u32>,
    /// Reverse index: pool_index.0 → list of (adjacency_node_index, edge_index_in_list)
    pub pool_edge_positions: Vec<Vec<(usize, usize)>>,
    /// Cached cycle-capable coverage from Tarjan bridge search.
    /// Refreshed when thinning removes edges or on first build, not on weight-only rescoring.
    pub coverage: Option<std::sync::Arc<crate::pipeline::cycle_finder::CycleCapableCoverage>>,
}

#[derive(Debug, Clone, Copy)]
pub struct CycleSearchPass {
    pub max_hops: u32,
    pub max_cycles: usize,
}

#[derive(Debug, Clone, Copy)]
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
            virtual_hubs: Vec::new(),
            v4_singleton_hub: None,
            pool_edge_positions: Vec::new(),
            coverage: None,
        }
    }

    #[inline]
    #[must_use]
    pub fn node_count(&self) -> u32 {
        self.adjacency.len() as u32
    }

    #[inline]
    #[must_use]
    pub fn is_virtual_node(&self, node: u32) -> bool {
        node >= self.token_count
    }

    #[inline]
    #[must_use]
    pub fn virtual_hub_index(&self, node: u32) -> Option<usize> {
        if !self.is_virtual_node(node) {
            return None;
        }
        let idx = node.saturating_sub(self.token_count) as usize;
        self.virtual_hubs.get(idx).map(|_| idx)
    }

    pub fn add_direct_edge(&mut self, from: TokenIndex, graph_edge: GraphEdge) {
        self.push_edge_at(from.0, graph_edge);
    }

    pub fn push_edge_at(&mut self, from_node: u32, mut graph_edge: GraphEdge) {
        if graph_edge.phase == GraphHopPhase::Direct {
            graph_edge.target_node = graph_edge.edge.token_out.0;
        }
        let idx = from_node as usize;
        if idx >= self.adjacency.len() {
            self.adjacency.resize(idx + 1, Vec::new());
        }
        let pos = self.adjacency[idx].len();
        let pool_idx = graph_edge.edge.pool_index.0 as usize;
        self.adjacency[idx].push(graph_edge);
        if pool_idx >= self.pool_edge_positions.len() {
            self.pool_edge_positions.resize(pool_idx + 1, Vec::new());
        }
        self.pool_edge_positions[pool_idx].push((idx, pos));
    }

    /// Legacy helper — direct token edges only.
    pub fn add_edge(&mut self, from: TokenIndex, graph_edge: GraphEdge) {
        self.add_direct_edge(from, graph_edge);
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
    pool_metas.iter().find(|meta| meta.pool_index == index)
}

#[must_use]
pub fn compare_cycle_score(a: &FoundCycle, b: &FoundCycle) -> std::cmp::Ordering {
    // Primary key: U256 cycle_ratio (exact fixed-point). Higher ratio = more profitable at margin.
    // This eliminates f64 precision loss from score-based ranking.
    // When both cycle_ratio are U256::ZERO (cache restore path), fall back to f64 score.
    b.cycle_ratio
        .cmp(&a.cycle_ratio)
        .then_with(|| a.score.total_cmp(&b.score))
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