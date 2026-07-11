use std::sync::Arc;

use crate::core::types::FoundCycle;

/// Minimum pool-count delta before forcing a connectivity rebuild (bootstrap arena).
const POOL_COUNT_REBUILD_DELTA: usize = 64;
/// Larger delta once the routable arena is warm — steady-state rebuilds stay rare.
const WARM_POOL_COUNT_REBUILD_DELTA: usize = 256;
/// Routable pool count treated as a warm graph for rebuild throttling.
const WARM_POOL_THRESHOLD: usize = 3_000;
const ELIGIBLE_POOL_REBUILD_DELTA: usize = 64;
const DEFAULT_CYCLE_REFIND_INTERVAL: u64 = 8;

#[must_use]
pub fn default_cycle_refind_interval() -> u64 {
    DEFAULT_CYCLE_REFIND_INTERVAL
}

#[derive(Default)]
pub struct GraphCache {
    rebuild_interval: u64,
    cycle_refind_interval: u64,
    graph: Option<Arc<crate::pipeline::types::RoutingGraph>>,
    cycles: Option<Arc<Vec<FoundCycle>>>,
    lf_pass_count: u64,
    cached_pool_count: usize,
    cached_eligible_pool_count: usize,
    cached_layout_fingerprint: u64,
    cached_state_generation: u64,
}

impl GraphCache {
    #[must_use]
    pub fn with_rebuild_interval(interval: u64) -> Self {
        Self::with_intervals(interval, default_cycle_refind_interval())
    }

    #[must_use]
    pub fn with_intervals(rebuild_interval: u64, cycle_refind_interval: u64) -> Self {
        Self {
            rebuild_interval: rebuild_interval.max(1),
            cycle_refind_interval: cycle_refind_interval.max(1),
            ..Self::default()
        }
    }

    pub fn advance_pass(&mut self) -> u64 {
        self.lf_pass_count += 1;
        self.lf_pass_count
    }

    fn pool_count_rebuild_due(&self, routable_pool_count: usize) -> bool {
        if self.graph.is_none() {
            return true;
        }
        let delta = routable_pool_count.abs_diff(self.cached_pool_count);
        let pct_threshold = self.cached_pool_count / 20;
        let min_delta = if self.cached_pool_count >= WARM_POOL_THRESHOLD {
            WARM_POOL_COUNT_REBUILD_DELTA
        } else {
            POOL_COUNT_REBUILD_DELTA
        };
        delta >= min_delta.max(pct_threshold)
    }

    #[must_use]
    pub fn needs_connectivity_rebuild(
        &self,
        routable_pool_count: usize,
        layout_fingerprint: u64,
    ) -> bool {
        self.lf_pass_count.is_multiple_of(self.rebuild_interval)
            || self.graph.is_none()
            || self.pool_count_rebuild_due(routable_pool_count)
            || self.cached_layout_fingerprint != layout_fingerprint
    }

    #[must_use]
    pub fn needs_cycle_refind(
        &self,
        routable_pool_count: usize,
        layout_fingerprint: u64,
        _state_generation: u64,
    ) -> bool {
        if self.needs_connectivity_rebuild(routable_pool_count, layout_fingerprint) {
            return true;
        }
        if self.cycles.as_ref().is_none_or(|c| c.is_empty()) {
            return true;
        }
        // LF rescoring reflects pool-state deltas; full enumeration is periodic.
        self.lf_pass_count
            .is_multiple_of(self.cycle_refind_interval)
    }

    pub fn store(
        &mut self,
        graph: Arc<crate::pipeline::types::RoutingGraph>,
        cycles: Option<Arc<Vec<FoundCycle>>>,
        pool_count: usize,
        layout_fingerprint: u64,
        state_generation: u64,
        eligible_pool_count: usize,
    ) {
        self.graph = Some(graph);
        self.cycles = cycles.and_then(|c| (!c.is_empty()).then_some(c));
        self.cached_pool_count = pool_count;
        self.cached_eligible_pool_count = eligible_pool_count;
        self.cached_layout_fingerprint = layout_fingerprint;
        self.cached_state_generation = state_generation;
    }

    /// True when pools gained eligibility since the last connectivity build.
    /// Pools losing eligibility only need edge rescoring (dead edges).
    #[must_use]
    pub fn connectivity_stale(&self, eligible_pool_count: usize) -> bool {
        if self.graph.is_none() || eligible_pool_count <= self.cached_eligible_pool_count {
            return false;
        }
        let delta = eligible_pool_count - self.cached_eligible_pool_count;
        delta >= ELIGIBLE_POOL_REBUILD_DELTA.max(self.cached_eligible_pool_count / 20)
    }

    #[must_use]
    pub fn cached_state_generation(&self) -> u64 {
        self.cached_state_generation
    }

    #[must_use]
    pub fn cached_eligible_pool_count(&self) -> usize {
        self.cached_eligible_pool_count
    }

    #[must_use]
    pub fn graph(&self) -> Option<Arc<crate::pipeline::types::RoutingGraph>> {
        self.graph.clone()
    }

    #[must_use]
    pub fn cycles(&self) -> Option<Arc<Vec<FoundCycle>>> {
        self.cycles.clone()
    }

    /// Apply dirty-pool rescoring in place (no graph data clone when unique)
    /// then update meta + store current gen. Returns handle to (mutated) graph.
    #[allow(clippy::too_many_arguments)]
    pub fn rescore_dirty_and_update(
        &mut self,
        arena: &crate::pipeline::arena::StateArena,
        dirty_pools: &[crate::core::types::PoolIndex],
        arena_pool_count: usize,
        new_state_generation: u64,
        layout_fingerprint: u64,
        routable_count: usize,
        eligible_count: usize,
    ) -> Option<Arc<crate::pipeline::types::RoutingGraph>> {
        if let Some(g) = &mut self.graph {
            let gm = Arc::make_mut(g);
            crate::pipeline::graph::rescore_dirty_pools_or_full(
                arena,
                gm,
                dirty_pools,
                arena_pool_count,
            );
        }
        let g = Arc::clone(self.graph.as_ref()?);
        let cyc = self.cycles.as_ref().cloned();
        self.store(
            Arc::clone(&g),
            cyc,
            routable_count,
            layout_fingerprint,
            new_state_generation,
            eligible_count,
        );
        Some(g)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{CycleEdges, Edge, FoundCycle, PoolIndex, ProtocolType, TokenIndex};
    use alloy::primitives::U256;

    fn dummy_cycle() -> FoundCycle {
        FoundCycle {
            start_token: TokenIndex(0),
            edges: CycleEdges::from_slice(&[Edge {
                pool_index: PoolIndex(0),
                token_in: TokenIndex(0),
                token_out: TokenIndex(1),
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            }]),
            hop_count: 1,
            log_weight: -0.1,
            cumulative_fee_bps: 30,
            score: -0.1,
            cycle_ratio: U256::ZERO,
        }
    }

    #[test]
    fn empty_cycle_cache_triggers_refind() {
        let mut cache = GraphCache::with_rebuild_interval(60);
        cache.advance_pass();
        cache.store(
            Arc::new(crate::pipeline::types::RoutingGraph::default()),
            Some(Arc::new(Vec::new())),
            10,
            0,
            0,
            0,
        );
        assert!(cache.needs_cycle_refind(10, 0, 0));
    }

    #[test]
    fn small_pool_count_delta_does_not_force_rebuild() {
        let mut cache = GraphCache::with_rebuild_interval(60);
        cache.advance_pass();
        cache.store(
            Arc::new(crate::pipeline::types::RoutingGraph::default()),
            Some(Arc::new(vec![dummy_cycle()])),
            1_000,
            1,
            5,
            800,
        );
        assert!(!cache.needs_connectivity_rebuild(1_050, 1));
        assert!(!cache.needs_cycle_refind(1_050, 1, 6));
    }

    #[test]
    fn warm_graph_requires_larger_pool_delta_for_rebuild() {
        let mut cache = GraphCache::with_rebuild_interval(60);
        cache.advance_pass();
        cache.store(
            Arc::new(crate::pipeline::types::RoutingGraph::default()),
            Some(Arc::new(vec![dummy_cycle()])),
            4_000,
            1,
            5,
            3_500,
        );
        assert!(!cache.needs_connectivity_rebuild(4_100, 1));
        assert!(!cache.needs_connectivity_rebuild(4_200, 1));
        assert!(cache.needs_connectivity_rebuild(4_300, 1));
    }

    #[test]
    fn state_generation_change_does_not_force_cycle_refind() {
        let mut cache = GraphCache::with_intervals(60, 8);
        cache.advance_pass();
        cache.store(
            Arc::new(crate::pipeline::types::RoutingGraph::default()),
            Some(Arc::new(vec![dummy_cycle()])),
            1_000,
            1,
            5,
            800,
        );
        assert!(!cache.needs_cycle_refind(1_000, 1, 5));
        assert!(!cache.needs_cycle_refind(1_000, 1, 6));
    }

    #[test]
    fn cycle_refind_runs_on_interval_pass() {
        let mut cache = GraphCache::with_intervals(60, 8);
        for _ in 0..7 {
            cache.advance_pass();
        }
        cache.store(
            Arc::new(crate::pipeline::types::RoutingGraph::default()),
            Some(Arc::new(vec![dummy_cycle()])),
            1_000,
            1,
            5,
            800,
        );
        assert!(!cache.needs_cycle_refind(1_000, 1, 6));
        cache.advance_pass();
        assert!(cache.needs_cycle_refind(1_000, 1, 6));
    }

    #[test]
    fn large_pool_count_delta_triggers_rebuild_and_refind() {
        let mut cache = GraphCache::with_rebuild_interval(60);
        cache.advance_pass();
        cache.store(
            Arc::new(crate::pipeline::types::RoutingGraph::default()),
            Some(Arc::new(vec![dummy_cycle()])),
            1_000,
            1,
            5,
            800,
        );
        assert!(cache.needs_connectivity_rebuild(1_100, 1));
        assert!(cache.needs_cycle_refind(1_100, 1, 6));
    }

    #[test]
    fn layout_fingerprint_change_triggers_rebuild() {
        let mut cache = GraphCache::with_rebuild_interval(60);
        cache.advance_pass();
        cache.store(
            Arc::new(crate::pipeline::types::RoutingGraph::default()),
            Some(Arc::new(vec![dummy_cycle()])),
            10,
            1,
            0,
            8,
        );
        assert!(!cache.needs_connectivity_rebuild(10, 1));
        assert!(cache.needs_connectivity_rebuild(10, 2));
        assert!(cache.needs_cycle_refind(10, 2, 0));
    }

    #[test]
    fn same_pool_count_with_layout_change_triggers_rebuild() {
        let mut cache = GraphCache::with_rebuild_interval(60);
        cache.advance_pass();
        cache.store(
            Arc::new(crate::pipeline::types::RoutingGraph::default()),
            Some(Arc::new(vec![dummy_cycle()])),
            1_000,
            11,
            5,
            800,
        );
        assert!(!cache.needs_connectivity_rebuild(1_000, 11));
        assert!(cache.needs_connectivity_rebuild(1_000, 12));
        assert!(cache.needs_cycle_refind(1_000, 12, 6));
    }

    #[test]
    fn eligible_pool_gain_triggers_connectivity_stale() {
        let mut cache = GraphCache::with_rebuild_interval(60);
        cache.advance_pass();
        cache.store(
            Arc::new(crate::pipeline::types::RoutingGraph::default()),
            Some(Arc::new(vec![dummy_cycle()])),
            1_000,
            1,
            5,
            800,
        );
        assert!(!cache.connectivity_stale(800));
        assert!(!cache.connectivity_stale(799));
        assert!(!cache.connectivity_stale(801));
        assert!(cache.connectivity_stale(864));
    }

    #[test]
    fn state_generation_change_does_not_force_refind_without_interval() {
        let mut cache = GraphCache::with_intervals(60, 8);
        cache.advance_pass();
        cache.store(
            Arc::new(crate::pipeline::types::RoutingGraph::default()),
            Some(Arc::new(vec![dummy_cycle()])),
            1_000,
            1,
            7,
            800,
        );
        assert!(!cache.needs_cycle_refind(1_000, 1, 7));
        assert!(!cache.needs_cycle_refind(1_000, 1, 8));
    }
}
