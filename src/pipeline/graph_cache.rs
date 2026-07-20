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
    /// Family tags for `cached_pool_count` pools at last store.
    cached_family_prefix: u64,
    cached_state_generation: u64,
    /// Capped `attach_missing` left work — keep scanning until a non-capped pass.
    attach_catchup_pending: bool,
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

    /// Pure membership growth is handled by `attach_missing` (coverage refreshed).
    /// A full rebuild is reserved for interval recompute or shrink/reorder.
    fn pool_count_rebuild_due(&self, routable_pool_count: usize) -> bool {
        if self.graph.is_none() {
            return true;
        }
        // Shrink: discovery/prune dropped pools — indices may be invalid.
        if routable_pool_count < self.cached_pool_count {
            let lost = self.cached_pool_count - routable_pool_count;
            let min_delta = if self.cached_pool_count >= WARM_POOL_THRESHOLD {
                WARM_POOL_COUNT_REBUILD_DELTA
            } else {
                POOL_COUNT_REBUILD_DELTA
            };
            return lost >= min_delta.max(self.cached_pool_count / 20);
        }
        false
    }

    #[must_use]
    pub fn needs_connectivity_rebuild(
        &self,
        routable_pool_count: usize,
        layout_fingerprint: u64,
    ) -> bool {
        if self.graph.is_none() {
            return true;
        }
        if self.lf_pass_count.is_multiple_of(self.rebuild_interval) {
            return true;
        }
        if self.pool_count_rebuild_due(routable_pool_count) {
            return true;
        }
        // Fingerprint change with same or fewer pools ⇒ reorder / drop / replace.
        // Pure growth (more pools, new fingerprint) is patched via attach_missing
        // on the interval, not a full rebuild every LF warmup tick.
        if self.cached_layout_fingerprint != layout_fingerprint
            && routable_pool_count <= self.cached_pool_count
        {
            return true;
        }
        false
    }

    /// True when cached cycles remain index-valid after this membership change.
    /// Growth (append-only arena) keeps PoolIndex; shrink/reorder does not.
    /// `family_prefix` must be [`StateArena::routing_family_prefix_fingerprint`]
    /// over the previously cached pool count.
    #[must_use]
    pub fn cycle_cache_still_valid(
        &self,
        routable_pool_count: usize,
        layout_fingerprint: u64,
        family_prefix: u64,
    ) -> bool {
        if self.cycles.as_ref().is_none_or(|c| c.is_empty()) {
            return false;
        }
        if routable_pool_count < self.cached_pool_count {
            return false;
        }
        // In-place family flip during growth (Balancer→V3) — layout fp is new
        // from appends so the count<= gate would keep poison cycles.
        if family_prefix != self.cached_family_prefix {
            return false;
        }
        if self.cached_layout_fingerprint != layout_fingerprint
            && routable_pool_count <= self.cached_pool_count
        {
            return false;
        }
        true
    }

    #[must_use]
    pub fn cached_pool_count(&self) -> usize {
        self.cached_pool_count
    }

    #[must_use]
    pub fn needs_cycle_refind(
        &self,
        routable_pool_count: usize,
        layout_fingerprint: u64,
        state_generation: u64,
        dirty_pool_count: usize,
        arena_pool_count: usize,
    ) -> bool {
        let _ = (routable_pool_count, layout_fingerprint);
        // Connectivity rebuild is handled by LF via `(needs_rebuild && !cycle_cache_valid)`.
        // Coupling rebuild→refind here defeated keep_cycles on growth/interval rebuilds.
        if self.cycles.as_ref().is_none_or(|c| c.is_empty()) {
            return true;
        }
        // Mirror graph rescoring: a majority-dirty tick can invalidate cached routes.
        if state_generation != self.cached_state_generation
            && dirty_pool_count > 0
            && arena_pool_count > 0
            && dirty_pool_count > arena_pool_count / 2
        {
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
        family_prefix: u64,
    ) {
        self.graph = Some(graph);
        self.cycles = cycles.and_then(|c| (!c.is_empty()).then_some(c));
        self.cached_pool_count = pool_count;
        self.cached_eligible_pool_count = eligible_pool_count;
        self.cached_layout_fingerprint = layout_fingerprint;
        self.cached_family_prefix = family_prefix;
        self.cached_state_generation = state_generation;
    }

    /// True when pools gained eligibility since the last connectivity build.
    /// Pools losing eligibility only need edge rescoring (dead edges).
    /// Also true while a prior capped attach still has missing pools to catch up.
    #[must_use]
    pub fn connectivity_stale(&self, eligible_pool_count: usize) -> bool {
        if self.attach_catchup_pending {
            return true;
        }
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

    pub fn set_attach_catchup_pending(&mut self, pending: bool) {
        self.attach_catchup_pending = pending;
    }

    #[must_use]
    pub fn attach_catchup_pending(&self) -> bool {
        self.attach_catchup_pending
    }

    /// Drop cached cycles after arena index rebuild (TokenIndex reassignment).
    pub fn invalidate_cycles(&mut self) {
        self.cycles = None;
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
        let mut rescore_report = crate::pipeline::graph::GraphRescoreReport::default();
        if let Some(g) = &mut self.graph {
            let gm = Arc::make_mut(g);
            rescore_report = crate::pipeline::graph::rescore_dirty_pools_or_full(
                arena,
                gm,
                dirty_pools,
                arena_pool_count,
            );
        }
        if let Some(mode) = rescore_report.mode {
            crate::debug!(
                "graph rescore: mode={mode:?} dirty_pools={} edges_touched={}",
                rescore_report.dirty_pools,
                rescore_report.edges_touched,
            );
        }
        let g = Arc::clone(self.graph.as_ref()?);
        let cyc = self.cycles.clone();
        let family_prefix = arena.routing_family_prefix_fingerprint(routable_count);
        self.store(
            Arc::clone(&g),
            cyc,
            routable_count,
            layout_fingerprint,
            new_state_generation,
            eligible_count,
            family_prefix,
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
            0,
        );
        assert!(cache.needs_cycle_refind(10, 0, 0, 0, 0));
    }

    #[test]
    fn pure_growth_does_not_force_rebuild() {
        // Growth is attach_missing territory — full rebuild only on interval / shrink / reorder.
        let mut cache = GraphCache::with_rebuild_interval(60);
        cache.advance_pass();
        cache.store(
            Arc::new(crate::pipeline::types::RoutingGraph::default()),
            Some(Arc::new(vec![dummy_cycle()])),
            1_000,
            1,
            5,
            800,
            0,
        );
        assert!(!cache.needs_connectivity_rebuild(1_050, 1));
        assert!(!cache.needs_connectivity_rebuild(1_500, 99));
        assert!(cache.cycle_cache_still_valid(1_500, 99, 0));
        assert!(!cache.needs_cycle_refind(1_050, 1, 6, 0, 1_000));
    }

    #[test]
    fn interval_rebuild_pass_does_not_force_cycle_refind() {
        // keep_cycles relies on this: rebuild interval alone must not imply full DFS.
        let mut cache = GraphCache::with_intervals(4, 8);
        for _ in 0..3 {
            cache.advance_pass();
        }
        cache.store(
            Arc::new(crate::pipeline::types::RoutingGraph::default()),
            Some(Arc::new(vec![dummy_cycle()])),
            1_000,
            1,
            5,
            800,
            0,
        );
        cache.advance_pass(); // pass 4 — rebuild due, cycle refind interval not yet
        assert!(cache.needs_connectivity_rebuild(1_000, 1));
        assert!(cache.cycle_cache_still_valid(1_000, 1, 0));
        assert!(!cache.needs_cycle_refind(1_000, 1, 5, 0, 1_000));
    }

    #[test]
    fn large_shrink_forces_rebuild() {
        let mut cache = GraphCache::with_rebuild_interval(60);
        cache.advance_pass();
        cache.store(
            Arc::new(crate::pipeline::types::RoutingGraph::default()),
            Some(Arc::new(vec![dummy_cycle()])),
            4_000,
            1,
            5,
            3_500,
            0,
        );
        // Small shrink under warm threshold — keep cache path.
        assert!(!cache.needs_connectivity_rebuild(3_900, 1));
        // Large shrink — rebuild.
        assert!(cache.needs_connectivity_rebuild(3_500, 1));
        assert!(!cache.cycle_cache_still_valid(3_500, 1, 0));
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
            0,
        );
        assert!(!cache.needs_cycle_refind(1_000, 1, 5, 0, 1_000));
        assert!(!cache.needs_cycle_refind(1_000, 1, 6, 0, 1_000));
    }

    #[test]
    fn majority_dirty_pools_force_cycle_refind() {
        let mut cache = GraphCache::with_intervals(60, 8);
        cache.advance_pass();
        cache.store(
            Arc::new(crate::pipeline::types::RoutingGraph::default()),
            Some(Arc::new(vec![dummy_cycle()])),
            1_000,
            1,
            5,
            800,
            0,
        );
        assert!(!cache.needs_cycle_refind(1_000, 1, 6, 10, 100));
        assert!(cache.needs_cycle_refind(1_000, 1, 6, 51, 100));
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
            0,
        );
        assert!(!cache.needs_cycle_refind(1_000, 1, 6, 0, 1_000));
        cache.advance_pass();
        assert!(cache.needs_cycle_refind(1_000, 1, 6, 0, 1_000));
    }

    #[test]
    fn large_pool_count_growth_uses_attach_not_rebuild() {
        let mut cache = GraphCache::with_rebuild_interval(60);
        cache.advance_pass();
        cache.store(
            Arc::new(crate::pipeline::types::RoutingGraph::default()),
            Some(Arc::new(vec![dummy_cycle()])),
            1_000,
            1,
            5,
            800,
            0,
        );
        assert!(!cache.needs_connectivity_rebuild(1_100, 1));
        // Interval / empty cycles still force refind; growth alone does not.
        assert!(!cache.needs_cycle_refind(1_100, 1, 6, 0, 1_000));
        assert!(cache.cycle_cache_still_valid(1_100, 1, 0));
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
            0,
        );
        assert!(!cache.needs_connectivity_rebuild(10, 1));
        assert!(cache.needs_connectivity_rebuild(10, 2));
        // Rebuild alone does not force refind — LF uses cycle_cache_still_valid.
        assert!(!cache.cycle_cache_still_valid(10, 2, 0));
        assert!(!cache.needs_cycle_refind(10, 2, 0, 0, 10));
    }

    #[test]
    fn pure_growth_fingerprint_does_not_force_rebuild() {
        // Append-only arena growth changes layout fingerprint but should patch
        // via attach_missing until pool-count / eligible thresholds fire.
        let mut cache = GraphCache::with_rebuild_interval(60);
        cache.advance_pass();
        cache.store(
            Arc::new(crate::pipeline::types::RoutingGraph::default()),
            Some(Arc::new(vec![dummy_cycle()])),
            1_000,
            1,
            5,
            800,
            0,
        );
        // +30 pools, new fingerprint: below rebuild delta (64 / 5%).
        assert!(!cache.needs_connectivity_rebuild(1_030, 99));
        // Shrinkage/reorder with fingerprint change still rebuilds.
        assert!(cache.needs_connectivity_rebuild(1_000, 99));
        assert!(cache.needs_connectivity_rebuild(990, 99));
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
            0,
        );
        assert!(!cache.needs_connectivity_rebuild(1_000, 11));
        assert!(cache.needs_connectivity_rebuild(1_000, 12));
        assert!(!cache.cycle_cache_still_valid(1_000, 12, 0));
        assert!(!cache.needs_cycle_refind(1_000, 12, 6, 0, 1_000));
    }

    #[test]
    fn family_prefix_change_invalidates_cycles_during_growth() {
        // Growth changes layout fp but must not mask in-place family flips.
        let mut cache = GraphCache::with_rebuild_interval(60);
        cache.advance_pass();
        cache.store(
            Arc::new(crate::pipeline::types::RoutingGraph::default()),
            Some(Arc::new(vec![dummy_cycle()])),
            1_000,
            1,
            5,
            800,
            11,
        );
        assert!(cache.cycle_cache_still_valid(1_050, 99, 11));
        assert!(!cache.cycle_cache_still_valid(1_050, 99, 12));
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
            0,
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
            0,
        );
        assert!(!cache.needs_cycle_refind(1_000, 1, 7, 0, 1_000));
        assert!(!cache.needs_cycle_refind(1_000, 1, 8, 0, 1_000));
    }
}
