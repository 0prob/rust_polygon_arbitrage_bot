use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::core::types::FoundCycle;
use crate::pipeline::types::RoutingGraph;
use crate::services::state_cache::StateCache;

static REBUILD_INTERVAL: AtomicU64 = AtomicU64::new(60);

/// Configure how often the routing graph is force-rebuilt even when the pool fingerprint is stable.
pub fn set_graph_rebuild_interval(interval: u64) {
    REBUILD_INTERVAL.store(interval.max(1), Ordering::Relaxed);
}

fn full_rebuild_interval() -> u64 {
    REBUILD_INTERVAL.load(Ordering::Relaxed).max(1)
}

#[derive(Default)]
pub struct GraphCache {
    graph: Option<Arc<RoutingGraph>>,
    cycles: Option<Arc<Vec<FoundCycle>>>,
    pool_fingerprint: u64,
    lf_pass_count: u64,
}

impl GraphCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn needs_rebuild(&self, cache: &StateCache, pool_count: usize) -> bool {
        let fp = pool_fingerprint(cache, pool_count);
        self.lf_pass_count.is_multiple_of(full_rebuild_interval())
            || self.graph.is_none()
            || self.pool_fingerprint != fp
    }

    pub fn store(&mut self, graph: Arc<RoutingGraph>, cycles: Option<Arc<Vec<FoundCycle>>>, cache: &StateCache, pool_count: usize) {
        let fp = pool_fingerprint(cache, pool_count);
        self.graph = Some(graph);
        self.pool_fingerprint = fp;
        self.cycles = cycles;
        self.lf_pass_count += 1;
    }

    pub fn get_cached_cycles(&self, cache: &StateCache, pool_count: usize) -> Option<Arc<Vec<FoundCycle>>> {
        let fp = pool_fingerprint(cache, pool_count);
        if fp == self.pool_fingerprint {
            self.cycles.as_ref().map(Arc::clone)
        } else {
            None
        }
    }

    pub fn lf_pass_count(&self) -> u64 {
        self.lf_pass_count
    }

    pub fn graph(&self) -> Option<Arc<RoutingGraph>> {
        self.graph.as_ref().map(Arc::clone)
    }
}

/// O(1) fingerprint from cache generation + pool topology size.
pub fn pool_fingerprint(cache: &StateCache, pool_count: usize) -> u64 {
    cache
        .generation()
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(pool_count as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{PoolState, ProtocolType, V2PoolState};
    use crate::pipeline::arena::StateArena;
    use crate::pipeline::graph::{build_graph, pool_meta_from_pair};
    use alloy::primitives::Address;
    use ruint::aliases::U256;

    fn make_state_cache() -> StateCache {
        StateCache::default()
    }

    #[test]
    fn reuses_graph_when_pool_set_unchanged() {
        let state_cache = StateCache::default();
        let mut cache = GraphCache::new();
        let mut arena = StateArena::new();
        let t0 = arena.register_token(Address::repeat_byte(1));
        let t1 = arena.register_token(Address::repeat_byte(2));
        let reserve = U256::from(1_000_000u128) * U256::from(10u128).pow(U256::from(18));
        let p = arena.register_pool(
            Address::repeat_byte(0x10),
            PoolState::V2(V2PoolState {
                reserve0: reserve,
                reserve1: reserve,
                fee: U256::ZERO,
                fee_denominator: U256::ZERO,
            }),
        );
        state_cache.insert(
            Address::repeat_byte(0x10),
            PoolState::V2(V2PoolState {
                reserve0: reserve,
                reserve1: reserve,
                fee: U256::ZERO,
                fee_denominator: U256::ZERO,
            }),
        );
        let pools = vec![pool_meta_from_pair(
            p,
            ProtocolType::UniswapV2,
            t0,
            t1,
            Some(30),
        )];

        // First call — should rebuild
        assert!(cache.needs_rebuild(&state_cache, pools.len()));
        let g = Arc::new(build_graph(&arena, &pools));
        cache.store(Arc::clone(&g), None, &state_cache, pools.len());

        // Second call — should reuse
        assert!(!cache.needs_rebuild(&state_cache, pools.len()));
        let g2 = cache.graph().unwrap();
        assert_eq!(g.token_count, g2.token_count);
        assert!(Arc::ptr_eq(&g, &g2));
        assert_eq!(cache.lf_pass_count(), 1);
    }

    #[test]
    fn rebuilds_when_pool_state_updates() {
        let state_cache = StateCache::default();
        let mut cache = GraphCache::new();
        let mut arena = StateArena::new();
        let t0 = arena.register_token(Address::repeat_byte(1));
        let t1 = arena.register_token(Address::repeat_byte(2));
        let addr = Address::repeat_byte(0x10);
        let p = arena.register_pool(addr, PoolState::Invalid);
        let pools = vec![pool_meta_from_pair(
            p,
            ProtocolType::UniswapV2,
            t0,
            t1,
            Some(30),
        )];
        state_cache.insert(addr, PoolState::Invalid);

        assert!(cache.needs_rebuild(&state_cache, pools.len()));
        let g_invalid = Arc::new(build_graph(&arena, &pools));
        cache.store(g_invalid, None, &state_cache, pools.len());

        let cached = cache.graph().unwrap();
        assert!(cached.adjacency.iter().all(|a| a.is_empty()));

        let reserve = U256::from(1_000_000u128) * U256::from(10u128).pow(U256::from(18));
        let tradable = PoolState::V2(V2PoolState {
            reserve0: reserve,
            reserve1: reserve,
            fee: U256::ZERO,
            fee_denominator: U256::ZERO,
        });
        arena.register_pool(addr, tradable.clone());
        state_cache.insert(addr, tradable);

        // Fingerprint changed — should trigger rebuild
        assert!(cache.needs_rebuild(&state_cache, pools.len()));
        let g_tradable = Arc::new(build_graph(&arena, &pools));
        cache.store(g_tradable, None, &state_cache, pools.len());
        let cached2 = cache.graph().unwrap();
        assert_eq!(
            cached2.adjacency.iter().map(|a| a.len()).sum::<usize>(),
            2
        );
    }
}
