use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::core::types::FoundCycle;
use crate::pipeline::arena::StateArena;
use crate::pipeline::graph::build_graph;
use crate::pipeline::types::{PoolMeta, RoutingGraph};
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

    pub fn get_or_build(
        &mut self,
        cache: &StateCache,
        arena: &StateArena,
        pools: &[PoolMeta],
    ) -> Arc<RoutingGraph> {
        let fp = pool_fingerprint(cache, pools.len());
        let force_rebuild = self.lf_pass_count.is_multiple_of(full_rebuild_interval())
            || self.graph.is_none()
            || self.pool_fingerprint != fp;

        if force_rebuild {
            self.graph = Some(Arc::new(build_graph(arena, pools)));
            self.pool_fingerprint = fp;
            self.cycles = None;
        }
        self.lf_pass_count += 1;
        match self.graph.as_ref() {
            Some(graph) => Arc::clone(graph),
            None => {
                tracing::error!("graph cache missing graph after rebuild");
                Arc::new(build_graph(arena, pools))
            }
        }
    }

    pub fn try_get_cached(
        &self,
        cache: &StateCache,
        pools: &[PoolMeta],
    ) -> Option<Arc<Vec<FoundCycle>>> {
        let fp = pool_fingerprint(cache, pools.len());
        if fp == self.pool_fingerprint {
            self.cycles.as_ref().map(Arc::clone)
        } else {
            None
        }
    }

    pub fn store_cycles(&mut self, cycles: Arc<Vec<FoundCycle>>) {
        self.cycles = Some(cycles);
    }

    pub fn lf_pass_count(&self) -> u64 {
        self.lf_pass_count
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
    use crate::pipeline::graph::pool_meta_from_pair;
    use crate::services::state_cache::StateCache;
    use alloy::primitives::Address;
    use ruint::aliases::U256;

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
        let g1 = cache.get_or_build(&state_cache, &arena, &pools);
        let g2 = cache.get_or_build(&state_cache, &arena, &pools);
        assert_eq!(g1.token_count, g2.token_count);
        assert!(Arc::ptr_eq(&g1, &g2));
        assert_eq!(cache.lf_pass_count(), 2);
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
        let g_invalid = cache.get_or_build(&state_cache, &arena, &pools);
        assert!(g_invalid.adjacency.iter().all(|a| a.is_empty()));

        let reserve = U256::from(1_000_000u128) * U256::from(10u128).pow(U256::from(18));
        let tradable = PoolState::V2(V2PoolState {
            reserve0: reserve,
            reserve1: reserve,
            fee: U256::ZERO,
            fee_denominator: U256::ZERO,
        });
        arena.register_pool(addr, tradable.clone());
        state_cache.insert(addr, tradable);
        let g_tradable = cache.get_or_build(&state_cache, &arena, &pools);
        assert_eq!(
            g_tradable.adjacency.iter().map(|a| a.len()).sum::<usize>(),
            2
        );
    }
}
