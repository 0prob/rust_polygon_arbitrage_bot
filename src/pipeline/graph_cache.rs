use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::core::types::{FoundCycle, PoolIndex, PoolState};
use crate::pipeline::arena::StateArena;
use crate::pipeline::graph::{build_graph, rescore_graph_in_place, rescore_pools_in_place};
use crate::pipeline::types::{PoolMeta, RoutingGraph, union_pool_set_fingerprint};
use crate::services::state_cache::StateCache;

static REBUILD_INTERVAL: AtomicU64 = AtomicU64::new(60);

/// Configure how often the routing graph is force-rebuilt even when connectivity is stable.
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
    /// Which pools are tradable and present — changes when edges appear/disappear.
    connectivity_fp: u64,
    /// StateCache generation when graph weights / cycle scores were last refreshed.
    state_generation: u64,
    /// Union of pool indices across cached cycles — invalidates when route pool sets change.
    cycles_pool_fp: u64,
    lf_pass_count: u64,
}

impl GraphCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// True when adjacency structure must be rebuilt (new/removed/tradability-changed pools).
    pub fn needs_connectivity_rebuild(&self, connectivity_fp: u64) -> bool {
        self.lf_pass_count.is_multiple_of(full_rebuild_interval())
            || self.graph.is_none()
            || self.connectivity_fp != connectivity_fp
    }

    /// True when cycle enumeration must run again (topology or pool-state generation changed).
    pub fn needs_cycle_refind(&self, connectivity_fp: u64, state_generation: u64) -> bool {
        self.needs_connectivity_rebuild(connectivity_fp)
            || self.cycles.is_none()
            || self.state_generation != state_generation
    }

    pub fn store(
        &mut self,
        graph: Arc<RoutingGraph>,
        cycles: Option<Arc<Vec<FoundCycle>>>,
        connectivity_fp: u64,
        state_generation: u64,
    ) {
        self.graph = Some(graph);
        self.connectivity_fp = connectivity_fp;
        self.state_generation = state_generation;
        self.cycles_pool_fp = cycles
            .as_ref()
            .map(|c| union_pool_set_fingerprint(c.as_ref()))
            .unwrap_or(0);
        self.cycles = cycles;
        self.lf_pass_count += 1;
    }

    pub fn lf_pass_count(&self) -> u64 {
        self.lf_pass_count
    }

    pub fn graph(&self) -> Option<Arc<RoutingGraph>> {
        self.graph.as_ref().map(Arc::clone)
    }

    pub fn cycles(&self) -> Option<Arc<Vec<FoundCycle>>> {
        self.cycles.as_ref().map(Arc::clone)
    }

    pub fn state_generation(&self) -> u64 {
        self.state_generation
    }

    /// Obtain a graph, reusing cached adjacency and only rescoring weights when connectivity is stable.
    pub fn get_or_rescore_graph(
        &mut self,
        arena: &StateArena,
        pools: &[PoolMeta],
        connectivity_fp: u64,
        state_generation: u64,
        dirty_pools: &[PoolIndex],
    ) -> Arc<RoutingGraph> {
        if self.needs_connectivity_rebuild(connectivity_fp) {
            let graph = Arc::new(build_graph(arena, pools));
            self.graph = Some(Arc::clone(&graph));
            self.connectivity_fp = connectivity_fp;
            self.state_generation = state_generation;
            self.cycles = None;
            return graph;
        }

        if self.state_generation != state_generation
            && let Some(mut existing) = self.graph.take()
        {
            const PARTIAL_RESCORE_MAX: usize = 64;
            let use_partial = !dirty_pools.is_empty() && dirty_pools.len() <= PARTIAL_RESCORE_MAX;
            if let Some(graph) = Arc::get_mut(&mut existing) {
                if use_partial {
                    rescore_pools_in_place(arena, graph, dirty_pools);
                } else {
                    rescore_graph_in_place(arena, graph);
                }
            } else {
                let mut graph = (*existing).clone();
                if use_partial {
                    rescore_pools_in_place(arena, &mut graph, dirty_pools);
                } else {
                    rescore_graph_in_place(arena, &mut graph);
                }
                existing = Arc::new(graph);
            }
            self.graph = Some(Arc::clone(&existing));
            self.state_generation = state_generation;
            // Reserve-only updates change edge weights; cached enumeration/scores are stale.
            self.cycles = None;
            return existing;
        }

        self.graph.clone().unwrap_or_else(|| {
            let graph = Arc::new(build_graph(arena, pools));
            self.graph = Some(Arc::clone(&graph));
            self.connectivity_fp = connectivity_fp;
            self.state_generation = state_generation;
            graph
        })
    }
}

/// Fingerprint of which pools contribute edges — stable across reserve-only updates.
pub fn connectivity_fingerprint(arena: &StateArena, pools: &[PoolMeta]) -> u64 {
    use rustc_hash::FxHasher;

    let mut h = FxHasher::default();
    pools.len().hash(&mut h);
    for meta in pools {
        if arena
            .pool_state(meta.pool_index)
            .is_some_and(PoolState::is_tradable)
        {
            meta.pool_index.0.hash(&mut h);
            meta.protocol.hash(&mut h);
            meta.token0.0.hash(&mut h);
            meta.token1.0.hash(&mut h);
        }
    }
    h.finish()
}

/// Legacy helper — kept for benchmarks; prefer `connectivity_fingerprint` + `StateCache::generation`.
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
    use crate::pipeline::graph::pool_meta_from_pair;
    use alloy::primitives::Address;
    use ruint::aliases::U256;

    #[test]
    fn reuses_graph_when_only_reserves_change() {
        let state_cache = StateCache::default();
        let mut cache = GraphCache::new();
        let mut arena = StateArena::new();
        let t0 = arena.register_token(Address::repeat_byte(1));
        let t1 = arena.register_token(Address::repeat_byte(2));
        let reserve = U256::from(1_000_000u128) * U256::from(10u128).pow(U256::from(18));
        let addr = Address::repeat_byte(0x10);
        let p = arena.register_pool(
            addr,
            PoolState::V2(V2PoolState {
                reserve0: reserve,
                reserve1: reserve,
                fee: U256::ZERO,
                fee_denominator: U256::ZERO,
            }),
        );
        state_cache.insert(
            addr,
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

        let conn_fp = connectivity_fingerprint(&arena, &pools);
        let gen0 = state_cache.generation();
        let g = cache.get_or_rescore_graph(&arena, &pools, conn_fp, gen0, &[]);
        let weight_before = g.adjacency[t0.0 as usize][0].log_weight;
        cache.store(Arc::clone(&g), None, conn_fp, gen0);

        let new_reserve = reserve * U256::from(2u8);
        let updated = PoolState::V2(V2PoolState {
            reserve0: new_reserve,
            reserve1: reserve,
            fee: U256::ZERO,
            fee_denominator: U256::ZERO,
        });
        arena.register_pool(addr, updated.clone());
        state_cache.insert(addr, updated);

        let conn_fp2 = connectivity_fingerprint(&arena, &pools);
        assert_eq!(conn_fp, conn_fp2);
        assert!(!cache.needs_connectivity_rebuild(conn_fp2));

        let gen1 = state_cache.generation();
        let g2 = cache.get_or_rescore_graph(&arena, &pools, conn_fp2, gen1, &[p]);
        assert!(Arc::ptr_eq(&cache.graph().unwrap(), &g2));
        assert_ne!(g2.adjacency[t0.0 as usize][0].log_weight, weight_before);
    }

    #[test]
    fn rebuilds_when_pool_becomes_tradable() {
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

        let conn_invalid = connectivity_fingerprint(&arena, &pools);
        let g_invalid =
            cache.get_or_rescore_graph(&arena, &pools, conn_invalid, state_cache.generation(), &[]);
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

        let conn_tradable = connectivity_fingerprint(&arena, &pools);
        assert_ne!(conn_invalid, conn_tradable);
        assert!(cache.needs_connectivity_rebuild(conn_tradable));

        let g_tradable =
            cache.get_or_rescore_graph(&arena, &pools, conn_tradable, state_cache.generation(), &[]);
        assert_eq!(
            g_tradable.adjacency.iter().map(|a| a.len()).sum::<usize>(),
            2
        );
    }

    #[test]
    fn partial_rescore_updates_single_pool() {
        let state_cache = StateCache::default();
        let mut cache = GraphCache::new();
        let mut arena = StateArena::new();
        let t0 = arena.register_token(Address::repeat_byte(1));
        let t1 = arena.register_token(Address::repeat_byte(2));
        let t2 = arena.register_token(Address::repeat_byte(3));
        let reserve = U256::from(1_000_000u128) * U256::from(10u128).pow(U256::from(18));
        let addr_a = Address::repeat_byte(0x10);
        let addr_b = Address::repeat_byte(0x11);
        let p_a = arena.register_pool(
            addr_a,
            PoolState::V2(V2PoolState {
                reserve0: reserve,
                reserve1: reserve,
                fee: U256::ZERO,
                fee_denominator: U256::ZERO,
            }),
        );
        let p_b = arena.register_pool(
            addr_b,
            PoolState::V2(V2PoolState {
                reserve0: reserve,
                reserve1: reserve * U256::from(2u8),
                fee: U256::ZERO,
                fee_denominator: U256::ZERO,
            }),
        );
        state_cache.insert(
            addr_a,
            PoolState::V2(V2PoolState {
                reserve0: reserve,
                reserve1: reserve,
                fee: U256::ZERO,
                fee_denominator: U256::ZERO,
            }),
        );
        state_cache.insert(
            addr_b,
            PoolState::V2(V2PoolState {
                reserve0: reserve,
                reserve1: reserve * U256::from(2u8),
                fee: U256::ZERO,
                fee_denominator: U256::ZERO,
            }),
        );
        let _ = state_cache.take_dirty_pool_indices(arena.address_to_pool());
        let pools = vec![
            pool_meta_from_pair(p_a, ProtocolType::UniswapV2, t0, t1, Some(30)),
            pool_meta_from_pair(p_b, ProtocolType::UniswapV2, t1, t2, Some(30)),
        ];
        let conn_fp = connectivity_fingerprint(&arena, &pools);
        let gen0 = state_cache.generation();
        let g = cache.get_or_rescore_graph(&arena, &pools, conn_fp, gen0, &[]);
        let weight_b_before = g.adjacency[t1.0 as usize]
            .iter()
            .find(|ge| ge.edge.pool_index == p_b)
            .map(|ge| ge.log_weight)
            .unwrap();

        arena.register_pool(
            addr_b,
            PoolState::V2(V2PoolState {
                reserve0: reserve * U256::from(4u8),
                reserve1: reserve,
                fee: U256::ZERO,
                fee_denominator: U256::ZERO,
            }),
        );
        state_cache.insert(
            addr_b,
            PoolState::V2(V2PoolState {
                reserve0: reserve * U256::from(4u8),
                reserve1: reserve,
                fee: U256::ZERO,
                fee_denominator: U256::ZERO,
            }),
        );
        let gen1 = state_cache.generation();
        let dirty = state_cache.take_dirty_pool_indices(arena.address_to_pool());
        assert_eq!(dirty, vec![p_b]);
        let g2 = cache.get_or_rescore_graph(&arena, &pools, conn_fp, gen1, &dirty);
        let weight_b_after = g2.adjacency[t1.0 as usize]
            .iter()
            .find(|ge| ge.edge.pool_index == p_b)
            .map(|ge| ge.log_weight)
            .unwrap();
        assert_ne!(weight_b_before, weight_b_after);
    }

    #[test]
    fn refinds_cycles_when_state_generation_changes() {
        use crate::core::types::FoundCycle;

        let state_cache = StateCache::default();
        let mut cache = GraphCache::new();
        let mut arena = StateArena::new();
        let t0 = arena.register_token(Address::repeat_byte(1));
        let t1 = arena.register_token(Address::repeat_byte(2));
        let reserve = U256::from(1_000_000u128) * U256::from(10u128).pow(U256::from(18));
        let addr = Address::repeat_byte(0x10);
        let p = arena.register_pool(
            addr,
            PoolState::V2(V2PoolState {
                reserve0: reserve,
                reserve1: reserve,
                fee: U256::ZERO,
                fee_denominator: U256::ZERO,
            }),
        );
        state_cache.insert(
            addr,
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
        let conn_fp = connectivity_fingerprint(&arena, &pools);
        let gen0 = state_cache.generation();
        let graph = cache.get_or_rescore_graph(&arena, &pools, conn_fp, gen0, &[]);
        let placeholder = FoundCycle {
            start_token: t0,
            edges: smallvec::smallvec![],
            hop_count: 0,
            log_weight: 0.0,
            cumulative_fee_bps: 0,
            score: 0.0,
        };
        cache.store(graph, Some(Arc::new(vec![placeholder])), conn_fp, gen0);

        state_cache.insert(
            addr,
            PoolState::V2(V2PoolState {
                reserve0: reserve * U256::from(2u8),
                reserve1: reserve,
                fee: U256::ZERO,
                fee_denominator: U256::ZERO,
            }),
        );
        let gen1 = state_cache.generation();
        assert!(cache.needs_cycle_refind(conn_fp, gen1));

        // Production path: rescore runs before needs_cycle_refind (see run_lf_cpu_work).
        let _ = cache.get_or_rescore_graph(&arena, &pools, conn_fp, gen1, &[p]);
        assert!(
            cache.needs_cycle_refind(conn_fp, gen1),
            "rescoring must invalidate cached cycles so LF re-enumerates routes"
        );
    }
}
