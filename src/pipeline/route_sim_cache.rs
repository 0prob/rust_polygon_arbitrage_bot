use std::sync::atomic::{AtomicU64, Ordering};

use alloy::primitives::U256;
use parking_lot::Mutex;
use rustc_hash::{FxBuildHasher, FxHashMap};

use crate::core::types::{CycleEdges, Edge};
use crate::pipeline::types::MinimalSimResult;

/// Sized for zero-profit memos + multi-amount Brent ladders. Live hit_rate≈24%
/// with 8k and non-zero evictions under HF probe load; 16k cuts thrash.
const ROUTE_SIM_CACHE_CAPACITY: usize = 16_384;
const ROUTE_SIM_CACHE_SHARDS: usize = 16;
const ROUTE_SIM_CACHE_SHARD_CAPACITY: usize = ROUTE_SIM_CACHE_CAPACITY / ROUTE_SIM_CACHE_SHARDS;
const ROUTE_SIM_LOG_INTERVAL: u64 = 10_000;

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
struct RouteSimKey {
    route_state_revision: u64,
    route_edges: CycleEdges,
    amount: U256,
}

#[derive(Debug, Default)]
pub struct RouteSimCacheStats {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub inserts: AtomicU64,
    pub evictions: AtomicU64,
}

impl RouteSimCacheStats {
    #[must_use]
    pub fn hit_rate_bps(&self) -> u64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits.saturating_add(misses);
        if total == 0 {
            return 0;
        }
        hits.saturating_mul(10_000) / total
    }
}

#[derive(Debug, Default)]
pub struct RouteSimCache {
    entries: [Mutex<FxHashMap<RouteSimKey, MinimalSimResult>>; ROUTE_SIM_CACHE_SHARDS],
    pub stats: RouteSimCacheStats,
    last_logged_traffic: AtomicU64,
}

impl RouteSimCache {
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: std::array::from_fn(|_| {
                Mutex::new(FxHashMap::with_capacity_and_hasher(
                    ROUTE_SIM_CACHE_SHARD_CAPACITY,
                    FxBuildHasher,
                ))
            }),
            stats: RouteSimCacheStats::default(),
            last_logged_traffic: AtomicU64::new(0),
        }
    }

    #[inline]
    const fn shard_index(route_fp: u64) -> usize {
        route_fp as usize & (ROUTE_SIM_CACHE_SHARDS - 1)
    }

    #[must_use]
    pub fn get(
        &self,
        route_state_revision: u64,
        route_fp: u64,
        route_edges: &[Edge],
        amount: U256,
    ) -> Option<MinimalSimResult> {
        let key = RouteSimKey {
            route_state_revision,
            route_edges: CycleEdges::from_slice(route_edges),
            amount,
        };
        if let Some(entry) = self.entries[Self::shard_index(route_fp)]
            .lock()
            .get(&key)
            .copied()
        {
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            return Some(entry);
        }
        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    pub fn insert(
        &self,
        route_state_revision: u64,
        route_fp: u64,
        route_edges: &[Edge],
        amount: U256,
        sim: MinimalSimResult,
    ) {
        let mut entries = self.entries[Self::shard_index(route_fp)].lock();
        if entries.len() >= ROUTE_SIM_CACHE_SHARD_CAPACITY {
            // Drop stale revisions first. Same-revision pressure used to full-clear
            // the shard (live: thrash under multi-amount Brent on hot routes);
            // sample-drop to half capacity preserves recent same-revision hits.
            let before = entries.len();
            entries.retain(|k, _| k.route_state_revision == route_state_revision);
            if entries.len() >= ROUTE_SIM_CACHE_SHARD_CAPACITY {
                let target = ROUTE_SIM_CACHE_SHARD_CAPACITY / 2;
                let drop_n = entries.len().saturating_sub(target);
                let victims: Vec<RouteSimKey> = entries.keys().take(drop_n).cloned().collect();
                for key in victims {
                    entries.remove(&key);
                }
            }
            let removed = before.saturating_sub(entries.len());
            if removed > 0 {
                self.stats
                    .evictions
                    .fetch_add(removed as u64, Ordering::Relaxed);
            }
        }
        entries.insert(
            RouteSimKey {
                route_state_revision,
                route_edges: CycleEdges::from_slice(route_edges),
                amount,
            },
            sim,
        );
        self.stats.inserts.fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot when the cache has seen traffic this tick.
    pub fn debug_log_if_active(&self, label: &str) {
        let hits = self.stats.hits.load(Ordering::Relaxed);
        let misses = self.stats.misses.load(Ordering::Relaxed);
        let traffic = hits.saturating_add(misses);
        if traffic == 0 || !self.should_log_traffic(traffic) {
            return;
        }
        crate::info!(
            "route sim cache {label}: hit_rate_bps={} hits={hits} misses={misses} inserts={} evictions={} entries={}",
            self.stats.hit_rate_bps(),
            self.stats.inserts.load(Ordering::Relaxed),
            self.stats.evictions.load(Ordering::Relaxed),
            self.entries
                .iter()
                .map(|entries| entries.lock().len())
                .sum::<usize>()
        );
    }

    fn should_log_traffic(&self, traffic: u64) -> bool {
        let last = self.last_logged_traffic.load(Ordering::Relaxed);
        if last != 0 && traffic.saturating_sub(last) < ROUTE_SIM_LOG_INTERVAL {
            return false;
        }
        self.last_logged_traffic
            .compare_exchange(last, traffic, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    }

    #[cfg(test)]
    fn entry_count(&self) -> usize {
        self.entries
            .iter()
            .map(|entries| entries.lock().len())
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{PoolIndex, ProtocolType, TokenIndex};

    fn route_edges(pool: u32) -> CycleEdges {
        CycleEdges::from_slice(&[Edge {
            pool_index: PoolIndex(pool),
            token_in: TokenIndex(0),
            token_out: TokenIndex(1),
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: true,
        }])
    }

    #[test]
    fn route_state_revision_partitions_entries() {
        let cache = RouteSimCache::new();
        cache.insert(
            7,
            11,
            &route_edges(11),
            U256::from(1u8),
            MinimalSimResult {
                profit: U256::ZERO,
                amount_out: U256::ZERO,
                total_gas: 0,
            },
        );
        assert_eq!(cache.entry_count(), 1);
    }

    #[test]
    fn different_route_state_revisions_do_not_alias() {
        let cache = RouteSimCache::new();
        let sim = MinimalSimResult {
            profit: U256::ONE,
            amount_out: U256::ONE,
            total_gas: 1,
        };
        let edges = route_edges(10);
        cache.insert(1, 10, &edges, U256::from(100u64), sim);
        cache.insert(2, 10, &edges, U256::from(100u64), sim);
        assert!(cache.get(1, 10, &edges, U256::from(100u64)).is_some());
        assert!(cache.get(2, 10, &edges, U256::from(100u64)).is_some());
        assert_eq!(cache.entry_count(), 2);
    }

    #[test]
    fn colliding_fingerprints_do_not_alias_distinct_routes() {
        let cache = RouteSimCache::new();
        let a = route_edges(1);
        let b = route_edges(2);
        let amount = U256::from(100u64);
        let sim_a = MinimalSimResult {
            profit: U256::from(1u8),
            amount_out: U256::from(101u64),
            total_gas: 1,
        };
        let sim_b = MinimalSimResult {
            profit: U256::from(2u8),
            amount_out: U256::from(102u64),
            total_gas: 2,
        };
        cache.insert(7, 0, &a, amount, sim_a);
        cache.insert(7, 0, &b, amount, sim_b);
        assert_eq!(
            cache.get(7, 0, &a, amount).expect("route a").profit,
            sim_a.profit
        );
        assert_eq!(
            cache.get(7, 0, &b, amount).expect("route b").profit,
            sim_b.profit
        );
    }

    #[test]
    fn route_fingerprints_use_independent_shards() {
        assert_ne!(RouteSimCache::shard_index(1), RouteSimCache::shard_index(2));
    }

    #[test]
    fn cache_diagnostic_logs_initial_and_periodic_traffic() {
        let cache = RouteSimCache::new();

        assert!(cache.should_log_traffic(1));
        assert!(!cache.should_log_traffic(9_999));
        assert!(cache.should_log_traffic(10_001));
    }

    #[test]
    fn insert_at_capacity_prefers_dropping_stale_revisions() {
        let cache = RouteSimCache::new();
        let sim = MinimalSimResult {
            profit: U256::ONE,
            amount_out: U256::ONE,
            total_gas: 1,
        };
        let edges = route_edges(0);
        for i in 0..ROUTE_SIM_CACHE_CAPACITY {
            cache.insert(1, i as u64, &edges, U256::from(i as u64), sim);
        }
        assert_eq!(cache.entry_count(), ROUTE_SIM_CACHE_CAPACITY);
        cache.insert(2, 0, &edges, U256::from(0u8), sim);
        // Stale revision-1 entries dropped; new revision-2 entry present.
        assert!(cache.get(2, 0, &edges, U256::from(0u8)).is_some());
        assert!(cache.get(1, 0, &edges, U256::from(0u64)).is_none());
        assert!(cache.entry_count() < ROUTE_SIM_CACHE_CAPACITY);
    }

    #[test]
    fn same_revision_pressure_sample_drops_half_shard() {
        let cache = RouteSimCache::new();
        let sim = MinimalSimResult {
            profit: U256::ONE,
            amount_out: U256::ONE,
            total_gas: 1,
        };
        // Fill one shard with distinct amounts under the same revision.
        let fp = 0u64;
        let edges = route_edges(0);
        for i in 0..ROUTE_SIM_CACHE_SHARD_CAPACITY {
            cache.insert(7, fp, &edges, U256::from(i as u64 + 1), sim);
        }
        assert_eq!(cache.entry_count(), ROUTE_SIM_CACHE_SHARD_CAPACITY);
        // Next insert must not wipe the whole shard.
        cache.insert(7, fp, &edges, U256::from(u64::MAX), sim);
        assert!(cache.get(7, fp, &edges, U256::from(u64::MAX)).is_some());
        let n = cache.entry_count();
        assert!(n > ROUTE_SIM_CACHE_SHARD_CAPACITY / 2);
        assert!(n <= ROUTE_SIM_CACHE_SHARD_CAPACITY / 2 + 1);
    }
}
