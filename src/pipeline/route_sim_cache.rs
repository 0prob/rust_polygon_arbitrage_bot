use std::sync::atomic::{AtomicU64, Ordering};

use alloy::primitives::U256;
use dashmap::DashMap;
use rustc_hash::FxBuildHasher;

use crate::pipeline::types::MinimalSimResult;

const ROUTE_SIM_CACHE_CAPACITY: usize = 4096;

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
struct RouteSimKey {
    generation: u64,
    route_fp: u64,
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

/// Cross-tick route simulation cache keyed by `(state_generation, route_fp, amount)`.
#[derive(Debug, Default)]
pub struct RouteSimCache {
    entries: DashMap<RouteSimKey, MinimalSimResult, FxBuildHasher>,
    pruned_generation: AtomicU64,
    pub stats: RouteSimCacheStats,
}

impl RouteSimCache {
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: DashMap::with_capacity_and_hasher(ROUTE_SIM_CACHE_CAPACITY, FxBuildHasher),
            pruned_generation: AtomicU64::new(u64::MAX),
            stats: RouteSimCacheStats::default(),
        }
    }

    pub fn clear_stale(&self, current_generation: u64) {
        if self.pruned_generation.load(Ordering::Acquire) == current_generation {
            return;
        }
        self.entries
            .retain(|key, _| key.generation == current_generation);
        self.pruned_generation
            .store(current_generation, Ordering::Release);
    }

    #[must_use]
    pub fn get(&self, generation: u64, route_fp: u64, amount: U256) -> Option<MinimalSimResult> {
        let key = RouteSimKey {
            generation,
            route_fp,
            amount,
        };
        if let Some(entry) = self.entries.get(&key) {
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            return Some(*entry);
        }
        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    pub fn insert(&self, generation: u64, route_fp: u64, amount: U256, sim: MinimalSimResult) {
        if self.entries.len() >= ROUTE_SIM_CACHE_CAPACITY {
            let before = self.entries.len();
            self.clear_stale(generation);
            let cleared = before.saturating_sub(self.entries.len());
            if cleared > 0 {
                self.stats
                    .evictions
                    .fetch_add(cleared as u64, Ordering::Relaxed);
            }
            if self.entries.len() >= ROUTE_SIM_CACHE_CAPACITY
                && let Some(victim) = self.entries.iter().next().map(|entry| *entry.key())
            {
                self.entries.remove(&victim);
                self.stats.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.entries.insert(
            RouteSimKey {
                generation,
                route_fp,
                amount,
            },
            sim,
        );
        self.stats.inserts.fetch_add(1, Ordering::Relaxed);
    }

    /// Debug snapshot when the cache has seen traffic this tick.
    pub fn debug_log_if_active(&self, label: &str) {
        let hits = self.stats.hits.load(Ordering::Relaxed);
        let misses = self.stats.misses.load(Ordering::Relaxed);
        if hits.saturating_add(misses) == 0 {
            return;
        }
        crate::debug!(
            "route_sim_cache {label}: hit_rate_bps={} hits={hits} misses={misses} inserts={} evictions={} entries={}",
            self.stats.hit_rate_bps(),
            self.stats.inserts.load(Ordering::Relaxed),
            self.stats.evictions.load(Ordering::Relaxed),
            self.entries.len()
        );
    }

    #[cfg(test)]
    fn entry_count(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clear_stale_keeps_current_generation_entries() {
        let cache = RouteSimCache::new();
        cache.insert(
            7,
            11,
            U256::from(1u8),
            MinimalSimResult {
                profit: U256::ZERO,
                amount_out: U256::ZERO,
                total_gas: 0,
            },
        );
        cache.clear_stale(7);

        assert_eq!(cache.entry_count(), 1);
    }
}
