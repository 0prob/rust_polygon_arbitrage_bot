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
    pub stats: RouteSimCacheStats,
}

impl RouteSimCache {
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: DashMap::with_capacity_and_hasher(ROUTE_SIM_CACHE_CAPACITY, FxBuildHasher),
            stats: RouteSimCacheStats::default(),
        }
    }

    pub fn clear_stale(&self, current_generation: u64) {
        self.entries
            .retain(|key, _| key.generation == current_generation);
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

    pub fn insert(
        &self,
        generation: u64,
        route_fp: u64,
        amount: U256,
        sim: MinimalSimResult,
    ) {
        if self.entries.len() >= ROUTE_SIM_CACHE_CAPACITY {
            self.clear_stale(generation);
            if self.entries.len() >= ROUTE_SIM_CACHE_CAPACITY
                && let Some(victim) = self.entries.iter().next().map(|entry| *entry.key())
            {
                self.entries.remove(&victim);
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
}