use rustc_hash::FxHashMap;
use std::sync::Arc;
use std::time::Instant;

use alloy::primitives::U256;
use arc_swap::ArcSwap;

use crate::core::types::{FoundCycle, TokenIndex};
use crate::pipeline::arena::StateArena;
use crate::pipeline::types::PoolMeta;
use crate::services::discovery::DiscoveredPool;

#[derive(Debug, Clone)]
pub struct HfSnapshot {
    pub generation: u64,
    pub state_block: u64,
    pub cycles: Vec<Arc<FoundCycle>>,
    pub token_to_matic_rates: Arc<FxHashMap<TokenIndex, U256>>,
    pub token_decimals: Arc<FxHashMap<alloy::primitives::Address, u8>>,
    pub pool_metas: Arc<Vec<PoolMeta>>,
    pub arena: StateArena,
    pub discovered_pools: Arc<Vec<DiscoveredPool>>,
    /// Monotonic clock snapshot of when oracle rates were last built for this snapshot.
    pub rates_built_at: Option<Instant>,
}

impl Default for HfSnapshot {
    fn default() -> Self {
        Self {
            generation: 0,
            state_block: 0,
            cycles: Vec::new(),
            token_to_matic_rates: Arc::new(FxHashMap::default()),
            token_decimals: Arc::new(FxHashMap::default()),
            pool_metas: Arc::new(Vec::new()),
            arena: StateArena::default(),
            discovered_pools: Arc::new(Vec::new()),
            rates_built_at: None,
        }
    }
}

/// Lock-free LF → HF snapshot handoff.
///
/// Readers call [`SnapshotStore::read`] once per work chunk and keep the returned
/// `Arc` for the whole HF tick (arc-swap "consistent snapshots" pattern).
pub struct SnapshotStore {
    inner: ArcSwap<HfSnapshot>,
}

impl SnapshotStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: ArcSwap::from_pointee(HfSnapshot::default()),
        }
    }

    /// Owned snapshot for an entire HF evaluation tick.
    ///
    /// Uses [`ArcSwap::load_full`] because HF holds the snapshot across many
    /// phases; that avoids consuming per-thread borrow slots and is faster
    /// than `load().clone()` per arc-swap performance guidance.
    pub fn read(&self) -> Arc<HfSnapshot> {
        self.inner.load_full()
    }

    pub fn generation(&self) -> u64 {
        self.inner.load().generation
    }

    pub fn publish(&self, mut snapshot: HfSnapshot) {
        snapshot.generation = self.inner.load().generation.saturating_add(1);
        self.inner.store(Arc::new(snapshot));
    }
}

impl Default for SnapshotStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_returns_consistent_generation_after_publish() {
        let store = SnapshotStore::new();
        assert_eq!(store.generation(), 0);

        store.publish(HfSnapshot {
            state_block: 42,
            ..Default::default()
        });

        let snap = store.read();
        assert_eq!(snap.generation, 1);
        assert_eq!(snap.state_block, 42);
        assert_eq!(store.generation(), 1);
    }

    #[test]
    fn read_keeps_same_version_within_a_tick() {
        let store = SnapshotStore::new();
        store.publish(HfSnapshot::default());

        let first = store.read();
        let second = store.read();
        assert!(Arc::ptr_eq(&first, &second));
    }
}
