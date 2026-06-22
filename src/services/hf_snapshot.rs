use rustc_hash::FxHashMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;
use ruint::aliases::U256;

use crate::core::types::{FoundCycle, TokenIndex};
use crate::pipeline::arena::StateArena;
use crate::pipeline::types::PoolMeta;
use crate::services::discovery::DiscoveredPool;

#[derive(Debug, Clone)]
pub struct HfSnapshot {
    pub generation: u64,
    pub cycles: Vec<FoundCycle>,
    pub token_to_matic_rates: FxHashMap<TokenIndex, U256>,
    pub token_decimals: HashMap<alloy::primitives::Address, u8>,
    pub pool_metas: Vec<PoolMeta>,
    pub arena: StateArena,
    pub discovered_pools: Vec<DiscoveredPool>,
}

impl Default for HfSnapshot {
    fn default() -> Self {
        Self {
            generation: 0,
            cycles: Vec::new(),
            token_to_matic_rates: FxHashMap::default(),
            token_decimals: HashMap::new(),
            pool_metas: Vec::new(),
            arena: StateArena::new(),
            discovered_pools: Vec::new(),
        }
    }
}

pub struct SnapshotStore {
    inner: ArcSwap<HfSnapshot>,
    generation: AtomicU64,
}

impl Default for SnapshotStore {
    fn default() -> Self {
        Self::init()
    }
}

impl SnapshotStore {
    pub fn new() -> Self {
        Self::init()
    }

    fn init() -> Self {
        Self {
            inner: ArcSwap::from_pointee(HfSnapshot::default()),
            generation: AtomicU64::new(0),
        }
    }

    pub fn read(&self) -> Arc<HfSnapshot> {
        self.inner.load_full()
    }

    pub fn publish(&self, mut snapshot: HfSnapshot) {
        let generation = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
        snapshot.generation = generation;
        self.inner.store(Arc::new(snapshot));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_and_default_produce_identical_state() {
        let via_new = SnapshotStore::new();
        let via_default = SnapshotStore::default();

        let snap_new = via_new.read();
        let snap_default = via_default.read();

        // Both should have generation 0
        assert_eq!(snap_new.generation, 0);
        assert_eq!(snap_default.generation, 0);

        // Both should have empty collections
        assert!(snap_new.cycles.is_empty());
        assert!(snap_default.cycles.is_empty());

        assert!(snap_new.token_to_matic_rates.is_empty());
        assert!(snap_default.token_to_matic_rates.is_empty());

        assert!(snap_new.token_decimals.is_empty());
        assert!(snap_default.token_decimals.is_empty());

        assert!(snap_new.pool_metas.is_empty());
        assert!(snap_default.pool_metas.is_empty());

        assert!(snap_new.discovered_pools.is_empty());
        assert!(snap_default.discovered_pools.is_empty());
    }
}
