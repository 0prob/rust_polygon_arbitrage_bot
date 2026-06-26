use std::sync::Arc;

use alloy::primitives::Address;
use rustc_hash::FxHashMap;

use crate::core::types::{PoolIndex, PoolState, ProtocolType, TokenIndex};
use crate::services::discovery::{DiscoveredPool, discovered_to_pool_meta};
use crate::services::state_cache::StateCache;

#[derive(Debug, Default, Clone)]
struct ArenaInner {
    tokens: Vec<Address>,
    pools: Vec<PoolState>,
    pool_addresses: Vec<Address>,
    address_to_pool: FxHashMap<Address, PoolIndex>,
    address_to_token: FxHashMap<Address, TokenIndex>,
}

/// Contiguous memory store for tokens and pool states.
///
/// Heavy vectors live behind `Arc` so HF ticks can clone cheaply and overlay hot
/// pool states from cache without copying the full arena.
#[derive(Debug)]
pub struct StateArena {
    inner: Arc<ArenaInner>,
    hot_patches: FxHashMap<PoolIndex, Arc<PoolState>>,
}

impl Default for StateArena {
    fn default() -> Self {
        Self {
            inner: Arc::new(ArenaInner::default()),
            hot_patches: FxHashMap::default(),
        }
    }
}

impl Clone for StateArena {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            hot_patches: FxHashMap::default(),
        }
    }
}

impl StateArena {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn pool_count(&self) -> usize {
        self.inner.pools.len()
    }

    pub fn token_count(&self) -> u32 {
        self.inner.tokens.len() as u32
    }

    pub fn address_to_pool(&self) -> &FxHashMap<Address, PoolIndex> {
        &self.inner.address_to_pool
    }

    pub fn token_address(&self, index: TokenIndex) -> Option<Address> {
        self.inner.tokens.get(index.0 as usize).copied()
    }

    pub fn pool_state(&self, index: PoolIndex) -> Option<&PoolState> {
        if let Some(state) = self.hot_patches.get(&index) {
            return Some(state.as_ref());
        }
        self.inner.pools.get(index.0 as usize)
    }

    pub fn pool_state_mut(&mut self, index: PoolIndex) -> Option<&mut PoolState> {
        self.hot_patches.remove(&index);
        Arc::make_mut(&mut self.inner)
            .pools
            .get_mut(index.0 as usize)
    }

    pub fn register_token(&mut self, address: Address) -> TokenIndex {
        let inner = Arc::make_mut(&mut self.inner);
        if let Some(&idx) = inner.address_to_token.get(&address) {
            return idx;
        }
        let idx = TokenIndex(inner.tokens.len() as u32);
        inner.tokens.push(address);
        inner.address_to_token.insert(address, idx);
        idx
    }

    pub fn pool_address(&self, index: PoolIndex) -> Option<Address> {
        self.inner.pool_addresses.get(index.0 as usize).copied()
    }

    pub fn register_pool(&mut self, address: Address, state: PoolState) -> PoolIndex {
        let inner = Arc::make_mut(&mut self.inner);
        if let Some(&idx) = inner.address_to_pool.get(&address) {
            if let Some(slot) = inner.pools.get_mut(idx.0 as usize) {
                *slot = state;
            }
            self.hot_patches.remove(&idx);
            return idx;
        }
        let idx = PoolIndex(inner.pools.len() as u32);
        inner.pools.push(state);
        inner.pool_addresses.push(address);
        inner.address_to_pool.insert(address, idx);
        idx
    }

    /// Overlay fresh pool states from cache (HF hot-path; Arc clone only).
    pub fn apply_hot_cache(&mut self, cache: &StateCache, addresses: &[Address]) {
        self.hot_patches.clear();
        for address in addresses {
            let Some(state) = cache.get_arc(address) else {
                continue;
            };
            if let Some(&idx) = self.inner.address_to_pool.get(address) {
                self.hot_patches.insert(idx, state);
            }
        }
    }

    /// Patch canonical pool slots in place (legacy; prefer `apply_hot_cache` on HF path).
    pub fn refresh_pools_from_cache(&mut self, cache: &StateCache, addresses: &[Address]) {
        self.hot_patches.clear();
        let inner = Arc::make_mut(&mut self.inner);
        for address in addresses {
            let Some(state) = cache.get_arc(address) else {
                continue;
            };
            if let Some(idx) = inner.address_to_pool.get(address)
                && let Some(slot) = inner.pools.get_mut(idx.0 as usize)
            {
                *slot = (*state).clone();
            }
        }
    }

    /// Register tradable pools only — skips unfetched or non-tradable cache entries
    /// so routing work stays proportional to graph-eligible pools.
    pub fn sync_from_discovery(
        &mut self,
        cache: &StateCache,
        pools: &[DiscoveredPool],
    ) -> Vec<crate::pipeline::types::PoolMeta> {
        let mut metas = Vec::new();
        for pool in pools {
            let Some(state) = cache.get_arc(&pool.address) else {
                continue;
            };
            if !state.is_tradable() {
                continue;
            }
            let token_indices: Vec<TokenIndex> = pool
                .tokens
                .iter()
                .map(|addr| self.register_token(*addr))
                .collect();
            let pool_index = self.register_pool(pool.address, (*state).clone());
            let mut meta = discovered_to_pool_meta(pool, pool_index, &token_indices);
            if pool.protocol == ProtocolType::BalancerV2
                && let PoolState::Balancer(b) = state.as_ref()
                && let Some(id) = b.pool_id
            {
                meta.pool_id = Some(id);
            }
            metas.push(meta);
        }
        metas
    }
}
