use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use alloy::primitives::Address;
use parking_lot::{Mutex as ParkingMutex, RwLock};
use rustc_hash::{FxHashMap, FxHashSet};

use crate::core::types::{PoolIndex, PoolState};

const DEFAULT_MAX_ENTRIES: usize = 50_000;
const DEFAULT_INVALID_RETRY_TTL: Duration = Duration::from_secs(30);
const DEFAULT_STALE_TRADABLE_TTL: Duration = Duration::from_secs(300);
const EVICT_INTERVAL: u64 = 64;
const EVICT_INTERVAL_MASK: u64 = EVICT_INTERVAL.wrapping_sub(1);

#[derive(Debug, Clone)]
struct CachedEntry {
    pub state: Arc<PoolState>,
    pub updated_at: Instant,
}

#[derive(Debug)]
pub struct StateCache {
    inner: RwLock<FxHashMap<Address, CachedEntry>>,
    max_entries: usize,
    ttl: Duration,
    invalid_retry_ttl: Duration,
    stale_tradable_ttl: Duration,
    eviction_counter: AtomicU64,
    generation: AtomicU64,
    /// Pools touched since last graph partial-rescore drain.
    dirty: ParkingMutex<FxHashSet<Address>>,
}

impl Default for StateCache {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_ENTRIES, Duration::from_secs(600))
    }
}

impl StateCache {
    #[must_use]
    pub fn new(max_entries: usize, ttl: Duration) -> Self {
        Self {
            inner: RwLock::new(FxHashMap::default()),
            max_entries,
            ttl,
            invalid_retry_ttl: DEFAULT_INVALID_RETRY_TTL,
            stale_tradable_ttl: DEFAULT_STALE_TRADABLE_TTL,
            eviction_counter: AtomicU64::new(0),
            generation: AtomicU64::new(0),
            dirty: ParkingMutex::new(FxHashSet::default()),
        }
    }

    fn mark_dirty(&self, address: Address) {
        self.dirty.lock().insert(address);
    }

    /// Resolve dirty pool addresses to arena indices and clear the dirty set.
    pub fn take_dirty_pool_indices(
        &self,
        address_to_pool: &FxHashMap<Address, PoolIndex>,
    ) -> Vec<PoolIndex> {
        self.dirty
            .lock()
            .drain()
            .filter_map(|addr| address_to_pool.get(&addr).copied())
            .collect()
    }

    #[must_use]
    pub fn with_ttls(mut self, invalid_retry: Duration, stale_tradable: Duration) -> Self {
        self.invalid_retry_ttl = invalid_retry;
        self.stale_tradable_ttl = stale_tradable;
        self
    }

    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    /// Count addresses whose cached state is tradable (single read-lock pass).
    pub fn count_tradable(&self, addresses: &[Address]) -> usize {
        self.count_tradable_iter(addresses.iter())
    }

    pub fn count_tradable_iter<'a>(
        &self,
        addresses: impl IntoIterator<Item = &'a Address>,
    ) -> usize {
        let guard = self.inner.read();
        addresses
            .into_iter()
            .filter(|addr| {
                guard.get(*addr).is_some_and(|entry| {
                    entry.updated_at.elapsed() <= self.ttl && entry.state.is_tradable()
                })
            })
            .count()
    }

    /// Monotonic version for execution-time stale-state rejection.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn lookup_pool_state(&self, address: &Address) -> Option<Arc<PoolState>> {
        let guard = self.inner.read();
        let entry = guard.get(address)?;
        (entry.updated_at.elapsed() <= self.ttl).then(|| Arc::clone(&entry.state))
    }

    pub fn get(&self, address: &Address) -> Option<PoolState> {
        self.lookup_pool_state(address)
            .map(|state| (*state).clone())
    }

    pub fn get_arc(&self, address: &Address) -> Option<Arc<PoolState>> {
        self.lookup_pool_state(address)
    }

    /// Tradable, unexpired pool states keyed by address. One read-lock pass over
    /// the cache (~tens of thousands) instead of per-pool lookups over discovery.
    pub fn tradable_snapshot(&self) -> Vec<(Address, Arc<PoolState>)> {
        let guard = self.inner.read();
        guard
            .iter()
            .filter_map(|(address, entry)| {
                (entry.updated_at.elapsed() <= self.ttl && entry.state.is_tradable())
                    .then(|| (*address, Arc::clone(&entry.state)))
            })
            .collect()
    }

    /// Read a coherent set of unexpired pool states and the generation that
    /// produced them. Holding one read lock prevents WSS/RPC writers from
    /// mixing cache generations inside a single HF evaluation arena.
    pub fn get_arcs_with_generation(
        &self,
        addresses: &[Address],
    ) -> (Vec<(Address, Arc<PoolState>)>, u64) {
        let guard = self.inner.read();
        // Load generation while the read lock is still held so no concurrent
        // writer can advance it between reading states and loading generation.
        let generation = self.generation.load(Ordering::Acquire);
        let states = addresses
            .iter()
            .filter_map(|address| {
                let entry = guard.get(address)?;
                (entry.updated_at.elapsed() <= self.ttl)
                    .then(|| (*address, Arc::clone(&entry.state)))
            })
            .collect();
        (states, generation)
    }

    pub fn contains(&self, address: &Address) -> bool {
        self.lookup_pool_state(address).is_some()
    }

    /// Apply an in-place mutation when a full pool entry already exists.
    pub fn patch_pool(&self, address: Address, mut f: impl FnMut(&mut PoolState)) -> bool {
        let mut guard = self.inner.write();
        let Some(entry) = guard.get_mut(&address) else {
            return false;
        };
        // ponytail: Arc::make_mut provides COW semantics — no deep clone when
        // the Arc is uniquely held (common for hot-patched pool states from WSS).
        f(Arc::make_mut(&mut entry.state));
        entry.updated_at = Instant::now();
        self.generation.fetch_add(1, Ordering::Release);
        self.mark_dirty(address);
        true
    }

    pub fn insert(&self, address: Address, state: PoolState) {
        let mut guard = self.inner.write();
        if guard.len() >= self.max_entries && !guard.contains_key(&address) {
            let count = self.eviction_counter.fetch_add(1, Ordering::Relaxed);
            if count & EVICT_INTERVAL_MASK == 0 {
                guard.retain(|_, v| v.updated_at.elapsed() <= self.ttl);
            }
            if guard.len() >= self.max_entries
                && let Some(key) = guard.keys().next().copied()
            {
                guard.remove(&key);
            }
        }
        guard.insert(
            address,
            CachedEntry {
                state: Arc::new(state),
                updated_at: Instant::now(),
            },
        );
        self.generation.fetch_add(1, Ordering::Release);
        self.mark_dirty(address);
    }

    pub fn remove(&self, address: &Address) -> bool {
        let removed = self.inner.write().remove(address).is_some();
        if removed {
            self.generation.fetch_add(1, Ordering::Release);
            self.mark_dirty(*address);
        }
        removed
    }

    pub fn addresses(&self) -> Vec<Address> {
        self.inner.read().keys().copied().collect()
    }

    /// Single read-lock pass over pools that need fetch (1=never, 2=invalid, 3=stale).
    pub fn for_each_fetch_candidate<'a>(
        &self,
        pools: impl IntoIterator<Item = &'a crate::services::discovery::DiscoveredPool>,
        mut f: impl FnMut(&'a crate::services::discovery::DiscoveredPool, u8),
    ) {
        let guard = self.inner.read();
        for pool in pools {
            let class = match guard.get(&pool.address) {
                None => 1,
                Some(entry) if entry.state.is_tradable() => {
                    if entry.updated_at.elapsed() > self.stale_tradable_ttl {
                        3
                    } else {
                        continue;
                    }
                }
                Some(entry) => {
                    if entry.updated_at.elapsed() > self.invalid_retry_ttl {
                        2
                    } else {
                        continue;
                    }
                }
            };
            f(pool, class);
        }
    }

    /// Classify a slice of addresses for fetch priority.
    /// Reads under a read lock, defers eviction to a write pass.
    /// Returns: (never_fetched, invalid_retry, stale_or_expired_tradable).
    ///
    /// Expired entries remain as non-tradable history until capacity eviction.
    /// Collapsing them into `never_fetched` makes newest-first discovery select
    /// the same rolling window after every TTL instead of advancing coverage.
    ///
    /// Entries past the global TTL are classified as stale (they need re-fetch)
    /// regardless of tradability, since `get`/`lookup_pool_state` will reject them.
    pub fn classify_for_fetch<'a>(
        &self,
        addresses: &'a [Address],
    ) -> (Vec<&'a Address>, Vec<&'a Address>, Vec<&'a Address>) {
        let mut never = Vec::new();
        let mut invalid = Vec::new();
        let mut stale = Vec::new();
        let guard = self.inner.read();
        for addr in addresses {
            let Some(entry) = guard.get(addr) else {
                never.push(addr);
                continue;
            };
            // Past the global TTL — treat as stale regardless of tradability.
            if entry.updated_at.elapsed() > self.ttl {
                stale.push(addr);
                continue;
            }
            if entry.state.is_tradable() {
                if entry.updated_at.elapsed() > self.stale_tradable_ttl {
                    stale.push(addr);
                }
            } else if entry.updated_at.elapsed() > self.invalid_retry_ttl {
                invalid.push(addr);
            }
        }
        (never, invalid, stale)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_insert_and_get() {
        let cache = StateCache::default();
        let addr = Address::ZERO;
        cache.insert(addr, PoolState::Invalid);
        let got = cache.get(&addr);
        assert!(got.is_some());
    }

    #[test]
    fn expired_entries_are_not_served_or_reclassified_as_never_seen() {
        let cache = StateCache::new(10, Duration::ZERO).with_ttls(Duration::ZERO, Duration::ZERO);
        let expired = Address::with_last_byte(1);
        let unseen = Address::with_last_byte(2);
        cache.insert(expired, PoolState::Invalid);

        assert!(cache.get(&expired).is_none());
        let addresses = [expired, unseen];
        let (never, invalid, stale) = cache.classify_for_fetch(&addresses);
        assert_eq!(never, vec![&unseen]);
        // With zero TTL the entry is immediately expired — classified as stale,
        // not invalid, because the global TTL check fires before the invalid check.
        assert!(invalid.is_empty());
        assert_eq!(stale, vec![&expired]);
    }

    #[test]
    fn count_tradable_ignores_missing_and_invalid_entries() {
        let cache = StateCache::default();
        let tradable_addr = Address::with_last_byte(1);
        let invalid_addr = Address::with_last_byte(2);
        let missing_addr = Address::with_last_byte(3);
        cache.insert(
            tradable_addr,
            PoolState::V2(crate::core::types::V2PoolState {
                reserve0: crate::core::constants::MIN_HOP_TOKEN_BALANCE,
                reserve1: crate::core::constants::MIN_HOP_TOKEN_BALANCE
                    + alloy::primitives::U256::from(1u64),
                fee: alloy::primitives::U256::from(997u64),
                fee_denominator: alloy::primitives::U256::from(1_000u64),
                block_timestamp_last: 1,
            }),
        );
        cache.insert(invalid_addr, PoolState::Invalid);

        let addresses = [tradable_addr, invalid_addr, missing_addr];
        assert_eq!(cache.count_tradable(&addresses), 1);
    }

    #[test]
    fn tradable_counts_ignore_expired_entries() {
        let cache = StateCache::new(10, Duration::ZERO);
        let address = Address::with_last_byte(1);
        cache.insert(
            address,
            PoolState::V2(crate::core::types::V2PoolState {
                reserve0: alloy::primitives::U256::from(1_000u64),
                reserve1: alloy::primitives::U256::from(2_000u64),
                fee: alloy::primitives::U256::from(997u64),
                fee_denominator: alloy::primitives::U256::from(1_000u64),
                block_timestamp_last: 1,
            }),
        );
        let discovered_addr = address;
        let addresses = [discovered_addr];

        assert_eq!(cache.count_tradable(&[address]), 0);
        assert_eq!(cache.count_tradable(&addresses), 0);
    }

    #[test]
    fn remove_drops_retained_history_and_advances_generation() {
        let cache = StateCache::default();
        let address = Address::with_last_byte(1);
        cache.insert(address, PoolState::Invalid);
        let generation = cache.generation();

        assert!(cache.remove(&address));
        assert!(!cache.contains(&address));
        assert!(cache.generation() > generation);
        assert!(!cache.remove(&address));
    }

    #[test]
    fn take_dirty_pool_indices_drains_touched_pools() {
        let cache = StateCache::default();
        let addr = Address::with_last_byte(9);
        let mut map = FxHashMap::default();
        map.insert(addr, PoolIndex(3));
        cache.insert(addr, PoolState::Invalid);
        let dirty = cache.take_dirty_pool_indices(&map);
        assert_eq!(dirty, vec![PoolIndex(3)]);
        assert!(cache.take_dirty_pool_indices(&map).is_empty());
    }

    #[test]
    fn coherent_read_returns_generation_for_copied_states() {
        let cache = StateCache::default();
        let address = Address::with_last_byte(1);
        cache.insert(address, PoolState::Invalid);

        let (states, generation) = cache.get_arcs_with_generation(&[address]);
        assert_eq!(states.len(), 1);
        assert_eq!(generation, cache.generation());
    }
}
