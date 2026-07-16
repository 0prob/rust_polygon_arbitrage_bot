use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use alloy::primitives::Address;
use parking_lot::{Mutex as ParkingMutex, RwLock};
use rustc_hash::{FxHashMap, FxHashSet};

use std::collections::BTreeMap;

use crate::core::types::{PoolIndex, PoolState};
use crate::services::discovery::DiscoveredPool;

const DEFAULT_MAX_ENTRIES: usize = 50_000;
const DEFAULT_INVALID_RETRY_TTL: Duration = Duration::from_secs(30);
const DEFAULT_STALE_TRADABLE_TTL: Duration = Duration::from_secs(300);

#[derive(Debug, Clone)]
struct CachedEntry {
    pub state: Arc<PoolState>,
    pub updated_at: Instant,
    pub revision: u64,
}

#[derive(Debug)]
pub struct StateCache {
    inner: RwLock<FxHashMap<Address, CachedEntry>>,
    max_entries: usize,
    ttl: Duration,
    invalid_retry_ttl: Duration,
    stale_tradable_ttl: Duration,
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

    /// Tradable pools in the discovery index — one read lock over ~50k cache entries
    /// instead of ~263k discovered rows.
    pub fn count_tradable_in_discovery(&self, address_index: &FxHashMap<Address, usize>) -> usize {
        let guard = self.inner.read();
        guard
            .iter()
            .filter(|(addr, entry)| {
                address_index.contains_key(*addr)
                    && entry.updated_at.elapsed() <= self.ttl
                    && entry.state.is_tradable()
            })
            .count()
    }

    /// Per-protocol cached vs tradable counts for pipeline survival (single read lock).
    pub fn count_discovery_stages_by_protocol(
        &self,
        pools: &[DiscoveredPool],
    ) -> (BTreeMap<String, usize>, BTreeMap<String, usize>) {
        let mut cached = BTreeMap::new();
        let mut tradable = BTreeMap::new();
        let guard = self.inner.read();
        for pool in pools {
            let Some(entry) = guard.get(&pool.address) else {
                continue;
            };
            if entry.updated_at.elapsed() > self.ttl {
                continue;
            }
            let label = pool.protocol_label.clone();
            *cached.entry(label.clone()).or_default() += 1;
            if entry.state.is_tradable() {
                *tradable.entry(label).or_default() += 1;
            }
        }
        (cached, tradable)
    }

    fn pool_past_invalid_retry_entry(
        &self,
        address: Address,
        entry: &CachedEntry,
    ) -> Option<Address> {
        if entry.state.is_tradable() {
            return None;
        }
        let elapsed = entry.updated_at.elapsed();
        if elapsed > self.ttl {
            return None;
        }
        (elapsed > self.invalid_retry_ttl).then_some(address)
    }

    /// Pools eligible for dead-pool pruning (invalid past retry TTL, one read lock).
    pub fn pools_past_invalid_retry(&self, pools: &[DiscoveredPool]) -> Vec<Address> {
        let guard = self.inner.read();
        pools
            .iter()
            .filter_map(|pool| {
                let entry = guard.get(&pool.address)?;
                self.pool_past_invalid_retry_entry(pool.address, entry)
            })
            .collect()
    }

    /// Scan cache only — O(cache) instead of O(discovery) for LF dead-pool prune.
    pub fn pools_past_invalid_retry_indexed(
        &self,
        address_index: &FxHashMap<Address, usize>,
    ) -> Vec<Address> {
        if address_index.is_empty() {
            return Vec::new();
        }
        let guard = self.inner.read();
        guard
            .iter()
            .filter_map(|(address, entry)| {
                if !address_index.contains_key(address) {
                    return None;
                }
                self.pool_past_invalid_retry_entry(*address, entry)
            })
            .collect()
    }

    /// Monotonic version for execution-time stale-state rejection.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether any cached row exists (including expired), for fetch target scans.
    pub fn has_any_entry(&self, address: &Address) -> bool {
        self.inner.read().contains_key(address)
    }

    fn lookup_pool_state(&self, address: &Address) -> Option<Arc<PoolState>> {
        let guard = self.inner.read();
        let entry = guard.get(address)?;
        (entry.updated_at.elapsed() <= self.ttl).then(|| Arc::clone(&entry.state))
    }

    /// Deep-clones pool state — prefer [`Self::get_arc`] on hot paths (HF/WSS).
    #[cold]
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
            .filter(|(_, entry)| {
                entry.updated_at.elapsed() <= self.ttl && entry.state.is_tradable()
            })
            .map(|(address, entry)| (*address, Arc::clone(&entry.state)))
            .collect()
    }

    /// Tradable pools sorted by discovery index in one read-lock pass.
    pub fn tradable_by_discovery_index(
        &self,
        address_index: &FxHashMap<Address, usize>,
    ) -> Vec<(usize, Address, Arc<PoolState>)> {
        let guard = self.inner.read();
        let mut out: Vec<(usize, Address, Arc<PoolState>)> = guard
            .iter()
            .filter(|(_, entry)| {
                entry.updated_at.elapsed() <= self.ttl && entry.state.is_tradable()
            })
            .filter_map(|(address, entry)| {
                address_index
                    .get(address)
                    .map(|&idx| (idx, *address, Arc::clone(&entry.state)))
            })
            .collect();
        out.sort_unstable_by_key(|(idx, _, _)| *idx);
        out
    }

    /// Read a coherent set of unexpired pool states and the generation that
    /// produced them. Holding one read lock prevents WSS/RPC writers from
    /// mixing cache generations inside a single HF evaluation arena.
    pub fn get_arcs_with_generation(
        &self,
        addresses: &[Address],
    ) -> (Vec<(Address, Arc<PoolState>, u64)>, u64) {
        let guard = self.inner.read();
        // Load generation while the read lock is still held so no concurrent
        // writer can advance it between reading states and loading generation.
        let generation = self.generation.load(Ordering::Acquire);
        let mut states = Vec::with_capacity(addresses.len());
        for address in addresses {
            let Some(entry) = guard.get(address) else {
                continue;
            };
            if entry.updated_at.elapsed() <= self.ttl {
                states.push((*address, Arc::clone(&entry.state), entry.revision));
            }
        }
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
        entry.revision = self
            .generation
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        self.mark_dirty(address);
        true
    }

    fn eviction_tier_candidate(
        best: Option<(Address, Instant)>,
        addr: Address,
        updated_at: Instant,
    ) -> Option<(Address, Instant)> {
        match best {
            None => Some((addr, updated_at)),
            Some((_, best_at)) if updated_at < best_at => Some((addr, updated_at)),
            Some((best_addr, best_at)) if updated_at == best_at && addr < best_addr => {
                Some((addr, updated_at))
            }
            Some(current) => Some(current),
        }
    }

    fn pick_eviction_victim(
        guard: &FxHashMap<Address, CachedEntry>,
        ttl: Duration,
    ) -> Option<Address> {
        let mut expired = None;
        let mut invalid = None;
        let mut untradable = None;
        let mut tradable = None;
        for (addr, entry) in guard {
            let updated_at = entry.updated_at;
            if updated_at.elapsed() > ttl {
                expired = Self::eviction_tier_candidate(expired, *addr, updated_at);
                continue;
            }
            if matches!(*entry.state, PoolState::Invalid) {
                invalid = Self::eviction_tier_candidate(invalid, *addr, updated_at);
                continue;
            }
            if !entry.state.is_tradable() {
                untradable = Self::eviction_tier_candidate(untradable, *addr, updated_at);
                continue;
            }
            tradable = Self::eviction_tier_candidate(tradable, *addr, updated_at);
        }
        expired
            .or(invalid)
            .or(untradable)
            .or(tradable)
            .map(|(addr, _)| addr)
    }

    pub fn insert(&self, address: Address, state: PoolState) {
        let mut guard = self.inner.write();
        if guard.len() >= self.max_entries
            && !guard.contains_key(&address)
            && guard.len() >= self.max_entries
            && let Some(victim) = Self::pick_eviction_victim(&guard, self.ttl)
        {
            guard.remove(&victim);
        }
        let revision = self
            .generation
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        guard.insert(
            address,
            CachedEntry {
                state: Arc::new(state),
                updated_at: Instant::now(),
                revision,
            },
        );
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

    fn fetch_class_for_entry(&self, entry: &CachedEntry) -> Option<u8> {
        if entry.state.is_tradable() {
            (entry.updated_at.elapsed() > self.stale_tradable_ttl).then_some(3)
        } else if entry.updated_at.elapsed() > self.invalid_retry_ttl {
            Some(2)
        } else {
            None
        }
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
                Some(entry) => match self.fetch_class_for_entry(entry) {
                    Some(class) => class,
                    None => continue,
                },
            };
            f(pool, class);
        }
    }

    /// Scan cache entries only (O(cache)) for stale/invalid candidates still in discovery.
    pub fn for_each_cached_fetch_candidate<'a>(
        &self,
        address_index: &FxHashMap<Address, usize>,
        pools: &'a [DiscoveredPool],
        mut f: impl FnMut(&'a DiscoveredPool, u8),
    ) {
        let guard = self.inner.read();
        for (address, entry) in guard.iter() {
            let Some(class) = self.fetch_class_for_entry(entry) else {
                continue;
            };
            let Some(&idx) = address_index.get(address) else {
                continue;
            };
            let Some(pool) = pools.get(idx) else {
                continue;
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
    fn eviction_prefers_invalid_over_tradable() {
        let cache = StateCache::new(2, Duration::from_secs(600));
        let tradable = Address::with_last_byte(10);
        let invalid = Address::with_last_byte(11);
        let newcomer = Address::with_last_byte(12);
        cache.insert(
            tradable,
            PoolState::V2(crate::core::types::V2PoolState {
                reserve0: crate::core::constants::MIN_HOP_TOKEN_BALANCE,
                reserve1: crate::core::constants::MIN_HOP_TOKEN_BALANCE
                    + alloy::primitives::U256::from(1u64),
                fee: alloy::primitives::U256::from(997u64),
                fee_denominator: alloy::primitives::U256::from(1_000u64),
                block_timestamp_last: 1,
            }),
        );
        cache.insert(invalid, PoolState::Invalid);
        assert_eq!(cache.len(), 2);
        cache.insert(newcomer, PoolState::Invalid);
        assert_eq!(cache.len(), 2);
        assert!(cache.get(&tradable).is_some());
        assert!(cache.get(&newcomer).is_some());
        assert!(cache.get(&invalid).is_none());
    }

    #[test]
    fn count_tradable_in_discovery_uses_index_not_full_scan() {
        let cache = StateCache::default();
        let in_index = Address::with_last_byte(20);
        let off_index = Address::with_last_byte(21);
        let mut index = FxHashMap::default();
        index.insert(in_index, 0usize);
        cache.insert(
            in_index,
            PoolState::V2(crate::core::types::V2PoolState {
                reserve0: crate::core::constants::MIN_HOP_TOKEN_BALANCE,
                reserve1: crate::core::constants::MIN_HOP_TOKEN_BALANCE
                    + alloy::primitives::U256::from(1u64),
                fee: alloy::primitives::U256::from(997u64),
                fee_denominator: alloy::primitives::U256::from(1_000u64),
                block_timestamp_last: 1,
            }),
        );
        cache.insert(
            off_index,
            PoolState::V2(crate::core::types::V2PoolState {
                reserve0: crate::core::constants::MIN_HOP_TOKEN_BALANCE,
                reserve1: crate::core::constants::MIN_HOP_TOKEN_BALANCE
                    + alloy::primitives::U256::from(1u64),
                fee: alloy::primitives::U256::from(997u64),
                fee_denominator: alloy::primitives::U256::from(1_000u64),
                block_timestamp_last: 1,
            }),
        );
        assert_eq!(cache.count_tradable_in_discovery(&index), 1);
    }

    #[test]
    fn tradable_by_discovery_index_returns_sorted_indices() {
        let cache = StateCache::default();
        let a = Address::with_last_byte(1);
        let b = Address::with_last_byte(2);
        let c = Address::with_last_byte(3);
        for address in [b, a, c] {
            cache.insert(
                address,
                PoolState::V2(crate::core::types::V2PoolState {
                    reserve0: crate::core::constants::MIN_HOP_TOKEN_BALANCE,
                    reserve1: crate::core::constants::MIN_HOP_TOKEN_BALANCE
                        + alloy::primitives::U256::from(1u64),
                    fee: alloy::primitives::U256::from(997u64),
                    fee_denominator: alloy::primitives::U256::from(1_000u64),
                    block_timestamp_last: 1,
                }),
            );
        }

        let mut index = FxHashMap::default();
        index.insert(a, 2);
        index.insert(b, 0);
        index.insert(c, 1);

        let tradable = cache.tradable_by_discovery_index(&index);
        assert_eq!(tradable.len(), 3);
        assert_eq!(tradable[0].0, 0);
        assert_eq!(tradable[1].0, 1);
        assert_eq!(tradable[2].0, 2);
        assert_eq!(tradable[0].1, b);
        assert_eq!(tradable[1].1, c);
        assert_eq!(tradable[2].1, a);
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
    fn pools_past_invalid_retry_indexed_matches_discovery_scan() {
        use rustc_hash::FxHashMap;

        let cache = StateCache::default().with_ttls(Duration::from_millis(1), Duration::ZERO);
        let address = Address::with_last_byte(42);
        cache.insert(address, PoolState::Invalid);
        std::thread::sleep(Duration::from_millis(5));

        let pool = DiscoveredPool {
            address,
            pool_key: "k".to_string(),
            protocol: crate::core::types::ProtocolType::UniswapV2,
            protocol_label: "v2".to_string(),
            tokens: vec![],
            fee_bps: 30,
            tick_spacing: None,
            pool_id: None,
            pool_id_verified: false,
            hooks: None,
            pool_type: None,
            created_block: 0,
        };
        let mut index = FxHashMap::default();
        index.insert(address, 0usize);

        let via_discovery = cache.pools_past_invalid_retry(&[pool]);
        let via_index = cache.pools_past_invalid_retry_indexed(&index);
        assert_eq!(via_discovery, via_index);
        assert_eq!(via_index, vec![address]);
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
