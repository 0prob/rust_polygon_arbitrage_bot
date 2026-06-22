use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use alloy::primitives::Address;
use parking_lot::RwLock;
use rustc_hash::FxHashMap;

use crate::core::types::PoolState;

const DEFAULT_MAX_ENTRIES: usize = 50_000;
const DEFAULT_INVALID_RETRY_TTL: Duration = Duration::from_secs(30);
const DEFAULT_STALE_TRADABLE_TTL: Duration = Duration::from_secs(120);
const EVICT_INTERVAL: u64 = 64;

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
}

impl Default for StateCache {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_ENTRIES, Duration::from_secs(600))
    }
}

impl StateCache {
    pub fn new(max_entries: usize, ttl: Duration) -> Self {
        Self {
            inner: RwLock::new(FxHashMap::default()),
            max_entries,
            ttl,
            invalid_retry_ttl: DEFAULT_INVALID_RETRY_TTL,
            stale_tradable_ttl: DEFAULT_STALE_TRADABLE_TTL,
            eviction_counter: AtomicU64::new(0),
            generation: AtomicU64::new(0),
        }
    }

    pub fn with_ttls(mut self, invalid_retry: Duration, stale_tradable: Duration) -> Self {
        self.invalid_retry_ttl = invalid_retry;
        self.stale_tradable_ttl = stale_tradable;
        self
    }

    /// Monotonic counter bumped on every insert — cheap graph/cycle cache invalidation.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }

    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn lookup_pool_state(&self, address: &Address) -> Option<Arc<PoolState>> {
        {
            let guard = self.inner.read();
            let entry = guard.get(address)?;
            if entry.updated_at.elapsed() <= self.ttl {
                return Some(Arc::clone(&entry.state));
            }
        }
        let mut guard = self.inner.write();
        if let Some(entry) = guard.get(address)
            && entry.updated_at.elapsed() > self.ttl
        {
            guard.remove(address);
        }
        None
    }

    pub fn get(&self, address: &Address) -> Option<PoolState> {
        self.lookup_pool_state(address)
            .map(|state| (*state).clone())
    }

    pub fn get_arc(&self, address: &Address) -> Option<Arc<PoolState>> {
        self.lookup_pool_state(address)
    }

    pub fn contains(&self, address: &Address) -> bool {
        self.lookup_pool_state(address).is_some()
    }

    /// Apply an in-place mutation when a full pool entry already exists.
    pub fn patch_pool(
        &self,
        address: Address,
        mut f: impl FnMut(&mut PoolState),
    ) -> bool {
        let mut guard = self.inner.write();
        let Some(entry) = guard.get_mut(&address) else {
            return false;
        };
        let mut state = (*entry.state).clone();
        f(&mut state);
        entry.state = Arc::new(state);
        entry.updated_at = Instant::now();
        self.generation.fetch_add(1, Ordering::Relaxed);
        true
    }

    pub fn insert(&self, address: Address, state: PoolState) {
        let mut guard = self.inner.write();
        if guard.len() >= self.max_entries && !guard.contains_key(&address) {
            let count = self
                .eviction_counter
                .fetch_add(1, Ordering::Relaxed);
            if count.is_multiple_of(EVICT_INTERVAL) {
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
        self.generation.fetch_add(1, Ordering::Relaxed);
    }

    pub fn addresses(&self) -> Vec<Address> {
        self.inner.read().keys().copied().collect()
    }

    /// Classify a slice of addresses for fetch priority.
    /// Reads under a read lock, defers eviction to a write pass.
    /// Returns: (never_fetched_or_expired, invalid_retry, stale_tradable).
    pub fn classify_for_fetch<'a>(
        &self,
        addresses: &'a [Address],
    ) -> (Vec<&'a Address>, Vec<&'a Address>, Vec<&'a Address>) {
        let mut never = Vec::new();
        let mut invalid = Vec::new();
        let mut stale = Vec::new();
        let mut expired: Vec<&'a Address> = Vec::new();
        {
            let guard = self.inner.read();
            for addr in addresses {
                let entry = match guard.get(addr) {
                    Some(e) if e.updated_at.elapsed() <= self.ttl => e,
                    Some(_) => {
                        never.push(addr);
                        expired.push(addr);
                        continue;
                    }
                    None => {
                        never.push(addr);
                        continue;
                    }
                };
                if entry.state.is_tradable() {
                    if entry.updated_at.elapsed() > self.stale_tradable_ttl {
                        stale.push(addr);
                    }
                } else if entry.updated_at.elapsed() > self.invalid_retry_ttl {
                    invalid.push(addr);
                }
            }
        }
        if !expired.is_empty() {
            let mut guard = self.inner.write();
            for addr in expired {
                if let Some(entry) = guard.get(addr)
                    && entry.updated_at.elapsed() > self.ttl
                {
                    guard.remove(addr);
                }
            }
        }
        (never, invalid, stale)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_expired_entry_is_evicted() {
        let cache = StateCache::new(10, Duration::from_millis(50));
        let address = Address::ZERO;

        cache.insert(address, PoolState::Invalid);
        assert!(
            cache.contains(&address),
            "Entry should exist immediately after insert"
        );

        thread::sleep(Duration::from_millis(100));

        assert!(
            !cache.contains(&address),
            "Entry should be evicted after TTL expires"
        );
    }

    #[test]
    fn generation_increments_on_insert() {
        let cache = StateCache::default();
        assert_eq!(cache.generation(), 0);
        cache.insert(Address::ZERO, PoolState::Invalid);
        assert_eq!(cache.generation(), 1);
    }
}
