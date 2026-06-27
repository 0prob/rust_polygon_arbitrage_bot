use std::collections::{BTreeSet, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};

use alloy::eips::BlockNumberOrTag;
use alloy::network::Ethereum;
use alloy::primitives::Address;
use alloy::providers::Provider;
use parking_lot::Mutex;
use tracing::warn;

const DEFAULT_STALE_CAPACITY: usize = 20;

#[derive(Debug)]
struct NonceState {
    local_nonce: u64,
    in_flight: HashSet<u64>,
    stale: BTreeSet<u64>,
    max_stale: usize,
}

impl NonceState {
    fn init(max_stale: usize) -> Self {
        Self {
            local_nonce: 0,
            in_flight: HashSet::new(),
            stale: BTreeSet::new(),
            max_stale,
        }
    }

    fn next_available(&self) -> u64 {
        let mut n = self.local_nonce;
        if let Some(&min_in_flight) = self.in_flight.iter().min() {
            n = n.min(min_in_flight);
        }
        while self.in_flight.contains(&n) || self.stale.contains(&n) {
            n += 1;
        }
        n
    }

    fn prune_stale(&mut self) {
        self.stale.retain(|n| *n >= self.local_nonce);
    }
}

async fn pending_nonce<P: Provider<Ethereum>>(
    provider: &P,
    address: Address,
) -> anyhow::Result<u64> {
    provider
        .get_transaction_count(address)
        .block_id(BlockNumberOrTag::Pending.into())
        .await
        .map_err(Into::into)
}

/// Builder for NonceManager configuration
pub struct NonceManagerBuilder {
    address: Address,
    max_stale: usize,
}

impl NonceManagerBuilder {
    pub fn new(address: Address) -> Self {
        Self {
            address,
            max_stale: DEFAULT_STALE_CAPACITY,
        }
    }

    pub fn with_max_stale(mut self, max_stale: usize) -> Self {
        self.max_stale = max_stale;
        self
    }

    pub fn build(self) -> NonceManager {
        NonceManager {
            address: self.address,
            initialized: AtomicBool::new(false),
            state: Mutex::new(NonceState::init(self.max_stale)),
        }
    }
}

#[derive(Debug)]
pub struct NonceManager {
    address: Address,
    initialized: AtomicBool,
    state: Mutex<NonceState>,
}

impl NonceManager {
    pub fn new(address: Address) -> Self {
        NonceManagerBuilder::new(address).build()
    }

    pub fn address(&self) -> Address {
        self.address
    }

    pub fn is_initialized(&self) -> bool {
        self.initialized.load(Ordering::Acquire)
    }

    pub async fn initialize<P: Provider<Ethereum>>(&self, provider: &P) -> anyhow::Result<()> {
        let nonce = pending_nonce(provider, self.address).await?;
        let mut state = self.state.lock();
        state.local_nonce = nonce;
        state.in_flight.clear();
        state.stale.clear();
        self.initialized.store(true, Ordering::Release);
        Ok(())
    }

    pub fn next_nonce(&self) -> anyhow::Result<u64> {
        if !self.is_initialized() {
            return Err(anyhow::anyhow!("nonce manager not initialized"));
        }
        let mut state = self.state.lock();
        let nonce = state.next_available();
        state.in_flight.insert(nonce);
        Ok(nonce)
    }

    pub fn confirm(&self, confirmed: u64) {
        let mut state = self.state.lock();
        state.in_flight.remove(&confirmed);
        state.local_nonce = state.local_nonce.max(confirmed + 1);
        state.prune_stale();
    }

    pub fn release(&self, nonce: u64) {
        let mut state = self.state.lock();
        state.in_flight.remove(&nonce);
    }

    pub fn mark_stale(&self, nonce: u64) {
        let mut state = self.state.lock();
        state.in_flight.remove(&nonce);
        state.stale.insert(nonce);
        if state.stale.len() > state.max_stale {
            warn!(
                stale_count = state.stale.len(),
                max_stale = state.max_stale,
                "stale nonce set at capacity — run resync before submitting"
            );
        }
    }

    pub fn stale_count(&self) -> usize {
        let state = self.state.lock();
        state.stale.len()
    }

    pub fn in_flight_count(&self) -> usize {
        self.state.lock().in_flight.len()
    }

    pub async fn resync<P: Provider<Ethereum>>(&self, provider: &P) -> anyhow::Result<()> {
        let chain_nonce = pending_nonce(provider, self.address).await?;
        let mut state = self.state.lock();
        state.local_nonce = chain_nonce;
        state.in_flight.clear();
        state.stale.clear();
        self.initialized.store(true, Ordering::Release);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserves_and_confirms_sequential_nonces() {
        let mgr = NonceManager::new(Address::repeat_byte(0xab));
        mgr.initialized.store(true, Ordering::Release);
        {
            let mut s = mgr.state.lock();
            s.local_nonce = 5;
        }
        assert_eq!(mgr.next_nonce().unwrap(), 5);
        mgr.confirm(5);
        assert_eq!(mgr.next_nonce().unwrap(), 6);
    }

    #[test]
    fn releases_reserved_nonce() {
        let mgr = NonceManager::new(Address::repeat_byte(0xab));
        mgr.initialized.store(true, Ordering::Release);
        {
            let mut s = mgr.state.lock();
            s.local_nonce = 10;
        }
        let n = mgr.next_nonce().unwrap();
        mgr.release(n);
        assert_eq!(mgr.next_nonce().unwrap(), 10);
    }

    #[test]
    fn tracks_stale_nonces() {
        let mgr = NonceManager::new(Address::repeat_byte(0xab));
        mgr.initialized.store(true, Ordering::Release);
        {
            let mut s = mgr.state.lock();
            s.local_nonce = 3;
        }
        let n = mgr.next_nonce().unwrap();
        mgr.mark_stale(n);
        assert_eq!(mgr.stale_count(), 1);
        assert_eq!(mgr.next_nonce().unwrap(), 4);
    }

    #[test]
    fn allows_multiple_in_flight_nonces() {
        let mgr = NonceManager::new(Address::repeat_byte(0xab));
        mgr.initialized.store(true, Ordering::Release);
        {
            let mut s = mgr.state.lock();
            s.local_nonce = 10;
        }
        let n1 = mgr.next_nonce().unwrap();
        assert_eq!(n1, 10);
        let n2 = mgr.next_nonce().unwrap();
        assert_eq!(n2, 11);
    }

    #[test]
    fn returns_error_when_not_initialized() {
        let mgr = NonceManager::new(Address::repeat_byte(0xab));
        assert!(mgr.next_nonce().is_err());
    }

    #[test]
    fn concurrent_nonces_never_duplicate() {
        use std::sync::Arc;
        let mgr = Arc::new(NonceManager::new(Address::repeat_byte(0xab)));
        mgr.initialized.store(true, Ordering::Release);
        {
            let mut s = mgr.state.lock();
            s.local_nonce = 0;
        }

        let mut handles = Vec::new();
        for _ in 0..10 {
            let mgr = Arc::clone(&mgr);
            handles.push(std::thread::spawn(move || mgr.next_nonce()));
        }
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let ok: Vec<u64> = results.into_iter().filter_map(|r| r.ok()).collect();
        let unique: std::collections::HashSet<u64> = ok.iter().copied().collect();
        assert_eq!(ok.len(), unique.len(), "concurrent nonces must not duplicate");
    }
}
