use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};

use alloy::eips::BlockNumberOrTag;
use alloy::network::Ethereum;
use alloy::primitives::Address;
use alloy::providers::Provider;
use anyhow::bail;
use parking_lot::Mutex;
use rustc_hash::FxHashSet;

#[derive(Debug)]
struct NonceState {
    local_nonce: u64,
    in_flight: FxHashSet<u64>,
    min_in_flight: Option<u64>,
    stale: BTreeSet<u64>,
}

impl NonceState {
    fn init() -> Self {
        Self {
            local_nonce: 0,
            in_flight: FxHashSet::default(),
            min_in_flight: None,
            stale: BTreeSet::new(),
        }
    }

    fn next_available(&self) -> u64 {
        let mut n = self.local_nonce;
        if let Some(min_in_flight) = self.min_in_flight {
            n = n.min(min_in_flight);
        }
        while self.in_flight.contains(&n) || self.stale.contains(&n) {
            n += 1;
        }
        n
    }

    fn insert_in_flight(&mut self, nonce: u64) {
        if self.in_flight.insert(nonce) {
            self.min_in_flight = Some(self.min_in_flight.map_or(nonce, |min| min.min(nonce)));
        }
    }

    fn remove_in_flight(&mut self, nonce: u64) {
        if !self.in_flight.remove(&nonce) {
            return;
        }
        if self.min_in_flight == Some(nonce) {
            self.min_in_flight = self.in_flight.iter().copied().min();
        }
    }

    fn clear_in_flight(&mut self) {
        self.in_flight.clear();
        self.min_in_flight = None;
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

#[derive(Debug)]
pub struct NonceManager {
    address: Address,
    initialized: AtomicBool,
    state: Mutex<NonceState>,
}

impl NonceManager {
    #[must_use]
    pub fn new(address: Address) -> Self {
        Self {
            address,
            initialized: AtomicBool::new(false),
            state: Mutex::new(NonceState::init()),
        }
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
        state.clear_in_flight();
        state.stale.clear();
        self.initialized.store(true, Ordering::Release);
        Ok(())
    }

    pub fn next_nonce(&self) -> anyhow::Result<u64> {
        if !self.is_initialized() {
            bail!("nonce manager not initialized");
        }
        let mut state = self.state.lock();
        let nonce = state.next_available();
        state.insert_in_flight(nonce);
        Ok(nonce)
    }

    pub fn confirm(&self, confirmed: u64) {
        let mut state = self.state.lock();
        state.remove_in_flight(confirmed);
        state.local_nonce = state.local_nonce.max(confirmed + 1);
        state.prune_stale();
    }

    pub fn release(&self, nonce: u64) {
        let mut state = self.state.lock();
        state.remove_in_flight(nonce);
    }

    pub fn mark_stale(&self, nonce: u64) {
        let mut state = self.state.lock();
        state.remove_in_flight(nonce);
        state.stale.insert(nonce);
    }

    /// Reset a nonce from stale back to in-flight for replacement attempts.
    /// Returns the nonce if it was stale (replacement-ready), None otherwise.
    pub fn replace_nonce(&self, nonce: u64) -> Option<u64> {
        let mut state = self.state.lock();
        if state.stale.remove(&nonce) || state.in_flight.contains(&nonce) {
            state.insert_in_flight(nonce);
            return Some(nonce);
        }
        None
    }

    pub fn stale_count(&self) -> usize {
        let state = self.state.lock();
        state.stale.len()
    }

    pub fn in_flight_count(&self) -> usize {
        self.state.lock().in_flight.len()
    }

    pub async fn resync<P: Provider<Ethereum>>(&self, provider: &P) -> anyhow::Result<()> {
        self.initialize(provider).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_not_initialized() {
        let mgr = NonceManager::new(Address::ZERO);
        assert!(!mgr.is_initialized());
    }

    #[test]
    fn test_min_in_flight_tracks_removals() {
        let mut state = NonceState::init();
        state.local_nonce = 7;

        state.insert_in_flight(9);
        state.insert_in_flight(7);
        state.insert_in_flight(8);
        assert_eq!(state.min_in_flight, Some(7));
        assert_eq!(state.next_available(), 10);

        state.remove_in_flight(7);
        assert_eq!(state.min_in_flight, Some(8));
        assert_eq!(state.next_available(), 7);
        state.stale.insert(7);
        state.stale.insert(8);
        assert_eq!(state.next_available(), 10);

        state.remove_in_flight(8);
        assert_eq!(state.min_in_flight, Some(9));
        state.remove_in_flight(9);
        assert_eq!(state.min_in_flight, None);
        assert_eq!(state.next_available(), 9);
    }
}
