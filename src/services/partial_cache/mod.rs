mod decode;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use alloy::primitives::{Address, B256, U256};
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use rustc_hash::FxHashSet;
use tokio::sync::watch;

pub use decode::{LogPatch, V2_SYNC_TOPIC, V3_SWAP_TOPIC, decode_pool_log, is_streamable_protocol};

use crate::core::types::{PoolState, ProtocolType};
use crate::services::state_cache::StateCache;

/// Minimal hot-path pool snapshot (~128 bytes per V3 pool).
#[derive(Debug, Clone, Copy)]
pub struct SlimPoolState {
    pub protocol: ProtocolType,
    pub sqrt_price_x96: U256,
    pub liquidity: u128,
    pub tick: i32,
    pub reserve0: U256,
    pub reserve1: U256,
    pub patched_at_ms: u64,
}

impl SlimPoolState {
    pub fn from_v3(sqrt_price_x96: U256, liquidity: u128, tick: i32, now_ms: u64) -> Self {
        Self {
            protocol: ProtocolType::UniswapV3,
            sqrt_price_x96,
            liquidity,
            tick,
            reserve0: U256::ZERO,
            reserve1: U256::ZERO,
            patched_at_ms: now_ms,
        }
    }

    pub fn from_v2(reserve0: U256, reserve1: U256, now_ms: u64) -> Self {
        Self {
            protocol: ProtocolType::UniswapV2,
            sqrt_price_x96: U256::ZERO,
            liquidity: 0,
            tick: 0,
            reserve0,
            reserve1,
            patched_at_ms: now_ms,
        }
    }
}

/// Signals HF evaluation after a stream patch lands in the partial cache.
#[derive(Clone)]
pub struct StreamTrigger {
    tx: watch::Sender<u64>,
    rx: watch::Receiver<u64>,
    stream_tick: Arc<AtomicU64>,
}

impl StreamTrigger {
    pub fn new() -> Self {
        let (tx, rx) = watch::channel(0u64);
        Self {
            tx,
            rx,
            stream_tick: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.rx.clone()
    }

    pub fn notify(&self) {
        let n = self.stream_tick.fetch_add(1, Ordering::Relaxed) + 1;
        let _ = self.tx.send(n);
    }

    pub fn take_stream_triggered(&self) -> bool {
        self.stream_tick.swap(0, Ordering::AcqRel) > 0
    }
}

impl Default for StreamTrigger {
    fn default() -> Self {
        Self::new()
    }
}

/// Lock-free RAM cache for target pool contracts only.
pub struct PartialPoolCache {
    pools: DashMap<Address, SlimPoolState>,
    patches: AtomicU64,
    trigger: StreamTrigger,
    dirty: parking_lot::Mutex<FxHashSet<Address>>,
}

impl PartialPoolCache {
    pub fn new() -> Self {
        Self {
            pools: DashMap::new(),
            patches: AtomicU64::new(0),
            trigger: StreamTrigger::new(),
            dirty: Mutex::new(FxHashSet::default()),
        }
    }

    pub fn trigger(&self) -> &StreamTrigger {
        &self.trigger
    }

    pub fn patch_count(&self) -> u64 {
        self.patches.load(Ordering::Relaxed)
    }

    pub fn len(&self) -> usize {
        self.pools.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pools.is_empty()
    }

    pub fn get(&self, address: &Address) -> Option<SlimPoolState> {
        self.pools.get(address).map(|e| *e)
    }

    pub fn seed(&self, address: Address, state: SlimPoolState) {
        self.pools.insert(address, state);
    }

    pub fn seed_from_pool_state(&self, address: Address, state: &PoolState, now_ms: u64) {
        match state {
            PoolState::V2(v2) => {
                self.seed(
                    address,
                    SlimPoolState::from_v2(v2.reserve0, v2.reserve1, now_ms),
                );
            }
            PoolState::V3(v3) => {
                self.seed(
                    address,
                    SlimPoolState::from_v3(v3.sqrt_price_x96, v3.liquidity, v3.tick, now_ms),
                );
            }
            _ => {}
        }
    }

    pub fn seed_from_state_cache(&self, cache: &StateCache, addresses: &[Address], now_ms: u64) {
        for addr in addresses {
            if let Some(state) = cache.get(addr) {
                self.seed_from_pool_state(*addr, &state, now_ms);
            }
        }
    }

    pub fn apply_log(&self, pool: Address, topic0: B256, data: &[u8], now_ms: u64) -> bool {
        let Some(patch) = decode_pool_log(topic0, data) else {
            return false;
        };
        self.apply_patch(pool, patch, now_ms);
        true
    }

    pub fn apply_patch(&self, pool: Address, patch: LogPatch, now_ms: u64) {
        match patch {
            LogPatch::V2Reserves { reserve0, reserve1 } => {
                self.pools
                    .insert(pool, SlimPoolState::from_v2(reserve0, reserve1, now_ms));
            }
            LogPatch::V3Slot {
                sqrt_price_x96,
                liquidity,
                tick,
            } => {
                if let Some(mut entry) = self.pools.get_mut(&pool) {
                    entry.sqrt_price_x96 = sqrt_price_x96;
                    entry.liquidity = liquidity;
                    entry.tick = tick;
                    entry.patched_at_ms = now_ms;
                } else {
                    self.pools.insert(
                        pool,
                        SlimPoolState::from_v3(sqrt_price_x96, liquidity, tick, now_ms),
                    );
                }
            }
        }
        self.dirty.lock().insert(pool);
        self.patches.fetch_add(1, Ordering::Relaxed);
        self.trigger.notify();
    }

    /// Merge slim snapshots into the shared `StateCache` for pools that already have full state.
    /// Only flushes pools that have been modified since last drain — prevents unnecessary
    /// StateCache generation bumps when seeding re-applies identical values.
    pub fn flush_to_state_cache(&self, cache: &StateCache, addresses: &[Address]) -> usize {
        let dirty_addrs: Vec<Address> = {
            let mut d = self.dirty.lock();
            let addrs: Vec<Address> = addresses.iter().filter(|a| d.contains(*a)).copied().collect();
            d.clear();
            addrs
        };
        if dirty_addrs.is_empty() {
            return 0;
        }
        let mut flushed = 0usize;
        for addr in &dirty_addrs {
            let Some(slim) = self.get(addr) else {
                continue;
            };
            if cache.patch_pool(*addr, |state| apply_slim_to_pool_state(state, &slim)) {
                flushed += 1;
            }
        }
        flushed
    }

    pub fn tracked_addresses(&self) -> Vec<Address> {
        self.pools.iter().map(|e| *e.key()).collect()
    }
}

impl Default for PartialPoolCache {
    fn default() -> Self {
        Self::new()
    }
}

fn apply_slim_to_pool_state(state: &mut PoolState, slim: &SlimPoolState) {
    match (state, slim.protocol) {
        (PoolState::V2(v2), ProtocolType::UniswapV2) => {
            v2.reserve0 = slim.reserve0;
            v2.reserve1 = slim.reserve1;
        }
        (PoolState::V3(v3), ProtocolType::UniswapV3) => {
            v3.sqrt_price_x96 = slim.sqrt_price_x96;
            v3.liquidity = slim.liquidity;
            v3.tick = slim.tick;
        }
        _ => {}
    }
}

/// Shared set of pool addresses for chunked `eth_subscribe` filters.
#[derive(Clone)]
pub struct StreamAddressSet {
    inner: Arc<RwLock<Vec<Address>>>,
    addr_tx: watch::Sender<Vec<Address>>,
}

impl StreamAddressSet {
    pub fn new() -> Self {
        let (addr_tx, _) = watch::channel(Vec::new());
        Self {
            inner: Arc::new(RwLock::new(Vec::new())),
            addr_tx,
        }
    }

    pub fn read(&self) -> parking_lot::RwLockReadGuard<'_, Vec<Address>> {
        self.inner.read()
    }

    /// Replace tracked addresses; returns true when the set changed.
    pub fn replace(&self, addrs: Vec<Address>) -> bool {
        let mut guard = self.inner.write();
        if *guard == addrs {
            return false;
        }
        guard.clone_from(&addrs);
        let _ = self.addr_tx.send(addrs);
        true
    }

    pub fn watch(&self) -> watch::Receiver<Vec<Address>> {
        self.addr_tx.subscribe()
    }
}

impl Default for StreamAddressSet {
    fn default() -> Self {
        Self::new()
    }
}

/// Build the top-N streamable pool addresses ranked by cycle hot-set membership,
/// graph edge centrality, and recent WSS patch activity.
pub fn select_stream_targets(
    discovered: &[crate::services::discovery::DiscoveredPool],
    hot: &[Address],
    graph: Option<&crate::pipeline::types::RoutingGraph>,
    pool_metas: &[crate::pipeline::types::PoolMeta],
    arena: &crate::pipeline::arena::StateArena,
    partial_cache: &PartialPoolCache,
    cap: usize,
    now_ms: u64,
) -> Vec<Address> {
    use rustc_hash::{FxBuildHasher, FxHashMap, FxHashSet};

    if cap == 0 {
        return Vec::new();
    }

    let hot_set: FxHashSet<Address> = hot.iter().copied().collect();
    let addr_to_pool = arena.address_to_pool();

    let mut edge_counts: FxHashMap<Address, u32> =
        FxHashMap::with_capacity_and_hasher(pool_metas.len(), FxBuildHasher);
    for meta in pool_metas {
        if let Some(addr) = arena.pool_address(meta.pool_index) {
            edge_counts.entry(addr).or_insert(0);
        }
    }
    if let Some(graph) = graph {
        for edges in &graph.adjacency {
            for ge in edges {
                if let Some(addr) = arena.pool_address(ge.edge.pool_index) {
                    *edge_counts.entry(addr).or_default() += 1;
                }
            }
        }
    }

    let mut scored: Vec<(u64, Address)> = discovered
        .iter()
        .filter(|p| is_streamable_protocol(p.protocol))
        .map(|pool| {
            let centrality = edge_counts.get(&pool.address).copied().unwrap_or(0) as u64;
            let cycle_hot = u64::from(hot_set.contains(&pool.address)) * 10_000;
            let activity = partial_cache
                .get(&pool.address)
                .map(|s| s.patched_at_ms)
                .unwrap_or(0);
            let recency = activity.saturating_sub(now_ms.saturating_sub(300_000));
            let score = cycle_hot
                .saturating_add(centrality.saturating_mul(100))
                .saturating_add(recency / 1000);
            (score, pool.address)
        })
        .collect();

    scored.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));

    let mut out = Vec::with_capacity(cap.min(scored.len()));
    let mut seen = FxHashSet::default();
    for (_, addr) in scored {
        if out.len() >= cap {
            break;
        }
        if seen.insert(addr) {
            out.push(addr);
        }
    }

    // Ensure cycle-hot pools are always included even if discovery metadata lags.
    for addr in hot {
        if out.len() >= cap {
            break;
        }
        if seen.insert(*addr) && addr_to_pool.contains_key(addr) {
            out.push(*addr);
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::V2PoolState;

    #[test]
    fn flush_updates_v2_reserves() {
        let cache = StateCache::default();
        let partial = PartialPoolCache::new();
        let addr = Address::repeat_byte(0x11);
        cache.insert(
            addr,
            PoolState::V2(V2PoolState {
                reserve0: U256::from(100u64),
                reserve1: U256::from(200u64),
                fee: U256::from(30u64),
                fee_denominator: U256::from(10_000u64),
            }),
        );
        partial.apply_patch(addr, LogPatch::V2Reserves { reserve0: U256::from(150u64), reserve1: U256::from(250u64) }, 0);
        assert_eq!(partial.flush_to_state_cache(&cache, &[addr]), 1);
        match cache.get(&addr).unwrap() {
            PoolState::V2(s) => {
                assert_eq!(s.reserve0, U256::from(150u64));
                assert_eq!(s.reserve1, U256::from(250u64));
            }
            _ => panic!("expected v2"),
        }
    }
}
