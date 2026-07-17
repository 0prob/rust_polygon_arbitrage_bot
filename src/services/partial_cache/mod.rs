mod decode;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use alloy::primitives::{Address, B256, U256};
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use rustc_hash::{FxBuildHasher, FxHashSet};
use tokio::sync::watch;

pub use decode::{
    ALGEBRA_SWAP_TOPIC, LogPatch, V2_SYNC_TOPIC, V3_SWAP_TOPIC, decode_pool_log,
    is_streamable_protocol,
};

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
    pub activity_count: u64,
}

impl SlimPoolState {
    #[must_use]
    pub fn from_v3(sqrt_price_x96: U256, liquidity: u128, tick: i32, now_ms: u64) -> Self {
        Self::from_cl(
            ProtocolType::UniswapV3,
            sqrt_price_x96,
            liquidity,
            tick,
            now_ms,
        )
    }

    #[must_use]
    pub fn from_v4(sqrt_price_x96: U256, liquidity: u128, tick: i32, now_ms: u64) -> Self {
        Self::from_cl(
            ProtocolType::UniswapV4,
            sqrt_price_x96,
            liquidity,
            tick,
            now_ms,
        )
    }

    #[must_use]
    fn from_cl(
        protocol: ProtocolType,
        sqrt_price_x96: U256,
        liquidity: u128,
        tick: i32,
        now_ms: u64,
    ) -> Self {
        Self {
            protocol,
            sqrt_price_x96,
            liquidity,
            tick,
            reserve0: U256::ZERO,
            reserve1: U256::ZERO,
            patched_at_ms: now_ms,
            activity_count: 0,
        }
    }

    #[must_use]
    pub fn from_v2(reserve0: U256, reserve1: U256, now_ms: u64) -> Self {
        Self {
            protocol: ProtocolType::UniswapV2,
            sqrt_price_x96: U256::ZERO,
            liquidity: 0,
            tick: 0,
            reserve0,
            reserve1,
            patched_at_ms: now_ms,
            activity_count: 0,
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
    #[must_use]
    pub fn new() -> Self {
        let (tx, rx) = watch::channel(0u64);
        Self {
            tx,
            rx,
            stream_tick: Arc::new(AtomicU64::new(0)),
        }
    }

    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.rx.clone()
    }

    pub fn notify(&self) {
        let n = self.stream_tick.fetch_add(1, Ordering::Relaxed) + 1;
        let _ = self.tx.send(n);
    }

    #[must_use]
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
    pools: DashMap<Address, SlimPoolState, FxBuildHasher>,
    patches: AtomicU64,
    trigger: StreamTrigger,
    dirty: parking_lot::Mutex<FxHashSet<Address>>,
}

impl PartialPoolCache {
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Pre-size shards for the expected stream target count (see `with_shard_amount` in dashmap).
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            pools: DashMap::with_capacity_and_hasher(capacity, FxBuildHasher),
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
        // Seeding establishes newly tracked pools only. Replacing an existing
        // stream snapshot would erase activity history and could overwrite a
        // newer WSS state with an older LF cache value.
        self.pools.entry(address).or_insert(state);
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
            PoolState::V4(v4) => {
                self.seed(
                    address,
                    SlimPoolState::from_v4(v4.sqrt_price_x96, v4.liquidity, v4.tick, now_ms),
                );
            }
            _ => {}
        }
    }

    pub fn seed_from_state_cache(&self, cache: &StateCache, addresses: &[Address], now_ms: u64) {
        for addr in addresses {
            if let Some(state) = cache.get_arc(addr) {
                self.seed_from_pool_state(*addr, state.as_ref(), now_ms);
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
                    .entry(pool)
                    .and_modify(|state| {
                        state.reserve0 = reserve0;
                        state.reserve1 = reserve1;
                        state.patched_at_ms = now_ms;
                        state.activity_count = state.activity_count.saturating_add(1);
                    })
                    .or_insert_with(|| {
                        let mut state = SlimPoolState::from_v2(reserve0, reserve1, now_ms);
                        state.activity_count = 1;
                        state
                    });
            }
            LogPatch::V3Slot {
                sqrt_price_x96,
                liquidity,
                tick,
            } => {
                self.pools
                    .entry(pool)
                    .and_modify(|state| {
                        state.sqrt_price_x96 = sqrt_price_x96;
                        state.liquidity = liquidity;
                        state.tick = tick;
                        state.patched_at_ms = now_ms;
                        state.activity_count = state.activity_count.saturating_add(1);
                    })
                    .or_insert_with(|| {
                        let mut state =
                            SlimPoolState::from_v3(sqrt_price_x96, liquidity, tick, now_ms);
                        state.activity_count = 1;
                        state
                    });
            }
        }
        self.dirty.lock().insert(pool);
        let n = self.patches.fetch_add(1, Ordering::Relaxed) + 1;
        if n == 1 || n % 50 == 0 {
            crate::info!("WSS patch applied: n={n} pool={pool}");
        }
        self.trigger.notify();
    }

    /// Drop stream snapshots for pools no longer in the active WSS target set.
    pub fn retain_tracked(&self, keep: &[Address]) {
        let mut dirty = self.dirty.lock();
        if keep.is_empty() {
            self.pools.clear();
            dirty.clear();
            return;
        }
        let mut keep_set =
            FxHashSet::with_capacity_and_hasher(keep.len(), rustc_hash::FxBuildHasher);
        keep_set.extend(keep.iter().copied());
        self.pools.retain(|addr, _| keep_set.contains(addr));
        dirty.retain(|addr| keep_set.contains(addr));
    }

    /// Merge slim snapshots into the shared `StateCache` for pools that already have full state.
    /// Only flushes pools that have been modified since last drain — prevents unnecessary
    /// StateCache generation bumps when seeding re-applies identical values.
    pub fn flush_to_state_cache(&self, cache: &StateCache, addresses: &[Address]) -> usize {
        let dirty_addrs: Vec<Address> = {
            let mut d = self.dirty.lock();
            let addrs: Vec<Address> = addresses.iter().filter(|a| d.remove(*a)).copied().collect();
            addrs
        };
        if dirty_addrs.is_empty() {
            return 0;
        }
        let mut flushed = 0usize;
        let mut retry = Vec::new();
        for addr in &dirty_addrs {
            let Some(slim) = self.get(addr) else {
                retry.push(*addr);
                continue;
            };
            if cache.patch_pool(*addr, |state| apply_slim_to_pool_state(state, &slim)) {
                flushed += 1;
            } else {
                retry.push(*addr);
            }
        }
        if !retry.is_empty() {
            self.dirty.lock().extend(retry);
        }
        flushed
    }

    pub fn tracked_addresses(&self) -> Vec<Address> {
        self.pools.iter().map(|e| *e.key()).collect()
    }

    /// Pools patched since the last flush — small vs the full WSS target set.
    #[must_use]
    pub fn dirty_addresses(&self) -> Vec<Address> {
        self.dirty.lock().iter().copied().collect()
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
        (PoolState::V3(v3), ProtocolType::UniswapV3)
            if slim.protocol == ProtocolType::UniswapV3 =>
        {
            v3.sqrt_price_x96 = slim.sqrt_price_x96;
            v3.liquidity = slim.liquidity;
            v3.tick = slim.tick;
        }
        (PoolState::V4(v4), ProtocolType::UniswapV4)
            if slim.protocol == ProtocolType::UniswapV4 =>
        {
            v4.sqrt_price_x96 = slim.sqrt_price_x96;
            v4.liquidity = slim.liquidity;
            v4.tick = slim.tick;
        }
        _ => {}
    }
}

/// Shared set of pool addresses for chunked `eth_subscribe` filters.
#[derive(Clone)]
pub struct StreamAddressSet {
    inner: Arc<RwLock<Vec<Address>>>,
    addr_tx: watch::Sender<Vec<Address>>,
    /// Wall time of last accepted replace; used to freeze membership churn.
    last_replace_ms: Arc<AtomicU64>,
}

/// Symmetric difference size for two sorted, deduped address lists.
fn sorted_symmetric_diff_len(a: &[Address], b: &[Address]) -> usize {
    let mut i = 0;
    let mut j = 0;
    let mut diff = 0;
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Equal => {
                i += 1;
                j += 1;
            }
            std::cmp::Ordering::Less => {
                diff += 1;
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                diff += 1;
                j += 1;
            }
        }
    }
    diff + a.len().saturating_sub(i) + b.len().saturating_sub(j)
}

impl StreamAddressSet {
    #[must_use]
    pub fn new() -> Self {
        let (addr_tx, _) = watch::channel(Vec::new());
        Self {
            inner: Arc::new(RwLock::new(Vec::new())),
            addr_tx,
            last_replace_ms: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn read(&self) -> parking_lot::RwLockReadGuard<'_, Vec<Address>> {
        self.inner.read()
    }

    /// Replace tracked addresses; returns true when the set changed enough to
    /// warrant a WSS resubscribe. Small top-N churn is ignored so LF score
    /// reshuffles do not tear down live Sync/Swap subscriptions every cycle.
    #[must_use]
    pub fn replace(&self, mut addrs: Vec<Address>) -> bool {
        addrs.sort_unstable();
        addrs.dedup();
        let mut guard = self.inner.write();
        if *guard == addrs {
            return false;
        }
        if !guard.is_empty() && !addrs.is_empty() {
            let now = crate::util::now_ms();
            let last = self.last_replace_ms.load(Ordering::Relaxed);
            // Keep WSS filters stable long enough for Sync/Swap to flow; LF
            // bootstrap reshuffles top-N every cycle far above sym-diff hysteresis.
            if last > 0 && now.saturating_sub(last) < 120_000 {
                return false;
            }
            let diff = sorted_symmetric_diff_len(&guard, &addrs);
            let threshold = (guard.len().max(addrs.len()) / 12).max(16);
            if diff < threshold {
                return false;
            }
        }
        guard.clone_from(&addrs);
        let _ = self.addr_tx.send(addrs);
        self.last_replace_ms.store(crate::util::now_ms(), Ordering::Relaxed);
        true
    }

    #[must_use]
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
#[allow(clippy::too_many_arguments)]
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
    if let Some(graph) = graph {
        for edges in &graph.adjacency {
            for ge in edges {
                if !crate::pipeline::cycle_finder::is_live_graph_edge(ge) {
                    continue;
                }
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
            let (activity, activity_count) = partial_cache
                .get(&pool.address)
                .map_or((0, 0), |s| (s.patched_at_ms, s.activity_count.min(10_000)));
            let recency = activity.saturating_sub(now_ms.saturating_sub(300_000));
            let score = cycle_hot
                .saturating_add(centrality.saturating_mul(100))
                .saturating_add(activity_count.saturating_mul(25))
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
#[allow(clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn retain_tracked_prunes_stale_stream_pools() {
        let cache = PartialPoolCache::new();
        let keep = Address::from([1u8; 20]);
        let drop = Address::from([2u8; 20]);
        cache.apply_patch(
            keep,
            LogPatch::V2Reserves {
                reserve0: U256::from(1u8),
                reserve1: U256::from(2u8),
            },
            1,
        );
        cache.apply_patch(
            drop,
            LogPatch::V2Reserves {
                reserve0: U256::from(3u8),
                reserve1: U256::from(4u8),
            },
            1,
        );
        assert_eq!(cache.len(), 2);
        cache.retain_tracked(&[keep]);
        assert_eq!(cache.len(), 1);
        assert!(cache.get(&keep).is_some());
        assert!(cache.get(&drop).is_none());
    }

    #[test]
    fn stream_patches_accumulate_activity() {
        let cache = PartialPoolCache::new();
        let pool = Address::from([1u8; 20]);
        cache.apply_patch(
            pool,
            LogPatch::V2Reserves {
                reserve0: U256::from(10u8),
                reserve1: U256::from(20u8),
            },
            1,
        );
        cache.apply_patch(
            pool,
            LogPatch::V2Reserves {
                reserve0: U256::from(11u8),
                reserve1: U256::from(19u8),
            },
            2,
        );
        let state = cache.get(&pool).expect("patched pool");
        assert_eq!(state.activity_count, 2);
        assert_eq!(state.patched_at_ms, 2);
    }

    #[test]
    fn partial_flush_preserves_unselected_and_failed_dirty_updates() {
        let partial = PartialPoolCache::new();
        let canonical = StateCache::default();
        let selected = Address::with_last_byte(1);
        let deferred = Address::with_last_byte(2);
        let base = || {
            PoolState::V2(crate::core::types::V2PoolState {
                reserve0: U256::from(1u8),
                reserve1: U256::from(1u8),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 1,
            })
        };
        canonical.insert(selected, base());
        for (address, reserve) in [(selected, 10u8), (deferred, 20u8)] {
            partial.apply_patch(
                address,
                LogPatch::V2Reserves {
                    reserve0: U256::from(reserve),
                    reserve1: U256::from(reserve),
                },
                1,
            );
        }

        assert_eq!(partial.flush_to_state_cache(&canonical, &[selected]), 1);
        assert_eq!(partial.flush_to_state_cache(&canonical, &[deferred]), 0);

        canonical.insert(deferred, base());
        assert_eq!(partial.flush_to_state_cache(&canonical, &[deferred]), 1);
        let PoolState::V2(state) = canonical.get(&deferred).expect("deferred state") else {
            panic!("expected V2 state");
        };
        assert_eq!(state.reserve0, U256::from(20u8));
    }
}
