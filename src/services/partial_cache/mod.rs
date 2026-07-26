mod decode;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use alloy::primitives::{Address, B256, U256};
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock, RwLockReadGuard};
use rustc_hash::{FxBuildHasher, FxHashMap, FxHashSet};

type EdgeCentralityMap = FxHashMap<Address, u32>;
type EdgeCentralityCache = Option<(u64, Arc<EdgeCentralityMap>)>;
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

    /// Non-destructive: true when at least one notify is waiting to be taken.
    #[must_use]
    pub fn stream_triggered_pending(&self) -> bool {
        self.stream_tick.load(Ordering::Acquire) > 0
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
    dirty: Mutex<FxHashSet<Address>>,
    /// Streamable pools currently in the arena — topic Sync/Swap on these wake HF
    /// even when outside the smaller WSS interest top-N.
    universe: RwLock<FxHashSet<Address>>,
    /// Topic-observed pools not yet in the universe — LF merges into hot refresh
    /// so live Uni V3 venues enter the arena instead of staying wake_hf=false forever.
    observed_live: Mutex<FxHashSet<Address>>,
    /// Recently topic-observed venues kept in the wake universe across LF ticks
    /// that drain `observed` (activity-based retain bloated with interest-set noise).
    sticky_observed: Mutex<FxHashMap<Address, u64>>,
    /// Live-edge centrality per pool, keyed by layout⊕state generation.
    edge_centrality: Mutex<EdgeCentralityCache>,
}

impl PartialPoolCache {
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Pre-size the hot `pools` DashMap for the expected stream target count.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            pools: DashMap::with_capacity_and_hasher(capacity, FxBuildHasher),
            patches: AtomicU64::new(0),
            trigger: StreamTrigger::new(),
            dirty: Mutex::new(FxHashSet::default()),
            universe: RwLock::new(FxHashSet::with_capacity_and_hasher(capacity, FxBuildHasher)),
            observed_live: Mutex::new(FxHashSet::with_capacity_and_hasher(64, FxBuildHasher)),
            sticky_observed: Mutex::new(FxHashMap::with_capacity_and_hasher(64, FxBuildHasher)),
            edge_centrality: Mutex::new(None),
        }
    }

    /// Stamp topic-observed venues so they survive empty admit ticks in the universe.
    pub fn note_sticky_observed(&self, addrs: &[Address]) {
        if addrs.is_empty() {
            return;
        }
        const KEEP_MS: u64 = 120_000;
        let now = crate::util::now_ms();
        let mut sticky = self.sticky_observed.lock();
        sticky.retain(|_, ts| now.saturating_sub(*ts) < KEEP_MS);
        for &addr in addrs {
            sticky.insert(addr, now);
        }
    }

    /// Cached live-edge counts for stream ranking. `cache_key == 0` always rebuilds.
    fn live_edge_centrality(
        &self,
        cache_key: u64,
        graph: &crate::pipeline::types::RoutingGraph,
        arena: &crate::pipeline::arena::StateArena,
        capacity_hint: usize,
    ) -> Arc<EdgeCentralityMap> {
        if cache_key != 0 {
            let guard = self.edge_centrality.lock();
            if let Some((key, map)) = guard.as_ref()
                && *key == cache_key
            {
                return Arc::clone(map);
            }
        }
        let mut edge_counts =
            EdgeCentralityMap::with_capacity_and_hasher(capacity_hint, FxBuildHasher);
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
        let map = Arc::new(edge_counts);
        if cache_key != 0 {
            *self.edge_centrality.lock() = Some((cache_key, Arc::clone(&map)));
        }
        map
    }

    /// Replace the streamable wake universe with `addrs` plus sticky observed venues.
    pub fn set_stream_universe(&self, addrs: &[Address]) {
        const KEEP_MS: u64 = 120_000;
        let now = crate::util::now_ms();
        let sticky: Vec<Address> = {
            let mut s = self.sticky_observed.lock();
            s.retain(|_, ts| now.saturating_sub(*ts) < KEEP_MS);
            s.keys().copied().collect()
        };
        let size = {
            let mut universe = self.universe.write();
            universe.clear();
            universe.extend(addrs.iter().copied());
            universe.extend(sticky.iter().copied());
            universe.len()
        };
        {
            let mut observed = self.observed_live.lock();
            for addr in addrs {
                observed.remove(addr);
            }
        }
        crate::debug!("stream universe: pools={size} sources=cycle+observed+sticky");
    }

    /// After topic-observed pools are refreshed into the arena/universe, wake HF
    /// once — they often only traded before admission (live: wake_hf true stayed 0).
    pub fn wake_for_admitted_observed(&self, observed: &[Address]) {
        if observed.is_empty() {
            return;
        }
        let admitted: Vec<Address> = {
            let universe = self.universe.read();
            observed
                .iter()
                .copied()
                .filter(|addr| universe.contains(addr))
                .collect()
        };
        if admitted.is_empty() {
            return;
        }
        let now = crate::util::now_ms();
        let mut woke = 0u32;
        {
            let mut dirty = self.dirty.lock();
            for addr in admitted {
                dirty.insert(addr);
                // Stamp activity so HF `cycle_activity_score` can mark cycles
                // containing this pool as active (seed-only states had count=0).
                // or_insert: seed may have skipped Balancer/missing cache rows.
                self.pools
                    .entry(addr)
                    .and_modify(|state| {
                        state.patched_at_ms = now;
                        state.activity_count = state.activity_count.max(1);
                    })
                    .or_insert_with(|| {
                        let mut state = SlimPoolState::from_v3(U256::ZERO, 0, 0, now);
                        state.activity_count = 1;
                        state
                    });
                woke = woke.saturating_add(1);
            }
        }
        if woke > 0 {
            crate::debug!("stream observed-live: synthetic wake for {woke} admitted pools");
            self.trigger.notify();
        }
    }

    #[must_use]
    pub fn in_stream_universe(&self, addr: &Address) -> bool {
        self.universe.read().contains(addr)
    }

    /// Record a topic-observed pool outside the current universe for LF hot refresh.
    pub fn note_observed_live(&self, addr: Address) {
        if !self.universe.read().contains(&addr) {
            self.observed_live.lock().insert(addr);
        }
    }

    /// Drain topic-observed addresses for the next state-refresh hot set.
    #[must_use]
    pub fn take_observed_live(&self) -> Vec<Address> {
        // Atomic take — DashMap iter()+clear could drop concurrent inserts.
        self.observed_live.lock().drain().collect()
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
        self.apply_log_notify(pool, topic0, data, now_ms, true)
    }

    /// Apply a decoded pool log. When `wake_hf` is false, state/activity still
    /// update for stream-target ranking but HF is not notified (topic-wide
    /// Sync/Swap spam outside the interest set was thrashing empty HF ticks).
    pub fn apply_log_notify(
        &self,
        pool: Address,
        topic0: B256,
        data: &[u8],
        now_ms: u64,
        wake_hf: bool,
    ) -> bool {
        let Some(patch) = decode_pool_log(topic0, data) else {
            return false;
        };
        self.apply_patch_notify(pool, patch, now_ms, wake_hf);
        true
    }

    pub fn apply_patch(&self, pool: Address, patch: LogPatch, now_ms: u64) {
        self.apply_patch_notify(pool, patch, now_ms, true);
    }

    pub fn apply_patch_notify(&self, pool: Address, patch: LogPatch, now_ms: u64, wake_hf: bool) {
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
        if crate::log::every_n(&self.patches, 200) {
            let n = self.patches.load(Ordering::Relaxed);
            crate::debug!("wss patch: n={n} pool={pool} wake_hf={wake_hf}");
        }
        if wake_hf {
            self.dirty.lock().insert(pool);
            self.trigger.notify();
        }
    }

    /// Drop stream snapshots for pools no longer in the active WSS target set.
    /// Recently patched pools are retained so topic-wide Sync/Swap discovery can
    /// promote live venues into the next stream target ranking.
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
        let now = crate::util::now_ms();
        const KEEP_ACTIVE_MS: u64 = 300_000;
        self.pools.retain(|addr, state| {
            keep_set.contains(addr) || now.saturating_sub(state.patched_at_ms) <= KEEP_ACTIVE_MS
        });
        dirty.retain(|addr| {
            keep_set.contains(addr)
                || self
                    .pools
                    .get(addr)
                    .is_some_and(|s| now.saturating_sub(s.patched_at_ms) <= KEEP_ACTIVE_MS)
        });
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

    /// Mark pools dirty and wake HF without waiting for another topic Swap.
    /// Used after same-tick observed-live refresh admits venues into the arena.
    pub fn wake_dirty_pools(&self, addrs: &[Address]) {
        if addrs.is_empty() {
            return;
        }
        let now = crate::util::now_ms();
        {
            let mut dirty = self.dirty.lock();
            for addr in addrs {
                dirty.insert(*addr);
                // or_insert when LF seed missed the row (not in StateCache yet /
                // non-V2/V3 seed path) — otherwise activity stays 0 and HF never
                // marks live-touching cycles active (livehold: active_candidates=0).
                self.pools
                    .entry(*addr)
                    .and_modify(|state| {
                        state.patched_at_ms = now;
                        state.activity_count = state.activity_count.max(1);
                    })
                    .or_insert_with(|| {
                        let mut state = SlimPoolState::from_v3(U256::ZERO, 0, 0, now);
                        state.activity_count = 1;
                        state
                    });
            }
        }
        self.trigger.notify();
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
    /// Wall time of last accepted replace; used to coalesce membership churn.
    last_replace_ms: Arc<AtomicU64>,
    /// Set by WSS on log silence so the next LF tick can bypass hysteresis.
    force_replace: Arc<AtomicBool>,
    /// Bumped on silence-forced reselect to rotate centrality fill.
    reselect_epoch: Arc<AtomicU64>,
}

/// Minimum gap between accepted replaces under normal hysteresis (not force).
const STREAM_REPLACE_MIN_GAP_MS: u64 = 12_000;

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
            force_replace: Arc::new(AtomicBool::new(false)),
            reselect_epoch: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn read(&self) -> RwLockReadGuard<'_, Vec<Address>> {
        self.inner.read()
    }

    /// Request that the next [`Self::replace`] bypass hysteresis (WSS log silence).
    pub fn request_force_replace(&self) {
        self.force_replace.store(true, Ordering::Relaxed);
        self.reselect_epoch.fetch_add(1, Ordering::Relaxed);
    }

    #[must_use]
    pub fn force_replace_pending(&self) -> bool {
        self.force_replace.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn reselect_epoch(&self) -> u64 {
        self.reselect_epoch.load(Ordering::Relaxed)
    }

    /// Replace tracked addresses; returns true when the set changed enough to
    /// warrant a WSS resubscribe. Small top-N churn is ignored so LF score
    /// reshuffles do not tear down live Sync/Swap subscriptions every cycle.
    /// Silence-forced replaces bypass the min-gap / sym-diff gates.
    #[must_use]
    pub fn replace(&self, mut addrs: Vec<Address>) -> bool {
        addrs.sort_unstable();
        addrs.dedup();
        let force = self.force_replace.swap(false, Ordering::Relaxed);
        let mut guard = self.inner.write();
        if *guard == addrs {
            return false;
        }
        if !force && !guard.is_empty() && !addrs.is_empty() {
            let now = crate::util::now_ms();
            let last = self.last_replace_ms.load(Ordering::Relaxed);
            // Coalesce LF top-N churn; keep short enough that a cold bootstrap
            // set can be replaced after WSS silence (was 120s — locked dead sets).
            if last > 0 && now.saturating_sub(last) < STREAM_REPLACE_MIN_GAP_MS {
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
        self.last_replace_ms
            .store(crate::util::now_ms(), Ordering::Relaxed);
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

/// Rough tradability weight for stream target ranking (0 = skip-prefer).
fn stream_liquidity_score(state: &PoolState) -> u64 {
    match state {
        PoolState::V2(s) => {
            let min = s.reserve0.min(s.reserve1);
            if min.is_zero() {
                0
            } else {
                // log2-ish depth — prefer funded pools over empty shells.
                u64::from(256u32.saturating_sub(min.leading_zeros() as u32))
            }
        }
        PoolState::V3(s) | PoolState::V4(s) => {
            if s.liquidity == 0 || s.sqrt_price_x96.is_zero() {
                0
            } else {
                u64::from(128u32.saturating_sub(s.liquidity.leading_zeros())).saturating_mul(2)
            }
        }
        _ => 0,
    }
}

/// Drop dust shells from WSS filters (V2 min-reserve ≳ 2^40, V3 liq ≳ 2^20).
const MIN_STREAM_LIQ_SCORE: u64 = 48;

/// Build the top-N streamable pool addresses ranked by cycle hot-set membership,
/// on-chain liquidity, graph edge centrality, and recent WSS patch activity.
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
    select_stream_targets_with_epoch(
        discovered,
        hot,
        graph,
        pool_metas,
        arena,
        partial_cache,
        cap,
        now_ms,
        0,
        &[],
        0,
    )
}

/// Like [`select_stream_targets`], but `epoch` rotates the centrality fill and
/// `demote` penalizes a prior silent watch set after WSS force-reselect.
///
/// `centrality_cache_key` should mix layout fingerprint ⊕ state generation so
/// live-edge counts rebuild after connectivity or rescore changes (`0` = no cache).
#[allow(clippy::too_many_arguments)]
pub fn select_stream_targets_with_epoch(
    discovered: &[crate::services::discovery::DiscoveredPool],
    hot: &[Address],
    graph: Option<&crate::pipeline::types::RoutingGraph>,
    pool_metas: &[crate::pipeline::types::PoolMeta],
    arena: &crate::pipeline::arena::StateArena,
    partial_cache: &PartialPoolCache,
    cap: usize,
    now_ms: u64,
    epoch: u64,
    demote: &[Address],
    centrality_cache_key: u64,
) -> Vec<Address> {
    if cap == 0 {
        return Vec::new();
    }

    let hot_set: FxHashSet<Address> = hot.iter().copied().collect();
    let demote_set: FxHashSet<Address> = demote.iter().copied().collect();
    let addr_to_pool = arena.address_to_pool();

    let edge_counts = graph.map_or_else(Arc::default, |graph| {
        partial_cache.live_edge_centrality(centrality_cache_key, graph, arena, pool_metas.len())
    });

    let mut scored: Vec<(u64, Address)> = discovered
        .iter()
        .filter(|p| is_streamable_protocol(p.protocol))
        .filter_map(|pool| {
            let &pool_idx = addr_to_pool.get(&pool.address)?;
            let liq = arena
                .pool_state(pool_idx)
                .map(stream_liquidity_score)
                .unwrap_or(0);
            // Dust / empty shells almost never emit Sync/Swap — keep them out
            // of the WSS filter so the subscription covers live venues.
            if liq < MIN_STREAM_LIQ_SCORE && !hot_set.contains(&pool.address) {
                return None;
            }
            let centrality = edge_counts.get(&pool.address).copied().unwrap_or(0) as u64;
            let cycle_hot = u64::from(hot_set.contains(&pool.address)) * 100_000;
            let (activity, activity_count) = partial_cache
                .get(&pool.address)
                .map_or((0, 0), |s| (s.patched_at_ms, s.activity_count.min(10_000)));
            let recency = activity.saturating_sub(now_ms.saturating_sub(300_000));
            // Recently topic-patched venues jump the interest set so wake_hf can
            // fire on the next Sync/Swap (live: interest∩chain was empty).
            let live_boost = if activity > 0 && now_ms.saturating_sub(activity) < 120_000 {
                250_000
            } else {
                0
            };
            // Keep cycle-hot pools eligible after silence; only rotate the fill.
            let demote_penalty =
                u64::from(demote_set.contains(&pool.address) && !hot_set.contains(&pool.address))
                    * 50_000;
            let score = cycle_hot
                .saturating_add(live_boost)
                .saturating_add(liq.saturating_mul(1_000))
                .saturating_add(centrality.saturating_mul(100))
                .saturating_add(activity_count.saturating_mul(250))
                .saturating_add(recency / 1000)
                .saturating_sub(demote_penalty);
            Some((score, pool.address))
        })
        .collect();

    // Keep a bounded sorted prefix for rotation; drop the cold tail early.
    let sort_keep = scored.len().min(cap.saturating_mul(2).max(cap));
    if scored.len() > sort_keep {
        scored.select_nth_unstable_by(sort_keep - 1, |a, b| {
            b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1))
        });
        scored.truncate(sort_keep);
    }
    scored.sort_unstable_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));

    // After silence-forced reselect, rotate the non-hot fill so we do not
    // re-arm the same cold centrality block that produced zero logs.
    if epoch > 0 && scored.len() > cap {
        let hot_take = scored
            .iter()
            .take_while(|(s, _)| *s >= 100_000)
            .count()
            .min(cap);
        let fill = scored.split_off(hot_take);
        let rot = (epoch as usize).saturating_mul(cap / 4).min(fill.len());
        let mut rotated = fill[rot..].to_vec();
        rotated.extend_from_slice(&fill[..rot]);
        scored.extend(rotated);
    }

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

    #[test]
    fn stream_address_force_replace_bypasses_hysteresis() {
        let set = StreamAddressSet::new();
        let a: Vec<Address> = (1u8..=20).map(Address::with_last_byte).collect();
        let b: Vec<Address> = (21u8..=40).map(Address::with_last_byte).collect();
        assert!(set.replace(a));
        // Immediate small-gap replace of a disjoint set would normally freeze.
        assert!(!set.replace(b.clone()));
        set.request_force_replace();
        assert!(set.force_replace_pending());
        assert!(set.replace(b));
        assert!(!set.force_replace_pending());
    }
}
