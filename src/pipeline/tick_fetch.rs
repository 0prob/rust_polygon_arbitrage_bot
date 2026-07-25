use alloy::primitives::{Address, FixedBytes};
use parking_lot::Mutex;
use rustc_hash::{FxHashMap, FxHashSet};
use std::sync::{Arc, LazyLock};

use crate::core::constants::UNISWAP_V4_POOL_MANAGER;
use crate::core::math::tick_math::{MAX_TICK, MIN_TICK};
use crate::core::types::{FoundCycle, PoolIndex, ProtocolType, V3Tick};

type V4TickSpan = (PoolIndex, FixedBytes<32>, i32, i32, usize, usize, u128);

fn sort_v4_tick_spans_by_liquidity(spans: &mut [V4TickSpan]) {
    spans.sort_unstable_by_key(|span| std::cmp::Reverse(span.6));
}

/// After full hydrate (lens + algebra + wide) still tickless → skip re-RPC until
/// this deadline. Iter2: 65% of probe TickLens fetches were empty and recycled
/// into the cap within 45s while cycles_tickless stayed stuck.
pub const EMPTY_TICK_COOLDOWN_MS: u64 = 90_000;
/// After hydrate timeout (fetch never completed). Live: 30s under rate-limit left
/// high-priority pools stuck tickless while residual prep re-burned elsewhere.
pub const TICK_HYDRATE_TIMEOUT_COOLDOWN_MS: u64 = 12_000;
/// HF probe narrow-only empty miss — not a full-empty proof (no widen).
/// Lives in a separate map so LF can still widen; shared EMPTY cool was either
/// pinning LF (45s) or letting HF re-burn the probe cap every 5s (live iter2:
/// same miss pools ×10–16 while load_rate≈31%).
pub const PROBE_NARROW_MISS_COOLDOWN_MS: u64 = 30_000;
/// Cap the expensive wide TickLens pass (word_range×3) so sparse empties do not
/// dominate LF under rate limits.
const MAX_WIDE_TICK_POOLS: usize = 24;

static EMPTY_TICK_UNTIL_MS: LazyLock<Mutex<FxHashMap<Address, u64>>> =
    LazyLock::new(|| Mutex::new(FxHashMap::default()));
/// Probe-narrow misses only — checked by HF probe target selection, not LF.
static PROBE_NARROW_MISS_UNTIL_MS: LazyLock<Mutex<FxHashMap<Address, u64>>> =
    LazyLock::new(|| Mutex::new(FxHashMap::default()));

/// True when this pool recently stayed tickless after a full hydrate attempt.
#[must_use]
pub fn is_empty_tick_on_cooldown(pool: Address) -> bool {
    let now = crate::util::now_ms();
    EMPTY_TICK_UNTIL_MS
        .lock()
        .get(&pool)
        .is_some_and(|&until| now < until)
}

/// True when HF probe recently saw a narrow-only empty (skip re-fetch in probe).
#[must_use]
pub fn is_probe_narrow_miss_on_cooldown(pool: Address) -> bool {
    let now = crate::util::now_ms();
    PROBE_NARROW_MISS_UNTIL_MS
        .lock()
        .get(&pool)
        .is_some_and(|&until| now < until)
}

fn mark_probe_narrow_miss(pools: impl IntoIterator<Item = Address>) {
    let now = crate::util::now_ms();
    let until = now.saturating_add(PROBE_NARROW_MISS_COOLDOWN_MS);
    let mut map = PROBE_NARROW_MISS_UNTIL_MS.lock();
    map.retain(|_, u| *u > now);
    for pool in pools {
        map.entry(pool)
            .and_modify(|deadline| *deadline = (*deadline).max(until))
            .or_insert(until);
    }
}

fn mark_tick_cooldown_addresses(pools: impl IntoIterator<Item = Address>, cooldown_ms: u64) {
    let now = crate::util::now_ms();
    let until = now.saturating_add(cooldown_ms);
    let mut map = EMPTY_TICK_UNTIL_MS.lock();
    map.retain(|_, u| *u > now);
    for pool in pools {
        map.entry(pool)
            .and_modify(|deadline| *deadline = (*deadline).max(until))
            .or_insert(until);
    }
}

fn mark_empty_tick_cooldown(pools: impl IntoIterator<Item = Address>) {
    mark_tick_cooldown_addresses(pools, EMPTY_TICK_COOLDOWN_MS);
}

/// Briefly suppress re-fetch after a hydrate timeout (fetch never completed).
pub fn mark_tick_hydrate_timeout_cooldown(pools: impl IntoIterator<Item = Address>) {
    mark_tick_cooldown_addresses(pools, TICK_HYDRATE_TIMEOUT_COOLDOWN_MS);
}

/// Clear address-keyed hydrate cooldown (e.g. after ticks load).
pub fn clear_tick_hydrate_cooldown(pool: Address) {
    clear_empty_tick_cooldown(pool);
    PROBE_NARROW_MISS_UNTIL_MS.lock().remove(&pool);
}

fn clear_empty_tick_cooldown(pool: Address) {
    EMPTY_TICK_UNTIL_MS.lock().remove(&pool);
}

/// V4 pools are keyed by pool id (not address), so they get their own map.
static EMPTY_V4_TICK_UNTIL_MS: LazyLock<Mutex<FxHashMap<FixedBytes<32>, u64>>> =
    LazyLock::new(|| Mutex::new(FxHashMap::default()));
/// V4 probe-narrow misses only — checked by HF probe target selection, not LF.
static PROBE_NARROW_MISS_V4_UNTIL_MS: LazyLock<Mutex<FxHashMap<FixedBytes<32>, u64>>> =
    LazyLock::new(|| Mutex::new(FxHashMap::default()));

/// True when this V4 pool recently stayed tickless after a full hydrate attempt.
#[must_use]
pub fn is_empty_v4_tick_on_cooldown(pool_id: FixedBytes<32>) -> bool {
    let now = crate::util::now_ms();
    EMPTY_V4_TICK_UNTIL_MS
        .lock()
        .get(&pool_id)
        .is_some_and(|&until| now < until)
}

/// True when HF probe recently saw a narrow-only empty for V4 (skip re-fetch in probe).
#[must_use]
pub fn is_probe_narrow_miss_v4_on_cooldown(pool_id: FixedBytes<32>) -> bool {
    let now = crate::util::now_ms();
    PROBE_NARROW_MISS_V4_UNTIL_MS
        .lock()
        .get(&pool_id)
        .is_some_and(|&until| now < until)
}

pub fn mark_probe_narrow_miss_v4(pool_ids: impl IntoIterator<Item = FixedBytes<32>>) {
    let now = crate::util::now_ms();
    let until = now.saturating_add(PROBE_NARROW_MISS_COOLDOWN_MS);
    let mut map = PROBE_NARROW_MISS_V4_UNTIL_MS.lock();
    map.retain(|_, u| *u > now);
    for pool_id in pool_ids {
        map.entry(pool_id)
            .and_modify(|deadline| *deadline = (*deadline).max(until))
            .or_insert(until);
    }
}

fn mark_v4_tick_cooldown(pool_ids: impl IntoIterator<Item = FixedBytes<32>>, cooldown_ms: u64) {
    let now = crate::util::now_ms();
    let until = now.saturating_add(cooldown_ms);
    let mut map = EMPTY_V4_TICK_UNTIL_MS.lock();
    map.retain(|_, u| *u > now);
    for pool_id in pool_ids {
        map.entry(pool_id)
            .and_modify(|deadline| *deadline = (*deadline).max(until))
            .or_insert(until);
    }
}

fn mark_empty_v4_tick_cooldown(pool_ids: impl IntoIterator<Item = FixedBytes<32>>) {
    mark_v4_tick_cooldown(pool_ids, EMPTY_TICK_COOLDOWN_MS);
}

/// Briefly suppress V4 re-fetch after a hydrate timeout.
pub fn mark_v4_tick_hydrate_timeout_cooldown(pool_ids: impl IntoIterator<Item = FixedBytes<32>>) {
    mark_v4_tick_cooldown(pool_ids, TICK_HYDRATE_TIMEOUT_COOLDOWN_MS);
}

/// Clear V4 pool-id hydrate cooldown (e.g. after ticks load).
pub fn clear_v4_tick_hydrate_cooldown(pool_id: FixedBytes<32>) {
    clear_empty_v4_tick_cooldown(pool_id);
    PROBE_NARROW_MISS_V4_UNTIL_MS.lock().remove(&pool_id);
}

/// True when either address- or pool-id-keyed hydrate cooldown is active.
#[must_use]
pub fn is_cl_tick_on_hydrate_cooldown(addr: Address, v4_pool_id: Option<FixedBytes<32>>) -> bool {
    is_empty_tick_on_cooldown(addr) || v4_pool_id.is_some_and(is_empty_v4_tick_on_cooldown)
}

fn clear_empty_v4_tick_cooldown(pool_id: FixedBytes<32>) {
    EMPTY_V4_TICK_UNTIL_MS.lock().remove(&pool_id);
}

#[derive(Clone, Copy, Debug, Default)]
pub struct TickEnrichment {
    pub loaded: usize,
    /// True when still-empty targets should retry on another state URL
    /// (hard RPC failure, or incomplete multicall left empties).
    pub rpc_failed: bool,
    /// TickLens/bitmap returned complete words but zero populated ticks.
    pub empty_pools: usize,
    /// Partial multicall word responses (reverts / missing returns).
    pub incomplete_pools: usize,
    /// Pools hydrated via Algebra tickTable after TickLens miss / label route.
    pub algebra_loaded: usize,
    pub algebra_no_tick_calls: usize,
    pub algebra_decode_rejects: usize,
}

impl TickEnrichment {
    #[cfg(test)]
    fn combine(self, other: Self) -> Self {
        Self {
            loaded: self.loaded.saturating_add(other.loaded),
            rpc_failed: self.rpc_failed || other.rpc_failed,
            empty_pools: self.empty_pools.saturating_add(other.empty_pools),
            incomplete_pools: self.incomplete_pools.saturating_add(other.incomplete_pools),
            algebra_loaded: self.algebra_loaded.saturating_add(other.algebra_loaded),
            algebra_no_tick_calls: self
                .algebra_no_tick_calls
                .saturating_add(other.algebra_no_tick_calls),
            algebra_decode_rejects: self
                .algebra_decode_rejects
                .saturating_add(other.algebra_decode_rejects),
        }
    }
}
use crate::core::v4_storage::{
    compute_v4_tick_bitmap_slot, compute_v4_tick_info_slot, decode_v4_tick_liquidity,
};
use crate::pipeline::arena::StateArena;
use crate::pipeline::types::PoolMeta;

const MAX_TICK_POOLS: usize = 512;
/// Cap per-pass CL tick-info reads (dense bitmaps can fan out quickly).
/// Shared by V4 extsload and Algebra `ticks()` hydrate.
const MAX_CL_TICK_INFO_READS: usize = 2_048;
/// Cap Algebra tickTable targets per pass. Live: 100+ pool batches rate-limit to zero.
const MAX_ALGEBRA_TICK_POOLS: usize = 32;

/// Drop tick bitmap before re-enriching. Prefer not calling on HF probe targets
/// that are already empty — clearing is only needed when replacing partial loads.
pub fn clear_v3_pool_ticks(arena: &mut StateArena, pool_addresses: &[Address]) {
    for pool in pool_addresses {
        let Some(&index) = arena.address_to_pool().get(pool) else {
            continue;
        };
        if let Some(crate::core::types::PoolState::V3(state)) = arena.pool_state_mut(index) {
            state.ticks = Arc::from([]);
        }
    }
}

pub fn clear_v4_pool_ticks(arena: &mut StateArena, targets: &[(PoolIndex, FixedBytes<32>)]) {
    for &(index, _) in targets {
        if let Some(crate::core::types::PoolState::V4(state)) = arena.pool_state_mut(index) {
            state.ticks = Arc::from([]);
        }
    }
}

/// Addresses among `pools` whose V3 state is still tickless.
#[must_use]
pub fn still_tickless_v3(arena: &StateArena, pools: &[Address]) -> Vec<Address> {
    pools
        .iter()
        .copied()
        .filter(|addr| {
            let Some(&idx) = arena.address_to_pool().get(addr) else {
                return false;
            };
            matches!(
                arena.pool_state(idx),
                Some(crate::core::types::PoolState::V3(s)) if s.ticks.is_empty()
            )
        })
        .collect()
}

/// V4 targets among `targets` whose state is still tickless.
#[must_use]
pub fn still_tickless_v4(
    arena: &StateArena,
    targets: &[(PoolIndex, FixedBytes<32>)],
) -> Vec<(PoolIndex, FixedBytes<32>)> {
    targets
        .iter()
        .copied()
        .filter(|(idx, _)| {
            matches!(
                arena.pool_state(*idx),
                Some(crate::core::types::PoolState::V4(s)) if s.ticks.is_empty()
            )
        })
        .collect()
}

/// Compressed tick index (`floor(tick / spacing)`) matching Uniswap V3 tick bitmap math.
#[inline]
#[must_use]
pub fn compress_cl_tick(tick: i32, spacing: i32) -> i32 {
    tick.div_euclid(spacing.max(1))
}

#[inline]
#[must_use]
pub fn cl_tick_bitmap_center_word(tick: i32, spacing: i32) -> i32 {
    compress_cl_tick(tick, spacing) >> 8
}

#[inline]
fn cl_tick_from_bitmap_bit(word: i32, bit: u16, spacing: i32) -> Option<i32> {
    let compressed = word.saturating_mul(256).saturating_add(i32::from(bit));
    let tick = compressed.saturating_mul(spacing.max(1));
    (MIN_TICK..=MAX_TICK).contains(&tick).then_some(tick)
}

fn finalize_cl_ticks(ticks: &mut Vec<V3Tick>) {
    ticks.sort_unstable_by_key(|t| t.tick);
    ticks.dedup_by(|a, b| a.tick == b.tick);
}

/// Bitmap-word visit order centered on the current-tick word (cap-friendly).
#[must_use]
fn cl_bitmap_center_out_offsets(word_count: usize) -> Vec<usize> {
    if word_count == 0 {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(word_count);
    let mid = word_count / 2;
    out.push(mid);
    for d in 1..=mid.max(word_count.saturating_sub(mid + 1)) {
        if mid >= d {
            out.push(mid - d);
        }
        if mid + d < word_count {
            out.push(mid + d);
        }
    }
    out
}

/// Algebra / Algebra-Integral labels for pools in `addresses` only (hydrate targets).
#[must_use]
pub fn collect_algebra_pools(
    arena: &StateArena,
    pool_metas: &[PoolMeta],
    addresses: &[Address],
) -> (FxHashSet<Address>, FxHashSet<Address>) {
    let mut algebra_pools = FxHashSet::default();
    let mut algebra_integral_pools = FxHashSet::default();
    algebra_pools.reserve(addresses.len().min(32));
    algebra_integral_pools.reserve(addresses.len().min(16));
    for &addr in addresses {
        let Some(&idx) = arena.address_to_pool().get(&addr) else {
            continue;
        };
        let Some(meta) = crate::pipeline::types::pool_meta_at(pool_metas, idx) else {
            continue;
        };
        let Some(label) = meta.protocol_label.as_deref() else {
            continue;
        };
        if crate::core::protocol::is_algebra_integral_protocol_label(label) {
            algebra_integral_pools.insert(addr);
        }
        if crate::core::protocol::is_algebra_protocol_label(label) {
            algebra_pools.insert(addr);
        }
    }
    (algebra_pools, algebra_integral_pools)
}

#[must_use]
pub fn collect_v3_pool_addresses<C: AsRef<FoundCycle>>(
    arena: &StateArena,
    cycles: &[C],
) -> Vec<Address> {
    let mut out = Vec::with_capacity(cycles.len().min(MAX_TICK_POOLS));
    let mut seen: FxHashSet<Address> = FxHashSet::default();
    'cycles: for cycle in cycles {
        for edge in &cycle.as_ref().edges {
            if edge.protocol != ProtocolType::UniswapV3 {
                continue;
            }
            let Some(addr) = arena.pool_address(edge.pool_index) else {
                continue;
            };
            if seen.insert(addr) {
                out.push(addr);
                if out.len() >= MAX_TICK_POOLS {
                    break 'cycles;
                }
            }
        }
    }
    out
}

#[must_use]
pub fn collect_v4_tick_targets<C: AsRef<FoundCycle>>(
    cycles: &[C],
    pool_metas: &[PoolMeta],
) -> Vec<(PoolIndex, FixedBytes<32>)> {
    let mut out = Vec::new();
    let mut seen: FxHashSet<PoolIndex> = FxHashSet::default();
    'cycles: for cycle in cycles {
        for edge in &cycle.as_ref().edges {
            if edge.protocol != ProtocolType::UniswapV4 {
                continue;
            }
            if !seen.insert(edge.pool_index) {
                continue;
            }
            let Some(meta) = crate::pipeline::types::pool_meta_at(pool_metas, edge.pool_index)
            else {
                continue;
            };
            let Some(pool_id) = meta.pool_id else {
                continue;
            };
            out.push((edge.pool_index, pool_id));
            if out.len() >= MAX_TICK_POOLS {
                break 'cycles;
            }
        }
    }
    out
}

/// Shared state-RPC fallback loop for LF / HF probe / dispatch CL tick hydration.
#[derive(Debug, Clone, Copy, Default)]
pub struct ClTickHydratePass {
    pub v3_loaded: usize,
    pub v3_empty: usize,
    pub v3_incomplete: usize,
    pub v3_algebra_loaded: usize,
    pub v4_loaded: usize,
    pub v3_pending: bool,
    pub v4_pending: bool,
    pub v3_ms: u64,
    pub v4_ms: u64,
}

#[allow(clippy::too_many_arguments)]
pub async fn hydrate_cl_ticks_with_rpc_fallback(
    rpc: &crate::infra::rpc::RpcPool,
    arena: &mut StateArena,
    v3: &[Address],
    v4: &[(PoolIndex, FixedBytes<32>)],
    algebra_pools: &FxHashSet<Address>,
    algebra_integral_pools: &FxHashSet<Address>,
    v3_word_range: i16,
    v4_word_range: i16,
    allow_widen: bool,
    block_number: Option<u64>,
    log_label: &str,
) -> ClTickHydratePass {
    let mut pass = ClTickHydratePass {
        v3_pending: !v3.is_empty(),
        v4_pending: !v4.is_empty(),
        ..ClTickHydratePass::default()
    };
    for (url_index, url) in rpc.state_url_candidates().iter().enumerate() {
        let Ok(provider) = rpc.connect_state_at(url) else {
            rpc.deprioritize_state_url(url);
            continue;
        };
        let mut v3_pending = pass.v3_pending;
        let mut v4_pending = pass.v4_pending;
        let (v3_res, v4_res, v3_ms_add, v4_ms_add) =
            crate::infra::rpc_budget::scope_rpc_budget(url, async {
                let mut v3_res = None;
                let mut v4_res = None;
                let mut v3_ms_add = 0u64;
                let mut v4_ms_add = 0u64;
                if v3_pending {
                    let needed = still_tickless_v3(arena, v3);
                    if needed.is_empty() {
                        v3_pending = false;
                    } else {
                        let started = crate::util::now_ms();
                        v3_res = Some(
                            enrich_v3_ticks(
                                &provider,
                                arena,
                                &needed,
                                v3_word_range,
                                algebra_pools,
                                algebra_integral_pools,
                                block_number,
                                allow_widen,
                            )
                            .await,
                        );
                        v3_ms_add = crate::util::now_ms().saturating_sub(started);
                    }
                }
                if v4_pending {
                    let needed = still_tickless_v4(arena, v4);
                    if needed.is_empty() {
                        v4_pending = false;
                    } else {
                        let started = crate::util::now_ms();
                        v4_res = Some(
                            enrich_v4_ticks(
                                &provider,
                                arena,
                                &needed,
                                v4_word_range,
                                block_number,
                                allow_widen,
                            )
                            .await,
                        );
                        v4_ms_add = crate::util::now_ms().saturating_sub(started);
                    }
                }
                (v3_res, v4_res, v3_ms_add, v4_ms_add)
            })
            .await;
        pass.v3_ms = pass.v3_ms.saturating_add(v3_ms_add);
        pass.v4_ms = pass.v4_ms.saturating_add(v4_ms_add);
        pass.v3_pending = v3_pending;
        pass.v4_pending = v4_pending;
        if let Some(res) = v3_res {
            pass.v3_loaded = pass.v3_loaded.saturating_add(res.loaded);
            pass.v3_empty = res.empty_pools;
            pass.v3_incomplete = res.incomplete_pools;
            pass.v3_algebra_loaded = pass.v3_algebra_loaded.saturating_add(res.algebra_loaded);
            pass.v3_pending = res.rpc_failed;
        }
        if let Some(res) = v4_res {
            pass.v4_loaded = pass.v4_loaded.saturating_add(res.loaded);
            pass.v4_pending = res.rpc_failed;
        }
        if !pass.v3_pending && !pass.v4_pending {
            if url_index > 0 {
                crate::info!(
                    "{log_label} fallback ok (url_index={url_index}, v3_loaded={}, v4_loaded={})",
                    pass.v3_loaded,
                    pass.v4_loaded,
                );
            }
            return pass;
        }
        rpc.deprioritize_state_url(url);
        crate::warn!(
            "{log_label} RPC failed — trying fallback (url_index={url_index}, v3_pending={}, v4_pending={})",
            pass.v3_pending,
            pass.v4_pending,
        );
    }
    pass
}

#[allow(clippy::too_many_arguments)]
pub async fn enrich_v3_ticks<
    P: alloy::providers::Provider<alloy::network::Ethereum> + Clone + Send + 'static,
>(
    provider: &P,
    arena: &mut StateArena,
    pool_addresses: &[Address],
    word_range: i16,
    algebra_pools: &FxHashSet<Address>,
    algebra_integral_pools: &FxHashSet<Address>,
    block_number: Option<u64>,
    // HF probe: skip widen (2nd multicall wave) — live 1–4 pools timed out at 600–800ms.
    allow_widen: bool,
) -> TickEnrichment {
    use alloy::sol_types::SolCall;

    use crate::abis::ITickLens;
    use crate::core::constants::TICK_LENS_POLYGON;
    use crate::pipeline::multicall::{MulticallItem, encode_call};

    if pool_addresses.is_empty() {
        return TickEnrichment::default();
    }
    let tick_lens = TICK_LENS_POLYGON;
    let word_count = word_range.saturating_mul(2).saturating_add(1) as usize;
    let mut items = Vec::with_capacity(pool_addresses.len().saturating_mul(word_count));
    let mut spans: Vec<(usize, usize, PoolIndex)> = Vec::with_capacity(pool_addresses.len());

    let mut algebra_targets = Vec::new();
    for &pool in pool_addresses {
        let Some(idx) = arena.address_to_pool().get(&pool).copied() else {
            continue;
        };
        let (tick, spacing) = match arena.pool_state(idx) {
            Some(crate::core::types::PoolState::V3(s)) => (s.tick, s.tick_spacing),
            _ => continue,
        };
        let center_word = cl_tick_bitmap_center_word(tick, spacing);
        let word_min = center_word - word_range as i32;
        let word_max = center_word + word_range as i32;
        let start = items.len();
        if algebra_pools.contains(&pool) {
            algebra_targets.push((pool, idx, spacing, word_min, word_max));
            continue;
        }
        for word in word_min..=word_max {
            items.push(MulticallItem {
                target: tick_lens,
                data: encode_call(&ITickLens::getPopulatedTicksInWordCall {
                    pool,
                    tickBitmapIndex: word as i16,
                }),
            });
        }
        spans.push((start, items.len(), idx));
    }

    if items.is_empty() && algebra_targets.is_empty() {
        return TickEnrichment::default();
    }

    let mut updated = 0usize;
    let mut empty_pools = 0usize;
    let mut incomplete_pools = 0usize;
    // TickLens miss / revert often means Algebra (QuickSwap) — retry those via tickTable.
    let mut algebra_fallback: Vec<(Address, PoolIndex, i32, i32, i32)> = Vec::new();
    let mut tick_lens_rpc_failed = false;
    if !items.is_empty() {
        match crate::pipeline::multicall::execute_tick_lens_multicall_at(
            provider,
            &items,
            block_number,
        )
        .await
        {
            Ok(results) => {
                for (start, end, idx) in &spans {
                    let mut ticks: Vec<V3Tick> = Vec::new();
                    let mut complete = true;
                    for bytes in &results[*start..*end] {
                        let Some(bytes) = bytes else {
                            complete = false;
                            continue;
                        };
                        let Ok(populated) =
                            ITickLens::getPopulatedTicksInWordCall::abi_decode_returns(bytes)
                        else {
                            complete = false;
                            continue;
                        };
                        for pt in populated {
                            let tick = pt.tick.as_i32();
                            if !(MIN_TICK..=MAX_TICK).contains(&tick) {
                                continue;
                            }
                            ticks.push(V3Tick {
                                tick,
                                liquidity_gross: pt.liquidityGross,
                                liquidity_net: pt.liquidityNet,
                            });
                        }
                    }
                    if !complete {
                        incomplete_pools += 1;
                        // ponytail: partial depth beats tickless (same as V4)
                        if !ticks.is_empty() {
                            finalize_cl_ticks(&mut ticks);
                            if let Some(crate::core::types::PoolState::V3(s)) =
                                arena.pool_state_mut(*idx)
                            {
                                s.ticks = Arc::from(ticks);
                                updated += 1;
                            }
                        } else if let Some(fb) = algebra_fallback_target(arena, *idx, word_range) {
                            // Empty+incomplete: unlabeled Algebra often fails TickLens
                            algebra_fallback.push(fb);
                        }
                        continue;
                    }
                    if ticks.is_empty() {
                        // Complete empty TickLens words → widen TickLens, not Algebra.
                        // Unlabeled Algebra almost always reverts/incomplete (path above);
                        // probing every sparse UniV3 via tickTable was the live rate-limit storm.
                        empty_pools += 1;
                        continue;
                    }
                    finalize_cl_ticks(&mut ticks);
                    if let Some(crate::core::types::PoolState::V3(s)) = arena.pool_state_mut(*idx) {
                        s.ticks = Arc::from(ticks);
                        updated += 1;
                    }
                }
                if incomplete_pools > 0 {
                    crate::warn!(
                        "v3 tick lens partial/incomplete: incomplete_pools={incomplete_pools} loaded={updated}"
                    );
                }
            }
            Err(error) => {
                crate::warn!(
                    "v3 tick lens multicall failed ({} pools) — algebra only for labeled: {error:#}",
                    pool_addresses.len(),
                );
                tick_lens_rpc_failed = true;
                // Hard fail must not dump every UniV3 into Algebra tickTable —
                // live: 100+ pool OOG/rate-limit → identical Algebra storm.
                for &pool in pool_addresses {
                    if !(algebra_pools.contains(&pool) || algebra_integral_pools.contains(&pool)) {
                        continue;
                    }
                    let Some(idx) = arena.address_to_pool().get(&pool).copied() else {
                        continue;
                    };
                    if let Some(fb) = algebra_fallback_target(arena, idx, word_range) {
                        algebra_fallback.push(fb);
                    }
                }
            }
        }
    }

    let direct_loaded = updated;
    // Labeled algebra first, then TickLens-empty/incomplete fallbacks (dedupe by pool index).
    let mut algebra_seen: FxHashSet<PoolIndex> = FxHashSet::default();
    let mut all_algebra = Vec::with_capacity(algebra_targets.len() + algebra_fallback.len());
    for t in algebra_targets.into_iter().chain(algebra_fallback) {
        if algebra_seen.insert(t.1) {
            all_algebra.push(t);
        }
    }
    let algebra_target_count = all_algebra.len();
    // Pools that already took the tickTable path (labeled or TickLens-miss fallback).
    let algebra_attempted: FxHashSet<Address> =
        all_algebra.iter().map(|(addr, _, _, _, _)| *addr).collect();
    let algebra = enrich_algebra_ticks(
        provider,
        arena,
        &all_algebra,
        algebra_integral_pools,
        block_number,
    )
    .await;
    let mut algebra_no_tick_calls = algebra.algebra_no_tick_calls;
    let mut algebra_decode_rejects = algebra.algebra_decode_rejects;
    updated += algebra.loaded;

    // Wider window for still-tickless pools. UniV3 → TickLens; Algebra (labeled or
    // prior tickTable attempt) → tickTable widen. TickLens-only widen never helps
    // QuickSwap; unlabeled Algebra fallbacks need both when still empty.
    // Cap + liquidity-rank: full widen of 30+ empties dominates LF under rate limits.
    let mut wide_loaded = 0usize;
    // Only pools that saw a full hydrate (wide pass, or word_range already maxed)
    // may enter the empty cooldown — capped-out narrow misses must stay eligible.
    let mut wide_attempted: FxHashSet<Address> = FxHashSet::default();
    let widen_available = allow_widen && word_range < 48;
    if updated < pool_addresses.len() && widen_available {
        let wide_range = word_range.saturating_mul(3).clamp(24, 48);
        let mut still_empty: Vec<(Address, u128)> = Vec::new();
        let mut seen: FxHashSet<Address> = FxHashSet::default();
        for &pool in pool_addresses {
            let Some(idx) = arena.address_to_pool().get(&pool).copied() else {
                continue;
            };
            let Some(crate::core::types::PoolState::V3(s)) = arena.pool_state(idx) else {
                continue;
            };
            if s.ticks.is_empty() && seen.insert(pool) {
                still_empty.push((pool, s.liquidity));
            }
        }
        if !still_empty.is_empty() {
            still_empty.sort_unstable_by_key(|entry| std::cmp::Reverse(entry.1));
            let wide_targets: Vec<Address> = still_empty
                .into_iter()
                .take(MAX_WIDE_TICK_POOLS)
                .map(|(p, _)| p)
                .collect();
            wide_attempted.extend(wide_targets.iter().copied());
            let mut lens_wide = Vec::new();
            let mut algebra_wide = Vec::new();
            for pool in wide_targets {
                let labeled =
                    algebra_pools.contains(&pool) || algebra_integral_pools.contains(&pool);
                if labeled || algebra_attempted.contains(&pool) {
                    let Some(idx) = arena.address_to_pool().get(&pool).copied() else {
                        continue;
                    };
                    if let Some(fb) = algebra_fallback_target(arena, idx, wide_range) {
                        algebra_wide.push(fb);
                    }
                }
                // Labeled Algebra skips TickLens entirely; unlabeled still needs a
                // wider TickLens pass when the Algebra probe was a false positive.
                if !labeled {
                    lens_wide.push(pool);
                }
            }
            if !lens_wide.is_empty() {
                wide_loaded +=
                    enrich_v3_tick_lens_only(provider, arena, &lens_wide, wide_range, block_number)
                        .await;
            }
            if !algebra_wide.is_empty() {
                let alg = enrich_algebra_ticks(
                    provider,
                    arena,
                    &algebra_wide,
                    algebra_integral_pools,
                    block_number,
                )
                .await;
                wide_loaded += alg.loaded;
                algebra_no_tick_calls += alg.algebra_no_tick_calls;
                algebra_decode_rejects += alg.algebra_decode_rejects;
            }
            updated += wide_loaded;
        }
    }

    // Cooldown empties after a full attempt; briefly suppress re-fetch on success
    // so hot-cache tick drops don't thrash TickLens every HF tick.
    let mut still_tickless = Vec::new();
    let mut loaded_ok = Vec::new();
    for &pool in pool_addresses {
        let Some(idx) = arena.address_to_pool().get(&pool).copied() else {
            continue;
        };
        match arena.pool_state(idx) {
            Some(crate::core::types::PoolState::V3(s)) if !s.ticks.is_empty() => {
                loaded_ok.push(pool);
            }
            Some(crate::core::types::PoolState::V3(s)) if s.ticks.is_empty() => {
                still_tickless.push(pool);
            }
            _ => {}
        }
    }
    if !loaded_ok.is_empty() {
        // Success must clear miss/empty cool so a later hot-cache tick drop can
        // re-hydrate. Writing RECENT into EMPTY_TICK (prior behavior) falsely
        // armed tickless_stuck_skip while ticks were gone — pure poison.
        for &pool in &loaded_ok {
            clear_tick_hydrate_cooldown(pool);
        }
    }
    // URL fallback when empties remain after hard RPC fail or incomplete words.
    // Genuinely empty (complete probe) gets cooldown instead — not another URL.
    let needs_url_fallback = !still_tickless.is_empty()
        && (tick_lens_rpc_failed || algebra.rpc_failed || incomplete_pools > 0);
    if !still_tickless.is_empty() && !needs_url_fallback {
        if allow_widen {
            // LF/dispatch: EMPTY cool only after wide attempt, or word_range already maxed.
            let cool = still_tickless
                .iter()
                .copied()
                .filter(|p| word_range >= 48 || wide_attempted.contains(p));
            mark_empty_tick_cooldown(cool);
        } else {
            // HF probe narrow-only: not a full-empty proof — don't touch shared
            // EMPTY (blocks LF widen). Deprioritize for probe selection only.
            mark_probe_narrow_miss(still_tickless.iter().copied());
        }
    } else if !still_tickless.is_empty() && incomplete_pools > 0 {
        // Incomplete words without finishing URL fallback — short suppress so the
        // next HF tick doesn't re-clear+refetch the same batch immediately.
        mark_tick_hydrate_timeout_cooldown(still_tickless.iter().copied());
    }

    if empty_pools > 0 || incomplete_pools > 0 || algebra.loaded > 0 || wide_loaded > 0 {
        // Funnel is debug; surface a warn only when the batch is almost empty after work.
        let msg = format!(
            "v3 tick hydration: targets={} direct_loaded={} direct_empty={} incomplete={} algebra_targets={} algebra_loaded={} algebra_no_tick_calls={} algebra_decode_rejects={} wide_loaded={} loaded={} still_empty={}",
            pool_addresses.len(),
            direct_loaded,
            empty_pools,
            incomplete_pools,
            algebra_target_count,
            algebra.loaded,
            algebra_no_tick_calls,
            algebra_decode_rejects,
            wide_loaded,
            updated,
            still_tickless.len(),
        );
        if updated == 0 && !pool_addresses.is_empty() {
            crate::warn!("{msg} — TickLens empty; check RPC rate limits / word_range");
        } else {
            crate::debug!("{msg}");
        }
        // Sample the first still-tickless target for offline diagnosis.
        for &pool in pool_addresses {
            let Some(idx) = arena.address_to_pool().get(&pool).copied() else {
                continue;
            };
            if let Some(crate::core::types::PoolState::V3(s)) = arena.pool_state(idx)
                && s.ticks.is_empty()
            {
                crate::debug!(
                    "v3 tick hydration miss: pool={pool} tick={} spacing={} liquidity={} labeled_algebra={}",
                    s.tick,
                    s.tick_spacing,
                    s.liquidity,
                    algebra_pools.contains(&pool) || algebra_integral_pools.contains(&pool),
                );
                break;
            }
        }
    } else {
        crate::debug!(
            "v3 tick hydration: targets={} direct_loaded={} loaded={}",
            pool_addresses.len(),
            direct_loaded,
            updated
        );
    }
    TickEnrichment {
        loaded: updated,
        rpc_failed: needs_url_fallback,
        empty_pools,
        incomplete_pools,
        // Wide TickLens pass counts toward hydrated pools; keep separate from
        // algebra_loaded for diagnostics (was previously summed into this field).
        algebra_loaded: algebra.loaded,
        algebra_no_tick_calls,
        algebra_decode_rejects,
    }
}

fn algebra_fallback_target(
    arena: &StateArena,
    idx: PoolIndex,
    word_range: i16,
) -> Option<(Address, PoolIndex, i32, i32, i32)> {
    let addr = arena.pool_address(idx)?;
    let (tick, spacing) = match arena.pool_state(idx) {
        Some(crate::core::types::PoolState::V3(s)) => (s.tick, s.tick_spacing),
        _ => return None,
    };
    let center_word = cl_tick_bitmap_center_word(tick, spacing);
    Some((
        addr,
        idx,
        spacing,
        center_word - i32::from(word_range),
        center_word + i32::from(word_range),
    ))
}

/// TickLens-only widen pass (no Algebra / no further widen) for still-empty UniV3 pools.
async fn enrich_v3_tick_lens_only<
    P: alloy::providers::Provider<alloy::network::Ethereum> + Clone + Send + 'static,
>(
    provider: &P,
    arena: &mut StateArena,
    pool_addresses: &[Address],
    word_range: i16,
    block_number: Option<u64>,
) -> usize {
    use alloy::sol_types::SolCall;

    use crate::abis::ITickLens;
    use crate::core::constants::TICK_LENS_POLYGON;
    use crate::pipeline::multicall::{MulticallItem, encode_call};

    if pool_addresses.is_empty() {
        return 0;
    }
    let tick_lens = TICK_LENS_POLYGON;
    let word_count = word_range.saturating_mul(2).saturating_add(1) as usize;
    let mut items = Vec::with_capacity(pool_addresses.len().saturating_mul(word_count));
    let mut spans: Vec<(usize, usize, PoolIndex)> = Vec::with_capacity(pool_addresses.len());
    for &pool in pool_addresses {
        let Some(idx) = arena.address_to_pool().get(&pool).copied() else {
            continue;
        };
        let (tick, spacing) = match arena.pool_state(idx) {
            Some(crate::core::types::PoolState::V3(s)) => (s.tick, s.tick_spacing),
            _ => continue,
        };
        let center_word = cl_tick_bitmap_center_word(tick, spacing);
        let word_min = center_word - i32::from(word_range);
        let word_max = center_word + i32::from(word_range);
        let start = items.len();
        for word in word_min..=word_max {
            items.push(MulticallItem {
                target: tick_lens,
                data: encode_call(&ITickLens::getPopulatedTicksInWordCall {
                    pool,
                    tickBitmapIndex: word as i16,
                }),
            });
        }
        spans.push((start, items.len(), idx));
    }
    if items.is_empty() {
        return 0;
    }
    let Ok(results) =
        crate::pipeline::multicall::execute_tick_lens_multicall_at(provider, &items, block_number)
            .await
    else {
        return 0;
    };
    let mut loaded = 0usize;
    for (start, end, idx) in spans {
        let mut ticks: Vec<V3Tick> = Vec::new();
        for bytes in &results[start..end] {
            let Some(bytes) = bytes else {
                continue;
            };
            let Ok(populated) = ITickLens::getPopulatedTicksInWordCall::abi_decode_returns(bytes)
            else {
                continue;
            };
            for pt in populated {
                let tick = pt.tick.as_i32();
                if !(MIN_TICK..=MAX_TICK).contains(&tick) {
                    continue;
                }
                ticks.push(V3Tick {
                    tick,
                    liquidity_gross: pt.liquidityGross,
                    liquidity_net: pt.liquidityNet,
                });
            }
        }
        if ticks.is_empty() {
            continue;
        }
        finalize_cl_ticks(&mut ticks);
        if let Some(crate::core::types::PoolState::V3(s)) = arena.pool_state_mut(idx) {
            s.ticks = Arc::from(ticks);
            loaded += 1;
        }
    }
    loaded
}

pub async fn enrich_v4_ticks<
    P: alloy::providers::Provider<alloy::network::Ethereum> + Clone + Send + 'static,
>(
    provider: &P,
    arena: &mut StateArena,
    targets: &[(PoolIndex, FixedBytes<32>)],
    word_range: i16,
    block_number: Option<u64>,
    // HF probe: skip widen (2nd multicall wave) — free-tier rate limits make
    // probe+widen of empty V4 batches a self-inflicted 429 storm.
    allow_widen: bool,
) -> TickEnrichment {
    use crate::pipeline::abi_cache::{decode_abi_word, encode_extsload};
    use crate::pipeline::multicall::{MulticallItem, execute_multicall_at};
    use alloy::primitives::U256;

    if targets.is_empty() {
        return TickEnrichment::default();
    }

    let manager = UNISWAP_V4_POOL_MANAGER;
    let mut bitmap_calls = Vec::new();
    let mut spans = Vec::new();
    for &(idx, pool_id) in targets {
        let Some(crate::core::types::PoolState::V4(s)) = arena.pool_state(idx) else {
            continue;
        };
        let spacing = s.tick_spacing.max(1);
        let center_word = cl_tick_bitmap_center_word(s.tick, spacing);
        let word_min = center_word - word_range as i32;
        let word_max = center_word + word_range as i32;
        let start = bitmap_calls.len();
        for word in word_min..=word_max {
            let slot = compute_v4_tick_bitmap_slot(&pool_id, word as i16);
            bitmap_calls.push(MulticallItem {
                target: manager,
                data: encode_extsload(slot),
            });
        }
        spans.push((
            idx,
            pool_id,
            spacing,
            word_min,
            start,
            bitmap_calls.len(),
            s.liquidity,
        ));
    }
    if bitmap_calls.is_empty() {
        return TickEnrichment::default();
    }

    let bitmaps = match execute_multicall_at(provider, &bitmap_calls, block_number).await {
        Ok(bitmaps) => bitmaps,
        Err(error) => {
            crate::warn!(
                "v4 tick bitmap multicall failed ({} pools): {error:#}",
                targets.len()
            );
            return TickEnrichment {
                rpc_failed: true,
                ..TickEnrichment::default()
            };
        }
    };

    // Dense spacing=1 pools can exhaust the tick-info budget; hydrate high-liquidity
    // pools first so the cap does not starve the routes that matter.
    sort_v4_tick_spans_by_liquidity(&mut spans);

    let mut tick_calls = Vec::new();
    let mut tick_owners = Vec::new();
    let mut incomplete_pools = FxHashSet::default();
    let mut capped = false;
    for (idx, pool_id, spacing, word_min, start, end, _) in spans {
        let mut complete = true;
        let mut pool_capped = false;
        if tick_calls.len() >= MAX_CL_TICK_INFO_READS {
            incomplete_pools.insert(idx);
            capped = true;
            continue;
        }
        // Center-out word order: when capped, keep ticks nearest the current price.
        'words: for offset in cl_bitmap_center_out_offsets(end - start) {
            let Some(bytes) = bitmaps[start + offset].as_ref() else {
                complete = false;
                continue;
            };
            let Some(bitmap) = decode_abi_word(bytes) else {
                complete = false;
                continue;
            };
            for bit in 0..256u16 {
                if tick_calls.len() >= MAX_CL_TICK_INFO_READS {
                    // ponytail: stop this pool only — keep budget peers eligible
                    incomplete_pools.insert(idx);
                    capped = true;
                    pool_capped = true;
                    break 'words;
                }
                if ((bitmap >> bit) & U256::from(1u8)).is_zero() {
                    continue;
                }
                let word = word_min + offset as i32;
                let Some(tick) = cl_tick_from_bitmap_bit(word, bit, spacing) else {
                    continue;
                };
                let slot = compute_v4_tick_info_slot(&pool_id, tick);
                tick_calls.push(MulticallItem {
                    target: manager,
                    data: encode_extsload(slot),
                });
                tick_owners.push((idx, tick));
            }
        }
        if !complete && !pool_capped {
            incomplete_pools.insert(idx);
        }
    }

    let mut updated = 0usize;

    let empty_pools = if tick_calls.is_empty() {
        targets.len()
    } else {
        let states = match execute_multicall_at(provider, &tick_calls, block_number).await {
            Ok(states) => states,
            Err(error) => {
                crate::warn!(
                    "v4 tick state multicall failed ({} tick reads): {error:#}",
                    tick_calls.len(),
                );
                return TickEnrichment {
                    rpc_failed: true,
                    ..TickEnrichment::default()
                };
            }
        };

        let mut grouped: rustc_hash::FxHashMap<PoolIndex, Vec<V3Tick>> =
            rustc_hash::FxHashMap::default();
        for ((idx, tick), bytes) in tick_owners.into_iter().zip(states) {
            // Missing individual tick reads must not discard the rest of the pool —
            // partial depth beats staying tickless (shallow-cap path already gates size).
            let Some(bytes) = bytes else {
                continue;
            };
            let Some(raw) = decode_abi_word(&bytes) else {
                continue;
            };
            let (liquidity_gross, liquidity_net) = decode_v4_tick_liquidity(raw);
            if liquidity_gross > 0 {
                grouped.entry(idx).or_default().push(V3Tick {
                    tick,
                    liquidity_gross,
                    liquidity_net,
                });
            }
        }

        for (idx, mut ticks) in grouped {
            if ticks.is_empty() {
                continue;
            }
            finalize_cl_ticks(&mut ticks);
            if let Some(crate::core::types::PoolState::V4(state)) = arena.pool_state_mut(idx) {
                state.ticks = Arc::from(ticks);
                updated += 1;
            }
        }
        targets
            .iter()
            .filter(|(idx, _)| match arena.pool_state(*idx) {
                Some(crate::core::types::PoolState::V4(s)) => s.ticks.is_empty(),
                _ => false,
            })
            .count()
    };
    if !incomplete_pools.is_empty() || capped {
        crate::warn!(
            "v4 tick hydration partial/capped: incomplete_pools={} capped={capped} loaded={updated}",
            incomplete_pools.len()
        );
    }

    // Wider bitmap window for still-empty V4 (same sparse-tick pattern as V3).
    // Liquidity-ranked cap mirrors V3 — full widen of 90+ V4 empties was ~2s p50.
    let mut wide = TickEnrichment::default();
    // Only pools that saw a full hydrate (wide pass, or word_range already maxed)
    // may enter the empty cooldown — capped-out narrow misses must stay eligible.
    let mut wide_attempted: FxHashSet<FixedBytes<32>> = FxHashSet::default();
    let widen_available = allow_widen && word_range < 48;
    if updated < targets.len() && widen_available {
        let wide_range = word_range.saturating_mul(3).clamp(24, 48);
        let mut still: Vec<(PoolIndex, FixedBytes<32>, u128)> = targets
            .iter()
            .copied()
            .filter_map(|(idx, pool_id)| match arena.pool_state(idx) {
                Some(crate::core::types::PoolState::V4(s)) if s.ticks.is_empty() => {
                    Some((idx, pool_id, s.liquidity))
                }
                _ => None,
            })
            .collect();
        if !still.is_empty() {
            still.sort_unstable_by_key(|entry| std::cmp::Reverse(entry.2));
            let wide_targets: Vec<_> = still
                .into_iter()
                .take(MAX_WIDE_TICK_POOLS)
                .map(|(idx, pool_id, _)| (idx, pool_id))
                .collect();
            wide_attempted.extend(wide_targets.iter().map(|&(_, id)| id));
            wide = enrich_v4_ticks_once(provider, arena, &wide_targets, wide_range, block_number)
                .await;
            updated += wide.loaded;
        }
    }

    // Cooldown empties after a completed probe; briefly suppress re-fetch on
    // success (hot-cache tick drops). Incomplete/capped → URL fallback + short cool.
    let mut still_tickless: Vec<FixedBytes<32>> = Vec::new();
    let mut loaded_ok: Vec<FixedBytes<32>> = Vec::new();
    for &(idx, pool_id) in targets {
        match arena.pool_state(idx) {
            Some(crate::core::types::PoolState::V4(s)) if !s.ticks.is_empty() => {
                loaded_ok.push(pool_id);
            }
            Some(crate::core::types::PoolState::V4(s)) if s.ticks.is_empty() => {
                still_tickless.push(pool_id);
            }
            _ => {}
        }
    }
    if !loaded_ok.is_empty() {
        // Clear empty/timeout cool so a later tick drop can re-hydrate (see V3).
        for pool_id in loaded_ok {
            clear_v4_tick_hydrate_cooldown(pool_id);
        }
    }
    let incomplete_count = incomplete_pools.len();
    let needs_url_fallback =
        !still_tickless.is_empty() && (incomplete_count > 0 || capped || wide.rpc_failed);
    if !still_tickless.is_empty() && !needs_url_fallback {
        if allow_widen {
            // LF/dispatch: EMPTY cool only after wide attempt, or word_range already maxed.
            let cool = still_tickless
                .iter()
                .copied()
                .filter(|id| word_range >= 48 || wide_attempted.contains(id));
            mark_empty_v4_tick_cooldown(cool);
        } else {
            // HF probe narrow-only: not a full-empty proof — don't touch shared
            // EMPTY (blocks LF widen). Deprioritize for probe selection only.
            mark_probe_narrow_miss_v4(still_tickless.iter().copied());
        }
    } else if !still_tickless.is_empty() && incomplete_count > 0 {
        mark_v4_tick_hydrate_timeout_cooldown(still_tickless.iter().copied());
    }

    if empty_pools > 0 || wide.loaded > 0 || updated > 0 || wide.rpc_failed {
        for &(idx, pool_id) in targets {
            if let Some(crate::core::types::PoolState::V4(s)) = arena.pool_state(idx)
                && s.ticks.is_empty()
            {
                let addr = arena.pool_address(idx).unwrap_or_default();
                crate::debug!(
                    "v4 tick hydration miss: pool={addr} pool_id={pool_id} tick={} spacing={} liquidity={} wide_attempted={} wide_loaded={} wide_rpc_failed={}",
                    s.tick,
                    s.tick_spacing,
                    s.liquidity,
                    wide_attempted.contains(&pool_id),
                    wide.loaded,
                    wide.rpc_failed,
                );
                break;
            }
        }
    }

    TickEnrichment {
        loaded: updated,
        rpc_failed: needs_url_fallback,
        empty_pools,
        incomplete_pools: incomplete_count,
        ..TickEnrichment::default()
    }
}

/// Single-pass V4 bitmap→tick enrich without further widen (used by wide retry).
async fn enrich_v4_ticks_once<
    P: alloy::providers::Provider<alloy::network::Ethereum> + Clone + Send + 'static,
>(
    provider: &P,
    arena: &mut StateArena,
    targets: &[(PoolIndex, FixedBytes<32>)],
    word_range: i16,
    block_number: Option<u64>,
) -> TickEnrichment {
    use crate::pipeline::abi_cache::{decode_abi_word, encode_extsload};
    use crate::pipeline::multicall::{MulticallItem, execute_multicall_at};
    use alloy::primitives::U256;

    if targets.is_empty() {
        return TickEnrichment::default();
    }
    let manager = UNISWAP_V4_POOL_MANAGER;
    let mut bitmap_calls = Vec::new();
    let mut spans = Vec::new();
    for &(idx, pool_id) in targets {
        let Some(crate::core::types::PoolState::V4(s)) = arena.pool_state(idx) else {
            continue;
        };
        let spacing = s.tick_spacing.max(1);
        let center_word = cl_tick_bitmap_center_word(s.tick, spacing);
        let word_min = center_word - i32::from(word_range);
        let word_max = center_word + i32::from(word_range);
        let start = bitmap_calls.len();
        for word in word_min..=word_max {
            let slot = compute_v4_tick_bitmap_slot(&pool_id, word as i16);
            bitmap_calls.push(MulticallItem {
                target: manager,
                data: encode_extsload(slot),
            });
        }
        spans.push((idx, pool_id, spacing, word_min, start, bitmap_calls.len()));
    }
    if bitmap_calls.is_empty() {
        return TickEnrichment::default();
    }
    let Ok(bitmaps) = execute_multicall_at(provider, &bitmap_calls, block_number).await else {
        return TickEnrichment {
            rpc_failed: true,
            ..TickEnrichment::default()
        };
    };
    spans.sort_unstable_by(|a, b| {
        let liq = |idx: PoolIndex| match arena.pool_state(idx) {
            Some(crate::core::types::PoolState::V4(s)) => s.liquidity,
            _ => 0,
        };
        liq(b.0).cmp(&liq(a.0))
    });
    let mut tick_calls = Vec::new();
    let mut tick_owners = Vec::new();
    for (idx, pool_id, spacing, word_min, start, end) in spans {
        'words: for offset in cl_bitmap_center_out_offsets(end - start) {
            let Some(bytes) = bitmaps[start + offset].as_ref() else {
                continue;
            };
            let Some(bitmap) = decode_abi_word(bytes) else {
                continue;
            };
            for bit in 0..256u16 {
                if tick_calls.len() >= MAX_CL_TICK_INFO_READS {
                    break 'words;
                }
                if ((bitmap >> bit) & U256::from(1u8)).is_zero() {
                    continue;
                }
                let word = word_min + offset as i32;
                let Some(tick) = cl_tick_from_bitmap_bit(word, bit, spacing) else {
                    continue;
                };
                let slot = compute_v4_tick_info_slot(&pool_id, tick);
                tick_calls.push(MulticallItem {
                    target: manager,
                    data: encode_extsload(slot),
                });
                tick_owners.push((idx, tick));
            }
        }
    }
    if tick_calls.is_empty() {
        return TickEnrichment {
            empty_pools: targets.len(),
            ..TickEnrichment::default()
        };
    }
    let Ok(states) = execute_multicall_at(provider, &tick_calls, block_number).await else {
        return TickEnrichment {
            rpc_failed: true,
            ..TickEnrichment::default()
        };
    };
    let mut grouped: rustc_hash::FxHashMap<PoolIndex, Vec<V3Tick>> =
        rustc_hash::FxHashMap::default();
    for ((idx, tick), bytes) in tick_owners.into_iter().zip(states) {
        let Some(bytes) = bytes else {
            continue;
        };
        let Some(raw) = decode_abi_word(&bytes) else {
            continue;
        };
        let (liquidity_gross, liquidity_net) = decode_v4_tick_liquidity(raw);
        if liquidity_gross > 0 {
            grouped.entry(idx).or_default().push(V3Tick {
                tick,
                liquidity_gross,
                liquidity_net,
            });
        }
    }
    let mut loaded = 0usize;
    for (idx, mut ticks) in grouped {
        finalize_cl_ticks(&mut ticks);
        if let Some(crate::core::types::PoolState::V4(state)) = arena.pool_state_mut(idx) {
            state.ticks = Arc::from(ticks);
            loaded += 1;
        }
    }
    TickEnrichment {
        loaded,
        ..TickEnrichment::default()
    }
}

fn decode_algebra_tick_entry(bytes: &[u8], tick: i32, integral: bool) -> Option<V3Tick> {
    use crate::abis::{IAlgebraIntegralPool, IAlgebraPool};
    use alloy::sol_types::SolCall;

    if integral {
        let state = IAlgebraIntegralPool::ticksCall::abi_decode_returns(bytes).ok()?;
        let liquidity_gross = u128::try_from(state.liquidityTotal).ok()?;
        if liquidity_gross == 0 {
            return None;
        }
        Some(V3Tick {
            tick,
            liquidity_gross,
            liquidity_net: state.liquidityDelta,
        })
    } else {
        let state = IAlgebraPool::ticksCall::abi_decode_returns(bytes).ok()?;
        if !state.initialized || state.liquidityTotal == 0 {
            return None;
        }
        Some(V3Tick {
            tick,
            liquidity_gross: state.liquidityTotal,
            liquidity_net: state.liquidityDelta,
        })
    }
}

async fn enrich_algebra_ticks<
    P: alloy::providers::Provider<alloy::network::Ethereum> + Clone + Send + 'static,
>(
    provider: &P,
    arena: &mut StateArena,
    targets: &[(Address, PoolIndex, i32, i32, i32)],
    integral_pools: &FxHashSet<Address>,
    block_number: Option<u64>,
) -> TickEnrichment {
    use crate::abis::IAlgebraPool;
    use crate::pipeline::multicall::{MulticallItem, encode_call, execute_multicall_at};
    use alloy::primitives::U256;
    use alloy::sol_types::SolCall;

    if targets.is_empty() {
        return TickEnrichment::default();
    }

    // Liquidity-rank + cap: live 100+ pool tickTable multicalls rate-limit to zero.
    let mut ranked: Vec<(Address, PoolIndex, i32, i32, i32, u128)> = targets
        .iter()
        .copied()
        .map(|(pool, idx, spacing, word_min, word_max)| {
            let liq = match arena.pool_state(idx) {
                Some(crate::core::types::PoolState::V3(s)) => s.liquidity,
                _ => 0,
            };
            (pool, idx, spacing, word_min, word_max, liq)
        })
        .collect();
    ranked.sort_unstable_by_key(|entry| std::cmp::Reverse(entry.5));
    let truncated = ranked.len() > MAX_ALGEBRA_TICK_POOLS;
    ranked.truncate(MAX_ALGEBRA_TICK_POOLS);

    let word_count: usize = ranked
        .iter()
        .map(|(_, _, _, word_min, word_max, _)| (word_max - word_min + 1) as usize)
        .sum();
    let mut bitmap_calls = Vec::with_capacity(word_count);
    let mut spans = Vec::with_capacity(ranked.len());
    for &(pool, idx, spacing, word_min, word_max, _) in &ranked {
        let start = bitmap_calls.len();
        for word in word_min..=word_max {
            bitmap_calls.push(MulticallItem {
                target: pool,
                data: encode_call(&IAlgebraPool::tickTableCall {
                    wordPosition: word as i16,
                }),
            });
        }
        spans.push((pool, idx, spacing, word_min, start, bitmap_calls.len()));
    }
    if bitmap_calls.is_empty() {
        return TickEnrichment::default();
    }
    let bitmaps = match execute_multicall_at(provider, &bitmap_calls, block_number).await {
        Ok(bitmaps) => bitmaps,
        Err(error) => {
            crate::warn!(
                "algebra tick bitmap multicall failed ({} pools): {error:#}",
                ranked.len(),
            );
            return TickEnrichment {
                rpc_failed: true,
                ..TickEnrichment::default()
            };
        }
    };

    let mut tick_calls = Vec::new();
    let mut tick_owners = Vec::new();
    let mut incomplete_pools = 0usize;
    let mut no_tick_calls = 0usize;
    let mut capped = false;
    for (pool, idx, spacing, word_min, start, end) in spans {
        if tick_calls.len() >= MAX_CL_TICK_INFO_READS {
            incomplete_pools += 1;
            capped = true;
            continue;
        }
        let tick_start = tick_calls.len();
        // Center-out: when tick budget hits, keep depth nearest the current price.
        'words: for offset in cl_bitmap_center_out_offsets(end - start) {
            let Some(bytes) = bitmaps[start + offset].as_ref() else {
                continue;
            };
            let Ok(bitmap) = IAlgebraPool::tickTableCall::abi_decode_returns(bytes) else {
                continue;
            };
            let bitmap = U256::from(bitmap);
            for bit in 0..256u16 {
                if tick_calls.len() >= MAX_CL_TICK_INFO_READS {
                    // ponytail: stop this pool only — keep budget peers eligible
                    incomplete_pools += 1;
                    capped = true;
                    break 'words;
                }
                if ((bitmap >> bit) & U256::from(1u8)).is_zero() {
                    continue;
                }
                let word = word_min + offset as i32;
                let Some(tick) = cl_tick_from_bitmap_bit(word, bit, spacing) else {
                    continue;
                };
                let Ok(tick_i24) = tick.try_into() else {
                    continue;
                };
                tick_calls.push(MulticallItem {
                    target: pool,
                    data: encode_call(&IAlgebraPool::ticksCall { tick: tick_i24 }),
                });
                tick_owners.push((pool, idx, tick));
            }
        }
        if tick_calls.len() == tick_start {
            no_tick_calls += 1;
        }
    }
    if tick_calls.is_empty() {
        return TickEnrichment {
            // Truncated peers stay eligible via URL / next pass.
            rpc_failed: truncated,
            algebra_no_tick_calls: no_tick_calls,
            ..TickEnrichment::default()
        };
    }
    let states = match execute_multicall_at(provider, &tick_calls, block_number).await {
        Ok(states) => states,
        Err(error) => {
            crate::warn!(
                "algebra tick state multicall failed ({} tick reads): {error:#}",
                tick_calls.len(),
            );
            return TickEnrichment {
                rpc_failed: true,
                incomplete_pools,
                ..TickEnrichment::default()
            };
        }
    };
    let mut grouped: rustc_hash::FxHashMap<PoolIndex, Vec<V3Tick>> =
        rustc_hash::FxHashMap::default();
    let mut decode_rejects = 0usize;
    for ((pool, idx, tick), bytes) in tick_owners.into_iter().zip(states) {
        let Some(bytes) = bytes else {
            continue;
        };
        // Prefer labeled ABI, then fall back to the other Algebra ticks() layout.
        // Mis-labeled QuickSwap Integral pools otherwise stay permanently tickless.
        let prefer_integral = integral_pools.contains(&pool);
        let Some(tick_entry) = decode_algebra_tick_entry(&bytes, tick, prefer_integral)
            .or_else(|| decode_algebra_tick_entry(&bytes, tick, !prefer_integral))
        else {
            decode_rejects += 1;
            continue;
        };
        grouped.entry(idx).or_default().push(tick_entry);
    }
    let mut updated = 0;
    for (idx, mut ticks) in grouped {
        finalize_cl_ticks(&mut ticks);
        if let Some(crate::core::types::PoolState::V3(state)) = arena.pool_state_mut(idx) {
            state.ticks = Arc::from(ticks);
            updated += 1;
        }
    }
    if incomplete_pools > 0 || capped || truncated {
        crate::warn!(
            "algebra tick hydration partial/capped: incomplete_pools={incomplete_pools} capped={capped} truncated={truncated} loaded={updated}"
        );
    }
    TickEnrichment {
        loaded: updated,
        // Cap/truncate left empties → retry other URL; genuine empties cooldown upstream.
        rpc_failed: capped || truncated,
        incomplete_pools,
        algebra_no_tick_calls: no_tick_calls,
        algebra_decode_rejects: decode_rejects,
        ..TickEnrichment::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compress_cl_tick_matches_uniswap_floor_division_for_negatives() {
        assert_eq!(compress_cl_tick(61, 60), 1);
        assert_eq!(compress_cl_tick(-61, 60), -2);
        assert_eq!(compress_cl_tick(-120, 60), -2);
        assert_eq!(compress_cl_tick(0, 60), 0);
    }

    #[test]
    fn bitmap_bit_reconstructs_spacing_aligned_tick() {
        assert_eq!(cl_tick_from_bitmap_bit(0, 1, 60), Some(60));
        assert_eq!(cl_tick_from_bitmap_bit(-1, 0, 60), Some(-256 * 60));
    }

    #[test]
    fn cl_bitmap_center_out_offsets_visits_mid_first() {
        assert!(cl_bitmap_center_out_offsets(0).is_empty());
        assert_eq!(cl_bitmap_center_out_offsets(1), vec![0]);
        assert_eq!(cl_bitmap_center_out_offsets(5), vec![2, 1, 3, 0, 4]);
        let mut seen = cl_bitmap_center_out_offsets(21);
        assert_eq!(seen.len(), 21);
        assert_eq!(seen[0], 10);
        seen.sort_unstable();
        assert_eq!(seen, (0..21).collect::<Vec<_>>());
    }

    #[test]
    fn v4_tick_spans_prioritize_liquidity_descending() {
        let mut spans = vec![
            (PoolIndex(0), FixedBytes::ZERO, 1, 0, 0, 1, 10),
            (PoolIndex(1), FixedBytes::ZERO, 1, 0, 1, 2, 30),
            (PoolIndex(2), FixedBytes::ZERO, 1, 0, 2, 3, 20),
        ];

        sort_v4_tick_spans_by_liquidity(&mut spans);

        assert_eq!(
            spans.iter().map(|span| span.0).collect::<Vec<_>>(),
            vec![PoolIndex(1), PoolIndex(2), PoolIndex(0)]
        );
    }

    #[test]
    fn finalize_cl_ticks_dedups_duplicate_indices() {
        let mut ticks = vec![
            V3Tick {
                tick: 60,
                liquidity_gross: 1,
                liquidity_net: 1,
            },
            V3Tick {
                tick: 60,
                liquidity_gross: 2,
                liquidity_net: 2,
            },
            V3Tick {
                tick: -60,
                liquidity_gross: 3,
                liquidity_net: 3,
            },
        ];
        finalize_cl_ticks(&mut ticks);
        assert_eq!(ticks.len(), 2);
        assert_eq!(ticks[0].tick, -60);
        assert_eq!(ticks[1].tick, 60);
    }

    #[test]
    fn tick_enrichment_combines_provider_failures() {
        let result = TickEnrichment {
            loaded: 3,
            ..TickEnrichment::default()
        }
        .combine(TickEnrichment {
            loaded: 2,
            rpc_failed: true,
            ..TickEnrichment::default()
        });

        assert_eq!(result.loaded, 5);
        assert!(result.rpc_failed);
    }

    #[test]
    fn still_tickless_v3_filters_hydrated_pools() {
        use crate::core::types::{PoolState, V3PoolState, V3Tick};
        use alloy::primitives::{Address, U256};
        use std::sync::Arc;

        let empty_addr = Address::from([1u8; 20]);
        let hydrated_addr = Address::from([2u8; 20]);
        let mut arena = StateArena::default();
        arena.register_pool(
            empty_addr,
            Arc::new(PoolState::V3(V3PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                liquidity: 1,
                tick: 0,
                fee: U256::from(3000u32),
                tick_spacing: 60,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
                ticks: Arc::from([]),
            })),
        );
        arena.register_pool(
            hydrated_addr,
            Arc::new(PoolState::V3(V3PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                liquidity: 1,
                tick: 0,
                fee: U256::from(3000u32),
                tick_spacing: 60,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
                ticks: Arc::from([V3Tick {
                    tick: -60,
                    liquidity_gross: 1,
                    liquidity_net: 1,
                }]),
            })),
        );
        let still = still_tickless_v3(&arena, &[empty_addr, hydrated_addr]);
        assert_eq!(still, vec![empty_addr]);
    }

    #[test]
    fn v4_probe_narrow_miss_cooldown_lifecycle() {
        let pool_id = FixedBytes::from([0xab; 32]);
        assert!(!is_probe_narrow_miss_v4_on_cooldown(pool_id));
        assert!(!is_empty_v4_tick_on_cooldown(pool_id));

        mark_probe_narrow_miss_v4([pool_id]);
        assert!(is_probe_narrow_miss_v4_on_cooldown(pool_id));
        // Does not touch shared EMPTY_V4_TICK (which would block LF widen).
        assert!(!is_empty_v4_tick_on_cooldown(pool_id));

        clear_v4_tick_hydrate_cooldown(pool_id);
        assert!(!is_probe_narrow_miss_v4_on_cooldown(pool_id));
    }
}
