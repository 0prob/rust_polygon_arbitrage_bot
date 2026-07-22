use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use alloy::network::Ethereum;
use alloy::primitives::Address;
use alloy::providers::Provider;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::core::constants::is_polygon_hub_token;
use crate::core::protocol::is_fetchable_protocol;
use crate::core::types::ProtocolType;
use crate::infra::pool_meta_cache::PoolMetaCache;
use crate::pipeline::pool_fetch::{PoolFetchResult, fetch_pools_batched};
use crate::services::discovery::DiscoveredPool;
use crate::services::state_cache::StateCache;

/// Extra never-fetched slots per LF batch for CLMM / multi-token protocols that
/// are often starved by V2 volume in plain round-robin.
const CLMM_MULTI_FETCH_BIAS: usize = 2;
/// Extra V4 hydration slots per round — large indexer set, singleton-hub graph depends on live slot0.
const V4_FETCH_BIAS: usize = 4;
/// Discovery rows scanned per LF pass for never-fetched pools (full list is ~260k).
const NEVER_FETCH_SCAN_CHUNK: usize = 12_288;

#[derive(Debug, Clone, Default)]
pub struct FetchTargetsResult {
    pub updated: usize,
    pub attempted: bool,
    pub rate_limited: bool,
    /// Addresses selected for this attempt (for incremental URL fallback).
    pub targeted: Vec<Address>,
}

impl FetchTargetsResult {
    #[must_use]
    pub fn requires_provider_fallback(&self) -> bool {
        self.attempted && (self.updated == 0 || self.rate_limited)
    }
}

/// Fetchable protocol families — round-robin ensures each gets hydration slots per batch.
const FETCHABLE_PROTOCOLS: [ProtocolType; 8] = [
    ProtocolType::UniswapV2,
    ProtocolType::UniswapV3,
    ProtocolType::UniswapV4,
    ProtocolType::BalancerV2,
    ProtocolType::CurveStable,
    ProtocolType::CurveCrypto,
    ProtocolType::Dodo,
    ProtocolType::Woofi,
];

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
struct FetchRank(u8, bool, Reverse<u64>);

#[derive(Clone, Copy)]
struct RankedPool<'a> {
    rank: FetchRank,
    pool: &'a DiscoveredPool,
}

impl PartialEq for RankedPool<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.rank == other.rank && self.pool.address == other.pool.address
    }
}

impl Eq for RankedPool<'_> {}

impl PartialOrd for RankedPool<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RankedPool<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.rank
            .cmp(&other.rank)
            .then_with(|| self.pool.address.cmp(&other.pool.address))
    }
}

fn fetch_rank(pool: &DiscoveredPool, class: u8) -> FetchRank {
    FetchRank(
        class,
        !pool.tokens.iter().any(|token| is_polygon_hub_token(*token)),
        Reverse(pool.created_block),
    )
}

/// Fetch up to `max_pools` missing/stale pools and write into `cache`.
/// Pools whose addresses appear in `priority` are fetched first.
/// Targets are grouped by protocol and fetched concurrently via `tokio::join!`
/// to maximize RPC throughput when multiple protocol families need refresh.
#[allow(clippy::too_many_arguments)]
pub async fn fetch_missing_pool_states<P: Provider<Ethereum> + Clone + Send + 'static>(
    provider: P,
    cache: Arc<StateCache>,
    pools: &[DiscoveredPool],
    max_pools: usize,
    max_multicall_calls: usize,
    batch_pace_ms: u64,
    priority: &[Address],
    block_number: Option<u64>,
    meta_cache: &PoolMetaCache,
) -> FetchTargetsResult {
    let targets = select_fetch_targets(pools.iter(), cache.as_ref(), max_pools, priority);
    if targets.is_empty() {
        return FetchTargetsResult::default();
    }
    run_fetch_targets(
        provider,
        cache,
        &targets,
        max_multicall_calls,
        batch_pace_ms,
        block_number,
        meta_cache,
    )
    .await
}

/// Full-discovery refresh: cache-first stale/invalid scan + rotating never-fetched window.
#[allow(clippy::too_many_arguments)]
pub async fn fetch_missing_pool_states_indexed<P: Provider<Ethereum> + Clone + Send + 'static>(
    provider: P,
    cache: Arc<StateCache>,
    pools: &[DiscoveredPool],
    address_index: &FxHashMap<Address, usize>,
    never_scan_cursor: &AtomicUsize,
    max_pools: usize,
    max_multicall_calls: usize,
    batch_pace_ms: u64,
    priority: &[Address],
    block_number: Option<u64>,
    meta_cache: &PoolMetaCache,
) -> FetchTargetsResult {
    let targets = select_fetch_targets_indexed(
        pools,
        address_index,
        never_scan_cursor,
        cache.as_ref(),
        max_pools,
        priority,
    );
    if targets.is_empty() {
        return FetchTargetsResult::default();
    }
    run_fetch_targets(
        provider,
        cache,
        &targets,
        max_multicall_calls,
        batch_pace_ms,
        block_number,
        meta_cache,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn fetch_pool_states_at_addresses<P: Provider<Ethereum> + Clone + Send + 'static>(
    provider: P,
    cache: Arc<StateCache>,
    pools: &[DiscoveredPool],
    address_index: &FxHashMap<Address, usize>,
    addresses: &[Address],
    max_multicall_calls: usize,
    batch_pace_ms: u64,
    block_number: Option<u64>,
    meta_cache: &PoolMetaCache,
) -> FetchTargetsResult {
    let targets = select_fetch_targets_at_addresses(pools, address_index, addresses);
    if targets.is_empty() {
        return FetchTargetsResult::default();
    }
    run_fetch_targets(
        provider,
        cache,
        &targets,
        max_multicall_calls,
        batch_pace_ms,
        block_number,
        meta_cache,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_fetch_targets<P: Provider<Ethereum> + Clone + Send + 'static>(
    provider: P,
    cache: Arc<StateCache>,
    targets: &[&DiscoveredPool],
    max_multicall_calls: usize,
    batch_pace_ms: u64,
    block_number: Option<u64>,
    meta_cache: &PoolMetaCache,
) -> FetchTargetsResult {
    let mut v2_targets: Vec<&DiscoveredPool> = Vec::new();
    let mut other_targets: Vec<&DiscoveredPool> = Vec::new();
    for &t in targets {
        if t.protocol == ProtocolType::UniswapV2 {
            v2_targets.push(t);
        } else {
            other_targets.push(t);
        }
    }

    let provider2 = provider.clone();
    let cache2 = Arc::clone(&cache);
    let (v2_result, other_result) = tokio::join!(
        async {
            let started = crate::util::now_ms();
            if v2_targets.is_empty() {
                (PoolFetchResult::default(), 0u64)
            } else {
                let updated = fetch_pools_batched(
                    provider.clone(),
                    Arc::clone(&cache),
                    &v2_targets,
                    max_multicall_calls,
                    batch_pace_ms,
                    block_number,
                    meta_cache,
                )
                .await;
                (updated, crate::util::now_ms().saturating_sub(started))
            }
        },
        async {
            let started = crate::util::now_ms();
            if other_targets.is_empty() {
                (PoolFetchResult::default(), 0u64)
            } else {
                let updated = fetch_pools_batched(
                    provider2,
                    cache2,
                    &other_targets,
                    max_multicall_calls,
                    batch_pace_ms,
                    block_number,
                    meta_cache,
                )
                .await;
                (updated, crate::util::now_ms().saturating_sub(started))
            }
        },
    );
    let (v2_fetch, v2_ms) = v2_result;
    let (other_fetch, other_ms) = other_result;
    let updated_v2 = v2_fetch.updated;
    let updated_other = other_fetch.updated;
    crate::debug!(
        "pool fetch branches: targets={} v2_targets={} other_targets={} v2_updated={updated_v2} v2_rate_limited={} other_updated={updated_other} other_rate_limited={} v2_ms={v2_ms} other_ms={other_ms} max_multicall_calls={max_multicall_calls} batch_pace_ms={batch_pace_ms} pinned_block={block_number:?}",
        targets.len(),
        v2_targets.len(),
        other_targets.len(),
        v2_fetch.rate_limited,
        other_fetch.rate_limited,
    );
    FetchTargetsResult {
        updated: updated_v2.saturating_add(updated_other),
        attempted: true,
        rate_limited: v2_fetch.rate_limited || other_fetch.rate_limited,
        targeted: targets.iter().map(|t| t.address).collect(),
    }
}

fn select_fetch_targets<'a>(
    pools: impl IntoIterator<Item = &'a DiscoveredPool>,
    cache: &StateCache,
    max_pools: usize,
    priority: &[Address],
) -> Vec<&'a DiscoveredPool> {
    if max_pools == 0 {
        return Vec::new();
    }

    let priority_set: FxHashSet<Address> = priority.iter().copied().collect();

    let mut priority_candidates = Vec::with_capacity(priority_set.len());
    let mut per_protocol: [BinaryHeap<RankedPool<'a>>; FETCHABLE_PROTOCOLS.len()] =
        std::array::from_fn(|_| BinaryHeap::new());
    cache.for_each_fetch_candidate(
        pools
            .into_iter()
            .filter(|p| is_fetchable_protocol(p.protocol)),
        |pool, class| {
            enqueue_fetch_candidate(
                pool,
                class,
                &priority_set,
                max_pools,
                &mut priority_candidates,
                &mut per_protocol,
            );
        },
    );

    finalize_fetch_targets(max_pools, priority_candidates, per_protocol)
}

fn select_fetch_targets_indexed<'a>(
    pools: &'a [DiscoveredPool],
    address_index: &FxHashMap<Address, usize>,
    never_scan_cursor: &AtomicUsize,
    cache: &StateCache,
    max_pools: usize,
    priority: &[Address],
) -> Vec<&'a DiscoveredPool> {
    if max_pools == 0 || pools.is_empty() {
        return Vec::new();
    }

    let priority_set: FxHashSet<Address> = priority.iter().copied().collect();
    let mut priority_candidates = Vec::with_capacity(priority_set.len());
    let mut per_protocol: [BinaryHeap<RankedPool<'a>>; FETCHABLE_PROTOCOLS.len()] =
        std::array::from_fn(|_| BinaryHeap::new());

    cache.for_each_cached_fetch_candidate(address_index, pools, |pool, class| {
        enqueue_fetch_candidate(
            pool,
            class,
            &priority_set,
            max_pools,
            &mut priority_candidates,
            &mut per_protocol,
        );
    });

    let scan_budget = NEVER_FETCH_SCAN_CHUNK.min(pools.len());
    let start = never_scan_cursor.fetch_add(scan_budget, Ordering::Relaxed) % pools.len();
    let missing = cache.missing_addresses(
        (0..scan_budget).map(|offset| pools[(start + offset) % pools.len()].address),
    );
    for offset in 0..scan_budget {
        let pool = &pools[(start + offset) % pools.len()];
        if !is_fetchable_protocol(pool.protocol) {
            continue;
        }
        if !missing.contains(&pool.address) {
            continue;
        }
        enqueue_fetch_candidate(
            pool,
            1,
            &priority_set,
            max_pools,
            &mut priority_candidates,
            &mut per_protocol,
        );
    }

    finalize_fetch_targets(max_pools, priority_candidates, per_protocol)
}

fn select_fetch_targets_at_addresses<'a>(
    pools: &'a [DiscoveredPool],
    address_index: &FxHashMap<Address, usize>,
    addresses: &[Address],
) -> Vec<&'a DiscoveredPool> {
    addresses
        .iter()
        .filter_map(|address| address_index.get(address).and_then(|&idx| pools.get(idx)))
        .collect()
}

fn enqueue_fetch_candidate<'a>(
    pool: &'a DiscoveredPool,
    class: u8,
    priority_set: &FxHashSet<Address>,
    max_pools: usize,
    priority_candidates: &mut Vec<&'a DiscoveredPool>,
    per_protocol: &mut [BinaryHeap<RankedPool<'a>>; FETCHABLE_PROTOCOLS.len()],
) {
    if priority_set.contains(&pool.address) {
        priority_candidates.push(pool);
    } else if let Some(slot) = pool.protocol.fetch_slot() {
        let ranked = RankedPool {
            rank: fetch_rank(pool, class),
            pool,
        };
        let queue = &mut per_protocol[slot];
        if queue.len() < max_pools {
            queue.push(ranked);
        } else if queue.peek().is_some_and(|worst| ranked < *worst) {
            let _ = queue.pop();
            queue.push(ranked);
        }
    }
}

fn finalize_fetch_targets<'a>(
    max_pools: usize,
    priority_candidates: Vec<&'a DiscoveredPool>,
    per_protocol: [BinaryHeap<RankedPool<'a>>; FETCHABLE_PROTOCOLS.len()],
) -> Vec<&'a DiscoveredPool> {
    let mut out: Vec<&'a DiscoveredPool> = Vec::with_capacity(max_pools);
    let mut selected: FxHashSet<Address> = FxHashSet::default();
    for pool in priority_candidates {
        if out.len() == max_pools {
            return out;
        }
        if selected.insert(pool.address) {
            out.push(pool);
        }
    }

    let mut per_protocol: [Vec<RankedPool<'a>>; FETCHABLE_PROTOCOLS.len()] =
        per_protocol.map(BinaryHeap::into_vec);
    for queue in &mut per_protocol {
        queue.sort_unstable();
    }

    let mut cursors = [0usize; FETCHABLE_PROTOCOLS.len()];

    loop {
        if out.len() >= max_pools {
            break;
        }
        let mut progressed = false;
        for slot in 0..FETCHABLE_PROTOCOLS.len() {
            if out.len() >= max_pools {
                break;
            }
            let queue = &per_protocol[slot];
            let cursor = &mut cursors[slot];
            let take = match FETCHABLE_PROTOCOLS[slot] {
                ProtocolType::UniswapV4 => V4_FETCH_BIAS,
                ProtocolType::UniswapV3
                | ProtocolType::BalancerV2
                | ProtocolType::CurveStable
                | ProtocolType::CurveCrypto => CLMM_MULTI_FETCH_BIAS,
                _ => 1,
            };
            let mut taken = 0usize;
            while *cursor < queue.len() && taken < take {
                let pool = queue[*cursor].pool;
                *cursor += 1;
                if selected.insert(pool.address) {
                    out.push(pool);
                    progressed = true;
                    taken += 1;
                }
            }
        }
        if !progressed {
            break;
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool(address_byte: u8, created_block: u64) -> DiscoveredPool {
        DiscoveredPool {
            pool_key: format!("{address_byte:02x}"),
            address: Address::from([address_byte; 20]),
            protocol: ProtocolType::UniswapV2,
            protocol_label: "UNISWAP_V2".into(),
            tokens: vec![Address::from([1u8; 20]), Address::from([2u8; 20])],
            fee_bps: 30,
            tick_spacing: None,
            pool_id: None,
            pool_id_verified: false,
            hooks: None,
            pool_type: None,
            created_block,
        }
    }

    #[test]
    fn target_selection_keeps_priority_outside_ranked_prefix() {
        let pools = [pool(1, 3), pool(2, 2), pool(3, 1)];
        let cache = StateCache::default();
        let selected = select_fetch_targets(&pools, &cache, 1, &[pools[2].address]);

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].address, pools[2].address);
    }

    #[test]
    fn partial_rate_limited_fetch_requires_provider_fallback() {
        let result = FetchTargetsResult {
            updated: 3,
            attempted: true,
            rate_limited: true,
            ..FetchTargetsResult::default()
        };

        assert!(result.requires_provider_fallback());
    }

    #[test]
    fn complete_partial_fetch_does_not_require_provider_fallback() {
        let result = FetchTargetsResult {
            updated: 3,
            attempted: true,
            rate_limited: false,
            ..FetchTargetsResult::default()
        };

        assert!(!result.requires_provider_fallback());
    }

    #[test]
    fn indexed_retry_targets_borrow_discovery_rows() {
        let pools = [pool(1, 2), pool(2, 1)];
        let address_index = FxHashMap::from_iter(
            pools
                .iter()
                .enumerate()
                .map(|(idx, pool)| (pool.address, idx)),
        );
        let selected = select_fetch_targets_at_addresses(
            &pools,
            &address_index,
            &[pools[1].address, Address::ZERO],
        );

        assert_eq!(selected.len(), 1);
        assert!(std::ptr::eq(selected[0], &pools[1]));
    }
}
