use std::sync::Arc;

use alloy::network::Ethereum;
use alloy::primitives::Address;
use alloy::providers::Provider;
use rustc_hash::FxHashSet;

use crate::core::constants::is_polygon_hub_token;
use crate::core::protocol::is_fetchable_protocol;
use crate::core::types::ProtocolType;
use crate::infra::pool_meta_cache::PoolMetaCache;
use crate::pipeline::pool_fetch::fetch_pools_batched;
use crate::services::discovery::DiscoveredPool;
use crate::services::state_cache::StateCache;

/// Extra never-fetched slots per LF batch for CLMM / multi-token protocols that
/// are often starved by V2 volume in plain round-robin.
const CLMM_MULTI_FETCH_BIAS: usize = 2;

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

/// Fetch up to `max_pools` missing/stale pools and write into `cache`.
/// Pools whose addresses appear in `priority` are fetched first.
/// Targets are grouped by protocol and fetched concurrently via `tokio::join!`
/// to maximize RPC throughput when multiple protocol families need refresh.
#[allow(clippy::too_many_arguments)]
pub async fn fetch_missing_pool_states<P: Provider<Ethereum> + Clone + Send + 'static>(
    provider: P,
    cache: Arc<StateCache>,
    pools: &[&DiscoveredPool],
    max_pools: usize,
    max_multicall_calls: usize,
    batch_pace_ms: u64,
    priority: &[Address],
    block_number: Option<u64>,
    meta_cache: &PoolMetaCache,
) -> (usize, bool) {
    let targets = select_fetch_targets(pools.iter().copied(), cache.as_ref(), max_pools, priority);
    if targets.is_empty() {
        return (0, false);
    }

    // Group targets by protocol so we can parallelize fetch across protocol families.
    // V2 gets its own group (dominant count), the rest are batched together.
    let mut v2_targets: Vec<&DiscoveredPool> = Vec::new();
    let mut other_targets: Vec<&DiscoveredPool> = Vec::new();
    for &t in &targets {
        if t.protocol == ProtocolType::UniswapV2 {
            v2_targets.push(t);
        } else {
            other_targets.push(t);
        }
    }

    // ponytail: only split into 2 groups (V2 vs rest) — finer splits starve individual
    // multicall batching. The chunk-level parallelism inside fetch_pools_batched
    // (8 concurrent chunks) provides further parallelism.
    let provider2 = provider.clone();
    let cache2 = Arc::clone(&cache);
    let (updated_v2, updated_other) = tokio::join!(
        async {
            if v2_targets.is_empty() {
                0usize
            } else {
                fetch_pools_batched(
                    provider.clone(),
                    Arc::clone(&cache),
                    &v2_targets,
                    max_multicall_calls,
                    batch_pace_ms,
                    block_number,
                    meta_cache,
                )
                .await
            }
        },
        async {
            if other_targets.is_empty() {
                0usize
            } else {
                fetch_pools_batched(
                    provider2,
                    cache2,
                    &other_targets,
                    max_multicall_calls,
                    batch_pace_ms,
                    block_number,
                    meta_cache,
                )
                .await
            }
        },
    );
    (updated_v2 + updated_other, true)
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

    // Per-protocol queues with priority tag — one cache read-lock for classification.
    let mut per_protocol: [Vec<(&'a DiscoveredPool, u8)>; FETCHABLE_PROTOCOLS.len()] =
        std::array::from_fn(|_| Vec::new());
    cache.for_each_fetch_candidate(
        pools
            .into_iter()
            .filter(|p| is_fetchable_protocol(p.protocol)),
        |pool, class| {
            if let Some(slot) = pool.protocol.fetch_slot() {
                per_protocol[slot].push((pool, class));
            }
        },
    );

    for queue in &mut per_protocol {
        // Fetch never-seen pools before retries/stale refreshes, hub-connected
        // pools before long-tail spokes, and newest creation events first inside
        // each class. Hub-first ordering grows routable graph coverage faster.
        queue.sort_by_key(|(p, class)| {
            let hub_connected = p.tokens.iter().any(|t| is_polygon_hub_token(*t));
            (*class, !hub_connected, std::cmp::Reverse(p.created_block))
        });
    }

    let mut out: Vec<&'a DiscoveredPool> = Vec::with_capacity(max_pools);
    let mut selected: FxHashSet<Address> = FxHashSet::default();

    if !priority_set.is_empty() {
        for queue in &mut per_protocol {
            queue.retain(|(pool, _)| {
                if out.len() < max_pools
                    && priority_set.contains(&pool.address)
                    && selected.insert(pool.address)
                {
                    out.push(pool);
                    false
                } else {
                    true
                }
            });
        }
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
                ProtocolType::UniswapV3
                | ProtocolType::UniswapV4
                | ProtocolType::BalancerV2
                | ProtocolType::CurveStable
                | ProtocolType::CurveCrypto => CLMM_MULTI_FETCH_BIAS,
                _ => 1,
            };
            let mut taken = 0usize;
            while *cursor < queue.len() && taken < take {
                let (pool, _) = queue[*cursor];
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
