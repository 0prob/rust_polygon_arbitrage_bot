use std::sync::Arc;

use alloy::network::Ethereum;
use alloy::primitives::{Address, FixedBytes};
use alloy::providers::Provider;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::core::protocol::is_fetchable_protocol;
use crate::core::types::ProtocolType;
use crate::pipeline::pool_fetch::fetch_pools_batched;
use crate::services::discovery::DiscoveredPool;
use crate::services::state_cache::StateCache;

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

pub fn resolve_v4_pool_id(pool: &DiscoveredPool) -> Option<FixedBytes<32>> {
    if let Some(id) = pool.pool_id {
        return Some(id);
    }
    if pool.pool_key.len() == 66 {
        return pool.pool_key.parse().ok();
    }
    None
}

/// Fetch up to `max_pools` missing/stale pools and write into `cache`.
/// Pools whose addresses appear in `priority` are fetched first.
pub async fn fetch_missing_pool_states<P: Provider<Ethereum> + Clone>(
    provider: P,
    cache: Arc<StateCache>,
    pools: &[DiscoveredPool],
    max_pools: usize,
    max_multicall_calls: usize,
    priority: &[Address],
) -> (usize, bool) {
    let targets = select_fetch_targets(pools, cache.as_ref(), max_pools, priority);
    if targets.is_empty() {
        return (0, false);
    }
    (
        fetch_pools_batched(provider, cache, &targets, max_multicall_calls).await,
        true,
    )
}

fn select_fetch_targets<'a>(
    pools: &'a [DiscoveredPool],
    cache: &StateCache,
    max_pools: usize,
    priority: &[Address],
) -> Vec<&'a DiscoveredPool> {
    if max_pools == 0 {
        return Vec::new();
    }

    let fetchable: Vec<&'a DiscoveredPool> = pools
        .iter()
        .filter(|p| is_fetchable_protocol(p.protocol))
        .collect();

    let addresses: Vec<Address> = fetchable.iter().map(|p| p.address).collect();
    let (never, invalid, stale) = cache.classify_for_fetch(&addresses);

    let priority_set: FxHashSet<Address> = priority.iter().copied().collect();
    let never_set: FxHashSet<Address> = never.iter().map(|&&a| a).collect();
    let invalid_set: FxHashSet<Address> = invalid.iter().map(|&&a| a).collect();
    let stale_set: FxHashSet<Address> = stale.iter().map(|&&a| a).collect();

    // Per-protocol queues in fetch-priority order (never → invalid → stale), deduped.
    let mut per_protocol: FxHashMap<ProtocolType, Vec<&'a DiscoveredPool>> = FxHashMap::default();
    let mut seen: FxHashSet<Address> = FxHashSet::default();
    for bucket in [&never_set, &invalid_set, &stale_set] {
        for pool in &fetchable {
            if !bucket.contains(&pool.address) || !seen.insert(pool.address) {
                continue;
            }
            per_protocol.entry(pool.protocol).or_default().push(pool);
        }
    }

    let mut out: Vec<&'a DiscoveredPool> = Vec::with_capacity(max_pools);
    let mut selected: FxHashSet<Address> = FxHashSet::default();

    // HF hot pools first, any protocol.
    for proto in FETCHABLE_PROTOCOLS {
        let Some(queue) = per_protocol.get_mut(&proto) else {
            continue;
        };
        let mut i = 0;
        while i < queue.len() && out.len() < max_pools {
            let pool = queue[i];
            if priority_set.contains(&pool.address) && selected.insert(pool.address) {
                out.push(pool);
                queue.remove(i);
            } else {
                i += 1;
            }
        }
    }

    // Round-robin across protocol families so V3/V4/Curve are not starved by V2 volume.
    loop {
        if out.len() >= max_pools {
            break;
        }
        let mut progressed = false;
        for proto in FETCHABLE_PROTOCOLS {
            if out.len() >= max_pools {
                break;
            }
            let Some(queue) = per_protocol.get_mut(&proto) else {
                continue;
            };
            while let Some(pool) = queue.first().copied() {
                queue.remove(0);
                if selected.insert(pool.address) {
                    out.push(pool);
                    progressed = true;
                    break;
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
    use crate::core::types::{PoolState, ProtocolType};
    use crate::services::state_cache::StateCache;
    use alloy::primitives::Address;

    #[test]
    fn selects_unfetched_pools_in_order() {
        let cache = StateCache::default();
        let pool_a = DiscoveredPool {
            pool_key: "0x0000000000000000000000000000000000000001".into(),
            address: Address::repeat_byte(1),
            protocol: ProtocolType::UniswapV2,
            protocol_label: "V2".into(),
            tokens: vec![Address::repeat_byte(2), Address::repeat_byte(3)],
            fee_bps: 30,
            tick_spacing: None,
            pool_id: None,
            hooks: None,
            pool_type: None,
            created_block: 0,
        };
        let pool_b = DiscoveredPool {
            pool_key: "0x0000000000000000000000000000000000000002".into(),
            address: Address::repeat_byte(2),
            protocol: ProtocolType::UniswapV2,
            protocol_label: "V2".into(),
            tokens: vec![Address::repeat_byte(4), Address::repeat_byte(5)],
            fee_bps: 30,
            tick_spacing: None,
            pool_id: None,
            hooks: None,
            pool_type: None,
            created_block: 0,
        };
        let pools = [pool_a, pool_b];
        let targets = select_fetch_targets(&pools, &cache, 1, &[]);
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].address, Address::repeat_byte(1));
    }

    #[test]
    fn includes_curve_in_fetch_targets() {
        let cache = StateCache::default();
        let pool = DiscoveredPool {
            pool_key: "0x0000000000000000000000000000000000000001".into(),
            address: Address::repeat_byte(1),
            protocol: ProtocolType::CurveStable,
            protocol_label: "CURVE".into(),
            tokens: vec![Address::repeat_byte(2), Address::repeat_byte(3)],
            fee_bps: 4,
            tick_spacing: None,
            pool_id: None,
            hooks: None,
            pool_type: None,
            created_block: 0,
        };
        let pools = [pool];
        let targets = select_fetch_targets(&pools, &cache, 10, &[]);
        assert_eq!(targets.len(), 1);
    }

    #[test]
    fn prioritizes_never_fetched_over_invalid_retries() {
        use std::time::Duration;

        let cache = StateCache::default().with_ttls(Duration::ZERO, Duration::from_secs(120));
        let invalid_addr = Address::repeat_byte(1);
        let never_addr = Address::repeat_byte(2);
        cache.insert(invalid_addr, PoolState::Invalid);
        let pool_invalid = DiscoveredPool {
            pool_key: format!("{invalid_addr}"),
            address: invalid_addr,
            protocol: ProtocolType::UniswapV2,
            protocol_label: "V2".into(),
            tokens: vec![Address::repeat_byte(3), Address::repeat_byte(4)],
            fee_bps: 30,
            tick_spacing: None,
            pool_id: None,
            hooks: None,
            pool_type: None,
            created_block: 0,
        };
        let pool_never = DiscoveredPool {
            pool_key: format!("{never_addr}"),
            address: never_addr,
            protocol: ProtocolType::UniswapV2,
            protocol_label: "V2".into(),
            tokens: vec![Address::repeat_byte(5), Address::repeat_byte(6)],
            fee_bps: 30,
            tick_spacing: None,
            pool_id: None,
            hooks: None,
            pool_type: None,
            created_block: 0,
        };
        let pools = [pool_invalid, pool_never];
        let targets = select_fetch_targets(&pools, &cache, 1, &[]);
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].address, never_addr);
    }

    #[test]
    fn round_robins_across_protocols() {
        let cache = StateCache::default();
        let mk = |byte: u8, protocol: ProtocolType, label: &str| DiscoveredPool {
            pool_key: format!("0x{:040x}", byte),
            address: Address::repeat_byte(byte),
            protocol,
            protocol_label: label.into(),
            tokens: vec![Address::repeat_byte(byte.wrapping_add(1)), Address::repeat_byte(byte.wrapping_add(2))],
            fee_bps: 30,
            tick_spacing: None,
            pool_id: None,
            hooks: None,
            pool_type: None,
            created_block: 0,
        };
        // Six V2 pools listed before V3 — old FIFO logic would starve V3 in a batch of 3.
        let pools = [
            mk(1, ProtocolType::UniswapV2, "V2"),
            mk(2, ProtocolType::UniswapV2, "V2"),
            mk(3, ProtocolType::UniswapV2, "V2"),
            mk(4, ProtocolType::UniswapV2, "V2"),
            mk(5, ProtocolType::UniswapV2, "V2"),
            mk(6, ProtocolType::UniswapV2, "V2"),
            mk(7, ProtocolType::UniswapV3, "V3"),
            mk(8, ProtocolType::BalancerV2, "BAL"),
        ];
        let targets = select_fetch_targets(&pools, &cache, 3, &[]);
        assert_eq!(targets.len(), 3);
        let protos: Vec<ProtocolType> = targets.iter().map(|p| p.protocol).collect();
        assert!(
            protos.contains(&ProtocolType::UniswapV3),
            "V3 should appear in stratified batch: {protos:?}"
        );
        assert!(
            protos.contains(&ProtocolType::BalancerV2),
            "Balancer should appear in stratified batch: {protos:?}"
        );
    }
}
