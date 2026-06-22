use std::sync::Arc;

use alloy::network::Ethereum;
use alloy::primitives::{Address, FixedBytes};
use alloy::providers::Provider;

use crate::core::protocol::is_fetchable_protocol;
use crate::pipeline::pool_fetch::fetch_pools_batched;
use crate::services::discovery::DiscoveredPool;
use crate::services::state_cache::StateCache;

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
) -> usize {
    let targets = select_fetch_targets(pools, cache.as_ref(), max_pools, priority);
    if targets.is_empty() {
        return 0;
    }
    fetch_pools_batched(provider, cache, &targets, max_multicall_calls).await
}

fn select_fetch_targets<'a>(
    pools: &'a [DiscoveredPool],
    cache: &StateCache,
    max_pools: usize,
    priority: &[Address],
) -> Vec<&'a DiscoveredPool> {
    let fetchable: Vec<&'a DiscoveredPool> = pools
        .iter()
        .filter(|p| is_fetchable_protocol(p.protocol))
        .collect();

    let addresses: Vec<Address> = fetchable.iter().map(|p| p.address).collect();
    let (never, invalid, stale) = cache.classify_for_fetch(&addresses);

    let priority_set: std::collections::HashSet<Address> = priority.iter().copied().collect();
    let never_set: std::collections::HashSet<Address> = never.iter().map(|&&a| a).collect();
    let invalid_set: std::collections::HashSet<Address> = invalid.iter().map(|&&a| a).collect();
    let stale_set: std::collections::HashSet<Address> = stale.iter().map(|&&a| a).collect();

    let mut out = Vec::with_capacity(max_pools);
    for bucket in [&never_set, &invalid_set, &stale_set] {
        for pool in &fetchable {
            if out.len() >= max_pools {
                return out;
            }
            if !bucket.contains(&pool.address) {
                continue;
            }
            if priority_set.contains(&pool.address) {
                out.insert(0, pool);
            } else {
                out.push(pool);
            }
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
            created_block: 0,
        };
        let pools = [pool_invalid, pool_never];
        let targets = select_fetch_targets(&pools, &cache, 1, &[]);
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].address, never_addr);
    }
}
