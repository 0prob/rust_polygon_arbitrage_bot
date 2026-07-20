use std::sync::LazyLock;
use std::time::Duration;

use alloy::primitives::{Address, FixedBytes};
use reqwest::Client;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use crate::core::types::ProtocolType;
use crate::infra::http::{HttpClientOpts, build_static};
use crate::services::discovery::DiscoveredPool;

const POLYGON_POOLS_QUERY: &str = r"
query PolygonV2Pools {
  poolGetPools(first: 10000, where: { chainIn: POLYGON, protocolVersionIn: 2 }) {
    address
    id
    dynamicData {
      swapEnabled
      isInRecoveryMode
    }
  }
}
";

static BALANCER_HTTP: LazyLock<Client> = LazyLock::new(|| {
    build_static(
        HttpClientOpts {
            timeout: Duration::from_secs(10),
            pool_max_idle_per_host: 4,
            max_redirects: 0,
        },
        "balancer backend",
    )
});

#[derive(Debug, Serialize)]
struct GraphQlRequest<'a> {
    query: &'a str,
}

#[derive(Debug, Deserialize)]
struct GraphQlResponse {
    data: Option<GraphQlData>,
    #[serde(default)]
    errors: Vec<GraphQlError>,
}

#[derive(Debug, Deserialize)]
struct GraphQlData {
    #[serde(rename = "poolGetPools", default)]
    pools: Vec<BackendPool>,
}

#[derive(Debug, Deserialize)]
struct BackendPool {
    address: String,
    id: String,
    #[serde(rename = "dynamicData")]
    dynamic_data: Option<BackendDynamicData>,
}

#[derive(Debug, Deserialize)]
struct BackendDynamicData {
    #[serde(rename = "swapEnabled", default = "default_true")]
    swap_enabled: bool,
    #[serde(rename = "isInRecoveryMode", default)]
    is_in_recovery_mode: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
struct GraphQlError {
    message: String,
}

/// Enrich missing Balancer V2 pool IDs and drop pools the API marks non-tradable
/// (`swapEnabled=false` or `isInRecoveryMode=true`). Pools absent from the API
/// response are left alone (pagination / indexing gaps).
///
/// Returns `(enriched_ids, filtered_out)`.
pub async fn enrich_polygon_balancer_pool_ids(
    endpoint: &str,
    pools: &mut Vec<DiscoveredPool>,
) -> anyhow::Result<(usize, usize)> {
    if !pools.iter().any(|p| p.protocol == ProtocolType::BalancerV2) {
        return Ok((0, 0));
    }

    let response = BALANCER_HTTP
        .post(endpoint)
        .json(&GraphQlRequest {
            query: POLYGON_POOLS_QUERY,
        })
        .send()
        .await?
        .error_for_status()?
        .json::<GraphQlResponse>()
        .await?;

    if !response.errors.is_empty() {
        let messages = response
            .errors
            .into_iter()
            .map(|error| error.message)
            .collect::<Vec<_>>()
            .join("; ");
        anyhow::bail!("Balancer backend GraphQL error: {messages}");
    }

    let backend = response.data.map(|data| data.pools).unwrap_or_default();

    Ok(apply_backend_pools(pools, &backend))
}

fn pool_tradable(dynamic: Option<&BackendDynamicData>) -> bool {
    match dynamic {
        Some(d) => d.swap_enabled && !d.is_in_recovery_mode,
        // Missing dynamicData: treat as tradable (don't drop on parse gaps).
        None => true,
    }
}

fn apply_backend_pools(pools: &mut Vec<DiscoveredPool>, backend: &[BackendPool]) -> (usize, usize) {
    let mut by_address: FxHashMap<Address, (FixedBytes<32>, bool)> = FxHashMap::default();
    for pool in backend {
        let Ok(address) = pool.address.parse::<Address>() else {
            continue;
        };
        let Ok(id) = pool.id.parse::<FixedBytes<32>>() else {
            continue;
        };
        by_address.insert(address, (id, pool_tradable(pool.dynamic_data.as_ref())));
    }

    let before = pools.len();
    pools.retain(|pool| {
        if pool.protocol != ProtocolType::BalancerV2 {
            return true;
        }
        match by_address.get(&pool.address) {
            Some((_, false)) => false,
            _ => true,
        }
    });
    let filtered = before.saturating_sub(pools.len());

    let mut enriched = 0;
    for pool in pools.iter_mut() {
        if pool.protocol == ProtocolType::BalancerV2
            && pool.pool_id.is_none()
            && let Some((id, true)) = by_address.get(&pool.address)
        {
            pool.pool_id = Some(*id);
            pool.pool_id_verified = true;
            enriched += 1;
        }
    }
    (enriched, filtered)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::Address;

    fn bal_pool(addr_byte: u8, pool_id: Option<FixedBytes<32>>) -> DiscoveredPool {
        DiscoveredPool {
            pool_key: format!("0x{:040x}", addr_byte as u64),
            address: Address::repeat_byte(addr_byte),
            protocol: ProtocolType::BalancerV2,
            protocol_label: "BALANCER_V2".into(),
            tokens: vec![Address::repeat_byte(1), Address::repeat_byte(2)],
            fee_bps: 30,
            tick_spacing: None,
            pool_id,
            pool_id_verified: pool_id.is_some(),
            hooks: None,
            pool_type: Some("weighted".into()),
            created_block: 1,
        }
    }

    fn backend(addr_byte: u8, id_byte: u8, swap_enabled: bool, recovery: bool) -> BackendPool {
        BackendPool {
            address: format!("{:?}", Address::repeat_byte(addr_byte)),
            id: format!("{:?}", FixedBytes::<32>::repeat_byte(id_byte)),
            dynamic_data: Some(BackendDynamicData {
                swap_enabled,
                is_in_recovery_mode: recovery,
            }),
        }
    }

    #[test]
    fn filters_disabled_and_recovery_enriches_tradable() {
        let mut pools = vec![
            bal_pool(0x11, None),
            bal_pool(0x22, None),
            bal_pool(0x33, Some(FixedBytes::repeat_byte(0x99))),
            DiscoveredPool {
                protocol: ProtocolType::UniswapV2,
                protocol_label: "UNISWAP_V2".into(),
                ..bal_pool(0x44, None)
            },
        ];
        let backend = vec![
            backend(0x11, 0xaa, true, false),
            backend(0x22, 0xbb, false, false),
            backend(0x33, 0xcc, true, true),
        ];
        let (enriched, filtered) = apply_backend_pools(&mut pools, &backend);
        assert_eq!(filtered, 2);
        assert_eq!(enriched, 1);
        assert_eq!(pools.len(), 2);
        assert_eq!(pools[0].address, Address::repeat_byte(0x11));
        assert_eq!(pools[0].pool_id, Some(FixedBytes::repeat_byte(0xaa)));
        assert!(pools[0].pool_id_verified);
        assert_eq!(pools[1].protocol, ProtocolType::UniswapV2);
    }

    #[test]
    fn unknown_backend_pool_kept() {
        let mut pools = vec![bal_pool(0x55, None)];
        let (enriched, filtered) = apply_backend_pools(&mut pools, &[]);
        assert_eq!(filtered, 0);
        assert_eq!(enriched, 0);
        assert_eq!(pools.len(), 1);
    }
}
