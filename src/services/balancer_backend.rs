use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::Duration;

use alloy::primitives::{Address, FixedBytes};
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::core::types::ProtocolType;
use crate::infra::http::{HttpClientOpts, build_static};
use crate::services::discovery::DiscoveredPool;

const POLYGON_POOLS_QUERY: &str = r"
query PolygonV2Pools {
  poolGetPools(first: 10000, where: { chainIn: POLYGON, protocolVersionIn: 2 }) {
    address
    id
  }
}
";

static BALANCER_HTTP: LazyLock<Client> = LazyLock::new(|| {
    build_static(
        HttpClientOpts {
            timeout: Duration::from_secs(10),
            pool_max_idle_per_host: 4,
            max_redirects: 5,
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
    #[serde(rename = "poolGetPools")]
    pools: Vec<BackendPool>,
}

#[derive(Debug, Deserialize)]
struct BackendPool {
    address: String,
    id: String,
}

#[derive(Debug, Deserialize)]
struct GraphQlError {
    message: String,
}

pub async fn enrich_polygon_balancer_pool_ids(
    endpoint: &str,
    pools: &mut [DiscoveredPool],
) -> anyhow::Result<usize> {
    if !pools
        .iter()
        .any(|p| p.protocol == ProtocolType::BalancerV2 && p.pool_id.is_none())
    {
        return Ok(0);
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

    let by_address: HashMap<Address, FixedBytes<32>> = response
        .data
        .map(|data| data.pools)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|pool| Some((pool.address.parse().ok()?, pool.id.parse().ok()?)))
        .collect();

    Ok(enrich_from_map(pools, &by_address))
}

fn enrich_from_map(
    pools: &mut [DiscoveredPool],
    by_address: &HashMap<Address, FixedBytes<32>>,
) -> usize {
    let mut enriched = 0;
    for pool in pools {
        if pool.protocol == ProtocolType::BalancerV2
            && pool.pool_id.is_none()
            && let Some(id) = by_address.get(&pool.address)
        {
            pool.pool_id = Some(*id);
            pool.pool_id_verified = true;
            enriched += 1;
        }
    }
    enriched
}
