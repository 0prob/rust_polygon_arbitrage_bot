use alloy::primitives::{Address, FixedBytes, keccak256};

use crate::core::protocol::{fee_to_bps, is_fetchable_protocol, normalize_protocol};
use crate::core::types::{PoolIndex, ProtocolType, TokenIndex};
use crate::pipeline::types::PoolMeta;

#[derive(Debug, Clone)]
pub struct DiscoveredPool {
    /// Canonical pool key from Hasura (`id`): 20- or 32-byte hex.
    pub pool_key: String,
    /// Cache / arena key (contract address or synthetic for bytes32 pool ids).
    pub address: Address,
    pub protocol: ProtocolType,
    pub protocol_label: String,
    pub tokens: Vec<Address>,
    pub fee_bps: u32,
    pub tick_spacing: Option<i32>,
    pub pool_id: Option<FixedBytes<32>>,
    pub hooks: Option<Address>,
    pub created_block: u64,
}

#[derive(Debug, Clone, Default)]
pub struct TokenMeta {
    pub address: Address,
    pub decimals: u8,
}

pub fn is_valid_pool_key(id: &str) -> bool {
    let Some(hex) = id.strip_prefix("0x").or_else(|| id.strip_prefix("0X")) else {
        return false;
    };
    (hex.len() == 40 || hex.len() == 64) && hex.chars().all(|c| c.is_ascii_hexdigit())
}

pub fn parse_optional_bytes32(value: Option<&str>) -> Option<FixedBytes<32>> {
    let value = value?;
    let hex = value.strip_prefix("0x").unwrap_or(value);
    if hex.len() != 64 {
        return None;
    }
    let prefixed = if value.starts_with("0x") {
        value.to_string()
    } else {
        format!("0x{value}")
    };
    prefixed.parse().ok()
}

pub fn synthetic_cache_address(pool_id: &FixedBytes<32>) -> Address {
    let hash = keccak256(pool_id.as_slice());
    Address::from_slice(&hash[12..32])
}

pub fn is_hookless_v4_hooks(hooks: Address) -> bool {
    hooks.is_zero()
}

pub fn is_supported_v4_pool(protocol: ProtocolType, hooks: Option<Address>) -> bool {
    if protocol != ProtocolType::UniswapV4 {
        return true;
    }
    // Require explicit hookless confirmation — legacy schemas without `hooks` drop V4 pools.
    hooks.is_some_and(is_hookless_v4_hooks)
}

pub fn is_routable_pool(pool: &DiscoveredPool) -> bool {
    is_fetchable_protocol(pool.protocol) && is_supported_v4_pool(pool.protocol, pool.hooks)
}

fn resolve_pool_identity(
    id: &str,
    pool_id_raw: Option<&str>,
) -> Option<(String, Address, Option<FixedBytes<32>>)> {
    if !is_valid_pool_key(id) {
        return None;
    }
    let pool_key = id.to_ascii_lowercase();
    let hex = pool_key.strip_prefix("0x")?;

    if hex.len() == 64 {
        let pool_id: FixedBytes<32> = pool_key.parse().ok()?;
        let address = synthetic_cache_address(&pool_id);
        return Some((pool_key, address, Some(pool_id)));
    }

    let address: Address = pool_key.parse().ok()?;
    if address.is_zero() {
        return None;
    }
    let pool_id = parse_optional_bytes32(pool_id_raw);
    Some((pool_key, address, pool_id))
}

pub fn discovered_to_pool_meta(
    pool: &DiscoveredPool,
    pool_index: PoolIndex,
    token_indices: &[TokenIndex],
) -> PoolMeta {
    let token0 = token_indices.first().copied().unwrap_or(TokenIndex(0));
    let token1 = token_indices.get(1).copied().unwrap_or(token0);
    let bpt_index = if pool.protocol == ProtocolType::BalancerV2 {
        pool.tokens.iter().position(|t| *t == pool.address)
    } else {
        None
    };
    PoolMeta {
        pool_index,
        protocol: pool.protocol,
        tokens: token_indices.to_vec(),
        fee_bps: pool.fee_bps,
        token0,
        token1,
        bpt_index,
        pool_id: pool.pool_id,
        protocol_label: Some(pool.protocol_label.clone()),
        router: None,
        hooks: pool.hooks,
        tick_spacing: pool.tick_spacing,
    }
}

pub fn parse_pool_meta_row(
    id: &str,
    protocol: &str,
    tokens_raw: &serde_json::Value,
    fee: Option<i32>,
    tick_spacing: Option<i32>,
    pool_id_raw: Option<&str>,
    hooks_raw: Option<&str>,
    created_block: Option<i64>,
) -> Option<DiscoveredPool> {
    let (pool_key, address, pool_id) = resolve_pool_identity(id, pool_id_raw)?;

    let tokens: Vec<Address> = match tokens_raw {
        serde_json::Value::Array(arr) => arr
            .iter()
            .filter_map(|v| v.as_str().and_then(|s| s.parse().ok()))
            .collect(),
        serde_json::Value::String(s) => serde_json::from_str::<Vec<String>>(s)
            .ok()?
            .into_iter()
            .filter_map(|t| t.parse().ok())
            .collect(),
        _ => Vec::new(),
    };

    if tokens.len() < 2 {
        return None;
    }

    let proto = normalize_protocol(protocol);
    let fee_bps = fee_to_bps(protocol, fee.map(|f| f as u32));
    let mut hooks = hooks_raw.and_then(|h| h.parse().ok());

    if proto == ProtocolType::UniswapV4 {
        // Schema without `hooks` field → assume hookless (no hook info available).
        if hooks.is_none() {
            hooks = Some(Address::ZERO);
        }
        if !is_supported_v4_pool(proto, hooks) {
            return None;
        }
    }

    Some(DiscoveredPool {
        pool_key,
        address,
        protocol: proto,
        protocol_label: protocol.to_string(),
        tokens,
        fee_bps,
        tick_spacing,
        pool_id,
        hooks,
        created_block: created_block.unwrap_or(0).max(0) as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_v4_when_hooks_field_missing_assumes_hookless() {
        let pool = parse_pool_meta_row(
            "0x0000000000000000000000000000000000000a01",
            "UNISWAP_V4",
            &serde_json::json!([
                "0x000000000000000000000000000000000000aaaa",
                "0x000000000000000000000000000000000000bbbb"
            ]),
            Some(3000),
            Some(60),
            None,
            None,
            Some(100),
        )
        .expect("v4 pool without hooks field should be accepted as hookless");
        assert_eq!(pool.hooks, Some(Address::ZERO));
    }

    #[test]
    fn accepts_32_byte_v4_pool_id() {
        let pool_id = format!("0x{}", "ab".repeat(32));
        let pool = parse_pool_meta_row(
            &pool_id,
            "UNISWAP_V4",
            &serde_json::json!([
                "0x000000000000000000000000000000000000aaaa",
                "0x000000000000000000000000000000000000bbbb"
            ]),
            Some(3000),
            Some(60),
            None,
            Some("0x0000000000000000000000000000000000000000"),
            Some(100),
        )
        .expect("v4 pool");
        assert_eq!(pool.pool_key, pool_id.to_ascii_lowercase());
        assert!(pool.pool_id.is_some());
        assert_ne!(pool.address, Address::ZERO);
    }

    #[test]
    fn drops_v4_hook_pools() {
        let pool = parse_pool_meta_row(
            "0x0000000000000000000000000000000000000a01",
            "UNISWAP_V4",
            &serde_json::json!([
                "0x000000000000000000000000000000000000aaaa",
                "0x000000000000000000000000000000000000bbbb"
            ]),
            Some(500),
            Some(10),
            None,
            Some("0x0000000000000000000000000000000000000001"),
            Some(100),
        );
        assert!(pool.is_none());
    }

    #[test]
    fn preserves_balancer_pool_id_from_row() {
        let pool_id = "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef";
        let pool = parse_pool_meta_row(
            "0x0000000000000000000000000000000000000a01",
            "BALANCER_V2",
            &serde_json::json!([
                "0x000000000000000000000000000000000000aaaa",
                "0x000000000000000000000000000000000000bbbb"
            ]),
            Some(30),
            None,
            Some(pool_id),
            None,
            Some(100),
        )
        .expect("balancer pool");
        assert_eq!(pool.pool_id, Some(pool_id.parse().expect("pool id bytes")));
    }

    #[test]
    fn discovered_to_pool_meta_propagates_hooks_and_tick_spacing() {
        let hooks: Address = "0x0000000000000000000000000000000000000000"
            .parse()
            .unwrap();
        let pool = DiscoveredPool {
            pool_key: "0x0000000000000000000000000000000000000a01".into(),
            address: hooks,
            protocol: ProtocolType::UniswapV4,
            protocol_label: "UNISWAP_V4".into(),
            tokens: vec![hooks, hooks],
            fee_bps: 30,
            tick_spacing: Some(60),
            pool_id: None,
            hooks: Some(hooks),
            created_block: 0,
        };
        let meta = discovered_to_pool_meta(&pool, PoolIndex(0), &[TokenIndex(0), TokenIndex(1)]);
        assert_eq!(meta.tick_spacing, Some(60));
        assert_eq!(meta.hooks, Some(hooks));
    }
}
