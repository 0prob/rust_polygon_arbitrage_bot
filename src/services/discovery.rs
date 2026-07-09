use alloy::primitives::{Address, FixedBytes, keccak256};
use rustc_hash::{FxBuildHasher, FxHashMap};

use crate::core::protocol::{
    fee_to_bps, is_fetchable_protocol, is_known_protocol_label, normalize_balancer_pool_type,
    resolve_protocol_from_pg,
};
use crate::core::types::{PoolIndex, ProtocolType, TokenIndex};
use crate::pipeline::types::PoolMeta;

#[derive(Debug, Clone)]
pub struct DiscoveredPool {
    /// Canonical pool key from PostgreSQL (`id`): 20- or 32-byte hex.
    pub pool_key: String,
    /// Cache / arena key (contract address or synthetic for bytes32 pool ids).
    pub address: Address,
    pub protocol: ProtocolType,
    pub protocol_label: String,
    pub tokens: Vec<Address>,
    pub fee_bps: u32,
    pub tick_spacing: Option<i32>,
    pub pool_id: Option<FixedBytes<32>>,
    /// True when `pool_id` came from Balancer's backend or an on-chain call.
    pub pool_id_verified: bool,
    pub hooks: Option<Address>,
    /// PostgreSQL `poolType` hint (`crypto` / `stable` for Curve, `weighted` / `stable` for Balancer).
    pub pool_type: Option<String>,
    pub created_block: u64,
}

#[derive(Debug, Clone, Default)]
pub struct TokenMeta {
    pub address: Address,
    pub decimals: u8,
}

#[must_use]
pub fn parse_optional_bytes32(value: Option<&str>) -> Option<FixedBytes<32>> {
    let val = value?;
    let hex = val.strip_prefix("0x").unwrap_or(val);
    if hex.len() != 64 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    format!("0x{hex}").parse().ok()
}

#[must_use]
pub fn synthetic_cache_address(pool_id: &FixedBytes<32>) -> Address {
    Address::from_slice(&keccak256(pool_id.as_slice())[12..32])
}

/// Reject precompile-range and other non-contract keys that slip through hex parsing.
#[must_use]
pub fn is_plausible_contract_address(addr: Address) -> bool {
    const MIN_CONTRACT: Address =
        alloy::primitives::address!("0x000000000000000000000000000000000000ffff");
    !addr.is_zero() && addr > MIN_CONTRACT
}

#[must_use]
pub fn is_supported_v4_pool(protocol: ProtocolType, hooks: Option<Address>) -> bool {
    match protocol {
        ProtocolType::UniswapV4 => hooks.is_none_or(|h| h.is_zero()),
        _ => true,
    }
}

/// Read env var once; defaults to enabled (no change from current behaviour).
fn quickswap_v2_enabled() -> bool {
    use std::sync::OnceLock;
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("QUICKSWAP_V2_ENABLED").is_ok_and(|v| v.eq_ignore_ascii_case("true"))
    })
}

fn is_quickswap_v2_label(label: &str) -> bool {
    let b = label.as_bytes();
    b.windows(12)
        .any(|w| w.eq_ignore_ascii_case(b"quickswap_v2"))
        || b.windows(8).any(|w| w.eq_ignore_ascii_case(b"quick_v2"))
}

fn uniswap_v2_enabled() -> bool {
    use std::sync::OnceLock;
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("UNISWAP_V2_ENABLED").is_ok_and(|v| v.eq_ignore_ascii_case("true"))
    })
}

fn is_uniswap_v2_label(label: &str) -> bool {
    let b = label.as_bytes();
    b.windows(11).any(|w| w.eq_ignore_ascii_case(b"uniswap_v2"))
}

#[must_use]
pub fn is_routable_pool(pool: &DiscoveredPool) -> bool {
    is_fetchable_protocol(pool.protocol)
        && has_supported_token_shape(pool.protocol, &pool.tokens)
        && is_supported_v4_pool(pool.protocol, pool.hooks)
        // ponytail: env toggle for quickswap v2 pools
        && (quickswap_v2_enabled() || !is_quickswap_v2_label(&pool.protocol_label))
        // ponytail: env toggle for uniswap v2 pools
        && (uniswap_v2_enabled() || !is_uniswap_v2_label(&pool.protocol_label))
}

fn has_supported_token_shape(protocol: ProtocolType, tokens: &[Address]) -> bool {
    if tokens
        .iter()
        .any(|token| !is_plausible_contract_address(*token))
    {
        return false;
    }
    if tokens.len() > 1 {
        let mut seen = [None; 8];
        let mut seen_len = 0usize;
        for &token in tokens {
            if seen[..seen_len].contains(&Some(token)) {
                return false;
            }
            if seen_len < seen.len() {
                seen[seen_len] = Some(token);
                seen_len += 1;
            }
        }
    }
    match protocol {
        ProtocolType::UniswapV2
        | ProtocolType::UniswapV3
        | ProtocolType::UniswapV4
        | ProtocolType::Dodo => tokens.len() == 2,
        ProtocolType::CurveStable | ProtocolType::CurveCrypto => (2..=8).contains(&tokens.len()),
        ProtocolType::BalancerV2 | ProtocolType::Woofi => {
            (2..=usize::from(u8::MAX) + 1).contains(&tokens.len())
        }
    }
}

/// Resolve bytes32 pool id for Uniswap V4 hydration (PG column or 32-byte pool key).
#[must_use]
pub fn resolve_v4_pool_id(pool: &DiscoveredPool) -> Option<FixedBytes<32>> {
    pool.pool_id
        .or_else(|| resolve_v4_pool_id_from_key(&pool.pool_key))
}

fn resolve_v4_pool_id_from_key(pool_key: &str) -> Option<FixedBytes<32>> {
    if pool_key.len() == 66 {
        pool_key.parse().ok()
    } else {
        None
    }
}

/// Resolve pool key, cache address, and optional bytes32 pool_id from PostgreSQL row fields.
fn resolve_pool_identity(
    id: &str,
    pool_id_raw: Option<&str>,
    address_raw: Option<&str>,
) -> Option<(String, Address, Option<FixedBytes<32>>)> {
    let pool_key = id.to_ascii_lowercase();
    let hex = pool_key.strip_prefix("0x")?;
    if !((hex.len() == 40 || hex.len() == 64) && hex.chars().all(|c| c.is_ascii_hexdigit())) {
        return None;
    }

    if hex.len() == 64 {
        let pool_id: FixedBytes<32> = pool_key.parse().ok()?;
        let address = synthetic_cache_address(&pool_id);
        return Some((pool_key, address, Some(pool_id)));
    }

    let mut address: Address = pool_key.parse().ok()?;
    if !is_plausible_contract_address(address) {
        return None;
    }

    let pool_id = parse_optional_bytes32(pool_id_raw);

    // Prefer explicit `address` column for 20-byte pool contracts.
    if let Some(raw) = address_raw {
        let hex = raw.strip_prefix("0x").or(Some(raw));
        if hex.is_some_and(|h| h.len() == 40 && h.chars().all(|c| c.is_ascii_hexdigit()))
            && let Ok(addr) = raw.parse::<Address>()
            && is_plausible_contract_address(addr)
        {
            address = addr;
        }
    }

    Some((pool_key, address, pool_id))
}

#[must_use]
pub fn discovered_to_pool_meta(
    pool: &DiscoveredPool,
    pool_index: PoolIndex,
    token_indices: &[TokenIndex],
) -> PoolMeta {
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
        bpt_index,
        pool_id: if pool.protocol == ProtocolType::UniswapV4 {
            resolve_v4_pool_id(pool)
        } else {
            pool.pool_id
        },
        protocol_label: Some(pool.protocol_label.clone()),
        pool_type: pool.pool_type.clone(),
        hooks: pool.hooks,
        tick_spacing: pool.tick_spacing,
    }
}

#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn parse_pool_meta_row(
    id: &str,
    protocol: &str,
    tokens: &[String],
    fee: Option<i32>,
    tick_spacing: Option<i32>,
    pool_id_raw: Option<&str>,
    hooks_raw: Option<&str>,
    pool_type_raw: Option<&str>,
    created_block: Option<i64>,
    address_raw: Option<&str>,
) -> Option<DiscoveredPool> {
    let mut parsed = Vec::with_capacity(tokens.len());
    for token in tokens {
        parsed.push(token.parse().ok()?);
    }
    parse_pool_meta_impl(
        id,
        protocol,
        parsed,
        fee,
        tick_spacing,
        pool_id_raw,
        hooks_raw,
        pool_type_raw,
        created_block,
        address_raw,
    )
}

#[allow(clippy::too_many_arguments)]
fn parse_pool_meta_impl(
    id: &str,
    protocol: &str,
    tokens: Vec<Address>,
    fee: Option<i32>,
    tick_spacing: Option<i32>,
    pool_id_raw: Option<&str>,
    hooks_raw: Option<&str>,
    pool_type_raw: Option<&str>,
    created_block: Option<i64>,
    address_raw: Option<&str>,
) -> Option<DiscoveredPool> {
    let (pool_key, address, pool_id) = resolve_pool_identity(id, pool_id_raw, address_raw)?;

    if tokens.len() < 2 || !is_known_protocol_label(protocol) {
        return None;
    }

    let proto = resolve_protocol_from_pg(protocol, pool_type_raw);
    if !has_supported_token_shape(proto, &tokens) {
        return None;
    }
    let fee_bps = fee_to_bps(protocol, fee.map(|f| f as u32));
    let mut hooks = hooks_raw.and_then(|h| h.parse().ok());
    let pool_type = if proto == ProtocolType::BalancerV2 {
        let normalized = normalize_balancer_pool_type(pool_type_raw);
        // Never route an explicitly specialized Balancer pool through the
        // weighted fallback. Gyro and other specialized pools require
        // different invariants that are not implemented here.
        if pool_type_raw.is_some() && normalized.is_none() {
            return None;
        }
        normalized
    } else {
        pool_type_raw.map(str::to_string)
    };

    if proto == ProtocolType::UniswapV4 {
        let pool_id = pool_id.or_else(|| resolve_v4_pool_id_from_key(&pool_key));
        pool_id?;
        if hooks.is_none() {
            hooks = Some(Address::ZERO);
        }
        if !is_supported_v4_pool(proto, hooks) {
            return None;
        }
        return Some(DiscoveredPool {
            pool_key,
            address,
            protocol: proto,
            protocol_label: protocol.to_string(),
            tokens,
            fee_bps,
            tick_spacing,
            pool_id,
            pool_id_verified: false,
            hooks,
            pool_type,
            created_block: created_block.unwrap_or(0).max(0) as u64,
        });
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
        pool_id_verified: false,
        hooks,
        pool_type,
        created_block: created_block.unwrap_or(0).max(0) as u64,
    })
}

#[must_use]
pub fn pool_protocol_by_address(pools: &[DiscoveredPool]) -> FxHashMap<Address, ProtocolType> {
    let mut out = FxHashMap::with_capacity_and_hasher(pools.len(), FxBuildHasher);
    for pool in pools {
        out.insert(pool.address, pool.protocol);
    }
    out
}

/// Resolve hop protocol from the arena pool slot, not `pool_metas[pool_index]`.
#[must_use]
pub fn protocol_for_arena_pool(
    arena: &crate::pipeline::arena::StateArena,
    pool_index: PoolIndex,
    by_address: &FxHashMap<Address, ProtocolType>,
    fallback: ProtocolType,
) -> ProtocolType {
    arena
        .pool_address(pool_index)
        .and_then(|addr| by_address.get(&addr))
        .copied()
        .unwrap_or(fallback)
}

#[must_use]
pub fn discovered_pool_by_address(pools: &[DiscoveredPool]) -> FxHashMap<Address, &DiscoveredPool> {
    let mut out = FxHashMap::with_capacity_and_hasher(pools.len().saturating_mul(2), FxBuildHasher);
    for pool in pools {
        out.insert(pool.address, pool);
    }
    out
}

/// Log protocol distribution of discovered pools for routing health.
pub fn log_protocol_distribution(pools: &[DiscoveredPool]) {
    use std::collections::BTreeMap;
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for p in pools {
        if is_fetchable_protocol(p.protocol) {
            *counts.entry(p.protocol_label.as_str()).or_default() += 1;
        }
    }
    let routable: usize = counts.values().sum();
    crate::info!(
        "pool discovery: {routable} routable pools across {} protocols",
        counts.len(),
    );
    for (protocol_label, total) in &counts {
        crate::info!("  {protocol_label}: {total}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admits_hookless_uniswap_v4_pools() {
        assert!(is_supported_v4_pool(ProtocolType::UniswapV4, None));
        assert!(is_supported_v4_pool(
            ProtocolType::UniswapV4,
            Some(Address::ZERO)
        ));
        assert!(!is_supported_v4_pool(
            ProtocolType::UniswapV4,
            Some(
                "0x00000000000000000000000000000000000000ab"
                    .parse()
                    .expect("test pool address should parse")
            )
        ));
        assert!(is_supported_v4_pool(ProtocolType::UniswapV3, None));
    }

    #[test]
    fn rejects_uniswap_v4_without_pool_id() {
        let pool_id = "0x1111111111111111111111111111111111111111111111111111111111111111";
        assert!(
            parse_pool_meta_row(
                pool_id,
                "UNISWAP_V4",
                &[
                    "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270".to_string(),
                    "0x2791bca1f2de4661ed88a30c99a7a9449aa84174".to_string(),
                ],
                Some(3000),
                Some(60),
                None,
                Some("0x0000000000000000000000000000000000000000"),
                None,
                Some(1),
                None,
            )
            .is_some()
        );
        let pool = parse_pool_meta_row(
            pool_id,
            "UNISWAP_V4",
            &[
                "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270".to_string(),
                "0x2791bca1f2de4661ed88a30c99a7a9449aa84174".to_string(),
            ],
            Some(3000),
            None,
            None,
            Some("0x0000000000000000000000000000000000000000"),
            None,
            Some(1),
            None,
        )
        .expect("v4 pool id from bytes32 key");
        assert!(pool.tick_spacing.is_none());
        assert!(
            parse_pool_meta_row(
                "0x0000000000000000000000000000000000000001",
                "UNISWAP_V4",
                &[
                    "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270".to_string(),
                    "0x2791bca1f2de4661ed88a30c99a7a9449aa84174".to_string(),
                ],
                Some(3000),
                None,
                None,
                Some("0x0000000000000000000000000000000000000000"),
                None,
                Some(1),
                None,
            )
            .is_none()
        );
    }

    #[test]
    fn protocol_for_arena_pool_uses_address_not_vec_index() {
        use crate::core::types::{PoolIndex, PoolState, V2PoolState};
        use crate::pipeline::arena::StateArena;
        use alloy::primitives::U256;
        use std::sync::Arc;

        let v3_addr = Address::from([3u8; 20]);
        let bal_addr = Address::from([9u8; 20]);
        let discovered = vec![
            DiscoveredPool {
                pool_key: format!("{v3_addr}"),
                address: v3_addr,
                protocol: ProtocolType::UniswapV3,
                protocol_label: "QUICKSWAP_V3".into(),
                tokens: vec![Address::from([1u8; 20]), Address::from([2u8; 20])],
                fee_bps: 30,
                tick_spacing: Some(60),
                pool_id: None,
                pool_id_verified: false,
                hooks: None,
                pool_type: None,
                created_block: 1,
            },
            DiscoveredPool {
                pool_key: format!("{bal_addr}"),
                address: bal_addr,
                protocol: ProtocolType::BalancerV2,
                protocol_label: "BALANCER_V2".into(),
                tokens: vec![Address::from([1u8; 20]), Address::from([2u8; 20])],
                fee_bps: 30,
                tick_spacing: None,
                pool_id: None,
                pool_id_verified: false,
                hooks: None,
                pool_type: Some("weighted".into()),
                created_block: 1,
            },
        ];
        let by_address = pool_protocol_by_address(&discovered);
        let mut arena = StateArena::default();
        let v3_idx = arena.register_pool(
            v3_addr,
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: U256::from(1_000u64),
                reserve1: U256::from(2_000u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 1,
            })),
        );
        let _bal_idx = arena.register_pool(
            bal_addr,
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: U256::from(3_000u64),
                reserve1: U256::from(4_000u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 1,
            })),
        );

        // Misordered metas[0] claims pool_index=1 (balancer) while arena slot 0 is v3.
        let metas = vec![crate::pipeline::types::PoolMeta {
            pool_index: PoolIndex(1),
            protocol: ProtocolType::BalancerV2,
            tokens: Vec::new(),
            fee_bps: 30,
            bpt_index: None,
            pool_id: None,
            protocol_label: Some("BALANCER_V2".into()),
            pool_type: None,
            hooks: None,
            tick_spacing: None,
        }];
        let wrong = crate::pipeline::types::pool_meta_at(&metas, v3_idx)
            .map(|m| m.protocol)
            .unwrap_or(ProtocolType::BalancerV2);
        assert_eq!(wrong, ProtocolType::BalancerV2);

        assert_eq!(
            protocol_for_arena_pool(&arena, v3_idx, &by_address, ProtocolType::BalancerV2),
            ProtocolType::UniswapV3
        );
    }

    #[test]
    fn test_parse_pool_meta_row_invalid_key() {
        let r = parse_pool_meta_row(
            "bad_key",
            "uniswap_v2",
            &[],
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(r.is_none());
    }

    #[test]
    fn rejects_unknown_protocol_instead_of_assuming_v2() {
        let tokens = vec![
            "0x0000000000000000000000000000000000000001".to_string(),
            "0x0000000000000000000000000000000000000002".to_string(),
        ];
        assert!(
            parse_pool_meta_row(
                "0x0000000000000000000000000000000000000003",
                "NEW_UNKNOWN_DEX",
                &tokens,
                None,
                None,
                None,
                None,
                None,
                Some(1),
                None,
            )
            .is_none()
        );
    }

    #[test]
    fn rejects_malformed_duplicate_and_wrong_arity_tokens() {
        let id = "0x0000000000000000000000000000000000000003";
        let valid = "0x0000000000000000000000000000000000000001".to_string();
        let other = "0x0000000000000000000000000000000000000002".to_string();
        for tokens in [
            vec![valid.clone(), "not-an-address".to_string(), other.clone()],
            vec![valid.clone(), valid.clone()],
            vec![
                valid.clone(),
                other.clone(),
                Address::with_last_byte(4).to_string(),
            ],
        ] {
            assert!(
                parse_pool_meta_row(
                    id,
                    "UNISWAP_V2",
                    &tokens,
                    Some(30),
                    None,
                    None,
                    None,
                    None,
                    Some(1),
                    None,
                )
                .is_none()
            );
        }
    }

    #[test]
    fn rejects_unsupported_balancer_pool_types_before_routing() {
        let tokens = vec![
            "0x0000000000000000000000000000000000000001".to_string(),
            "0x0000000000000000000000000000000000000002".to_string(),
        ];
        let pool_id = format!("0x{}", "11".repeat(32));

        let pool_type = "GyroECLPPool";
        let pool = parse_pool_meta_row(
            "0x0000000000000000000000000000000000000003",
            "balancer_v2",
            &tokens,
            Some(30),
            None,
            Some(&pool_id),
            None,
            Some(pool_type),
            Some(1),
            None,
        );
        assert!(pool.is_none(), "{pool_type} must not use weighted math");
    }

    #[test]
    fn accepts_supported_or_unclassified_balancer_pool_types() {
        let tokens = vec![
            "0x0000000000000000000000000000010000000001".to_string(),
            "0x0000000000000000000000000000010000000002".to_string(),
        ];
        let pool_id = format!("0x{}", "11".repeat(32));

        for (pool_type, expected) in [
            (Some("WeightedPool"), Some("weighted")),
            (Some("ComposableStablePool"), Some("stable")),
            (Some("AaveLinearPool"), Some("linear")),
            (None, None),
        ] {
            let pool = parse_pool_meta_row(
                "0x0000000000000000000000000000010000000003",
                "balancer_v2",
                &tokens,
                Some(30),
                None,
                Some(&pool_id),
                None,
                pool_type,
                Some(1),
                None,
            )
            .expect("supported Balancer pool");
            assert_eq!(pool.pool_type.as_deref(), expected);
        }
    }
}
