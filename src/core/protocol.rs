use crate::core::types::ProtocolType;

fn contains_ignore_case(s: &str, needle: &str) -> bool {
    let s = s.as_bytes();
    let n = needle.as_bytes();
    s.len() >= n.len() && s.windows(n.len()).any(|w| w.eq_ignore_ascii_case(n))
}

/// Map PostgreSQL `protocol` + optional `poolType` to a simulation protocol family.
#[must_use]
pub fn resolve_protocol_from_pg(protocol: &str, pool_type: Option<&str>) -> Option<ProtocolType> {
    let base = normalize_protocol(protocol)?;
    Some(match base {
        ProtocolType::CurveStable | ProtocolType::CurveCrypto => {
            if pool_type.is_some_and(|t| contains_ignore_case(t, "crypto"))
                || contains_ignore_case(protocol, "crypto")
            {
                ProtocolType::CurveCrypto
            } else {
                ProtocolType::CurveStable
            }
        }
        _ => base,
    })
}

/// Map raw HyperIndex protocol labels to simulation protocol families.
#[must_use]
pub fn normalize_protocol(raw: &str) -> Option<ProtocolType> {
    if contains_ignore_case(raw, "woofi") {
        return Some(ProtocolType::Woofi);
    }
    if contains_ignore_case(raw, "dodo") {
        return Some(ProtocolType::Dodo);
    }
    if contains_ignore_case(raw, "balancer") {
        return Some(ProtocolType::BalancerV2);
    }
    if contains_ignore_case(raw, "curve") {
        if contains_ignore_case(raw, "crypto") {
            return Some(ProtocolType::CurveCrypto);
        }
        return Some(ProtocolType::CurveStable);
    }
    // QuickSwap V4 is Algebra Integral, not Uniswap V4 PoolManager. Keep both
    // Algebra generations on the concentrated-liquidity adapter; the original
    // label is preserved for callback/factory selection at execution time.
    if is_algebra_protocol_label(raw) {
        return Some(ProtocolType::UniswapV3);
    }
    if contains_ignore_case(raw, "v4") {
        return Some(ProtocolType::UniswapV4);
    }
    if contains_ignore_case(raw, "v3") || contains_ignore_case(raw, "elastic") {
        return Some(ProtocolType::UniswapV3);
    }
    if contains_ignore_case(raw, "v2") {
        return Some(ProtocolType::UniswapV2);
    }
    None
}

/// Normalize Balancer pool types supported by the local quote engine.
///
/// Explicit unknown types must not fall back to weighted math: linear, gyro,
/// and other specialized pools have different invariants and would be
/// systematically misquoted. Missing metadata remains `None` so on-chain
/// capability probes can classify ordinary weighted/stable pools later.
#[must_use]
pub fn normalize_balancer_pool_type(pool_type: Option<&str>) -> Option<String> {
    let pool_type = pool_type?;
    if contains_ignore_case(pool_type, "stable") {
        Some("stable".to_string())
    } else if contains_ignore_case(pool_type, "linear") {
        Some("linear".to_string())
    } else if contains_ignore_case(pool_type, "weighted") {
        Some("weighted".to_string())
    } else {
        None
    }
}

#[must_use]
pub fn is_algebra_protocol_label(raw: &str) -> bool {
    is_algebra_integral_protocol_label(raw)
        || contains_ignore_case(raw, "algebra")
        || contains_ignore_case(raw, "quickswap_v3")
        || contains_ignore_case(raw, "quick_v3")
}

/// QuickSwap V4 and other Algebra Integral deployments (distinct `ticks()` ABI).
#[must_use]
pub fn is_algebra_integral_protocol_label(raw: &str) -> bool {
    contains_ignore_case(raw, "quickswap_v4") || contains_ignore_case(raw, "quick_v4")
}

#[must_use]
pub fn is_known_protocol_label(raw: &str) -> bool {
    is_algebra_protocol_label(raw)
        || contains_ignore_case(raw, "woofi")
        || contains_ignore_case(raw, "dodo")
        || contains_ignore_case(raw, "balancer")
        || contains_ignore_case(raw, "curve")
        || contains_ignore_case(raw, "elastic")
        || contains_ignore_case(raw, "v2")
        || contains_ignore_case(raw, "v3")
        || contains_ignore_case(raw, "v4")
}

/// Convert protocol-native fee units to basis points for routing weights.
/// ponytail: 30 bps V2-style, 3000 pips V3-style.
/// Override with on-chain `fee()` calls if a pool's fee is routinely missing
/// from the indexer — each wrong-fee pool contaminates all cycles through it.
#[must_use]
pub fn fee_to_bps(protocol_label: &str, raw_fee: Option<u32>) -> u32 {
    let is_curve = contains_ignore_case(protocol_label, "curve");
    let is_pips_style = contains_ignore_case(protocol_label, "v4")
        || contains_ignore_case(protocol_label, "v3")
        || contains_ignore_case(protocol_label, "elastic")
        || is_algebra_protocol_label(protocol_label);
    let raw = raw_fee.unwrap_or(if is_pips_style { 3000 } else { 30 });
    if raw == 0 || raw >= 0x800000 {
        return 30;
    }
    if is_curve {
        return (raw / 1_000_000).min(9_999);
    }
    (raw / if is_pips_style { 100 } else { 1 }).min(9_999)
}

/// Pools we can hydrate on-chain today.
#[must_use]
pub fn is_fetchable_protocol(protocol: ProtocolType) -> bool {
    matches!(
        protocol,
        ProtocolType::UniswapV2
            | ProtocolType::UniswapV3
            | ProtocolType::UniswapV4
            | ProtocolType::BalancerV2
            | ProtocolType::CurveStable
            | ProtocolType::CurveCrypto
            | ProtocolType::Dodo
            | ProtocolType::Woofi
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_uniswap_v2() {
        assert_eq!(
            normalize_protocol("UNISWAP_V2"),
            Some(ProtocolType::UniswapV2)
        );
    }

    #[test]
    fn quickswap_generations_use_algebra_adapter() {
        assert_eq!(
            normalize_protocol("QUICKSWAP_V3"),
            Some(ProtocolType::UniswapV3)
        );
        assert_eq!(
            normalize_protocol("QUICKSWAP_V4"),
            Some(ProtocolType::UniswapV3)
        );
        assert!(is_algebra_protocol_label("QUICKSWAP_V4"));
        assert!(is_algebra_integral_protocol_label("QUICKSWAP_V4"));
        assert!(!is_algebra_integral_protocol_label("QUICKSWAP_V3"));
        assert_eq!(
            normalize_protocol("UNISWAP_V4"),
            Some(ProtocolType::UniswapV4)
        );
    }

    #[test]
    fn unknown_protocol_is_not_implicitly_v2() {
        assert!(!is_known_protocol_label("NEW_UNKNOWN_DEX"));
        assert_eq!(normalize_protocol("NEW_UNKNOWN_DEX"), None);
        assert_eq!(resolve_protocol_from_pg("NEW_UNKNOWN_DEX", None), None);
    }

    #[test]
    fn curve_crypto_is_fetchable() {
        assert!(is_fetchable_protocol(ProtocolType::CurveCrypto));
    }

    #[test]
    fn curve_fee_units_are_converted_to_basis_points() {
        assert_eq!(fee_to_bps("CURVE_STABLE", Some(4_000_000)), 4);
        assert_eq!(fee_to_bps("CURVE_CRYPTO", Some(5_000_000)), 5);
    }

    #[test]
    fn elastic_and_algebra_fees_use_pips_divisor() {
        assert_eq!(fee_to_bps("QUICKSWAP_ELASTIC", Some(3000)), 30);
        assert_eq!(fee_to_bps("ALGEBRA", Some(500)), 5);
        assert_eq!(fee_to_bps("QUICKSWAP_V4", Some(3000)), 30);
        assert_eq!(fee_to_bps("UNISWAP_V3", Some(3000)), 30);
        assert_eq!(fee_to_bps("UNISWAP_V2", Some(30)), 30);
    }

    #[test]
    fn balancer_pool_type_normalization_is_explicit_and_case_insensitive() {
        assert_eq!(
            normalize_balancer_pool_type(Some("WeightedPool")),
            Some("weighted".to_string())
        );
        assert_eq!(
            normalize_balancer_pool_type(Some("ComposableStablePool")),
            Some("stable".to_string())
        );
        assert_eq!(
            normalize_balancer_pool_type(Some("AaveLinearPool")),
            Some("linear".to_string())
        );
        assert_eq!(normalize_balancer_pool_type(Some("GyroECLPPool")), None);
        assert_eq!(normalize_balancer_pool_type(None), None);
    }
}
