use crate::core::types::ProtocolType;

/// Map raw HyperIndex protocol labels to simulation protocol families.
pub fn normalize_protocol(raw: &str) -> ProtocolType {
    let u = raw.to_ascii_uppercase();
    if u.contains("WOOFI") {
        return ProtocolType::Woofi;
    }
    if u.contains("DODO") {
        return ProtocolType::Dodo;
    }
    if u.contains("BALANCER") {
        return ProtocolType::BalancerV2;
    }
    if u.contains("CURVE") {
        if u.contains("CRYPTO") {
            return ProtocolType::CurveCrypto;
        }
        return ProtocolType::CurveStable;
    }
    if u.contains("V4") {
        return ProtocolType::UniswapV4;
    }
    if u.contains("V3") || u.contains("ELASTIC") || u.contains("RAMSES") {
        return ProtocolType::UniswapV3;
    }
    if u.contains("V2") {
        return ProtocolType::UniswapV2;
    }
    ProtocolType::UniswapV2
}

/// Protocol-native fee unit divisor (matches TS `resolveFeeUnitDivisor`).
pub fn fee_unit_divisor(protocol_label: &str) -> u32 {
    let u = protocol_label.to_ascii_uppercase();
    if u.contains("WOOFI") || u.contains("ELASTIC") || u.starts_with("KYBER") {
        return 10;
    }
    if u.contains("V4") || u.contains("V3") {
        return 100;
    }
    1
}

/// Default raw factory fee when Hasura omits `fee`.
pub fn default_pool_fee_raw(protocol_label: &str) -> u32 {
    let u = protocol_label.to_ascii_uppercase();
    if u.contains("V4") || u.contains("V3") {
        3000
    } else {
        30
    }
}

/// Convert protocol-native fee units to basis points for routing weights.
pub fn fee_to_bps(protocol_label: &str, raw_fee: Option<u32>) -> u32 {
    let raw = raw_fee.unwrap_or_else(|| default_pool_fee_raw(protocol_label));
    if raw == 0 {
        return 30;
    }
    (raw / fee_unit_divisor(protocol_label)).min(9_999)
}

/// Pools we can hydrate on-chain today.
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

    /// Polygon DEX labels from HyperIndex — all map to simulable protocol families.
    const POLYGON_DEX_LABELS: &[&str] = &[
        "UNISWAP_V2",
        "SUSHISWAP_V2",
        "QUICKSWAP_V2",
        "DFYN_V2",
        "APESWAP_V2",
        "MESHSWAP_V2",
        "JETSWAP_V2",
        "COMETHSWAP_V2",
        "UNISWAP_V3",
        "SUSHISWAP_V3",
        "QUICKSWAP_V3",
        "KYBERSWAP_ELASTIC",
        "RAMSES_V3",
        "CURVE",
        "BALANCER_V2",
        "DODO_V2",
        "UNISWAP_V4",
        "WOOFI",
        "UNKNOWN_V2",
        "UNKNOWN_V3",
    ];

    #[test]
    fn all_polygon_dex_labels_normalize_and_fetch() {
        for label in POLYGON_DEX_LABELS {
            let protocol = normalize_protocol(label);
            assert!(
                is_fetchable_protocol(protocol),
                "{label} -> {protocol:?} should be fetchable"
            );
            let bps = fee_to_bps(label, None);
            assert!(bps > 0 && bps <= 9_999, "{label} fee bps {bps}");
        }
    }

    #[test]
    fn normalizes_v3_labels() {
        assert_eq!(normalize_protocol("QUICKSWAP_V3"), ProtocolType::UniswapV3);
        assert_eq!(
            normalize_protocol("KYBERSWAP_ELASTIC"),
            ProtocolType::UniswapV3
        );
    }

    #[test]
    fn converts_v3_pips_to_bps() {
        assert_eq!(fee_to_bps("UNISWAP_V3", Some(3000)), 30);
        assert_eq!(fee_to_bps("UNISWAP_V4", Some(500)), 5);
    }

    #[test]
    fn keeps_v2_bps_unchanged() {
        assert_eq!(fee_to_bps("QUICKSWAP_V2", Some(30)), 30);
        assert_eq!(fee_to_bps("BALANCER_V2", Some(10)), 10);
    }

    #[test]
    fn converts_kyber_elastic_units() {
        assert_eq!(fee_to_bps("KYBERSWAP_ELASTIC", Some(300)), 30);
    }
}
