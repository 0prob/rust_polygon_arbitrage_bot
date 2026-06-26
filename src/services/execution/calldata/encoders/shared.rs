use alloy::primitives::{Address, FixedBytes};

use crate::core::types::{PoolState, V3PoolState};

/// Convert PoolState to V3PoolState if compatible (V3 or V4)
///
/// Returns Some(V3PoolState) if the pool is V3 or V4, None otherwise.
/// V4 pools are converted to V3-compatible format by extracting the common fields.
pub fn to_v3_state(state: &PoolState) -> Option<V3PoolState> {
    match state {
        PoolState::V3(s) | PoolState::V4(s) => Some(s.clone()),
        _ => None,
    }
}

/// Derive Balancer pool ID from pool address
///
/// Encodes the pool address into the last 20 bytes of a 32-byte FixedBytes,
/// with the first 12 bytes set to zero.
pub fn derive_balancer_pool_id(pool_address: Address) -> FixedBytes<32> {
    let mut id = FixedBytes::ZERO;
    id.0[12..32].copy_from_slice(pool_address.as_slice());
    id
}

/// Resolve Balancer pool ID, preferring explicit ID over derived
///
/// If explicit pool_id is provided, returns it directly.
/// Otherwise, derives the pool ID from the pool address.
pub fn resolve_balancer_pool_id(
    pool_address: Address,
    pool_id: Option<FixedBytes<32>>,
) -> anyhow::Result<FixedBytes<32>> {
    pool_id.ok_or_else(|| {
        anyhow::anyhow!(
            "missing Balancer pool_id for {pool_address} — indexer must supply poolId or pool state must be hydrated"
        )
    })
}

/// Check if Curve pool uses receiver parameter
///
/// StableSwap_NG pools have a different interface than standard StableSwap pools
/// and require special handling for the receiver parameter.
pub fn curve_uses_receiver(protocol_label: Option<&str>) -> bool {
    protocol_label
        .map(|l| l.to_ascii_uppercase().contains("STABLESWAP_NG"))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::V4PoolState;
    use alloy::primitives::U256;

    #[test]
    fn test_derive_balancer_pool_id_encodes_address() {
        let addr = Address::repeat_byte(0xab);
        let id = derive_balancer_pool_id(addr);

        // Verify first 12 bytes are zero
        assert_eq!(&id.0[0..12], &[0u8; 12]);

        // Verify last 20 bytes contain the address
        assert_eq!(&id.0[12..32], addr.as_slice());
    }

    #[test]
    fn test_derive_balancer_pool_id_different_addresses() {
        let addr1 = Address::repeat_byte(0xaa);
        let addr2 = Address::repeat_byte(0xbb);

        let id1 = derive_balancer_pool_id(addr1);
        let id2 = derive_balancer_pool_id(addr2);

        assert_ne!(id1, id2);
        assert_eq!(&id1.0[12..32], addr1.as_slice());
        assert_eq!(&id2.0[12..32], addr2.as_slice());
    }

    #[test]
    fn test_resolve_balancer_pool_id_prefers_explicit() {
        let addr = Address::repeat_byte(0xab);
        let explicit = FixedBytes::repeat_byte(0xcd);

        let resolved = resolve_balancer_pool_id(addr, Some(explicit)).expect("explicit id");

        assert_eq!(resolved, explicit);
    }

    #[test]
    fn test_resolve_balancer_pool_id_errors_when_none() {
        let addr = Address::repeat_byte(0xab);
        assert!(resolve_balancer_pool_id(addr, None).is_err());
    }

    #[test]
    fn test_curve_uses_receiver_detects_stableswap_ng_uppercase() {
        assert!(curve_uses_receiver(Some("STABLESWAP_NG")));
    }

    #[test]
    fn test_curve_uses_receiver_detects_stableswap_ng_lowercase() {
        assert!(curve_uses_receiver(Some("stableswap_ng")));
    }

    #[test]
    fn test_curve_uses_receiver_detects_stableswap_ng_mixed_case() {
        assert!(curve_uses_receiver(Some("StableSwap_NG")));
    }

    #[test]
    fn test_curve_uses_receiver_rejects_standard_stableswap() {
        assert!(!curve_uses_receiver(Some("StableSwap")));
    }

    #[test]
    fn test_curve_uses_receiver_rejects_other_labels() {
        assert!(!curve_uses_receiver(Some("Tricrypto")));
        assert!(!curve_uses_receiver(Some("Plain")));
    }

    #[test]
    fn test_curve_uses_receiver_rejects_none() {
        assert!(!curve_uses_receiver(None));
    }

    #[test]
    fn test_to_v3_state_v3_passthrough() {
        let v3_state = V3PoolState {
            sqrt_price_x96: U256::from(1000),
            tick: 0,
            liquidity: 100,
            fee: U256::from(500),
            tick_spacing: 1,
            ticks: Box::new([]),
        };
        let pool_state = PoolState::V3(v3_state.clone());

        let converted = to_v3_state(&pool_state);

        assert!(converted.is_some());
        let result = converted.unwrap();
        assert_eq!(result.sqrt_price_x96, v3_state.sqrt_price_x96);
        assert_eq!(result.tick, v3_state.tick);
        assert_eq!(result.liquidity, v3_state.liquidity);
        assert_eq!(result.fee, v3_state.fee);
        assert_eq!(result.tick_spacing, v3_state.tick_spacing);
    }

    #[test]
    fn test_to_v3_state_v4_conversion() {
        let v4_state = V4PoolState {
            sqrt_price_x96: U256::from(2000),
            tick: 100,
            liquidity: 200,
            fee: U256::from(1000),
            tick_spacing: 2,
            ticks: Box::new([]),
        };
        let pool_state = PoolState::V4(v4_state.clone());

        let converted = to_v3_state(&pool_state);

        assert!(converted.is_some());
        let result = converted.unwrap();
        assert_eq!(result.sqrt_price_x96, v4_state.sqrt_price_x96);
        assert_eq!(result.tick, v4_state.tick);
        assert_eq!(result.liquidity, v4_state.liquidity);
        assert_eq!(result.fee, v4_state.fee);
        assert_eq!(result.tick_spacing, v4_state.tick_spacing);
    }

    #[test]
    fn test_to_v3_state_rejects_invalid() {
        let pool_state = PoolState::Invalid;
        assert!(to_v3_state(&pool_state).is_none());
    }

    #[test]
    fn test_to_v3_state_rejects_curve() {
        use crate::core::types::CurvePoolState;
        let curve_state = CurvePoolState {
            balances: vec![U256::from(1000), U256::from(2000)],
            a: U256::from(100),
            fee: U256::from(1),
            rates: vec![],
            n_coins: 2,
            gamma: None,
            d: None,
        };
        let pool_state = PoolState::Curve(curve_state);
        assert!(to_v3_state(&pool_state).is_none());
    }
}
