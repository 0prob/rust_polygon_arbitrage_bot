use alloy::primitives::Address;
use alloy::sol_types::SolCall;

use crate::abis::{ExecutorCall, ICurveCryptoPool, ICurveStableNgPool, ICurveStablePool};
use crate::core::types::ProtocolType;
use crate::pipeline::arena::StateArena;
use crate::services::execution::calldata::approvals::encode_approve_if_needed;
use crate::services::execution::calldata::types::CalldataHop;
use crate::services::execution::profit::slippage_adjusted;
use crate::services::execution::quote::quote_hop_for_execution;
use alloy::primitives::U256;

use super::shared::curve_uses_receiver;

/// Encode a Curve pool hop into executor calls
///
/// Supports three Curve pool types:
/// - StableSwap_NG (uses receiver parameter)
/// - Crypto pools (different interface)
/// - Standard StableSwap pools
pub fn encode_curve_hop(
    hop: &CalldataHop,
    recipient: Address,
    arena: &StateArena,
    slippage_bps: u64,
) -> anyhow::Result<Vec<ExecutorCall>> {
    if hop.edge.token_in_idx == hop.edge.token_out_idx {
        anyhow::bail!("curve hop token indices must differ");
    }

    let quoted_out = quote_hop_for_execution(arena, hop).unwrap_or(hop.amount_out);
    let min_dy = slippage_adjusted(quoted_out, slippage_bps)
        .ok_or_else(|| anyhow::anyhow!("curve hop min out is zero"))?;

    let i = hop.edge.token_in_idx as i128;
    let j = hop.edge.token_out_idx as i128;
    let mut calls = vec![encode_approve_if_needed(
        recipient,
        hop.token_in,
        hop.pool_address,
        hop.amount_in,
    )];

    let exchange_data = if curve_uses_receiver(hop.protocol_label.as_deref()) {
        ICurveStableNgPool::exchangeCall {
            i,
            j,
            dx: hop.amount_in,
            min_dy,
            receiver: recipient,
        }
        .abi_encode()
    } else if matches!(hop.edge.protocol, ProtocolType::CurveCrypto) {
        ICurveCryptoPool::exchangeCall {
            i: U256::from(hop.edge.token_in_idx),
            j: U256::from(hop.edge.token_out_idx),
            dx: hop.amount_in,
            min_dy,
        }
        .abi_encode()
    } else {
        ICurveStablePool::exchangeCall {
            i,
            j,
            dx: hop.amount_in,
            min_dy,
        }
        .abi_encode()
    };

    calls.push(ExecutorCall {
        target: hop.pool_address,
        value: U256::ZERO,
        data: exchange_data.into(),
    });
    Ok(calls)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{Edge, PoolIndex, TokenIndex};

    fn create_test_hop() -> CalldataHop {
        CalldataHop {
            edge: Edge {
                pool_index: PoolIndex(0),
                token_in: TokenIndex(0),
                token_out: TokenIndex(1),
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::CurveStable,
                fee_bps: 4,
                zero_for_one: true,
            },
            pool_address: Address::repeat_byte(0x01),
            token_in: Address::repeat_byte(0x02),
            token_out: Address::repeat_byte(0x03),
            amount_in: U256::from(1000),
            amount_out: U256::from(900),
            pool_id: None,
            protocol_label: None,
            router: None,
            hooks: None,
        }
    }

    #[test]
    fn encode_curve_hop_returns_two_calls() {
        let hop = create_test_hop();
        let recipient = Address::repeat_byte(0x04);
        let arena = StateArena::new();

        let result = encode_curve_hop(&hop, recipient, &arena, 50);

        assert!(result.is_ok());
        let calls = result.unwrap();
        assert_eq!(calls.len(), 2, "Curve hop should generate exactly 2 calls");
    }

    #[test]
    fn encode_curve_hop_rejects_same_token_indices() {
        let mut hop = create_test_hop();
        hop.edge.token_out_idx = hop.edge.token_in_idx; // Same indices

        let recipient = Address::repeat_byte(0x04);
        let arena = StateArena::new();

        let result = encode_curve_hop(&hop, recipient, &arena, 50);

        // Should fail with token indices must differ error
        assert!(result.is_err());
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("token indices must differ")
        );
    }

    #[test]
    fn encode_curve_hop_first_call_is_approval() {
        let hop = create_test_hop();
        let recipient = Address::repeat_byte(0x04);
        let arena = StateArena::new();

        let result = encode_curve_hop(&hop, recipient, &arena, 50);
        let calls = result.unwrap();

        // First call should be approval to token_in
        assert_eq!(
            calls[0].target, hop.token_in,
            "First call should target token_in"
        );
        assert_eq!(
            calls[0].value,
            U256::ZERO,
            "Approval should have zero value"
        );
    }

    #[test]
    fn encode_curve_hop_second_call_is_exchange() {
        let hop = create_test_hop();
        let recipient = Address::repeat_byte(0x04);
        let arena = StateArena::new();

        let result = encode_curve_hop(&hop, recipient, &arena, 50);
        let calls = result.unwrap();

        // Second call should be to pool
        assert_eq!(
            calls[1].target, hop.pool_address,
            "Second call should target pool"
        );
        assert_eq!(
            calls[1].value,
            U256::ZERO,
            "Exchange should have zero value"
        );
    }

    #[test]
    fn encode_curve_hop_zero_slippage_fails() {
        let hop = CalldataHop {
            amount_out: U256::ZERO,
            ..create_test_hop()
        };
        let recipient = Address::repeat_byte(0x04);
        let arena = StateArena::new();

        let result = encode_curve_hop(&hop, recipient, &arena, 10000);

        // Should fail due to zero min_dy
        assert!(result.is_err());
    }

    #[test]
    fn encode_curve_hop_supports_crypto_pools() {
        let mut hop = create_test_hop();
        hop.edge.protocol = ProtocolType::CurveCrypto;

        let recipient = Address::repeat_byte(0x04);
        let arena = StateArena::new();

        let result = encode_curve_hop(&hop, recipient, &arena, 50);

        // Should succeed for crypto pools
        assert!(result.is_ok());
        let calls = result.unwrap();
        assert_eq!(calls.len(), 2);
    }
}
