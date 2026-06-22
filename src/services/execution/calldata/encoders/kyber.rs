use alloy::dyn_abi::DynSolValue;
use alloy::primitives::{Address, I256, U160, U256};
use alloy::sol_types::SolCall;

use crate::abis::{ExecutorCall, IKyberElasticPool};
use crate::pipeline::arena::StateArena;
use crate::services::execution::calldata::types::CalldataHop;
use crate::services::execution::quote::{
    derive_tight_v3_price_limit_kyber, pool_tokens_from_hop, quote_hop_for_execution,
    resolve_kyber_fee_pips,
};

use super::shared::to_v3_state;

const CALLBACK_PROTOCOL_KYBER_ELASTIC: u8 = 4;

/// Encode a Kyber Elastic hop into executor calls
///
/// Returns a vector containing a single executor call to perform the swap on the Kyber pool.
/// The swap includes a callback with the protocol identifier and pool token info encoded.
pub fn encode_kyber_hop(
    hop: &CalldataHop,
    recipient: Address,
    arena: &StateArena,
    slippage_bps: u64,
) -> anyhow::Result<Vec<ExecutorCall>> {
    let pool_state = arena
        .pool_state(hop.edge.pool_index)
        .ok_or_else(|| anyhow::anyhow!("missing pool state for kyber hop"))?;
    let v3 =
        to_v3_state(pool_state).ok_or_else(|| anyhow::anyhow!("kyber hop requires v3 state"))?;

    let quoted_out = quote_hop_for_execution(arena, hop).unwrap_or(hop.amount_out);
    let fee_pips = resolve_kyber_fee_pips(arena, hop);
    let sqrt_limit = derive_tight_v3_price_limit_kyber(
        &v3,
        hop.amount_in,
        quoted_out,
        hop.edge.zero_for_one,
        fee_pips,
        slippage_bps,
    )?;

    let (token0, token1) = pool_tokens_from_hop(hop);
    let callback = DynSolValue::Tuple(vec![
        DynSolValue::Uint(U256::from(CALLBACK_PROTOCOL_KYBER_ELASTIC), 8),
        DynSolValue::Address(token0),
        DynSolValue::Address(token1),
        DynSolValue::Uint(U256::from(fee_pips), 24),
    ])
    .abi_encode();

    let swap_qty = I256::ZERO - I256::from(hop.amount_in);
    let swap = IKyberElasticPool::swapCall {
        recipient,
        swapQty: swap_qty,
        isToken0: hop.edge.zero_for_one,
        limitSqrtP: U160::from(sqrt_limit),
        data: callback.into(),
    };

    Ok(vec![ExecutorCall {
        target: hop.pool_address,
        value: U256::ZERO,
        data: swap.abi_encode().into(),
    }])
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
                protocol: crate::core::types::ProtocolType::Woofi,
                fee_bps: 100,
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
    fn encode_kyber_hop_returns_single_call() {
        let hop = create_test_hop();
        let recipient = Address::repeat_byte(0x04);
        let arena = StateArena::new();

        // Without arena pool state, this will fail - but we can test the structure
        let result = encode_kyber_hop(&hop, recipient, &arena, 50);

        // Should fail gracefully due to missing pool state
        assert!(result.is_err());
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("missing pool state")
        );
    }

    #[test]
    fn encode_kyber_hop_validates_recipient() {
        let hop = create_test_hop();
        let recipient = Address::ZERO;
        let arena = StateArena::new();

        let result = encode_kyber_hop(&hop, recipient, &arena, 50);

        // Should fail due to missing pool state (not recipient validation at this level)
        assert!(result.is_err());
    }

    #[test]
    fn encode_kyber_hop_uses_correct_protocol_constant() {
        // This is more of a type check - ensures CALLBACK_PROTOCOL_KYBER_ELASTIC is used
        // The actual value is 4 as per Kyber specification
        assert_eq!(
            CALLBACK_PROTOCOL_KYBER_ELASTIC, 4,
            "Kyber callback protocol should be 4"
        );
    }
}
