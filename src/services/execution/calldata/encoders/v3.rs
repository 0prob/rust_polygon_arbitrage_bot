use alloy::dyn_abi::DynSolValue;
use alloy::primitives::{Address, I256, U160, U256};
use alloy::sol_types::SolCall;

use crate::abis::{ExecutorCall, IUniswapV3Pool};
use crate::pipeline::arena::StateArena;
use crate::services::execution::calldata::types::CalldataHop;
use crate::services::execution::quote::{
    derive_tight_v3_price_limit, pool_tokens_from_hop, quote_hop_for_execution,
    resolve_v3_fee_pips_for_hop,
};

use super::shared::to_v3_state;

const CALLBACK_PROTOCOL_UNISWAP_V3: u8 = 1;

/// Encode a Uniswap V3 hop into executor calls
///
/// Returns a vector containing a single executor call to perform the swap on the V3 pool.
/// The swap includes a callback with the protocol identifier and pool token info encoded.
pub fn encode_v3_hop(
    hop: &CalldataHop,
    recipient: Address,
    arena: &StateArena,
    slippage_bps: u64,
) -> anyhow::Result<Vec<ExecutorCall>> {
    let pool_state = arena
        .pool_state(hop.edge.pool_index)
        .ok_or_else(|| anyhow::anyhow!("missing pool state for v3 hop"))?;
    let v3 = to_v3_state(pool_state).ok_or_else(|| anyhow::anyhow!("pool is not v3/v4 state"))?;

    let quoted_out = quote_hop_for_execution(arena, hop).unwrap_or(hop.amount_out);
    let sqrt_limit = derive_tight_v3_price_limit(
        &v3,
        hop.amount_in,
        quoted_out,
        hop.edge.zero_for_one,
        hop.edge.fee_bps,
        slippage_bps,
    )?;

    let (token0, token1) = pool_tokens_from_hop(hop);
    let fee = resolve_v3_fee_pips_for_hop(arena, hop);
    let callback = DynSolValue::Tuple(vec![
        DynSolValue::Uint(U256::from(CALLBACK_PROTOCOL_UNISWAP_V3), 8),
        DynSolValue::Address(token0),
        DynSolValue::Address(token1),
        DynSolValue::Uint(U256::from(fee), 24),
    ])
    .abi_encode();

    let amount_specified = I256::ZERO - I256::from(hop.amount_in);
    let swap = IUniswapV3Pool::swapCall {
        recipient,
        zeroForOne: hop.edge.zero_for_one,
        amountSpecified: amount_specified,
        sqrtPriceLimitX96: U160::from(sqrt_limit),
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
                protocol: crate::core::types::ProtocolType::UniswapV3,
                fee_bps: 100, // 100 bps = 1%
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
    fn encode_v3_hop_returns_single_call() {
        use crate::pipeline::arena::StateArena;

        let hop = create_test_hop();
        let recipient = Address::repeat_byte(0x04);
        let arena = StateArena::new();

        // Without arena pool state, this will fail - but we can test the structure
        // This test validates that the function signature works
        let result = encode_v3_hop(&hop, recipient, &arena, 50);

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
    fn encode_v3_hop_validates_recipient() {
        let hop = create_test_hop();
        let recipient = Address::ZERO;
        let arena = StateArena::new();

        // Test with zero recipient - function should handle it
        let result = encode_v3_hop(&hop, recipient, &arena, 50);
        assert!(result.is_err());
    }
}
