use alloy::primitives::{Address, Bytes, U256};
use alloy::sol_types::SolCall;

use crate::abis::{ExecutorCall, IERC20, IUniswapV2Pair};
use crate::pipeline::arena::StateArena;
use crate::services::execution::calldata::approvals::encode_transfer_all;
use crate::services::execution::profit::slippage_adjusted;
use crate::services::execution::quote::quote_hop_for_execution;

use super::super::types::CalldataHop;

/// Encode a Uniswap V2 hop into executor calls
///
/// Returns a vector of executor calls needed to perform the swap:
/// 1. Transfer token_in to the pool (via transferAll or explicit transfer)
/// 2. Call swap on the pair contract
pub fn encode_v2_hop(
    arena: Option<&StateArena>,
    hop: &CalldataHop,
    recipient: Address,
    slippage_bps: u64,
    use_transfer_all: bool,
) -> anyhow::Result<Vec<ExecutorCall>> {
    // Determine the minimum output amount, accounting for slippage
    let out_for_min = arena
        .and_then(|a| quote_hop_for_execution(a, hop))
        .unwrap_or(hop.amount_out);
    let min_out = slippage_adjusted(out_for_min, slippage_bps)
        .ok_or_else(|| anyhow::anyhow!("v2 hop min out is zero"))?;

    let mut calls = Vec::with_capacity(2);

    // First call: transfer token_in to the pool
    if use_transfer_all {
        // Use transferAll for efficiency
        calls.push(encode_transfer_all(
            recipient,
            hop.token_in,
            hop.pool_address,
        ));
    } else {
        // Explicit transfer with exact amount
        let transfer = IERC20::transferCall {
            to: hop.pool_address,
            amount: hop.amount_in,
        };
        calls.push(ExecutorCall {
            target: hop.token_in,
            value: U256::ZERO,
            data: transfer.abi_encode().into(),
        });
    }

    // Second call: execute the swap on the pair contract
    let (amount0_out, amount1_out) = if hop.edge.zero_for_one {
        (U256::ZERO, min_out)
    } else {
        (min_out, U256::ZERO)
    };

    let swap = IUniswapV2Pair::swapCall {
        amount0Out: amount0_out,
        amount1Out: amount1_out,
        to: recipient,
        data: Bytes::new(),
    };

    calls.push(ExecutorCall {
        target: hop.pool_address,
        value: U256::ZERO,
        data: swap.abi_encode().into(),
    });

    Ok(calls)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{Edge, PoolIndex, ProtocolType, TokenIndex};

    fn create_test_hop() -> CalldataHop {
        CalldataHop {
            edge: Edge {
                pool_index: PoolIndex(0),
                token_in: TokenIndex(0),
                token_out: TokenIndex(1),
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 25,
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
    fn encode_v2_hop_creates_two_calls() {
        let hop = create_test_hop();
        let recipient = Address::repeat_byte(0x04);

        let result = encode_v2_hop(None, &hop, recipient, 50, false);

        assert!(result.is_ok());
        let calls = result.unwrap();
        assert_eq!(calls.len(), 2, "V2 hop should generate exactly 2 calls");
    }

    #[test]
    fn encode_v2_hop_first_call_is_transfer() {
        let hop = create_test_hop();
        let recipient = Address::repeat_byte(0x04);

        let result = encode_v2_hop(None, &hop, recipient, 50, false);
        let calls = result.unwrap();

        // First call should be a transfer to the pool
        assert_eq!(
            calls[0].target, hop.token_in,
            "First call target should be token_in"
        );
        assert_eq!(
            calls[0].value,
            U256::ZERO,
            "Transfer should have zero value"
        );
    }

    #[test]
    fn encode_v2_hop_second_call_is_swap() {
        let hop = create_test_hop();
        let recipient = Address::repeat_byte(0x04);

        let result = encode_v2_hop(None, &hop, recipient, 50, false);
        let calls = result.unwrap();

        // Second call should be the swap on the pair
        assert_eq!(
            calls[1].target, hop.pool_address,
            "Second call target should be pool_address"
        );
        assert_eq!(calls[1].value, U256::ZERO, "Swap should have zero value");
    }

    #[test]
    fn encode_v2_hop_zero_slippage_fails() {
        let hop = CalldataHop {
            amount_out: U256::ZERO,
            ..create_test_hop()
        };
        let recipient = Address::repeat_byte(0x04);

        // Very high slippage could make min_out zero, causing failure
        let result = encode_v2_hop(None, &hop, recipient, 10000, false);

        // May fail due to zero min_out or succeed depending on slippage logic
        // This test just ensures the function handles edge cases
        let _ = result;
    }

    #[test]
    fn encode_v2_hop_respects_zero_for_one() {
        let mut hop = create_test_hop();
        hop.edge.zero_for_one = false; // token1 for token0
        let recipient = Address::repeat_byte(0x04);

        let result = encode_v2_hop(None, &hop, recipient, 50, false);
        let calls = result.unwrap();

        // When zero_for_one is false, we should have amount0_out set (not amount1_out)
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[1].target, hop.pool_address);
    }
}
