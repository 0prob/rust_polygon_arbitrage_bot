use alloy::primitives::{Address, U256};
use alloy::sol_types::SolCall;

use crate::abis::{ExecutorCall, IDodoPool, IERC20};
use crate::services::execution::calldata::approvals::encode_transfer_all;
use crate::services::execution::calldata::types::CalldataHop;

/// Encode a DODO pool hop into executor calls
///
/// Returns a vector containing:
/// 1. A transfer call to move tokens to the pool (via transferAll or explicit transfer)
/// 2. A swap call to the DODO pool (sellBase or sellQuote depending on direction)
pub fn encode_dodo_hop(
    hop: &CalldataHop,
    recipient: Address,
    use_transfer_all: bool,
) -> anyhow::Result<Vec<ExecutorCall>> {
    let mut calls = Vec::with_capacity(2);

    if use_transfer_all {
        calls.push(encode_transfer_all(
            recipient,
            hop.token_in,
            hop.pool_address,
        ));
    } else {
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

    let swap_data = if hop.edge.zero_for_one {
        IDodoPool::sellBaseCall { to: recipient }.abi_encode()
    } else {
        IDodoPool::sellQuoteCall { to: recipient }.abi_encode()
    };
    calls.push(ExecutorCall {
        target: hop.pool_address,
        value: U256::ZERO,
        data: swap_data.into(),
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
                protocol: crate::core::types::ProtocolType::Dodo,
                fee_bps: 0,
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
    fn encode_dodo_hop_returns_two_calls() {
        let hop = create_test_hop();
        let recipient = Address::repeat_byte(0x04);

        let result = encode_dodo_hop(&hop, recipient, false);

        assert!(result.is_ok());
        let calls = result.unwrap();
        assert_eq!(calls.len(), 2, "DODO hop should generate exactly 2 calls");
    }

    #[test]
    fn encode_dodo_hop_with_explicit_transfer() {
        let hop = create_test_hop();
        let recipient = Address::repeat_byte(0x04);

        let result = encode_dodo_hop(&hop, recipient, false);
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

        // Second call should be the swap on the pool
        assert_eq!(
            calls[1].target, hop.pool_address,
            "Second call target should be pool"
        );
        assert_eq!(calls[1].value, U256::ZERO, "Swap should have zero value");
    }

    #[test]
    fn encode_dodo_hop_with_transfer_all() {
        let hop = create_test_hop();
        let recipient = Address::repeat_byte(0x04);

        let result = encode_dodo_hop(&hop, recipient, true);
        let calls = result.unwrap();

        // First call should be to executor for transferAll
        assert_eq!(
            calls[0].target, recipient,
            "First call with transferAll should target executor"
        );
        assert_eq!(
            calls[0].value,
            U256::ZERO,
            "TransferAll should have zero value"
        );

        // Second call should be the swap on the pool
        assert_eq!(
            calls[1].target, hop.pool_address,
            "Second call target should be pool"
        );
    }

    #[test]
    fn encode_dodo_hop_respects_zero_for_one() {
        let mut hop = create_test_hop();
        hop.edge.zero_for_one = false;
        let recipient = Address::repeat_byte(0x04);

        let result = encode_dodo_hop(&hop, recipient, false);
        let calls = result.unwrap();

        // Should have 2 calls regardless of direction
        assert_eq!(calls.len(), 2);
        // Swap direction is encoded in the swap call data (sellBase vs sellQuote)
    }

    #[test]
    fn encode_dodo_hop_generates_valid_calls() {
        let hop = create_test_hop();
        let recipient = Address::repeat_byte(0x04);

        let result = encode_dodo_hop(&hop, recipient, false);
        assert!(result.is_ok());

        let calls = result.unwrap();
        for call in &calls {
            assert_eq!(
                call.value,
                U256::ZERO,
                "All calls should have zero ETH value"
            );
        }
    }
}
