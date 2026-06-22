use alloy::primitives::{Address, U256};
use alloy::sol_types::SolCall;

use crate::abis::{BalancerFundManagement, BalancerSingleSwap, ExecutorCall, IBalancerVault};
use crate::core::constants::BALANCER_VAULT;
use crate::pipeline::arena::StateArena;
use crate::services::execution::calldata::approvals::encode_approve_if_needed;
use crate::services::execution::calldata::types::CalldataHop;
use crate::services::execution::profit::slippage_adjusted;
use crate::services::execution::quote::quote_hop_for_execution;

use super::shared::resolve_balancer_pool_id;

const BALANCER_GIVEN_IN: u8 = 0;

/// Encode a Balancer Vault hop into executor calls
///
/// Returns a vector containing:
/// 1. An approval call if needed to the Balancer Vault
/// 2. A swap call to the vault's single swap function
pub fn encode_balancer_hop(
    hop: &CalldataHop,
    recipient: Address,
    arena: &StateArena,
    slippage_bps: u64,
    deadline: U256,
) -> anyhow::Result<Vec<ExecutorCall>> {
    let vault: Address = BALANCER_VAULT;
    let pool_id = resolve_balancer_pool_id(hop.pool_address, hop.pool_id);
    let quoted_out = quote_hop_for_execution(arena, hop).unwrap_or(hop.amount_out);
    let limit = slippage_adjusted(quoted_out, slippage_bps)
        .ok_or_else(|| anyhow::anyhow!("balancer hop min out is zero"))?;

    let swap = IBalancerVault::swapCall {
        singleSwap: BalancerSingleSwap {
            poolId: pool_id,
            kind: BALANCER_GIVEN_IN,
            assetIn: hop.token_in,
            assetOut: hop.token_out,
            amount: hop.amount_in,
            userData: alloy::primitives::Bytes::new(),
        },
        funds: BalancerFundManagement {
            sender: recipient,
            fromInternalBalance: false,
            recipient,
            toInternalBalance: false,
        },
        limit,
        deadline,
    };

    Ok(vec![
        encode_approve_if_needed(recipient, hop.token_in, vault, hop.amount_in),
        ExecutorCall {
            target: vault,
            value: U256::ZERO,
            data: swap.abi_encode().into(),
        },
    ])
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
                protocol: crate::core::types::ProtocolType::BalancerV2,
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
    fn encode_balancer_hop_returns_two_calls() {
        let hop = create_test_hop();
        let recipient = Address::repeat_byte(0x04);
        let arena = StateArena::new();

        let result = encode_balancer_hop(&hop, recipient, &arena, 50, U256::from(1000000));

        assert!(result.is_ok());
        let calls = result.unwrap();
        assert_eq!(
            calls.len(),
            2,
            "Balancer hop should generate exactly 2 calls"
        );
    }

    #[test]
    fn encode_balancer_hop_first_call_is_approval() {
        let hop = create_test_hop();
        let recipient = Address::repeat_byte(0x04);
        let arena = StateArena::new();

        let result = encode_balancer_hop(&hop, recipient, &arena, 50, U256::from(1000000));
        let calls = result.unwrap();

        // First call should be approval to executor
        assert_eq!(
            calls[0].target, recipient,
            "First call should target executor"
        );
        assert_eq!(
            calls[0].value,
            U256::ZERO,
            "Approval should have zero value"
        );
    }

    #[test]
    fn encode_balancer_hop_second_call_is_swap() {
        let hop = create_test_hop();
        let recipient = Address::repeat_byte(0x04);
        let arena = StateArena::new();

        let result = encode_balancer_hop(&hop, recipient, &arena, 50, U256::from(1000000));
        let calls = result.unwrap();

        // Second call should be to vault
let vault = BALANCER_VAULT;
        assert_eq!(
            calls[1].target, vault,
            "Second call should target Balancer Vault"
        );
        assert_eq!(calls[1].value, U256::ZERO, "Swap should have zero value");
    }

    #[test]
    fn encode_balancer_hop_zero_slippage_fails() {
        let hop = CalldataHop {
            amount_out: U256::ZERO,
            ..create_test_hop()
        };
        let recipient = Address::repeat_byte(0x04);
        let arena = StateArena::new();

        let result = encode_balancer_hop(&hop, recipient, &arena, 10000, U256::from(1000000));

        // Should fail due to zero limit
        assert!(result.is_err());
    }
}
