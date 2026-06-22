use alloy::primitives::Address;
use alloy::sol_types::SolCall;

use crate::abis::{ExecutorCall, IWoofiRouter};
use crate::core::constants::WOOFI_ROUTER_V2;
use crate::pipeline::arena::StateArena;
use crate::services::execution::calldata::approvals::encode_approve_if_needed;
use crate::services::execution::calldata::types::CalldataHop;
use crate::services::execution::profit::slippage_adjusted;
use crate::services::execution::quote::quote_hop_for_execution;
use alloy::primitives::U256;

/// Encode a Woofi router hop into executor calls
///
/// Returns a vector containing:
/// 1. An approval call if needed to the Woofi router
/// 2. A swap call to the router
pub fn encode_woofi_hop(
    hop: &CalldataHop,
    recipient: Address,
    arena: &StateArena,
    slippage_bps: u64,
) -> anyhow::Result<Vec<ExecutorCall>> {
    let router = resolve_woofi_router(hop);
    let quoted_out = quote_hop_for_execution(arena, hop).unwrap_or(hop.amount_out);
    let min_to = slippage_adjusted(quoted_out, slippage_bps)
        .ok_or_else(|| anyhow::anyhow!("woofi hop min out is zero"))?;

    let swap = IWoofiRouter::swapCall {
        fromToken: hop.token_in,
        toToken: hop.token_out,
        fromAmount: hop.amount_in,
        minToAmount: min_to,
        to: recipient,
        rebateTo: Address::ZERO,
    };

    Ok(vec![
        encode_approve_if_needed(recipient, hop.token_in, router, hop.amount_in),
        ExecutorCall {
            target: router,
            value: U256::ZERO,
            data: swap.abi_encode().into(),
        },
    ])
}

/// Helper: Resolve Woofi router address
///
/// Uses explicit router if provided in hop, otherwise uses default Woofi V2 router
fn resolve_woofi_router(hop: &CalldataHop) -> Address {
    hop.router.unwrap_or(WOOFI_ROUTER_V2)
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
    fn encode_woofi_hop_returns_two_calls() {
        let hop = create_test_hop();
        let recipient = Address::repeat_byte(0x04);
        let arena = StateArena::new();

        let result = encode_woofi_hop(&hop, recipient, &arena, 50);

        assert!(result.is_ok());
        let calls = result.unwrap();
        assert_eq!(calls.len(), 2, "Woofi hop should generate exactly 2 calls");
    }

    #[test]
    fn encode_woofi_hop_first_call_is_approval() {
        let hop = create_test_hop();
        let recipient = Address::repeat_byte(0x04);
        let arena = StateArena::new();

        let result = encode_woofi_hop(&hop, recipient, &arena, 50);
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
    fn encode_woofi_hop_second_call_is_swap() {
        let hop = create_test_hop();
        let recipient = Address::repeat_byte(0x04);
        let arena = StateArena::new();

        let result = encode_woofi_hop(&hop, recipient, &arena, 50);
        let calls = result.unwrap();

        // Second call should be to router
        let router = resolve_woofi_router(&hop);
        assert_eq!(
            calls[1].target, router,
            "Second call should target Woofi router"
        );
        assert_eq!(calls[1].value, U256::ZERO, "Swap should have zero value");
    }

    #[test]
    fn encode_woofi_hop_zero_slippage_fails() {
        let hop = CalldataHop {
            amount_out: U256::ZERO,
            ..create_test_hop()
        };
        let recipient = Address::repeat_byte(0x04);
        let arena = StateArena::new();

        let result = encode_woofi_hop(&hop, recipient, &arena, 10000);

        // Should fail due to zero min_to
        assert!(result.is_err());
    }

    #[test]
    fn resolve_woofi_router_uses_explicit() {
        let router_addr = Address::repeat_byte(0xab);
        let hop = CalldataHop {
            router: Some(router_addr),
            ..create_test_hop()
        };

        let resolved = resolve_woofi_router(&hop);
        assert_eq!(resolved, router_addr, "Should use explicit router");
    }

    #[test]
    fn resolve_woofi_router_defaults_to_v2() {
        let hop = CalldataHop {
            router: None,
            ..create_test_hop()
        };

        let resolved = resolve_woofi_router(&hop);
        let default_router = WOOFI_ROUTER_V2;
        assert_eq!(resolved, default_router, "Should use default V2 router");
    }
}
