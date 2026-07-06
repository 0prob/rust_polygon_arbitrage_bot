use alloy::primitives::Address;
use alloy::sol_types::SolCall;

use crate::abis::{ExecutorCall, IWoofiRouter};
use crate::core::constants::WOOFI_ROUTER_V2;
use crate::pipeline::arena::StateArena;
use crate::services::execution::calldata::CalldataHop;
use crate::services::execution::calldata::approvals::encode_approve_if_needed;
use alloy::primitives::U256;

use super::shared::compute_min_out;

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
    let min_to = compute_min_out(arena, hop, slippage_bps, "woofi")?;

    let swap = IWoofiRouter::swapCall {
        fromToken: hop.token_in,
        toToken: hop.token_out,
        fromAmount: hop.amount_in,
        minToAmount: min_to,
        to: recipient,
        rebateTo: Address::ZERO,
    };

    Ok(vec![
        encode_approve_if_needed(hop.token_in, router, hop.amount_in),
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
