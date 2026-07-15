use alloy::dyn_abi::DynSolValue;
use alloy::primitives::{Address, U256};
use alloy::sol_types::SolCall;

use super::super::CalldataHop;
use super::shared::{compute_min_out, v2_callback_protocol_id};
use crate::abis::{ExecutorCall, IUniswapV2Pair};
use crate::pipeline::arena::StateArena;
use crate::services::execution::calldata::approvals::encode_token_transfer;

/// Encode a Uniswap V2 hop into executor calls.
///
/// Returns two executor calls:
/// 1. Transfer token_in to the pool (via transferAll or explicit transfer)
/// 2. Call swap on the pair contract with callback data (protocolId, token0, token1)
pub fn encode_v2_hop(
    arena: &StateArena,
    hop: &CalldataHop,
    recipient: Address,
    slippage_bps: u64,
    use_transfer_all: bool,
) -> anyhow::Result<Vec<ExecutorCall>> {
    if hop.edge.zero_for_one != (hop.token_in < hop.token_out) {
        anyhow::bail!("v2 zero_for_one must match sorted token0 (token_in < token_out)");
    }
    let min_out = compute_min_out(arena, hop, slippage_bps, "v2")?;

    let mut calls = Vec::with_capacity(2);

    calls.push(encode_token_transfer(
        recipient,
        hop.token_in,
        hop.pool_address,
        hop.amount_in,
        use_transfer_all,
    ));

    let (amount0_out, amount1_out) = if hop.edge.zero_for_one {
        (U256::ZERO, min_out)
    } else {
        (min_out, U256::ZERO)
    };

    let proto_id = v2_callback_protocol_id(hop.protocol_label.as_deref());
    let callback_data = DynSolValue::Tuple(vec![
        DynSolValue::Uint(U256::from(proto_id), 8),
        DynSolValue::Address(hop.token_in),
        DynSolValue::Address(hop.token_out),
    ])
    .abi_encode();

    let swap = IUniswapV2Pair::swapCall {
        amount0Out: amount0_out,
        amount1Out: amount1_out,
        to: recipient,
        data: callback_data.into(),
    };

    calls.push(ExecutorCall {
        target: hop.pool_address,
        value: U256::ZERO,
        data: swap.abi_encode().into(),
    });

    Ok(calls)
}
