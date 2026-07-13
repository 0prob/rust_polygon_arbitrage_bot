use alloy::dyn_abi::DynSolValue;
use alloy::primitives::{Address, I256, U160, U256};
use alloy::sol_types::SolCall;

use crate::abis::{ExecutorCall, IUniswapV3Pool};
use crate::pipeline::arena::StateArena;
use crate::services::execution::calldata::CalldataHop;
use crate::services::execution::quote::{
    derive_tight_v3_price_limit, pool_tokens_from_hop, quote_hop_for_execution,
    resolve_v3_fee_pips_for_hop,
};

use super::shared::{to_v3_state, v3_callback_protocol_id};

/// Encode a Uniswap V3 (or SushiSwap V3) hop into executor calls.
///
/// Returns a single executor call to swap on the V3 pool with callback data
/// containing (protocolId, token0, token1, fee_or_tickSpacing).
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

    let quoted_out = quote_hop_for_execution(arena, hop)
        .ok_or_else(|| anyhow::anyhow!("v3 execution quote unavailable"))?;
    let sqrt_limit = derive_tight_v3_price_limit(
        &v3,
        hop.amount_in,
        quoted_out,
        hop.edge.zero_for_one,
        hop.edge.fee_bps,
        slippage_bps,
        None,
    )?;

    let (token0, token1) = pool_tokens_from_hop(hop);
    let proto_id = v3_callback_protocol_id(hop.protocol_label.as_deref());
    let fourth = resolve_v3_callback_fourth_field(arena, hop);

    let callback = DynSolValue::Tuple(vec![
        DynSolValue::Uint(U256::from(proto_id), 8),
        DynSolValue::Address(token0),
        DynSolValue::Address(token1),
        fourth,
    ])
    .abi_encode();

    let swap = IUniswapV3Pool::swapCall {
        recipient,
        zeroForOne: hop.edge.zero_for_one,
        amountSpecified: -I256::from(hop.amount_in),
        sqrtPriceLimitX96: U160::from(sqrt_limit),
        data: callback.into(),
    };

    Ok(vec![ExecutorCall {
        target: hop.pool_address,
        value: U256::ZERO,
        data: swap.abi_encode().into(),
    }])
}

fn resolve_v3_callback_fourth_field(arena: &StateArena, hop: &CalldataHop) -> DynSolValue {
    let fee = resolve_v3_fee_pips_for_hop(arena, hop);
    DynSolValue::Uint(U256::from(fee), 24)
}
