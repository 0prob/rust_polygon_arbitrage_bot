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
    let fee_pips = resolve_v3_fee_pips_for_hop(arena, hop);
    let sqrt_limit = derive_tight_v3_price_limit(
        &v3,
        hop.amount_in,
        quoted_out,
        hop.edge.zero_for_one,
        hop.edge.fee_bps,
        slippage_bps,
        Some(fee_pips),
        false,
    )?;

    let (token0, token1) = pool_tokens_from_hop(hop);
    let proto_id = v3_callback_protocol_id(hop.protocol_label.as_deref());
    // Surface callback verify inputs — dry-run InvalidPoolCaller(expected=fee)
    // means factory getPool did not return msg.sender (fee word leaked as expected).
    crate::debug!(
        "v3 encode: pool={} proto_id={proto_id} fee_pips={fee_pips} edge_fee_bps={} ain={} aout={} zfo={} sqrt_limit={sqrt_limit} in={} out={} token0={token0} token1={token1} label={:?}",
        hop.pool_address,
        hop.edge.fee_bps,
        hop.amount_in,
        hop.amount_out,
        hop.edge.zero_for_one,
        hop.token_in,
        hop.token_out,
        hop.protocol_label
    );
    if fee_pips == 0 {
        anyhow::bail!("v3 encode refuse: fee_pips=0 pool={}", hop.pool_address);
    }

    let callback = DynSolValue::Tuple(vec![
        DynSolValue::Uint(U256::from(proto_id), 8),
        DynSolValue::Address(token0),
        DynSolValue::Address(token1),
        DynSolValue::Uint(U256::from(fee_pips), 24),
    ])
    .abi_encode();

    // V3 exact-in: amountSpecified is POSITIVE (negative means exact-output; the
    // negative-exact-in convention is V4-only). Fail closed if amount does not fit i256.
    let amount_spec = I256::try_from(hop.amount_in)
        .map_err(|_| anyhow::anyhow!("v3 amount_in does not fit i256"))?;
    let swap = IUniswapV3Pool::swapCall {
        recipient,
        zeroForOne: hop.edge.zero_for_one,
        amountSpecified: amount_spec,
        sqrtPriceLimitX96: U160::from(sqrt_limit),
        data: callback.into(),
    };

    Ok(vec![ExecutorCall {
        target: hop.pool_address,
        value: U256::ZERO,
        data: swap.abi_encode().into(),
    }])
}
