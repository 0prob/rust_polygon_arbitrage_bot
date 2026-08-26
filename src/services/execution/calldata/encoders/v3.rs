use alloy::dyn_abi::DynSolValue;
use alloy::primitives::{Address, I256, U160, U256};
use alloy::sol_types::SolCall;

use crate::abis::{ExecutorCall, IUniswapV3Pool};
use crate::pipeline::arena::StateArena;
use crate::services::execution::calldata::CalldataHop;
use crate::services::execution::quote::pool_tokens_from_hop;

use super::shared::{ClSwapLimit, resolve_cl_swap_limit, v3_callback_protocol_id};

/// Encode a Uniswap V3 (or SushiSwap V3) hop into executor calls.
///
/// Returns a single executor call to swap on the V3 pool with callback data
/// containing (protocolId, token0, token1, fee_or_tickSpacing).
///
/// `full_range_limit`: intermediate hops must fully consume `amount_in` so the
/// next hop's chain_in is funded. Tight limits + stale slot0 cause partial fills
/// → mid-hop TransferFailed (live WPOL→BRZ/CES). Last hop keeps a tight limit.
///
/// `prequoted_out`: reuse the execution quote from `encode_route` chain_in sizing
/// when present (avoids a second CL walk per hop).
pub fn encode_v3_hop(
    hop: &CalldataHop,
    recipient: Address,
    arena: &StateArena,
    slippage_bps: u64,
    full_range_limit: bool,
    prequoted_out: Option<U256>,
) -> anyhow::Result<Vec<ExecutorCall>> {
    let ClSwapLimit {
        fee_pips,
        sqrt_limit,
    } = resolve_cl_swap_limit(
        arena,
        hop,
        slippage_bps,
        full_range_limit,
        prequoted_out,
        false,
        "v3",
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
        sqrtPriceLimitX96: sqrt_limit_u160(sqrt_limit)?,
        data: callback.into(),
    };

    Ok(vec![ExecutorCall {
        target: hop.pool_address,
        value: U256::ZERO,
        data: swap.abi_encode().into(),
    }])
}

fn sqrt_limit_u160(sqrt_limit: U256) -> anyhow::Result<U160> {
    if sqrt_limit.bit_len() > 160 {
        anyhow::bail!("v3 sqrt price limit does not fit uint160");
    }
    Ok(U160::from(sqrt_limit))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_sqrt_limit_outside_uint160() {
        assert!(sqrt_limit_u160(U256::MAX).is_err());
    }
}
