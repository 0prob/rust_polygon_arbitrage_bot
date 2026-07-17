use alloy::primitives::{Address, Bytes, U256};
use alloy::sol_types::SolCall;

use super::super::CalldataHop;
use super::shared::compute_quoted_out;
use crate::abis::{ExecutorCall, IUniswapV2Pair};
use crate::pipeline::arena::StateArena;
use crate::services::execution::calldata::approvals::encode_token_transfer;

/// Encode a Uniswap V2 hop into executor calls.
///
/// Pre-fund style (router pattern):
/// 1. Transfer `token_in` to the pair (`transferAll` or exact)
/// 2. `swap(amountOut, to, data="")` — **empty** `data` so the pair does not invoke
///    `uniswapV2Call`. Non-empty data would re-enter the executor with flash-swap
///    payment semantics and fight the pre-fund path.
///
/// `amountOut` is the full quote for `amount_in` (not slippage-shaved). Slippage is
/// enforced by route-level `minProfit` / `ASSERT_PROFIT`. Shaving `amountOut` would
/// leave surplus input as an LP donation.
pub fn encode_v2_hop(
    arena: &StateArena,
    hop: &CalldataHop,
    recipient: Address,
    _slippage_bps: u64,
    use_transfer_all: bool,
) -> anyhow::Result<Vec<ExecutorCall>> {
    if hop.edge.zero_for_one != (hop.token_in < hop.token_out) {
        anyhow::bail!("v2 zero_for_one must match sorted token0 (token_in < token_out)");
    }
    let amount_out = compute_quoted_out(arena, hop, "v2")?;
    if amount_out.is_zero() {
        anyhow::bail!("v2 hop quoted amountOut is zero");
    }

    let mut calls = Vec::with_capacity(2);

    calls.push(encode_token_transfer(
        recipient,
        hop.token_in,
        hop.pool_address,
        hop.amount_in,
        use_transfer_all,
    ));

    let (amount0_out, amount1_out) = if hop.edge.zero_for_one {
        (U256::ZERO, amount_out)
    } else {
        (amount_out, U256::ZERO)
    };

    let swap = IUniswapV2Pair::swapCall {
        amount0Out: amount0_out,
        amount1Out: amount1_out,
        to: recipient,
        // Empty data: pre-fund payment; do not trigger uniswapV2Call.
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
    use crate::core::types::{Edge, PoolState, ProtocolType, V2PoolState};
    use crate::services::execution::calldata::CalldataHop;
    use std::sync::Arc;

    #[test]
    fn v2_swap_uses_empty_callback_data_and_full_quote_out() {
        let mut arena = StateArena::default();
        let t0 = Address::repeat_byte(0x01);
        let t1 = Address::repeat_byte(0x02);
        let pool = Address::repeat_byte(0x10);
        let i0 = arena.register_token(t0);
        let i1 = arena.register_token(t1);
        let pool_index = arena.register_pool(
            pool,
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: U256::from(1_000_000u64),
                reserve1: U256::from(1_000_000u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 1,
            })),
        );
        let hop = CalldataHop {
            edge: Edge {
                pool_index,
                token_in: i0,
                token_out: i1,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
            pool_address: pool,
            token_in: t0,
            token_out: t1,
            amount_in: U256::from(1_000u64),
            amount_out: U256::ZERO,
            pool_id: None,
            protocol_label: None,
            pool_type: None,
            router: None,
            hooks: None,
        };
        let calls = encode_v2_hop(&arena, &hop, Address::repeat_byte(0xee), 50, false)
            .expect("encode");
        assert_eq!(calls.len(), 2);
        // swap calldata: selector + amount0Out + amount1Out + to + data offset/len
        // data must be empty — last dynamic section length word is 0.
        let swap_data = calls[1].data.as_ref();
        assert!(swap_data.len() > 4 + 32 * 4);
        // ABI: amount1Out (zero_for_one) is second word after selector; must be full quote > 0
        let amount1 = U256::from_be_slice(&swap_data[4 + 32..4 + 64]);
        assert!(!amount1.is_zero());
        // Empty `data` encodes as offset + length 0 (no payload bytes after head).
        let data_offset = usize::try_from(U256::from_be_slice(&swap_data[4 + 96..4 + 128])).unwrap();
        let len_word_at = 4 + data_offset;
        let data_len = U256::from_be_slice(&swap_data[len_word_at..len_word_at + 32]);
        assert!(data_len.is_zero(), "v2 pre-fund path must use empty callback data");
    }
}
