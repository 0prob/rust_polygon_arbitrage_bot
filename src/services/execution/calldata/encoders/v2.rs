use alloy::primitives::{Address, Bytes, U256};
use alloy::sol_types::SolCall;

use super::super::CalldataHop;
use crate::abis::{ExecutorCall, IUniswapV2Pair};
use crate::pipeline::arena::StateArena;
use crate::services::execution::calldata::approvals::encode_token_transfer;

/// How to fund the pair before `swap`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V2Prefund {
    /// Exact ERC-20 `transfer(amount_in)` (hop-0 / after non-V2).
    Exact,
    /// Huff `transferAll` (executor holds residual from a prior non-V2 hop).
    TransferAll,
    /// Prior V2 hop already sent `token_in` to this pair via `swap(to=this)`.
    Skipped,
}

/// Encode a Uniswap V2 hop into executor calls.
///
/// Prefund + `swap(..., data="")`. Empty `data` avoids `uniswapV2Call` re-entry.
/// Consecutive V2 hops chain with `swap(to=next_pair)` and skip the intermediate
/// transfer — `transferAll` would see a zero executor balance and revert.
/// Returns `(calls, amount_out)` — `amount_out` is what `swap` sends to `swap_to`
/// (feed into the next V2 hop's `amount_in` when chaining).
pub fn encode_v2_hop(
    arena: &StateArena,
    hop: &CalldataHop,
    swap_to: Address,
    executor: Address,
    slippage_bps: u64,
    prefund: V2Prefund,
) -> anyhow::Result<(Vec<ExecutorCall>, U256)> {
    if hop.edge.zero_for_one != (hop.token_in < hop.token_out) {
        anyhow::bail!("v2 zero_for_one must match sorted token0 (token_in < token_out)");
    }
    // V2 swap is exact-out: amount{0,1}Out is what the pair sends. A re-quote
    // above assess `hop.amount_out` after reserve drift needs more input than
    // pre-funded → UniswapV2: K. Floor slip + take the tighter of re-quote vs
    // conservative_execution_hops (same as Curve/Balancer/Woofi).
    let bps = slippage_bps.max(crate::core::constants::EXECUTION_MIN_SLIPPAGE_BPS);
    let amount_out = {
        let quoted = super::shared::compute_min_out(arena, hop, bps, "v2")?;
        if hop.amount_out.is_zero() {
            quoted
        } else {
            hop.amount_out.min(quoted)
        }
    };
    if amount_out.is_zero() {
        anyhow::bail!("v2 hop quoted amountOut is zero");
    }
    crate::debug!(
        "v2 encode: pool={:#x} zfo={} ain={} aout={amount_out} slip_bps={bps} prefund={prefund:?} swap_to={swap_to:#x}",
        hop.pool_address,
        hop.edge.zero_for_one,
        hop.amount_in,
    );

    let mut calls = Vec::with_capacity(2);

    match prefund {
        V2Prefund::Exact => {
            calls.push(encode_token_transfer(
                executor,
                hop.token_in,
                hop.pool_address,
                hop.amount_in,
                false,
            ));
        }
        V2Prefund::TransferAll => {
            calls.push(encode_token_transfer(
                executor,
                hop.token_in,
                hop.pool_address,
                hop.amount_in,
                true,
            ));
        }
        V2Prefund::Skipped => {}
    }

    let (amount0_out, amount1_out) = if hop.edge.zero_for_one {
        (U256::ZERO, amount_out)
    } else {
        (amount_out, U256::ZERO)
    };

    let swap = IUniswapV2Pair::swapCall {
        amount0Out: amount0_out,
        amount1Out: amount1_out,
        to: swap_to,
        // Empty data: pre-fund payment; do not trigger uniswapV2Call.
        data: Bytes::new(),
    };

    calls.push(ExecutorCall {
        target: hop.pool_address,
        value: U256::ZERO,
        data: swap.abi_encode().into(),
    });

    Ok((calls, amount_out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{Edge, PoolState, ProtocolType, V2PoolState};
    use crate::services::execution::calldata::CalldataHop;
    use std::sync::Arc;

    #[test]
    fn v2_swap_uses_empty_callback_data_and_positive_amount_out() {
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
        let executor = Address::repeat_byte(0xee);
        let (calls, aout) =
            encode_v2_hop(&arena, &hop, executor, executor, 50, V2Prefund::Exact).expect("encode");
        assert_eq!(calls.len(), 2);
        assert!(!aout.is_zero());
        let swap_data = calls[1].data.as_ref();
        assert!(swap_data.len() > 4 + 32 * 4);
        let amount1 = U256::from_be_slice(&swap_data[4 + 32..4 + 64]);
        assert!(!amount1.is_zero());
        let data_offset = usize::try_from(U256::from_be_slice(&swap_data[4 + 96..4 + 128]))
            .expect("data offset fits usize");
        let len_word_at = 4 + data_offset;
        let data_len = U256::from_be_slice(&swap_data[len_word_at..len_word_at + 32]);
        assert!(
            data_len.is_zero(),
            "v2 pre-fund path must use empty callback data"
        );
    }

    #[test]
    fn v2_chain_skips_prefund_and_sends_to_next_pair() {
        let mut arena = StateArena::default();
        let t0 = Address::repeat_byte(0x01);
        let t1 = Address::repeat_byte(0x02);
        let pool = Address::repeat_byte(0x10);
        let next = Address::repeat_byte(0x11);
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
        let (calls, _) = encode_v2_hop(
            &arena,
            &hop,
            next,
            Address::repeat_byte(0xee),
            50,
            V2Prefund::Skipped,
        )
        .expect("encode");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].target, pool);
        let swap_data = calls[0].data.as_ref();
        let to = Address::from_slice(&swap_data[4 + 64 + 12..4 + 96]);
        assert_eq!(to, next);
    }
}
