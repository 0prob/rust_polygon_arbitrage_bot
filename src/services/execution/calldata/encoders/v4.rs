use alloy::primitives::{Address, I256, Signed, U256, Uint};
use alloy::sol_types::SolCall;

use crate::abis::{ExecutorCall, IUniswapV4PoolManager, V4PoolKey};
use crate::core::constants::UNISWAP_V4_POOL_MANAGER;
use crate::core::math::uniswap_v3::resolve_v3_fee_pips;
use crate::core::types::PoolState;
use crate::pipeline::arena::StateArena;
use crate::services::execution::calldata::CalldataHop;
use crate::services::execution::calldata::approvals::encode_approve_if_needed;
use crate::services::execution::calldata::encoders::shared::to_v3_state;
use crate::services::execution::quote::{derive_tight_v3_price_limit, quote_hop_for_execution};

/// Encode a Uniswap V4 hop into executor calls.
pub fn encode_v4_hop(
    hop: &CalldataHop,
    arena: &StateArena,
    slippage_bps: u64,
) -> anyhow::Result<Vec<ExecutorCall>> {
    let pool_manager: Address = UNISWAP_V4_POOL_MANAGER;
    let (fee, tick_spacing, hooks) = v4_static_fields(arena, hop);

    if hooks != Address::ZERO {
        anyhow::bail!("v4 hook pools are not supported");
    }

    let pool_state = arena
        .pool_state(hop.edge.pool_index)
        .ok_or_else(|| anyhow::anyhow!("missing pool state for v4 hop"))?;
    let v3 = to_v3_state(pool_state).ok_or_else(|| anyhow::anyhow!("pool is not v4 state"))?;

    let quoted_out = quote_hop_for_execution(arena, hop).unwrap_or(hop.amount_out);
    let sqrt_limit = derive_tight_v3_price_limit(
        &v3,
        hop.amount_in,
        quoted_out,
        hop.edge.zero_for_one,
        hop.edge.fee_bps,
        slippage_bps,
        None,
    )?;

    let (pool_key, zero_for_one) =
        build_v4_pool_key(hop.token_in, hop.token_out, fee, tick_spacing, hooks);
    let amount_spec = I256::ZERO - I256::from(hop.amount_in);

    // ponytail: flat ABI words for unlockCallback, 256B payload at fixed offset
    let mut unlock_inner = Vec::with_capacity(32 + 256);
    unlock_inner.extend_from_slice(&U256::from(256u16).to_be_bytes::<32>());
    append_address(&mut unlock_inner, pool_key.currency0);
    append_address(&mut unlock_inner, pool_key.currency1);
    unlock_inner.extend_from_slice(&[0u8; 29]);
    unlock_inner.extend_from_slice(&pool_key.fee.to_be_bytes::<3>());
    unlock_inner.extend_from_slice(&[0u8; 29]);
    unlock_inner.extend_from_slice(&pool_key.tickSpacing.to_be_bytes::<3>());
    append_address(&mut unlock_inner, pool_key.hooks);
    unlock_inner.extend_from_slice(&[0u8; 31]);
    unlock_inner.push(u8::from(zero_for_one));
    unlock_inner.extend_from_slice(&amount_spec.to_be_bytes::<32>());
    unlock_inner.extend_from_slice(&sqrt_limit.to_be_bytes::<32>());

    let unlock = IUniswapV4PoolManager::unlockCall {
        data: unlock_inner.into(),
    };

    Ok(vec![
        encode_approve_if_needed(hop.token_in, pool_manager, hop.amount_in),
        ExecutorCall {
            target: pool_manager,
            value: U256::ZERO,
            data: unlock.abi_encode().into(),
        },
    ])
}

fn append_address(out: &mut Vec<u8>, addr: Address) {
    out.extend_from_slice(&[0u8; 12]);
    out.extend_from_slice(addr.as_slice());
}

fn build_v4_pool_key(
    token_in: Address,
    token_out: Address,
    fee: u32,
    tick_spacing: i32,
    hooks: Address,
) -> (V4PoolKey, bool) {
    let (currency0, currency1) = if token_in < token_out {
        (token_in, token_out)
    } else {
        (token_out, token_in)
    };
    let zero_for_one = token_in == currency0;
    (
        V4PoolKey {
            currency0,
            currency1,
            fee: Uint::from(fee),
            tickSpacing: Signed::try_from(tick_spacing).unwrap_or(Signed::ZERO),
            hooks,
        },
        zero_for_one,
    )
}

fn v4_static_fields(arena: &StateArena, hop: &CalldataHop) -> (u32, i32, Address) {
    let hooks = hop.hooks.unwrap_or(Address::ZERO);
    match arena.pool_state(hop.edge.pool_index) {
        Some(PoolState::V4(s)) => {
            let fee = resolve_v3_fee_pips(s.fee, Some(hop.edge.fee_bps))
                .min(U256::from(0xffffffu32))
                .to::<u32>();
            (fee, s.tick_spacing, hooks)
        }
        _ => (hop.edge.fee_bps.saturating_mul(100), 60, hooks),
    }
}
