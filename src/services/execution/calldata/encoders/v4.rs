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
    if hop.edge.zero_for_one != (hop.token_in < hop.token_out) {
        anyhow::bail!("v4 zero_for_one must match sorted currency0 (token_in < token_out)");
    }

    let pool_state = arena
        .pool_state(hop.edge.pool_index)
        .ok_or_else(|| anyhow::anyhow!("missing pool state for v4 hop"))?;
    let v3 = to_v3_state(pool_state).ok_or_else(|| anyhow::anyhow!("pool is not v4 state"))?;

    let quoted_out = quote_hop_for_execution(arena, hop)
        .ok_or_else(|| anyhow::anyhow!("v4 execution quote unavailable"))?;
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
    let amount_pos = I256::try_from(hop.amount_in)
        .map_err(|_| anyhow::anyhow!("v4 amount_in does not fit i256"))?;
    let amount_spec = I256::ZERO - amount_pos;

    // Huff UNLOCK_CALLBACK reads PoolKey/swap params at unlock-data +0x100.
    // Layout: [offset=256][224B pad][256B payload] — without pad, swap gets garbage
    // and bare-reverts as empty nested revert on PoolManager.
    let mut unlock_inner = Vec::with_capacity(512);
    unlock_inner.extend_from_slice(&U256::from(256u16).to_be_bytes::<32>());
    unlock_inner.extend_from_slice(&[0u8; 224]);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{Edge, ProtocolType, TokenIndex, V4PoolState};
    use std::sync::Arc;

    #[test]
    fn pool_key_uses_exact_state_fee_before_rounded_edge_fee() {
        let mut arena = StateArena::default();
        let pool_index = arena.register_pool(
            Address::with_last_byte(1),
            Arc::new(PoolState::V4(V4PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                tick: 0,
                liquidity: 1_000_000,
                fee: U256::from(450u64),
                tick_spacing: 9,
                ticks: Arc::from([] as [crate::core::types::V3Tick; 0]),
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
            })),
        );
        let hop = CalldataHop {
            edge: Edge {
                pool_index,
                token_in: TokenIndex(0),
                token_out: TokenIndex(1),
                token_in_idx: 0,
                token_out_idx: 1,
                fee_bps: 4,
                zero_for_one: true,
                protocol: ProtocolType::UniswapV4,
            },
            pool_address: Address::with_last_byte(1),
            token_in: Address::with_last_byte(2),
            token_out: Address::with_last_byte(3),
            amount_in: U256::from(1_000u64),
            amount_out: U256::from(999u64),
            pool_id: None,
            protocol_label: None,
            pool_type: None,
            router: None,
            hooks: Some(Address::ZERO),
        };

        assert_eq!(v4_static_fields(&arena, &hop), (450, 9, Address::ZERO));
    }

    #[test]
    fn unlock_inner_pads_payload_to_huff_offset_256() {
        let mut arena = StateArena::default();
        let t0 = Address::from([1u8; 20]);
        let t1 = Address::from([2u8; 20]);
        let pool_index = arena.register_pool(
            Address::with_last_byte(9),
            Arc::new(PoolState::V4(V4PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                tick: 0,
                liquidity: 1_000_000_000_000u128,
                fee: U256::from(3000u64),
                tick_spacing: 60,
                ticks: Arc::from([crate::core::types::V3Tick {
                    tick: -887_220,
                    liquidity_gross: 1,
                    liquidity_net: 0,
                }]),
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
            })),
        );
        let hop = CalldataHop {
            edge: Edge {
                pool_index,
                token_in: TokenIndex(0),
                token_out: TokenIndex(1),
                token_in_idx: 0,
                token_out_idx: 1,
                fee_bps: 30,
                zero_for_one: true,
                protocol: ProtocolType::UniswapV4,
            },
            pool_address: Address::with_last_byte(9),
            token_in: t0,
            token_out: t1,
            amount_in: U256::from(1_000_000u64),
            amount_out: U256::from(999_000u64),
            pool_id: None,
            protocol_label: Some("UNISWAP_V4".into()),
            pool_type: None,
            router: None,
            hooks: Some(Address::ZERO),
        };
        let calls = encode_v4_hop(&hop, &arena, 20).expect("encode");
        let unlock = &calls[1].data;
        // unlock(bytes) ABI: selector + offset + length + inner
        let inner_len = U256::from_be_slice(&unlock[36..68]).to::<usize>();
        assert_eq!(inner_len, 512, "offset word + 224 pad + 256 payload");
        let inner = &unlock[68..68 + inner_len];
        assert_eq!(&inner[..32], &U256::from(256u16).to_be_bytes::<32>());
        assert!(inner[32..256].iter().all(|&b| b == 0));
        assert_ne!(inner[256..288], [0u8; 32]); // currency0 word present
    }
}
