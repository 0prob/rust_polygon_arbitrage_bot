use alloy::primitives::{Address, U256};

use crate::core::math::uniswap_v3::{resolve_v3_fee_pips, simulate_v3_swap};
use crate::core::types::{PoolState, V3PoolState};
use crate::pipeline::arena::StateArena;
use crate::pipeline::local_sim::{realign_multi_token_edge, simulate_hop_amount_out};
use crate::services::execution::calldata::CalldataHop;

#[must_use]
pub fn quote_hop_for_execution(arena: &StateArena, hop: &CalldataHop) -> Option<U256> {
    let state = arena.pool_state(hop.edge.pool_index)?;
    let mut edge = hop.edge;
    if !realign_multi_token_edge(arena, state, &mut edge) {
        return None;
    }
    simulate_hop_amount_out(state, &edge, hop.amount_in)
}

#[must_use]
pub fn resolve_v3_fee_pips_for_hop(arena: &StateArena, hop: &CalldataHop) -> u32 {
    match arena.pool_state(hop.edge.pool_index) {
        Some(PoolState::V3(s)) => resolve_v3_fee_pips(s.fee, Some(hop.edge.fee_bps), false)
            .min(U256::from(0xffffffu32))
            .to::<u32>(),
        Some(PoolState::V4(s)) => resolve_v3_fee_pips(s.fee, Some(hop.edge.fee_bps), true)
            .min(U256::from(0xffffffu32))
            .to::<u32>(),
        _ => hop.edge.fee_bps.min(0xffffff),
    }
}

/// On-chain token0/token1 for V2/V3/Algebra callback data — always address-sorted.
///
/// Do not derive from `zero_for_one`: that flag is the swap direction and can
/// disagree with meta token index order. Callback token0/token1 must match the
/// factory pair ordering (`token0 < token1`).
#[must_use]
pub fn pool_tokens_from_hop(hop: &CalldataHop) -> (Address, Address) {
    if hop.token_in < hop.token_out {
        (hop.token_in, hop.token_out)
    } else {
        (hop.token_out, hop.token_in)
    }
}

#[allow(clippy::too_many_arguments)]
pub fn derive_tight_v3_price_limit(
    state: &V3PoolState,
    amount_in: U256,
    quoted_out: U256,
    zero_for_one: bool,
    edge_fee_bps: u32,
    slippage_bps: u64,
    explicit_fee_pips: Option<u32>,
    allow_zero_pool_fee: bool,
) -> anyhow::Result<U256> {
    use crate::core::math::tick_math::{MAX_SQRT_RATIO, MAX_SQRT_RATIO_EXCLUSIVE, MIN_SQRT_RATIO};

    let sim = if let Some(pips) = explicit_fee_pips {
        let mut tmp = state.clone();
        tmp.fee = U256::from(pips);
        simulate_v3_swap(&tmp, amount_in, zero_for_one, None, allow_zero_pool_fee)
    } else {
        simulate_v3_swap(
            state,
            amount_in,
            zero_for_one,
            Some(edge_fee_bps),
            allow_zero_pool_fee,
        )
    };
    if sim.shallow {
        // Tickless pools are accepted by local_sim within the probe cap; cannot tighten
        // from incomplete coverage — use the same protocol extremes as simulate_v3_swap.
        if state.ticks.is_empty() && !quoted_out.is_zero() {
            return Ok(if zero_for_one {
                MIN_SQRT_RATIO + U256::ONE
            } else {
                MAX_SQRT_RATIO_EXCLUSIVE
            });
        }
        anyhow::bail!("v3 price limit: incomplete tick coverage");
    }
    if sim.sqrt_price_x96_after < MIN_SQRT_RATIO || sim.sqrt_price_x96_after >= MAX_SQRT_RATIO {
        anyhow::bail!("v3 price limit: invalid sqrt after swap");
    }

    let moved_ok = if zero_for_one {
        sim.sqrt_price_x96_after < state.sqrt_price_x96 && sim.sqrt_price_x96_after > MIN_SQRT_RATIO
    } else {
        sim.sqrt_price_x96_after > state.sqrt_price_x96 && sim.sqrt_price_x96_after < MAX_SQRT_RATIO
    };
    if !moved_ok && !quoted_out.is_zero() {
        anyhow::bail!("v3 price limit: sqrt did not move in swap direction");
    }

    let denom = U256::from(20_000u64);
    let slip = U256::from(slippage_bps);
    Ok(if zero_for_one {
        (sim.sqrt_price_x96_after * (denom - slip)) / denom
    } else {
        (sim.sqrt_price_x96_after * (denom + slip)) / denom
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{Edge, PoolIndex, ProtocolType, TokenIndex};
    use crate::services::execution::calldata::CalldataHop;
    use std::sync::Arc;

    #[test]
    fn pool_tokens_sorted_by_address_not_zero_for_one() {
        let low = Address::repeat_byte(0x01);
        let high = Address::repeat_byte(0xff);
        // zero_for_one=true would wrongly claim token_in is token0 if we keyed off the flag.
        let hop = CalldataHop {
            edge: Edge {
                pool_index: PoolIndex(0),
                token_in: TokenIndex(1),
                token_out: TokenIndex(0),
                token_in_idx: 1,
                token_out_idx: 0,
                protocol: ProtocolType::UniswapV3,
                fee_bps: 30,
                zero_for_one: true,
            },
            pool_address: Address::repeat_byte(0xaa),
            token_in: high,
            token_out: low,
            amount_in: U256::from(1u64),
            amount_out: U256::from(1u64),
            pool_id: None,
            protocol_label: None,
            pool_type: None,
            router: None,
            hooks: None,
        };
        let (t0, t1) = pool_tokens_from_hop(&hop);
        assert_eq!((t0, t1), (low, high));
    }

    #[test]
    fn price_limit_tickless_falls_back_to_protocol_extremes() {
        use crate::core::math::tick_math::{MAX_SQRT_RATIO_EXCLUSIVE, MIN_SQRT_RATIO};

        let state = V3PoolState {
            sqrt_price_x96: U256::from(1u128 << 96),
            liquidity: 1_000_000,
            tick: 0,
            fee: U256::from(3_000u32),
            tick_spacing: 60,
            unlocked: true,
            fee_protocol: 0,
            observation_cardinality: 1,
            ticks: Arc::from(Vec::new()),
        };

        let zfo = derive_tight_v3_price_limit(
            &state,
            U256::from(100u64),
            U256::from(1u64),
            true,
            30,
            0,
            None,
            false,
        )
        .expect("tickless zero_for_one should encode");
        assert_eq!(zfo, MIN_SQRT_RATIO + U256::ONE);

        let one_for_zero = derive_tight_v3_price_limit(
            &state,
            U256::from(100u64),
            U256::from(1u64),
            false,
            30,
            0,
            None,
            false,
        )
        .expect("tickless one_for_zero should encode");
        assert_eq!(one_for_zero, MAX_SQRT_RATIO_EXCLUSIVE);
    }

    #[test]
    fn price_limit_rejects_exhausted_tick_coverage() {
        use crate::core::types::V3Tick;

        // Tick only above spot; zero_for_one search finds nothing → exhausted shallow.
        let state = V3PoolState {
            sqrt_price_x96: U256::from(1u128 << 96),
            liquidity: 1_000_000,
            tick: 0,
            fee: U256::from(3_000u32),
            tick_spacing: 60,
            unlocked: true,
            fee_protocol: 0,
            observation_cardinality: 1,
            ticks: Arc::from(vec![V3Tick {
                tick: 60,
                liquidity_gross: 1_000_000,
                liquidity_net: 1_000_000,
            }]),
        };

        assert!(
            derive_tight_v3_price_limit(
                &state,
                U256::from(100u64),
                U256::from(1u64),
                true,
                30,
                0,
                None,
                false,
            )
            .is_err()
        );
    }
}
