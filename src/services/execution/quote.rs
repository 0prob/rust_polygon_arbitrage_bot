use alloy::primitives::{Address, U256};

use crate::core::math::uniswap_v3::{resolve_v3_fee_pips, simulate_v3_swap};
use crate::core::types::{PoolState, V3PoolState};
use crate::pipeline::arena::StateArena;
use crate::pipeline::local_sim::simulate_hop_amount_out;
use crate::services::execution::calldata::CalldataHop;

#[must_use]
pub fn quote_hop_for_execution(arena: &StateArena, hop: &CalldataHop) -> Option<U256> {
    let state = arena.pool_state(hop.edge.pool_index)?;
    simulate_hop_amount_out(state, &hop.edge, hop.amount_in)
}

#[must_use]
pub fn resolve_v3_fee_pips_for_hop(arena: &StateArena, hop: &CalldataHop) -> u32 {
    match arena.pool_state(hop.edge.pool_index) {
        Some(PoolState::V3(s) | PoolState::V4(s)) => resolve_v3_fee_pips(s.fee, Some(hop.edge.fee_bps))
            .min(U256::from(0xffffffu32))
            .to::<u32>(),
        _ => hop.edge.fee_bps.min(0xffffff),
    }
}

#[must_use]
pub fn pool_tokens_from_hop(hop: &CalldataHop) -> (Address, Address) {
    if hop.edge.zero_for_one {
        (hop.token_in, hop.token_out)
    } else {
        (hop.token_out, hop.token_in)
    }
}

pub fn derive_tight_v3_price_limit(
    state: &V3PoolState,
    amount_in: U256,
    quoted_out: U256,
    zero_for_one: bool,
    edge_fee_bps: u32,
    slippage_bps: u64,
    explicit_fee_pips: Option<u32>,
) -> anyhow::Result<U256> {
    use crate::core::math::tick_math::{MAX_SQRT_RATIO, MIN_SQRT_RATIO};

    let sim = if let Some(pips) = explicit_fee_pips {
        let mut tmp = state.clone();
        tmp.fee = U256::from(pips);
        simulate_v3_swap(&tmp, amount_in, zero_for_one, None)
    } else {
        simulate_v3_swap(state, amount_in, zero_for_one, Some(edge_fee_bps))
    };
    if sim.shallow {
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
    use std::sync::Arc;

    #[test]
    fn price_limit_rejects_a_shallow_quote() {
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

        assert!(
            derive_tight_v3_price_limit(
                &state,
                U256::from(100u64),
                U256::from(1u64),
                true,
                30,
                0,
                None,
            )
            .is_err()
        );
    }
}
