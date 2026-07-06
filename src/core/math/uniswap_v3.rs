use alloy::primitives::U256;

use crate::core::constants::{FEE_PIPS_SCALE, GAS_PER_TICK_CROSSED, GAS_V3_BASE};
use crate::core::types::{V3PoolState, V3Tick};

use super::swap_math::compute_swap_step;
use super::tick_math::{
    MAX_SQRT_RATIO, MAX_SQRT_RATIO_EXCLUSIVE, MAX_TICK, MIN_SQRT_RATIO, MIN_TICK,
    get_sqrt_ratio_at_tick, get_tick_at_sqrt_ratio_in_range,
};

pub const DEFAULT_V3_FEE_PIPS: u32 = 3000;
const SQRT_PRICE_LIMIT_ZERO_FOR_ONE: U256 = U256::from_limbs([4_295_128_740, 0, 0, 0]); // MIN + 1
const MAX_CUMULATIVE_TICK_MOVE: i32 = 500;
const MAX_ITERATIONS: u32 = 500;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V3SwapResult {
    pub amount_out: U256,
    pub sqrt_price_x96_after: U256,
    pub tick_after: i32,
    pub gas_estimate: u32,
    pub shallow: bool,
}

/// Binary search pre-sorted V3 ticks (ticks are stored sorted by tick value).
fn next_initialized_tick(ticks: &[V3Tick], current_tick: i32, zero_for_one: bool) -> Option<i32> {
    let i = ticks.partition_point(|t| t.tick <= current_tick);
    if zero_for_one {
        (i > 0).then(|| ticks[i - 1].tick)
    } else {
        (i < ticks.len()).then(|| ticks[i].tick)
    }
}

fn tick_liquidity_net(ticks: &[V3Tick], tick: i32) -> Option<i128> {
    let i = ticks.partition_point(|t| t.tick < tick);
    (i < ticks.len() && ticks[i].tick == tick).then(|| ticks[i].liquidity_net)
}

fn default_no_tick_step(tick_spacing: i32) -> i32 {
    (tick_spacing * 2).max(1)
}

#[must_use]
pub fn resolve_v3_fee_pips(pool_fee: U256, edge_fee_bps: Option<u32>) -> U256 {
    if let Some(bps) = edge_fee_bps {
        return U256::from(bps) * U256::from(100); // bps -> pips (1e4 -> 1e6)
    }
    if !pool_fee.is_zero() {
        return pool_fee;
    }
    U256::from(DEFAULT_V3_FEE_PIPS)
}

#[inline]
#[must_use]
pub fn simulate_v3_swap(
    state: &V3PoolState,
    amount_in: U256,
    zero_for_one: bool,
    edge_fee_bps: Option<u32>,
) -> V3SwapResult {
    let fallback_tick = state.tick;
    let fee_pips = resolve_v3_fee_pips(state.fee, edge_fee_bps);

    if amount_in.is_zero()
        || state.sqrt_price_x96 < MIN_SQRT_RATIO
        || state.sqrt_price_x96 >= MAX_SQRT_RATIO
        || state.liquidity == 0
        || fee_pips >= FEE_PIPS_SCALE
    {
        return V3SwapResult {
            amount_out: U256::ZERO,
            sqrt_price_x96_after: state.sqrt_price_x96,
            tick_after: fallback_tick,
            gas_estimate: 0,
            shallow: false,
        };
    }

    let sqrt_price_limit_x96 = if zero_for_one {
        SQRT_PRICE_LIMIT_ZERO_FOR_ONE
    } else {
        MAX_SQRT_RATIO_EXCLUSIVE
    };

    let ticks = state.ticks.as_ref();
    let has_ticks = !ticks.is_empty();

    let mut sqrt_price_x96 = state.sqrt_price_x96;
    let mut tick = fallback_tick;
    let mut liquidity: i128 = state.liquidity as i128;
    let mut amount_remaining = amount_in;
    let mut amount_calculated = U256::ZERO;
    let mut ticks_crossed = 0u32;
    let initial_tick = tick;
    let mut tick_data_exhausted = false;

    for _ in 0..MAX_ITERATIONS {
        if amount_remaining.is_zero() {
            break;
        }

        let tick_search = if zero_for_one { tick - 1 } else { tick };
        let mut next_tick = next_initialized_tick(ticks, tick_search, zero_for_one);

        if next_tick.is_none() && has_ticks {
            tick_data_exhausted = true;
            break;
        }

        if next_tick.is_none() && !has_ticks {
            let tick_step = default_no_tick_step(state.tick_spacing);
            let raw_next = if zero_for_one {
                tick - tick_step
            } else {
                tick + tick_step
            };
            let cumulative = if zero_for_one {
                initial_tick - raw_next
            } else {
                raw_next - initial_tick
            };

            next_tick = if cumulative > MAX_CUMULATIVE_TICK_MOVE {
                Some(if zero_for_one {
                    initial_tick - MAX_CUMULATIVE_TICK_MOVE
                } else {
                    initial_tick + MAX_CUMULATIVE_TICK_MOVE
                })
            } else {
                Some(raw_next)
            };

            if let Some(nt) = next_tick {
                next_tick = Some(nt.clamp(MIN_TICK, MAX_TICK));
            }
        }

        let sqrt_price_next_tick_x96 = next_tick
            .and_then(get_sqrt_ratio_at_tick)
            .unwrap_or(sqrt_price_limit_x96);

        let sqrt_ratio_target_x96 = if zero_for_one {
            sqrt_price_next_tick_x96.max(sqrt_price_limit_x96)
        } else {
            sqrt_price_next_tick_x96.min(sqrt_price_limit_x96)
        };

        let Some(step) = compute_swap_step(
            sqrt_price_x96,
            sqrt_ratio_target_x96,
            U256::from(liquidity.max(0) as u128),
            amount_remaining,
            fee_pips,
        ) else {
            break;
        };

        sqrt_price_x96 = step.sqrt_ratio_next_x96;
        amount_remaining = amount_remaining.saturating_sub(step.amount_in + step.fee_amount);
        amount_calculated += step.amount_out;

        if sqrt_price_x96 == sqrt_price_next_tick_x96 {
            if let Some(nt) = next_tick {
                if let Some(liquidity_net) = tick_liquidity_net(&state.ticks, nt) {
                    liquidity = if zero_for_one {
                        liquidity - liquidity_net
                    } else {
                        liquidity + liquidity_net
                    };
                    ticks_crossed += 1;
                } else {
                    liquidity = 0;
                }
                tick = if zero_for_one { nt - 1 } else { nt };
            }
        } else {
            let min_tick = if zero_for_one {
                next_tick.unwrap_or(MIN_TICK)
            } else {
                tick
            };
            let max_tick = if zero_for_one {
                tick
            } else {
                next_tick.map_or(MAX_TICK, |t| t - 1)
            };
            tick =
                get_tick_at_sqrt_ratio_in_range(sqrt_price_x96, min_tick, max_tick).unwrap_or(tick);
            break;
        }

        if liquidity <= 0 {
            if has_ticks && !amount_remaining.is_zero() {
                tick_data_exhausted = true;
            }
            break;
        }
    }

    let shallow = !has_ticks || tick_data_exhausted;
    let gas_estimate = GAS_V3_BASE + ticks_crossed * GAS_PER_TICK_CROSSED;

    V3SwapResult {
        amount_out: amount_calculated,
        sqrt_price_x96_after: sqrt_price_x96,
        tick_after: tick,
        gas_estimate,
        shallow,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_v3_fee_pips_default() {
        let fee = resolve_v3_fee_pips(U256::ZERO, None);
        assert_eq!(fee, U256::from(DEFAULT_V3_FEE_PIPS));
    }
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used)]
mod proptests {
    use proptest::prelude::*;
    use super::*;
    use crate::core::math::sqrt_price_math::{
        get_amount0_delta, get_amount1_delta, get_next_sqrt_price_from_input,
    };
    use crate::core::constants::FEE_PIPS_SCALE;

    fn sqrt_price() -> impl Strategy<Value = U256> {
        (..=u128::MAX).prop_map(|v| U256::from(v).max(MIN_SQRT_RATIO + U256::from(1)).min(MAX_SQRT_RATIO - U256::from(1)))
    }

    fn liquidity() -> impl Strategy<Value = U256> {
        (1u128..=u128::MAX).prop_map(U256::from)
    }

    proptest! {
        #[test]
        fn compute_swap_step_output_bounded(
            sqrt_a in sqrt_price(),
            sqrt_b in sqrt_price(),
            liq in liquidity(),
            amount in (1u128..u128::MAX).prop_map(U256::from),
            fee_pips in 1u32..1000000u32,
        ) {
            if sqrt_a == sqrt_b { return Ok(()); }
            let fee = U256::from(fee_pips);
            if fee >= FEE_PIPS_SCALE { return Ok(()); }

            if let Some(step) = compute_swap_step(sqrt_a, sqrt_b, liq, amount, fee) {
                prop_assert!(step.amount_out <= liq,
                    "output {} exceeds liquidity {}", step.amount_out, liq);
                let consumed = step.amount_in + step.fee_amount;
                prop_assert!(consumed <= amount || amount.is_zero(),
                    "consumed {} exceeds amount {}", consumed, amount);
            }
        }

        #[test]
        fn amount_delta_liquidity_invariant(
            sqrt_a in sqrt_price(),
            sqrt_b in sqrt_price(),
            liq in liquidity(),
        ) {
            if sqrt_a.is_zero() || sqrt_b.is_zero() || sqrt_a == sqrt_b { return Ok(()); }

            if let Some(amount0) = get_amount0_delta(sqrt_a, sqrt_b, liq, false) {
                if let Some(amount1) = get_amount1_delta(sqrt_a, sqrt_b, liq, false) {
                    if !amount0.is_zero() || !amount1.is_zero() {
                        prop_assert!(!liq.is_zero(), "nonzero delta needs liquidity");
                    }
                }
            }
        }

        #[test]
        fn get_next_sqrt_price_monotonic(
            sqrt_px in sqrt_price(),
            liq in liquidity(),
            amount in (1u128..u128::MAX).prop_map(U256::from),
        ) {
            let next = get_next_sqrt_price_from_input(sqrt_px, liq, amount, false);
            if let Some(next_px) = next {
                prop_assert!(next_px >= sqrt_px,
                    "price decreased on non-zero-for-one swap");
            }
        }
    }
}
