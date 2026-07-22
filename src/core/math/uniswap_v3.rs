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

/// Next initialized tick at or below/above `current_tick` (ticks sorted by `tick` asc).
fn next_initialized_tick_with_net(
    ticks: &[V3Tick],
    current_tick: i32,
    zero_for_one: bool,
) -> Option<(i32, i128)> {
    if ticks.is_empty() {
        return None;
    }
    let i = ticks.partition_point(|t| t.tick <= current_tick);
    if zero_for_one {
        (i > 0).then(|| {
            let t = &ticks[i - 1];
            (t.tick, t.liquidity_net)
        })
    } else {
        (i < ticks.len()).then(|| {
            let t = &ticks[i];
            (t.tick, t.liquidity_net)
        })
    }
}

fn default_no_tick_step(tick_spacing: i32) -> i32 {
    (tick_spacing * 2).max(1)
}

/// Resolve CL fee in pips. When `allow_zero_pool_fee` is set (Uniswap V4 slot0),
/// `pool_fee == 0` is kept — it is a valid zero-LP-fee pool, not "missing fee".
#[must_use]
pub fn resolve_v3_fee_pips(
    pool_fee: U256,
    edge_fee_bps: Option<u32>,
    allow_zero_pool_fee: bool,
) -> U256 {
    if allow_zero_pool_fee || !pool_fee.is_zero() {
        return pool_fee;
    }
    if let Some(bps) = edge_fee_bps.filter(|bps| *bps < 10_000) {
        return U256::from(bps) * U256::from(100); // bps -> pips (1e4 -> 1e6)
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
    allow_zero_pool_fee: bool,
) -> V3SwapResult {
    let fallback_tick = state.tick;
    let mut fee_pips = resolve_v3_fee_pips(state.fee, edge_fee_bps, allow_zero_pool_fee);
    // V4 packs directional protocol fee into `fee_protocol`; V3 leaves it 0.
    if state.fee_protocol != 0 {
        let proto = crate::core::v4_storage::v4_direction_protocol_fee_pips(
            state.fee_protocol,
            zero_for_one,
        );
        let lp = fee_pips.to::<u32>();
        fee_pips = U256::from(crate::core::v4_storage::v4_combined_swap_fee_pips(
            proto, lp,
        ));
    }

    if amount_in.is_zero()
        || !state.unlocked
        || state.sqrt_price_x96 < MIN_SQRT_RATIO
        || state.sqrt_price_x96 >= MAX_SQRT_RATIO
        || state.liquidity == 0
        || state.liquidity > i128::MAX as u128
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
    let no_tick_step = if has_ticks {
        0
    } else {
        default_no_tick_step(state.tick_spacing)
    };

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

        let tick_search = if zero_for_one {
            tick.saturating_sub(1)
        } else {
            tick
        };
        let mut next_tick = next_initialized_tick_with_net(ticks, tick_search, zero_for_one);

        if next_tick.is_none() && has_ticks {
            tick_data_exhausted = true;
            break;
        }

        if next_tick.is_none() && !has_ticks {
            let raw_next = if zero_for_one {
                tick - no_tick_step
            } else {
                tick + no_tick_step
            };
            let cumulative = if zero_for_one {
                initial_tick - raw_next
            } else {
                raw_next - initial_tick
            };

            let bounded = if cumulative > MAX_CUMULATIVE_TICK_MOVE {
                if zero_for_one {
                    initial_tick - MAX_CUMULATIVE_TICK_MOVE
                } else {
                    initial_tick + MAX_CUMULATIVE_TICK_MOVE
                }
            } else {
                raw_next
            };
            next_tick = Some((bounded.clamp(MIN_TICK, MAX_TICK), 0));
        }

        let sqrt_price_next_tick_x96 = next_tick
            .map(|(nt, _)| nt)
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
            if let Some((nt, liquidity_net)) = next_tick {
                if has_ticks {
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
                next_tick.map(|(nt, _)| nt).unwrap_or(MIN_TICK)
            } else {
                tick
            };
            let max_tick = if zero_for_one {
                tick
            } else {
                next_tick.map_or(MAX_TICK, |(t, _)| t - 1)
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
    use crate::core::types::V3PoolState;
    use std::sync::Arc;

    #[test]
    fn test_resolve_v3_fee_pips_default() {
        let fee = resolve_v3_fee_pips(U256::ZERO, None, false);
        assert_eq!(fee, U256::from(DEFAULT_V3_FEE_PIPS));
    }

    #[test]
    fn exact_pool_fee_precedes_rounded_edge_fee() {
        let fee = resolve_v3_fee_pips(U256::from(5_000u32), Some(25), false);
        assert_eq!(fee, U256::from(5_000u32));
    }

    #[test]
    fn explicit_zero_edge_fee_remains_zero() {
        let fee = resolve_v3_fee_pips(U256::ZERO, Some(0), false);
        assert_eq!(fee, U256::ZERO);
    }

    #[test]
    fn v4_zero_pool_fee_is_authoritative() {
        // Stale edge fee must not override slot0 lpFee=0.
        let fee = resolve_v3_fee_pips(U256::ZERO, Some(30), true);
        assert_eq!(fee, U256::ZERO);
    }

    #[test]
    fn locked_pool_returns_zero_output() {
        let state = V3PoolState {
            sqrt_price_x96: U256::from(1u128 << 96),
            liquidity: 1_000_000,
            tick: 0,
            fee: U256::from(3_000u32),
            tick_spacing: 60,
            unlocked: false,
            fee_protocol: 0,
            observation_cardinality: 1,
            ticks: Arc::from(Vec::new()),
        };
        let r = simulate_v3_swap(&state, U256::from(10u64), true, Some(30), false);
        assert!(r.amount_out.is_zero());
        assert!(!r.shallow);
    }

    #[test]
    fn tickless_pool_is_marked_shallow() {
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
        let r = simulate_v3_swap(&state, U256::from(10u64), true, Some(30), false);
        assert!(r.shallow);
        assert!(r.amount_out > U256::ZERO);
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use crate::core::constants::FEE_PIPS_SCALE;
    use crate::core::math::proptest_util::{U256_AMT_MAX, u256_nonzero};
    use crate::core::math::sqrt_price_math::{
        get_amount0_delta, get_amount1_delta, get_next_sqrt_price_from_input,
    };
    use proptest::prelude::*;

    fn sqrt_price() -> impl Strategy<Value = U256> {
        // Sample then clamp into the open Uniswap V3 sqrt-price band.
        (..=U256_AMT_MAX).prop_map(|v| {
            U256::from(v)
                .max(MIN_SQRT_RATIO + U256::from(1))
                .min(MAX_SQRT_RATIO - U256::from(1))
        })
    }

    proptest! {
        #[test]
        fn compute_swap_step_output_bounded(
            sqrt_a in sqrt_price(),
            sqrt_b in sqrt_price(),
            liq in u256_nonzero(),
            amount in u256_nonzero(),
            fee_pips in 1u32..1_000_000u32,
        ) {
            prop_assume!(sqrt_a != sqrt_b);
            let fee = U256::from(fee_pips);
            prop_assume!(fee < FEE_PIPS_SCALE);

            let Some(step) = compute_swap_step(sqrt_a, sqrt_b, liq, amount, fee) else {
                return Err(TestCaseError::reject("no swap step for inputs"));
            };
            let consumed = step.amount_in + step.fee_amount;
            prop_assert!(
                consumed <= amount,
                "consumed {consumed} exceeds amount {amount}"
            );
        }

        #[test]
        fn amount_delta_liquidity_invariant(
            sqrt_a in sqrt_price(),
            sqrt_b in sqrt_price(),
            liq in u256_nonzero(),
        ) {
            prop_assume!(!sqrt_a.is_zero() && !sqrt_b.is_zero() && sqrt_a != sqrt_b);

            let Some(amount0) = get_amount0_delta(sqrt_a, sqrt_b, liq, false) else {
                return Err(TestCaseError::reject("amount0 delta unavailable"));
            };
            let Some(amount1) = get_amount1_delta(sqrt_a, sqrt_b, liq, false) else {
                return Err(TestCaseError::reject("amount1 delta unavailable"));
            };
            prop_assume!(!amount0.is_zero() || !amount1.is_zero());
            prop_assert!(!liq.is_zero(), "nonzero delta needs liquidity");
        }

        #[test]
        fn get_next_sqrt_price_monotonic(
            sqrt_px in sqrt_price(),
            liq in u256_nonzero(),
            amount in u256_nonzero(),
        ) {
            let Some(next_px) = get_next_sqrt_price_from_input(sqrt_px, liq, amount, false) else {
                return Err(TestCaseError::reject("next sqrt price unavailable"));
            };
            prop_assert!(
                next_px >= sqrt_px,
                "price decreased on non-zero-for-one swap"
            );
        }
    }
}
