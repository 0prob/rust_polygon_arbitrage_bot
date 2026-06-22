use ruint::aliases::U256;

use crate::core::constants::FEE_PIPS_SCALE;
use crate::core::types::{V3PoolState, V3Tick};

use super::swap_math::compute_swap_step;
use super::tick_math::{
    MAX_SQRT_RATIO, MAX_TICK, MIN_SQRT_RATIO, MIN_TICK, get_sqrt_ratio_at_tick,
    get_tick_at_sqrt_ratio_in_range,
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

fn next_initialized_tick(
    sorted_ticks: &[i32],
    current_tick: i32,
    zero_for_one: bool,
) -> Option<i32> {
    if sorted_ticks.is_empty() {
        return None;
    }

    let mut low = 0usize;
    let mut high = sorted_ticks.len() - 1;
    let mut result = None;

    if zero_for_one {
        while low <= high {
            let mid = usize::midpoint(low, high);
            if sorted_ticks[mid] <= current_tick {
                result = Some(sorted_ticks[mid]);
                low = mid + 1;
            } else {
                if mid == 0 {
                    break;
                }
                high = mid - 1;
            }
        }
    } else {
        while low <= high {
            let mid = usize::midpoint(low, high);
            if sorted_ticks[mid] > current_tick {
                result = Some(sorted_ticks[mid]);
                if mid == 0 {
                    break;
                }
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
    }

    result
}

fn sorted_tick_indices(ticks: &[V3Tick]) -> Vec<i32> {
    let mut indices: Vec<i32> = ticks.iter().map(|t| t.tick).collect();
    indices.sort_unstable();
    indices
}

fn tick_liquidity_net(ticks: &[V3Tick], tick: i32) -> Option<i128> {
    ticks
        .iter()
        .find(|t| t.tick == tick)
        .map(|t| t.liquidity_net)
}

fn default_no_tick_step(tick_spacing: i32) -> i32 {
    (tick_spacing * 2).max(1)
}

pub fn resolve_v3_fee_pips(pool_fee: U256, edge_fee_bps: Option<u32>) -> U256 {
    if let Some(bps) = edge_fee_bps {
        return U256::from(bps) * U256::from(100); // bps -> pips (1e4 -> 1e6)
    }
    if !pool_fee.is_zero() {
        return pool_fee;
    }
    U256::from(DEFAULT_V3_FEE_PIPS)
}

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
        MAX_SQRT_RATIO - U256::from(1)
    };

    let sorted_ticks = sorted_tick_indices(&state.ticks);
    let has_ticks = !sorted_ticks.is_empty();

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
        let mut next_tick = next_initialized_tick(&sorted_ticks, tick_search, zero_for_one);

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
            if sqrt_price_next_tick_x96 < sqrt_price_limit_x96 {
                sqrt_price_limit_x96
            } else {
                sqrt_price_next_tick_x96
            }
        } else if sqrt_price_next_tick_x96 > sqrt_price_limit_x96 {
            sqrt_price_limit_x96
        } else {
            sqrt_price_next_tick_x96
        };

        let liq_u256 = U256::from(liquidity.max(0) as u128);
        let Some(step) = compute_swap_step(
            sqrt_price_x96,
            sqrt_ratio_target_x96,
            liq_u256,
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
                next_tick.map(|t| t - 1).unwrap_or(MAX_TICK)
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
    let gas_estimate = 185_000 + ticks_crossed * 25_000;

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
    use crate::core::math::tick_math::get_sqrt_ratio_at_tick;

    #[test]
    fn single_tick_swap_produces_output() {
        let sqrt = get_sqrt_ratio_at_tick(0).unwrap();
        let state = V3PoolState {
            sqrt_price_x96: sqrt,
            tick: 0,
            liquidity: 1_000_000_000_000,
            fee: U256::from(DEFAULT_V3_FEE_PIPS),
            tick_spacing: 60,
            ticks: Box::new([]),
        };
        let result = simulate_v3_swap(&state, U256::from(1_000_000), true, None);
        assert!(result.amount_out > U256::ZERO);
        assert!(result.shallow);
    }

    #[test]
    fn next_initialized_tick_handles_boundaries() {
        let sorted = vec![100, 200, 300];
        // zero_for_one = true, current_tick = 50. First element of sorted is 100 > 50.
        // This should not loop infinitely and should return None.
        let r = next_initialized_tick(&sorted, 50, true);
        assert_eq!(r, None);

        // zero_for_one = true, current_tick = 150. Smallest initialized tick <= 150 is 100.
        let r = next_initialized_tick(&sorted, 150, true);
        assert_eq!(r, Some(100));

        // zero_for_one = false, current_tick = 350. No initialized tick > 350.
        let r = next_initialized_tick(&sorted, 350, false);
        assert_eq!(r, None);

        // zero_for_one = false, current_tick = 150. Smallest initialized tick > 150 is 200.
        let r = next_initialized_tick(&sorted, 150, false);
        assert_eq!(r, Some(200));
    }
}
