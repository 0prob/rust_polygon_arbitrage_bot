use ruint::aliases::U256;

use crate::core::constants::FEE_PIPS_SCALE;

use super::full_math::{mul_div, mul_div_rounding_up};
use super::sqrt_price_math::{
    get_amount0_delta, get_amount1_delta, get_next_sqrt_price_from_input,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwapStepResult {
    pub sqrt_ratio_next_x96: U256,
    pub amount_in: U256,
    pub amount_out: U256,
    pub fee_amount: U256,
}

pub fn compute_swap_step(
    sqrt_ratio_current_x96: U256,
    sqrt_ratio_target_x96: U256,
    liquidity: U256,
    amount_remaining: U256,
    fee_pips: U256,
) -> Option<SwapStepResult> {
    let zero_for_one = sqrt_ratio_current_x96 >= sqrt_ratio_target_x96;

    let amount_remaining_less_fee =
        mul_div(amount_remaining, FEE_PIPS_SCALE - fee_pips, FEE_PIPS_SCALE)?;

    let amount_in = if zero_for_one {
        get_amount0_delta(
            sqrt_ratio_target_x96,
            sqrt_ratio_current_x96,
            liquidity,
            true,
        )?
    } else {
        get_amount1_delta(
            sqrt_ratio_current_x96,
            sqrt_ratio_target_x96,
            liquidity,
            true,
        )?
    };

    let sqrt_ratio_next_x96 = if amount_remaining_less_fee >= amount_in {
        sqrt_ratio_target_x96
    } else {
        get_next_sqrt_price_from_input(
            sqrt_ratio_current_x96,
            liquidity,
            amount_remaining_less_fee,
            zero_for_one,
        )?
    };

    let max = sqrt_ratio_target_x96 == sqrt_ratio_next_x96;

    let (amount_in, amount_out) = if zero_for_one {
        let actual_in = if max {
            amount_in
        } else {
            get_amount0_delta(sqrt_ratio_next_x96, sqrt_ratio_current_x96, liquidity, true)?
        };
        let actual_out = get_amount1_delta(
            sqrt_ratio_next_x96,
            sqrt_ratio_current_x96,
            liquidity,
            false,
        )?;
        (actual_in, actual_out)
    } else {
        let actual_in = if max {
            amount_in
        } else {
            get_amount1_delta(sqrt_ratio_current_x96, sqrt_ratio_next_x96, liquidity, true)?
        };
        let actual_out = get_amount0_delta(
            sqrt_ratio_current_x96,
            sqrt_ratio_next_x96,
            liquidity,
            false,
        )?;
        (actual_in, actual_out)
    };

    let fee_amount = if sqrt_ratio_next_x96 != sqrt_ratio_target_x96 {
        amount_remaining - amount_in
    } else {
        mul_div_rounding_up(amount_in, fee_pips, FEE_PIPS_SCALE - fee_pips)?
    };

    Some(SwapStepResult {
        sqrt_ratio_next_x96,
        amount_in,
        amount_out,
        fee_amount,
    })
}
