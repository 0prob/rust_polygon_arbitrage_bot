use ruint::aliases::U256;

use super::full_math::{div_rounding_up, mul_div, mul_div_rounding_up};

const Q96: U256 = U256::from_limbs([0, 1, 0, 0]);

pub fn get_next_sqrt_price_from_amount0_rounding_up(
    sqrt_px96: U256,
    liquidity: U256,
    amount: U256,
    add: bool,
) -> Option<U256> {
    if amount.is_zero() {
        return Some(sqrt_px96);
    }

    let numerator1 = liquidity << 96;

    if add {
        let product = amount * sqrt_px96;
        if product / amount == sqrt_px96 {
            let denominator = numerator1 + product;
            if denominator >= numerator1 {
                return mul_div_rounding_up(numerator1, sqrt_px96, denominator);
            }
        }
        div_rounding_up(numerator1, numerator1 / sqrt_px96 + amount)
    } else {
        let product = amount * sqrt_px96;
        if product / amount != sqrt_px96 {
            return None;
        }
        if numerator1 <= product {
            return None;
        }
        let denominator = numerator1 - product;
        mul_div_rounding_up(numerator1, sqrt_px96, denominator)
    }
}

pub fn get_next_sqrt_price_from_amount1_rounding_down(
    sqrt_px96: U256,
    liquidity: U256,
    amount: U256,
    add: bool,
) -> Option<U256> {
    if add {
        let quotient = (amount << 96) / liquidity;
        Some(sqrt_px96 + quotient)
    } else {
        let quotient = ((amount << 96) + liquidity - U256::from(1)) / liquidity;
        if sqrt_px96 <= quotient {
            return None;
        }
        Some(sqrt_px96 - quotient)
    }
}

pub fn get_next_sqrt_price_from_input(
    sqrt_px96: U256,
    liquidity: U256,
    amount_in: U256,
    zero_for_one: bool,
) -> Option<U256> {
    if sqrt_px96.is_zero() || liquidity.is_zero() {
        return None;
    }
    if zero_for_one {
        get_next_sqrt_price_from_amount0_rounding_up(sqrt_px96, liquidity, amount_in, true)
    } else {
        get_next_sqrt_price_from_amount1_rounding_down(sqrt_px96, liquidity, amount_in, true)
    }
}

#[allow(dead_code)]
pub fn get_next_sqrt_price_from_output(
    sqrt_px96: U256,
    liquidity: U256,
    amount_out: U256,
    zero_for_one: bool,
) -> Option<U256> {
    if sqrt_px96.is_zero() || liquidity.is_zero() {
        return None;
    }
    if zero_for_one {
        get_next_sqrt_price_from_amount1_rounding_down(sqrt_px96, liquidity, amount_out, false)
    } else {
        get_next_sqrt_price_from_amount0_rounding_up(sqrt_px96, liquidity, amount_out, false)
    }
}

pub fn get_amount0_delta(
    mut sqrt_ratio_a_x96: U256,
    mut sqrt_ratio_b_x96: U256,
    liquidity: U256,
    round_up: bool,
) -> Option<U256> {
    if sqrt_ratio_a_x96 > sqrt_ratio_b_x96 {
        std::mem::swap(&mut sqrt_ratio_a_x96, &mut sqrt_ratio_b_x96);
    }
    if sqrt_ratio_a_x96.is_zero() {
        return None;
    }

    let numerator1 = liquidity << 96;
    let numerator2 = sqrt_ratio_b_x96 - sqrt_ratio_a_x96;

    if round_up {
        let inner = mul_div_rounding_up(numerator1, numerator2, sqrt_ratio_b_x96)?;
        div_rounding_up(inner, sqrt_ratio_a_x96)
    } else {
        let inner = mul_div(numerator1, numerator2, sqrt_ratio_b_x96)?;
        Some(inner / sqrt_ratio_a_x96)
    }
}

pub fn get_amount1_delta(
    mut sqrt_ratio_a_x96: U256,
    mut sqrt_ratio_b_x96: U256,
    liquidity: U256,
    round_up: bool,
) -> Option<U256> {
    if sqrt_ratio_a_x96 > sqrt_ratio_b_x96 {
        std::mem::swap(&mut sqrt_ratio_a_x96, &mut sqrt_ratio_b_x96);
    }

    let delta = sqrt_ratio_b_x96 - sqrt_ratio_a_x96;
    if round_up {
        Some((liquidity * delta + (Q96 - U256::from(1))) >> 96)
    } else {
        Some((liquidity * delta) >> 96)
    }
}
