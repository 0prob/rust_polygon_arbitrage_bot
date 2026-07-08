use alloy::primitives::U256;

use super::{div_rounding_up, mul_div, mul_div_rounding_up};

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

    let numerator1: U256 = liquidity.checked_shl(96)?;

    if add {
        if let Some(product) = amount.checked_mul(sqrt_px96)
            && let Some(denominator) = numerator1.checked_add(product)
        {
            return mul_div_rounding_up(numerator1, sqrt_px96, denominator);
        }
        div_rounding_up(numerator1, numerator1 / sqrt_px96 + amount)
    } else {
        let product = amount.checked_mul(sqrt_px96)?;
        let denominator = numerator1.checked_sub(product)?;
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
        let quotient = ((amount << 96) + liquidity - U256::ONE) / liquidity;
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

    let numerator1: U256 = liquidity.checked_shl(96)?;
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
        liquidity
            .checked_mul(delta)
            .map(|p| (p + (Q96 - U256::ONE)) >> 96)
    } else {
        liquidity.checked_mul(delta).map(|p| p >> 96)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zero_amount_returns_some() {
        let px = Q96;
        let r = get_next_sqrt_price_from_amount0_rounding_up(px, U256::ONE, U256::ZERO, true);
        assert!(r.is_some());
    }
}
