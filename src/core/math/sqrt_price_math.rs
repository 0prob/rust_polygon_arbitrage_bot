use alloy::primitives::U256;

use super::{div_rounding_up, mul_div_ceil, mul_div_floor};

/// Uniswap V3 / V4 `FixedPoint96.Q96` = 2^96.
///
/// Prior bug: `from_limbs([0, 1, 0, 0])` is 2^64 — amount1 ceil rounding used
/// `(p + 2^64 - 1) >> 96` instead of `(p + 2^96 - 1) / 2^96`, systematically
/// under-charging amount1-in and mis-quoting one-for-zero swaps.
const Q96: U256 = U256::from_limbs([0, 1u64 << 32, 0, 0]);

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
            let next = mul_div_ceil(numerator1, sqrt_px96, denominator)?;
            return (!next.is_zero()).then_some(next);
        }
        // Overflow path (UniswapV3 SqrtPriceMath): div(num1, num1/sqrtP + amount).
        let next = div_rounding_up(numerator1, numerator1 / sqrt_px96 + amount)?;
        (!next.is_zero()).then_some(next)
    } else {
        let product = amount.checked_mul(sqrt_px96)?;
        let denominator = numerator1.checked_sub(product)?;
        let next = mul_div_ceil(numerator1, sqrt_px96, denominator)?;
        (!next.is_zero()).then_some(next)
    }
}

pub fn get_next_sqrt_price_from_amount1_rounding_down(
    sqrt_px96: U256,
    liquidity: U256,
    amount: U256,
    add: bool,
) -> Option<U256> {
    if liquidity.is_zero() {
        return None;
    }
    // Always U512 mul_div — `(amount << 96) / L` truncates when amount ≥ 2^160.
    if add {
        let quotient = mul_div_floor(amount, Q96, liquidity)?;
        sqrt_px96.checked_add(quotient)
    } else {
        let quotient = mul_div_ceil(amount, Q96, liquidity)?;
        sqrt_px96.checked_sub(quotient)
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
        // Dust-zero is valid; use ceil/floor helpers that allow it.
        let inner = mul_div_ceil(numerator1, numerator2, sqrt_ratio_b_x96)?;
        div_rounding_up(inner, sqrt_ratio_a_x96)
    } else {
        let inner = mul_div_floor(numerator1, numerator2, sqrt_ratio_b_x96)?;
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
    // FullMath.mulDiv(liquidity, Δ√P, Q96) — U512 path so deep-pool liquidity
    // (liq · Δ√P > 2^256) still quotes instead of failing closed.
    if round_up {
        mul_div_ceil(liquidity, delta, Q96)
    } else {
        mul_div_floor(liquidity, delta, Q96)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn q96_is_two_pow_ninety_six() {
        assert_eq!(Q96, U256::from(1u128) << 96);
        // Guard the old 2^64 mis-encoding.
        assert_ne!(Q96, U256::from_limbs([0, 1, 0, 0]));
    }

    #[test]
    fn test_zero_amount_returns_some() {
        let px = Q96;
        let r = get_next_sqrt_price_from_amount0_rounding_up(px, U256::ONE, U256::ZERO, true);
        assert!(r.is_some());
    }

    #[test]
    fn amount1_delta_at_unit_price_equals_liquidity() {
        // √P = Q96 (price 1), move to 2·Q96 → amount1 = L · Q96 / Q96 = L.
        let liq = U256::from(1_000_000u64);
        let a1 = get_amount1_delta(Q96, Q96 * U256::from(2u64), liq, false).expect("delta");
        assert_eq!(a1, liq);
        let a1_up = get_amount1_delta(Q96, Q96 * U256::from(2u64), liq, true).expect("delta up");
        assert_eq!(a1_up, liq);
    }

    #[test]
    fn amount1_delta_ceil_rounds_up() {
        // Choose Δ√P so L·Δ is not divisible by Q96.
        let liq = U256::from(3u64);
        let delta_sqrt = U256::from(5u64); // tiny price move
        let floor = get_amount1_delta(Q96, Q96 + delta_sqrt, liq, false).expect("floor");
        let ceil = get_amount1_delta(Q96, Q96 + delta_sqrt, liq, true).expect("ceil");
        // 3*5 / 2^96 = 0 floor; ceil must be 1 when product nonzero.
        assert_eq!(floor, U256::ZERO);
        assert_eq!(ceil, U256::ONE);
    }

    #[test]
    fn amount1_large_liquidity_does_not_overflow() {
        // liq · Q96 > U256::MAX when done as checked_mul then >>96; mul_div handles it.
        let liq = (U256::from(1u128) << 200) + U256::from(7u64);
        let a1 = get_amount1_delta(Q96, Q96 * U256::from(2u64), liq, false).expect("wide delta");
        assert_eq!(a1, liq);
    }

    #[test]
    fn next_price_from_amount1_matches_unit_step() {
        let liq = U256::from(1_000_000u64);
        let next =
            get_next_sqrt_price_from_amount1_rounding_down(Q96, liq, liq, true).expect("next");
        // Adding amount1=L at price=1 moves √P by Q96 → 2·Q96.
        assert_eq!(next, Q96 * U256::from(2u64));
    }
}
