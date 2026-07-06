use alloy::primitives::U256;

use super::log_exp_math::log_exp_pow;

pub const ONE: U256 = U256::from_limbs([1_000_000_000_000_000_000, 0, 0, 0]);
pub const MAX_POW_RELATIVE_ERROR: U256 = U256::from_limbs([10_000, 0, 0, 0]);

#[inline(always)]
pub fn mul_down(a: U256, b: U256) -> U256 {
    match a.checked_mul(b) {
        Some(p) => p / ONE,
        None => U256::ZERO,
    }
}

#[inline(always)]
pub fn mul_up(a: U256, b: U256) -> U256 {
    let Some(product) = a.checked_mul(b) else {
        return U256::ZERO;
    };
    if product % ONE == U256::ZERO {
        product / ONE
    } else {
        product / ONE + U256::from(1)
    }
}

#[inline(always)]
pub fn complement(x: U256) -> U256 {
    if x < ONE { ONE - x } else { U256::ZERO }
}

#[inline]
pub fn pow_down(x: U256, y: U256) -> U256 {
    if y == ONE {
        return x;
    }

    let raw = log_exp_pow(x, y);
    let max_error = mul_up(raw, MAX_POW_RELATIVE_ERROR) + U256::from(1);
    if raw < max_error {
        return U256::ZERO;
    }
    raw - max_error
}
