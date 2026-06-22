use ruint::aliases::U256;

use super::log_exp_math::log_exp_pow;

pub const ONE: U256 = U256::from_limbs([1_000_000_000_000_000_000, 0, 0, 0]);
pub const TWO: U256 = U256::from_limbs([2_000_000_000_000_000_000, 0, 0, 0]);
pub const FOUR: U256 = U256::from_limbs([4_000_000_000_000_000_000, 0, 0, 0]);
pub const MAX_POW_RELATIVE_ERROR: U256 = U256::from_limbs([10_000, 0, 0, 0]);

pub fn mul_down(a: U256, b: U256) -> U256 {
    (a * b) / ONE
}

pub fn mul_up(a: U256, b: U256) -> U256 {
    let product = a * b;
    if product % ONE == U256::ZERO {
        product / ONE
    } else {
        product / ONE + U256::from(1)
    }
}

pub fn complement(x: U256) -> U256 {
    if x < ONE { ONE - x } else { U256::ZERO }
}

pub fn pow_down(x: U256, y: U256) -> U256 {
    if y == ONE {
        return x;
    }
    if y == TWO {
        return mul_down(x, x);
    }
    if y == FOUR {
        let square = mul_down(x, x);
        return mul_down(square, square);
    }

    let raw = log_exp_pow(x, y);
    let max_error = mul_up(raw, MAX_POW_RELATIVE_ERROR) + U256::from(1);
    if raw < max_error {
        return U256::ZERO;
    }
    raw - max_error
}
