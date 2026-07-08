use alloy::primitives::{U256, U512};

use crate::util::u512_to_u256;
use super::log_exp_math::log_exp_pow;

pub const ONE: U256 = U256::from_limbs([1_000_000_000_000_000_000, 0, 0, 0]);
pub const ONE_U512: U512 = U512::from_limbs([1_000_000_000_000_000_000, 0, 0, 0, 0, 0, 0, 0]);
pub const MAX_POW_RELATIVE_ERROR: U256 = U256::from_limbs([10_000, 0, 0, 0]);

#[inline(always)]
pub fn mul_down(a: U256, b: U256) -> U256 {
    // U512 widening prevents silent ZERO on intermediate overflow.
    // Critical for Balancer weighted pools with large balances.
    let product = U512::from(a) * U512::from(b);
    u512_to_u256(product / ONE_U512)
}

#[inline(always)]
pub fn mul_up(a: U256, b: U256) -> U256 {
    let product = U512::from(a) * U512::from(b);
    let quotient = u512_to_u256(product / ONE_U512);
    if product % ONE_U512 > U512::ZERO {
        quotient.saturating_add(U256::from(1))
    } else {
        quotient
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
    raw.saturating_sub(max_error)
}
