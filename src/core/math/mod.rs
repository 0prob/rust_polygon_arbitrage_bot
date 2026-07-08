pub mod balancer;
pub mod curve;
pub mod curve_crypto;
pub mod dodo;
pub mod fixed_point;
pub(crate) mod log_exp_math;
pub mod sqrt_price_math;
pub mod swap_math;
pub mod tick_math;
pub mod uniswap_v2;
pub mod uniswap_v3;
pub mod woofi;

use alloy::primitives::{U256, U512};

use crate::util::{u512_to_u256, u512_to_u256_checked};

#[inline(always)]
pub(crate) fn mul_div(a: U256, b: U256, denominator: U256) -> Option<U256> {
    if denominator.is_zero() {
        return None;
    }
    // U512 widening prevents intermediate overflow when a*b exceeds U256::MAX
    // but the final (a*b)/denominator fits in U256.
    let product = U512::from(a) * U512::from(b);
    let result = u512_to_u256(product / U512::from(denominator));
    if result.is_zero() && !a.is_zero() && !b.is_zero() {
        return None;
    }
    Some(result)
}

#[inline(always)]
pub(crate) fn mul_div_rounding_up(a: U256, b: U256, denominator: U256) -> Option<U256> {
    if denominator.is_zero() {
        return None;
    }
    let product = U512::from(a) * U512::from(b);
    let result = u512_to_u256_checked(product / U512::from(denominator))?;
    if product % U512::from(denominator) > U512::ZERO {
        result.checked_add(U256::from(1))
    } else if result.is_zero() && !a.is_zero() && !b.is_zero() {
        None
    } else {
        Some(result)
    }
}

#[inline(always)]
pub(crate) fn div_rounding_up(a: U256, b: U256) -> Option<U256> {
    let result = a.checked_div(b)?;
    if a % b > U256::ZERO {
        Some(result + U256::from(1))
    } else {
        Some(result)
    }
}

#[inline(always)]
pub(crate) fn div_rounding_up_or_zero(a: U256, b: U256) -> U256 {
    div_rounding_up(a, b).unwrap_or(U256::ZERO)
}
