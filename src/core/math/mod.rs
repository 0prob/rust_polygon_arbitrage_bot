pub mod balancer;
pub mod curve;
pub mod curve_crypto;
pub mod dodo;
pub mod fixed_point;
pub(crate) mod log_exp_math;
#[cfg(test)]
pub(crate) mod proptest_util;
pub mod sqrt_price_math;
pub mod swap_math;
pub mod tick_math;
pub mod uniswap_v2;
pub mod uniswap_v3;
pub mod woofi;

use alloy::primitives::{U256, U512};

use crate::util::u512_to_u256_checked;

/// Floor(a·b/d). Treats a zero quotient with nonzero a,b as failure (fee / output paths).
#[inline(always)]
pub(crate) fn mul_div(a: U256, b: U256, denominator: U256) -> Option<U256> {
    let result = mul_div_floor(a, b, denominator)?;
    if result.is_zero() && !a.is_zero() && !b.is_zero() {
        return None;
    }
    Some(result)
}

/// Floor(a·b/d). Allows a zero quotient (amount deltas / price steps where dust is valid).
#[inline(always)]
pub(crate) fn mul_div_floor(a: U256, b: U256, denominator: U256) -> Option<U256> {
    if denominator.is_zero() {
        return None;
    }
    // U512 widening: a*b may exceed U256::MAX while the quotient still fits.
    let product = U512::from(a) * U512::from(b);
    u512_to_u256_checked(product / U512::from(denominator))
}

/// Ceil(a·b/d). Treats a zero quotient with nonzero a,b as failure (fee / output paths).
#[inline(always)]
pub(crate) fn mul_div_rounding_up(a: U256, b: U256, denominator: U256) -> Option<U256> {
    let result = mul_div_ceil(a, b, denominator)?;
    if result.is_zero() && !a.is_zero() && !b.is_zero() {
        None
    } else {
        Some(result)
    }
}

/// Ceil(a·b/d). Allows a zero quotient (amount deltas).
#[inline(always)]
pub(crate) fn mul_div_ceil(a: U256, b: U256, denominator: U256) -> Option<U256> {
    if denominator.is_zero() {
        return None;
    }
    let product = U512::from(a) * U512::from(b);
    let den = U512::from(denominator);
    let result = u512_to_u256_checked(product / den)?;
    if product % den > U512::ZERO {
        result.checked_add(U256::from(1))
    } else {
        Some(result)
    }
}

#[inline(always)]
pub(crate) fn div_rounding_up(a: U256, b: U256) -> Option<U256> {
    let result = a.checked_div(b)?;
    if !(a % b).is_zero() {
        result.checked_add(U256::from(1))
    } else {
        Some(result)
    }
}

#[inline(always)]
pub(crate) fn div_rounding_up_or_zero(a: U256, b: U256) -> U256 {
    div_rounding_up(a, b).unwrap_or(U256::ZERO)
}

#[cfg(test)]
mod mul_div_tests {
    use super::*;

    #[test]
    fn floor_allows_dust_zero_quotient() {
        // 1*1 / 2 = 0 — valid for amount deltas, rejected by mul_div.
        assert_eq!(
            mul_div_floor(U256::from(1), U256::from(1), U256::from(2)),
            Some(U256::ZERO)
        );
        assert!(mul_div(U256::from(1), U256::from(1), U256::from(2)).is_none());
    }

    #[test]
    fn ceil_rounds_up_and_allows_exact_zero_only_when_inputs_zero() {
        assert_eq!(
            mul_div_ceil(U256::from(3), U256::from(1), U256::from(2)),
            Some(U256::from(2))
        );
        assert_eq!(
            mul_div_ceil(U256::ZERO, U256::from(5), U256::from(3)),
            Some(U256::ZERO)
        );
    }
}
