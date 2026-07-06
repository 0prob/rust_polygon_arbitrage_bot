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

use alloy::primitives::U256;

#[inline(always)]
pub(crate) fn mul_div(a: U256, b: U256, denominator: U256) -> Option<U256> {
    a.checked_mul(b)?.checked_div(denominator)
}

#[inline(always)]
pub(crate) fn mul_div_rounding_up(a: U256, b: U256, denominator: U256) -> Option<U256> {
    let product = a.checked_mul(b)?;
    let result = product.checked_div(denominator)?;
    if product % denominator > U256::ZERO {
        Some(result + U256::from(1))
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
