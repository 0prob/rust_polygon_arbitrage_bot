use alloy::primitives::{U256, U512};

use crate::util::u512_to_u256;
use super::log_exp_math::log_exp_pow;

pub const ONE: U256 = U256::from_limbs([1_000_000_000_000_000_000, 0, 0, 0]);
pub const ONE_U512: U512 = U512::from_limbs([1_000_000_000_000_000_000, 0, 0, 0, 0, 0, 0, 0]);
pub const MAX_POW_RELATIVE_ERROR: U256 = U256::from_limbs([10_000, 0, 0, 0]);

/// log2(ratio / ONE) via integer bit decomposition; mantissa alone uses f64 in [1, 2).
#[inline]
#[must_use]
pub fn ratio_log2_delta(ratio: U256) -> f64 {
    if ratio.is_zero() {
        return f64::NEG_INFINITY;
    }
    let log_r = 256i32.saturating_sub(ratio.leading_zeros() as i32);
    let log_o = 256i32.saturating_sub(ONE.leading_zeros() as i32);
    let exp = log_r - log_o;
    let normalized = if exp >= 0 {
        if exp >= 256 {
            return f64::INFINITY;
        }
        ratio >> exp
    } else {
        ratio << (-exp).min(255) as u32
    };
    let mantissa = crate::util::u256_to_f64(normalized) / crate::util::u256_to_f64(ONE);
    if mantissa <= 0.0 || !mantissa.is_finite() {
        return 0.0;
    }
    f64::from(exp) + mantissa.log2()
}

/// Bellman-Ford edge weight: `-ln(ratio / ONE)` without converting full ratio to f64.
#[inline]
#[must_use]
pub fn edge_log_weight_from_ratio(ratio: U256) -> f64 {
    if ratio.is_zero() {
        return f64::INFINITY;
    }
    -ratio_log2_delta(ratio) * std::f64::consts::LN_2
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edge_log_weight_negative_when_ratio_above_one() {
        let ratio = ONE + ONE / U256::from(100u64);
        assert!(edge_log_weight_from_ratio(ratio) < 0.0);
    }

    #[test]
    fn edge_log_weight_positive_when_ratio_below_one() {
        let ratio = ONE - ONE / U256::from(100u64);
        assert!(edge_log_weight_from_ratio(ratio) > 0.0);
    }
}
