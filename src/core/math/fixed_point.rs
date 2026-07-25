use alloy::primitives::{U256, U512};

use super::log_exp_math::{log_exp_ln, log_exp_pow};
use crate::util::u512_to_u256;

pub const ONE: U256 = U256::from_limbs([1_000_000_000_000_000_000, 0, 0, 0]);
pub const ONE_U512: U512 = U512::from_limbs([1_000_000_000_000_000_000, 0, 0, 0, 0, 0, 0, 0]);
pub const MAX_POW_RELATIVE_ERROR: U256 = U256::from_limbs([10_000, 0, 0, 0]);
#[inline]
#[must_use]
pub fn edge_log_weight_from_ratio(ratio: U256) -> f64 {
    if ratio.is_zero() {
        return f64::INFINITY;
    }
    let scale = 1_000_000_000_000_000_000.0;
    if ratio >= ONE {
        let ln_ratio = log_exp_ln(ratio);
        if ln_ratio.is_zero() {
            return 0.0;
        }
        -crate::util::u256_to_f64(ln_ratio) / scale
    } else {
        let reciprocal = crate::util::u512_to_u256((ONE_U512 * ONE_U512) / U512::from(ratio));
        let ln_recip = log_exp_ln(reciprocal);
        if ln_recip.is_zero() {
            return 0.0;
        }
        crate::util::u256_to_f64(ln_recip) / scale
    }
}

#[inline(always)]
pub fn mul_down(a: U256, b: U256) -> U256 {
    if let Some(product) = a.checked_mul(b) {
        return product / ONE;
    }
    // U512 widening prevents silent ZERO on intermediate overflow.
    // Critical for Balancer weighted pools with large balances.
    let product = U512::from(a) * U512::from(b);
    u512_to_u256(product / ONE_U512)
}

#[inline(always)]
pub fn mul_up(a: U256, b: U256) -> U256 {
    if let Some(product) = a.checked_mul(b) {
        let q = product / ONE;
        if !(product % ONE).is_zero() {
            return q.saturating_add(U256::from(1));
        } else {
            return q;
        }
    }
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

    #[test]
    fn pow_down_live_weighted_pool_regression() {
        // Regression: ln() compared reduced input to exponent thresholds instead of
        // exp factors, yielding pow_down=0 and phantom Balancer weighted quotes.
        let base = U256::from(845_837_069_060_167_155u64);
        let exponent = U256::from(1_000_300_030_003_000_300u64);
        let out = pow_down(base, exponent);
        assert!(
            !out.is_zero(),
            "pow_down returned zero for live-pool inputs"
        );
    }
}
