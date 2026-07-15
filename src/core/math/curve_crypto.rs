use alloy::primitives::{U256, U512};
use smallvec::SmallVec;

use crate::core::constants::MAX_POOL_TOKENS;
use crate::core::types::CurvePoolState;

use super::fixed_point::ONE;

type CurveXp = SmallVec<[U256; MAX_POOL_TOKENS]>;
const A_MULTIPLIER: U256 = U256::from_limbs([10_000, 0, 0, 0]);
const MAX_ITERATIONS: u32 = 128;

const POW_10_10: U256 = U256::from_limbs([10_000_000_000, 0, 0, 0]);
const POW_10_14: U256 = U256::from_limbs([100_000_000_000_000, 0, 0, 0]);
const POW_10_16: U256 = U256::from_limbs([10_000_000_000_000_000, 0, 0, 0]);
// 10^20 = 5 * 2^64 + 0x6BC75E2D63100000
const POW_10_20: U256 = U256::from_limbs([0x6BC75E2D63100000, 5, 0, 0]);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NewtonResult {
    pub value: U256,
    pub converged: bool,
}

fn sort_desc(values: &mut [U256]) {
    values.sort_unstable_by(|a, b| b.cmp(a));
}

fn abs_diff_add_one(a: U256, b: U256) -> U256 {
    (if a > b { a - b } else { b - a }) + U256::from(1u8)
}

fn geometric_mean(x: &[U256]) -> U256 {
    let n = U256::from(x.len());
    if x.iter().any(U256::is_zero) {
        return U256::ZERO;
    }
    let mut d = x[0];
    for _ in 0..MAX_ITERATIONS {
        let mut tmp = ONE;
        for xi in x {
            tmp = (tmp * *xi) / d;
        }
        let d_prev = d;
        d = (d * ((n - U256::from(1u8)) * ONE + tmp)) / (n * ONE);
        let diff = if d > d_prev { d - d_prev } else { d_prev - d };
        if diff <= U256::from(1u8) || diff * ONE < d {
            return d;
        }
    }
    U256::ZERO
}

fn compute_k0(xp: &[U256], d: U256, n: U256) -> U256 {
    if d.is_zero() {
        return U256::ZERO;
    }
    let mut k0 = ONE;
    for x in xp {
        k0 = (k0 * *x * n) / d;
    }
    k0
}

pub fn curve_crypto_newton_d(ann: U256, gamma: U256, xp: &[U256]) -> NewtonResult {
    let n = U256::from(xp.len());
    if xp.len() < 2 || ann.is_zero() || gamma.is_zero() || xp.iter().any(U256::is_zero) {
        return NewtonResult {
            value: U256::ZERO,
            converged: false,
        };
    }
    let s: U256 = xp.iter().copied().sum();
    let mut d = n * geometric_mean(xp);
    if d.is_zero() {
        return NewtonResult {
            value: U256::ZERO,
            converged: false,
        };
    }
    for _ in 0..MAX_ITERATIONS {
        let d_prev = d;
        if d.is_zero() {
            break;
        }
        let k0 = compute_k0(xp, d, n);
        if k0.is_zero() {
            break;
        }
        let g1k0 = abs_diff_add_one(gamma + ONE, k0);
        if g1k0.is_zero() {
            break;
        }
        // U512 for mul1: low-gamma pools (gamma ~ 10^3) produce intermediates
        // exceeding U256::MAX.  Sequence: ONE*d/gamma * g1k0 / gamma * g1k0 * A_MULTIPLIER / ann.
        let mul1 = crate::util::u512_to_u256(
            (((U512::from(ONE) * U512::from(d)) / U512::from(gamma) * U512::from(g1k0))
                / U512::from(gamma)
                * U512::from(g1k0)
                * U512::from(A_MULTIPLIER))
                / U512::from(ann),
        );
        let mul2 = (U256::from(2u8) * ONE * n * k0) / g1k0;
        let neg_fprime = (s + (s * mul2) / ONE + (mul1 * n) / k0).saturating_sub((mul2 * d) / ONE);
        if neg_fprime.is_zero() {
            return NewtonResult {
                value: U256::ZERO,
                converged: false,
            };
        }
        let dplus = (d * (neg_fprime + s)) / neg_fprime;
        let mut dminus = (d * d) / neg_fprime;
        if ONE > k0 {
            dminus += (((d * mul1) / neg_fprime) / ONE * (ONE - k0)) / k0;
        } else {
            let subtrahend = (((d * mul1) / neg_fprime) / ONE * (k0 - ONE)) / k0;
            dminus = dminus.saturating_sub(subtrahend);
        }
        d = if dplus > dminus {
            dplus - dminus
        } else {
            (dminus - dplus) / U256::from(2u8)
        };
        let diff = if d > d_prev { d - d_prev } else { d_prev - d };
        let threshold = POW_10_16.max(d);
        if diff * POW_10_14 < threshold {
            return NewtonResult {
                value: d,
                converged: true,
            };
        }
    }
    NewtonResult {
        value: U256::ZERO,
        converged: false,
    }
}

#[must_use]
pub fn curve_crypto_newton_y(
    ann: U256,
    gamma: U256,
    xp: &[U256],
    d: U256,
    out_idx: usize,
) -> NewtonResult {
    let n = xp.len();
    if out_idx >= n || ann.is_zero() || gamma.is_zero() || d.is_zero() {
        return NewtonResult {
            value: U256::ZERO,
            converged: false,
        };
    }
    let n_u256 = U256::from(n as u64);
    let mut sorted: CurveXp = xp.iter().copied().collect();
    sorted[out_idx] = U256::ZERO;
    sort_desc(&mut sorted);
    let mut y = d / n_u256;
    let mut k0i = ONE;
    let mut si = U256::ZERO;
    let convergence_limit = {
        let a = sorted[0] / POW_10_14;
        let b = d / POW_10_14;
        if a > b {
            if a > U256::from(100u8) {
                a
            } else {
                U256::from(100u8)
            }
        } else if b > U256::from(100u8) {
            b
        } else {
            U256::from(100u8)
        }
    };

    for j in 2..=n {
        let xj = sorted[n - j];
        if xj.is_zero() {
            return NewtonResult {
                value: U256::ZERO,
                converged: false,
            };
        }
        y = (y * d) / (xj * n_u256);
        si += xj;
    }
    for &item in sorted.iter().take(n - 1) {
        k0i = (k0i * item * n_u256) / d;
    }

    for _ in 0..MAX_ITERATIONS {
        let y_prev = y;
        if y.is_zero() || d.is_zero() {
            break;
        }
        let k0 = (k0i * y * n_u256) / d;
        if k0.is_zero() {
            break;
        }
        let s = si + y;
        let g1k0 = abs_diff_add_one(gamma + ONE, k0);
        if g1k0.is_zero() {
            break;
        }
        // U512 for mul1 — same overflow regime as newton_d (low gamma).
        let mul1 = crate::util::u512_to_u256(
            (((U512::from(ONE) * U512::from(d)) / U512::from(gamma) * U512::from(g1k0))
                / U512::from(gamma)
                * U512::from(g1k0)
                * U512::from(A_MULTIPLIER))
                / U512::from(ann),
        );
        let mul2 = ONE + (U256::from(2u8) * ONE * k0) / g1k0;
        let mut yfprime = ONE * y + s * mul2 + mul1;
        let dyfprime = d * mul2;
        if yfprime < dyfprime {
            y = y_prev / U256::from(2u8);
            continue;
        }
        yfprime -= dyfprime;
        if y.is_zero() {
            return NewtonResult {
                value: U256::ZERO,
                converged: false,
            };
        }
        let fprime = yfprime / y;
        if fprime.is_zero() {
            return NewtonResult {
                value: U256::ZERO,
                converged: false,
            };
        }
        let mut y_minus = mul1 / fprime;
        let y_plus = (yfprime + ONE * d) / fprime + (y_minus * ONE) / k0;
        y_minus += (ONE * s) / fprime;
        y = if y_plus < y_minus {
            y_prev / U256::from(2u8)
        } else {
            y_plus - y_minus
        };
        let diff = if y > y_prev { y - y_prev } else { y_prev - y };
        let y_scaled = y / POW_10_14;
        let limit = if convergence_limit > y_scaled {
            convergence_limit
        } else {
            y_scaled
        };
        if diff < limit {
            let frac = (y * ONE) / d;
            let low = POW_10_16 - U256::from(1u8);
            let high = POW_10_20 + U256::from(1u8);
            if frac > low && frac < high {
                return NewtonResult {
                    value: y,
                    converged: true,
                };
            }
            return NewtonResult {
                value: U256::ZERO,
                converged: false,
            };
        }
    }
    NewtonResult {
        value: U256::ZERO,
        converged: false,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CurveCryptoReject {
    ZeroAmount,
    InvalidIndices,
    MissingGamma,
    NewtonD,
    NewtonY,
    ZeroOut,
}

#[must_use]
pub fn try_curve_crypto_amount_out(
    state: &CurvePoolState,
    amount_in: U256,
    token_in_idx: usize,
    token_out_idx: usize,
) -> Result<U256, CurveCryptoReject> {
    if amount_in.is_zero() {
        return Err(CurveCryptoReject::ZeroAmount);
    }
    if token_in_idx >= state.n_coins as usize
        || token_out_idx >= state.n_coins as usize
        || token_in_idx == token_out_idx
    {
        return Err(CurveCryptoReject::InvalidIndices);
    }
    let gamma = state.gamma.unwrap_or(U256::ZERO);
    let a = state.a;
    if gamma.is_zero() || a.is_zero() {
        return Err(CurveCryptoReject::MissingGamma);
    }
    let n = U256::from(state.n_coins);
    let ann = a * n * A_MULTIPLIER;
    let n_coins = state.n_coins as usize;
    let mut rates: CurveXp = SmallVec::with_capacity(n_coins);
    if state.rates.is_empty() {
        rates.extend(std::iter::repeat_n(ONE, n_coins));
    } else {
        rates.extend(state.rates.iter().copied());
    }
    let mut xp: CurveXp = SmallVec::with_capacity(n_coins);
    xp.extend(
        state
            .balances
            .iter()
            .zip(rates.iter())
            .map(|(b, r)| (*b * *r) / ONE),
    );
    if token_in_idx >= xp.len() || token_out_idx >= xp.len() {
        return Err(CurveCryptoReject::InvalidIndices);
    }
    xp[token_in_idx] += (amount_in * rates[token_in_idx]) / ONE;
    let d_result = curve_crypto_newton_d(ann, gamma, &xp);
    if !d_result.converged {
        return Err(CurveCryptoReject::NewtonD);
    }
    let y_result = curve_crypto_newton_y(ann, gamma, &xp, d_result.value, token_out_idx);
    if !y_result.converged {
        return Err(CurveCryptoReject::NewtonY);
    }
    let dy = xp[token_out_idx].saturating_sub(y_result.value);
    let fee = state.fee;
    let fee_denom = POW_10_10;
    let out_rate = rates[token_out_idx];
    if out_rate.is_zero() {
        return Err(CurveCryptoReject::ZeroOut);
    }
    let out = (dy * ONE) / out_rate;
    let fee_amount = (out * fee) / fee_denom;
    let out_after_fee = out.saturating_sub(fee_amount);
    let buffered = out_after_fee.saturating_sub(
        (out_after_fee * crate::core::math::curve::CURVE_OUTPUT_BUFFER)
            / crate::core::math::curve::CURVE_FEE_DENOMINATOR,
    );
    if buffered.is_zero() {
        return Err(CurveCryptoReject::ZeroOut);
    }
    Ok(buffered)
}

#[inline]
#[must_use]
pub fn get_curve_crypto_amount_out(
    state: &CurvePoolState,
    amount_in: U256,
    token_in_idx: usize,
    token_out_idx: usize,
) -> U256 {
    try_curve_crypto_amount_out(state, amount_in, token_in_idx, token_out_idx).unwrap_or(U256::ZERO)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::math::fixed_point::ONE;

    #[test]
    fn test_zero_amount_returns_zero() {
        let state = CurvePoolState {
            balances: vec![],
            a: U256::ZERO,
            fee: U256::ZERO,
            rates: vec![],
            n_coins: 0,
            gamma: None,
            d: None,
        };
        assert_eq!(
            get_curve_crypto_amount_out(&state, U256::ZERO, 0, 1),
            U256::ZERO
        );
    }

    #[test]
    fn crypto_two_coin_swap_returns_positive_output_within_reserve() {
        let state = CurvePoolState {
            balances: vec![
                U256::from(1_000_000u64) * ONE,
                U256::from(1_000_000u64) * ONE,
            ],
            a: U256::from(5_000u64),
            fee: U256::from(1_000u64),
            rates: vec![ONE, ONE],
            n_coins: 2,
            gamma: Some(U256::from(10_000u64)),
            d: None,
        };
        let out = get_curve_crypto_amount_out(&state, U256::from(10_000u64) * ONE, 0, 1);
        assert!(out > U256::ZERO);
        assert!(out <= state.balances[1]);
    }
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    fn non_zero() -> impl Strategy<Value = U256> {
        (1u128..=u128::MAX).prop_map(U256::from)
    }

    proptest! {
        #[test]
        fn newton_convergence_or_graceful(
            ann in (100u64..1_000_000u64).prop_map(U256::from),
            gamma in non_zero(),
            balance0 in non_zero(),
            balance1 in non_zero(),
        ) {
            let xp = vec![balance0, balance1];
            let result = curve_crypto_newton_d(ann, gamma, &xp);
            if result.converged {
                prop_assert!(!result.value.is_zero());
                let y_result = curve_crypto_newton_y(ann, gamma, &xp, result.value, 1);
                prop_assert!(y_result.converged || !y_result.value.is_zero());
            }
        }

        #[test]
        fn output_bounded(
            amount_in in non_zero(),
            ann in (100u64..1_000_000u64).prop_map(U256::from),
            gamma in non_zero(),
            balance0 in non_zero(),
            balance1 in non_zero(),
        ) {
            let state = CurvePoolState {
                balances: vec![balance0, balance1],
                a: ann,
                fee: U256::ZERO,
                rates: vec![],
                n_coins: 2,
                gamma: Some(gamma),
                d: None,
            };
            let out = get_curve_crypto_amount_out(&state, amount_in, 0, 1);
            if !out.is_zero() {
                prop_assert!(out <= state.balances[1],
                    "out={} exceeds balance={}", out, state.balances[1]);
            }
        }
    }
}
