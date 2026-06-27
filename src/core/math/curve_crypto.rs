use ruint::aliases::U256;

use crate::core::types::CurvePoolState;

use super::fixed_point::ONE;
const A_MULTIPLIER: U256 = U256::from_limbs([10_000, 0, 0, 0]);
const MAX_ITERATIONS: u32 = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NewtonResult {
    pub value: U256,
    pub converged: bool,
}

fn sort_desc(values: &[U256]) -> Vec<U256> {
    let mut out = values.to_vec();
    out.sort_by(|a, b| b.cmp(a));
    out
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
        let mut g1k0 = gamma + ONE;
        g1k0 = if g1k0 > k0 {
            g1k0 - k0 + U256::from(1u8)
        } else {
            k0 - g1k0 + U256::from(1u8)
        };
        if g1k0.is_zero() {
            break;
        }
        let mul1 = (((ONE * d) / gamma * g1k0) / gamma * g1k0 * A_MULTIPLIER) / ann;
        let mul2 = (U256::from(2u8) * ONE * n * k0) / g1k0;
        let neg_fprime = s + (s * mul2) / ONE + (mul1 * n) / k0 - (mul2 * d) / ONE;
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
        let threshold = U256::from(10u128).pow(U256::from(16)).max(d);
        if diff * U256::from(10u128).pow(U256::from(14)) < threshold {
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
    let mut x_sorted = xp.to_vec();
    x_sorted[out_idx] = U256::ZERO;
    let sorted = sort_desc(&x_sorted);
    let mut y = d / U256::from(n as u64);
    let mut k0i = ONE;
    let mut si = U256::ZERO;
    let convergence_limit = {
        let a = sorted[0] / U256::from(10u128).pow(U256::from(14));
        let b = d / U256::from(10u128).pow(U256::from(14));
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
        y = (y * d) / (xj * U256::from(n as u64));
        si += xj;
    }
    for &item in sorted.iter().take(n - 1) {
        k0i = (k0i * item * U256::from(n as u64)) / d;
    }

    for _ in 0..MAX_ITERATIONS {
        let y_prev = y;
        if y.is_zero() || d.is_zero() {
            break;
        }
        let k0 = (k0i * y * U256::from(n as u64)) / d;
        if k0.is_zero() {
            break;
        }
        let s = si + y;
        let mut g1k0 = gamma + ONE;
        g1k0 = if g1k0 > k0 {
            g1k0 - k0 + U256::from(1u8)
        } else {
            k0 - g1k0 + U256::from(1u8)
        };
        if g1k0.is_zero() {
            break;
        }
        let mul1 = (((ONE * d) / gamma * g1k0) / gamma * g1k0 * A_MULTIPLIER) / ann;
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
        let limit = if convergence_limit > y / U256::from(10u128).pow(U256::from(14)) {
            convergence_limit
        } else {
            y / U256::from(10u128).pow(U256::from(14))
        };
        if diff < limit {
            let frac = (y * ONE) / d;
            let low = U256::from(10u128).pow(U256::from(16)) - U256::from(1u8);
            let high = U256::from(10u128).pow(U256::from(20)) + U256::from(1u8);
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

pub fn get_curve_crypto_amount_out(
    state: &CurvePoolState,
    amount_in: U256,
    token_in_idx: usize,
    token_out_idx: usize,
) -> U256 {
    if amount_in.is_zero()
        || token_in_idx >= state.n_coins as usize
        || token_out_idx >= state.n_coins as usize
        || token_in_idx == token_out_idx
    {
        return U256::ZERO;
    }
    let gamma = state.gamma.unwrap_or(U256::ZERO);
    let a = state.a;
    if gamma.is_zero() || a.is_zero() {
        return U256::ZERO;
    }
    let n = U256::from(state.n_coins);
    let ann = a * n * A_MULTIPLIER;
    let rates = if state.rates.is_empty() {
        vec![ONE; state.n_coins as usize]
    } else {
        state.rates.clone()
    };
    let mut xp: Vec<U256> = state
        .balances
        .iter()
        .zip(rates.iter())
        .map(|(b, r)| (*b * *r) / ONE)
        .collect();
    xp[token_in_idx] += (amount_in * rates[token_in_idx]) / ONE;
    let d_result = curve_crypto_newton_d(ann, gamma, &xp);
    if !d_result.converged {
        return U256::ZERO;
    }
    let y_result = curve_crypto_newton_y(ann, gamma, &xp, d_result.value, token_out_idx);
    if !y_result.converged {
        return U256::ZERO;
    }
    let dy = xp[token_out_idx].saturating_sub(y_result.value);
    let fee = state.fee;
    let fee_denom = U256::from(10u128).pow(U256::from(10));
    let out_rate = rates[token_out_idx];
    if out_rate.is_zero() {
        return U256::ZERO;
    }
    let out = (dy * ONE) / out_rate;
    let fee_amount = (out * fee) / fee_denom;
    out.saturating_sub(fee_amount)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newton_d_converges_for_balanced_pool() {
        let one = ONE;
        let bal = U256::from(1_000_000u128) * one;
        let xp = vec![bal, bal];
        let ann = U256::from(85_000u64) * U256::from(4u64);
        let gamma = U256::from(10u128).pow(U256::from(15));
        let result = curve_crypto_newton_d(ann, gamma, &xp);
        assert!(result.converged);
        assert!(result.value > U256::ZERO);
    }
}
