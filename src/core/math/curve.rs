use ruint::aliases::U256;

use crate::core::types::CurvePoolState;

use super::fixed_point::ONE;
const CURVE_FEE_DENOMINATOR: U256 = U256::from_limbs([10_000_000_000, 0, 0, 0]);
const A_PRECISION: U256 = U256::from_limbs([100, 0, 0, 0]);
const MAX_ITERATIONS: u32 = 255;

fn get_d(xp: &[U256], a: U256) -> Option<U256> {
    if a.is_zero() || xp.len() < 2 || xp.iter().any(U256::is_zero) {
        return None;
    }

    let n = U256::from(xp.len());
    let s: U256 = xp.iter().copied().sum();
    if s.is_zero() {
        return Some(U256::ZERO);
    }

    let ann = a * n;
    if ann <= A_PRECISION {
        return None;
    }

    let mut d = s;
    let ann_s = (ann * s) / A_PRECISION;
    let ann_minus_p = ann - A_PRECISION;
    let n_plus_1 = n + U256::from(1);

    for _ in 0..MAX_ITERATIONS {
        let mut d_p = d;
        for x in xp {
            let xn = *x * n;
            if xn.is_zero() {
                return None;
            }
            d_p = (d_p * d) / xn;
        }
        let d_prev = d;
        let denominator = ((ann_minus_p * d) / A_PRECISION).saturating_add(n_plus_1 * d_p);
        if denominator.is_zero() {
            return None;
        }
        d = ((ann_s + d_p * n) * d) / denominator;
        let diff = if d > d_prev { d - d_prev } else { d_prev - d };
        if diff <= U256::from(1) {
            return Some(d);
        }
    }
    Some(d)
}

fn get_y(x: U256, i: usize, j: usize, xp: &[U256], a: U256, d: U256) -> Option<U256> {
    let n = U256::from(xp.len());
    let ann = a * n;
    if ann.is_zero() || d.is_zero() {
        return None;
    }

    let mut s_ = U256::ZERO;
    let mut c = d;
    for (k, xk) in xp.iter().enumerate() {
        let val = if k == i {
            x
        } else if k == j {
            continue;
        } else {
            *xk
        };
        if k != j {
            s_ += val;
            let vn = val * n;
            if vn.is_zero() {
                return None;
            }
            c = (c * d) / vn;
        }
    }

    c = (c * d * A_PRECISION) / (ann * n);
    let b = s_ + (d * A_PRECISION) / ann;

    let mut y = d;
    for _ in 0..MAX_ITERATIONS {
        let y_prev = y;
        let denominator = (U256::from(2) * y + b).saturating_sub(d);
        if denominator.is_zero() {
            return None;
        }
        y = (y * y + c) / denominator;
        let diff = if y > y_prev { y - y_prev } else { y_prev - y };
        if diff <= U256::from(1) {
            return Some(y);
        }
    }
    Some(y)
}

fn to_xp(balances: &[U256], rates: &[U256]) -> Vec<U256> {
    balances
        .iter()
        .zip(rates.iter())
        .map(|(b, r)| (*b * *r) / ONE)
        .collect()
}

pub fn get_curve_stable_amount_out(
    state: &CurvePoolState,
    amount_in: U256,
    token_in_idx: usize,
    token_out_idx: usize,
) -> U256 {
    if amount_in.is_zero()
        || state.a.is_zero()
        || token_in_idx == token_out_idx
        || token_in_idx >= state.balances.len()
        || token_out_idx >= state.balances.len()
    {
        return U256::ZERO;
    }

    let rates: Vec<U256> = if state.rates.is_empty() {
        vec![ONE; state.balances.len()]
    } else {
        state.rates.clone()
    };

    if rates.len() != state.balances.len() || state.fee >= CURVE_FEE_DENOMINATOR {
        return U256::ZERO;
    }

    let xp = to_xp(&state.balances, &rates);
    if xp.iter().any(U256::is_zero) {
        return U256::ZERO;
    }

    let d = match get_d(&xp, state.a) {
        Some(v) if !v.is_zero() => v,
        _ => return U256::ZERO,
    };

    let x = xp[token_in_idx] + (amount_in * rates[token_in_idx]) / ONE;
    let Some(y) = get_y(x, token_in_idx, token_out_idx, &xp, state.a, d) else {
        return U256::ZERO;
    };

    let dy = xp[token_out_idx] - y - U256::from(1);
    if dy.is_zero() {
        return U256::ZERO;
    }

    let fee_amount = (dy * state.fee) / CURVE_FEE_DENOMINATOR;
    let dy_after_fee = dy - fee_amount;
    if dy_after_fee.is_zero() {
        return U256::ZERO;
    }

    let out_rate = rates[token_out_idx];
    if out_rate.is_zero() {
        return U256::ZERO;
    }

    (dy_after_fee * ONE) / out_rate
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_swap_zero_a_returns_zero() {
        let e18 = ONE;
        let state = CurvePoolState {
            balances: vec![
                U256::from(1_000_000u128) * e18,
                U256::from(1_000_000u128) * e18,
            ],
            a: U256::ZERO,
            fee: U256::from(4_000_000u64),
            rates: vec![e18, e18],
            n_coins: 2,
            gamma: None,
            d: None,
        };
        assert_eq!(get_curve_stable_amount_out(&state, e18, 0, 1), U256::ZERO);
    }

    #[test]
    fn stable_swap_produces_output() {
        let e18 = ONE;
        let state = CurvePoolState {
            balances: vec![
                U256::from(1_000_000u128) * e18,
                U256::from(1_000_000u128) * e18,
            ],
            a: U256::from(100u128),
            fee: U256::from(4_000_000u64),
            rates: vec![e18, e18],
            n_coins: 2,
            gamma: None,
            d: None,
        };
        let out = get_curve_stable_amount_out(&state, e18, 0, 1);
        assert!(out > U256::ZERO);
        assert!(out < e18);
    }
}
