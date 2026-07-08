use alloy::primitives::U256;
use smallvec::SmallVec;

use crate::core::constants::MAX_POOL_TOKENS;
use crate::core::types::CurvePoolState;

use super::fixed_point::ONE;
pub(crate) const CURVE_FEE_DENOMINATOR: U256 = U256::from_limbs([10_000_000_000, 0, 0, 0]);
const A_PRECISION: U256 = U256::from_limbs([100, 0, 0, 0]);
const ONE_U256: U256 = U256::from_limbs([1, 0, 0, 0]);
const TWO_U256: U256 = U256::from_limbs([2, 0, 0, 0]);
const MAX_ITERATIONS: u32 = 128;
/// Safety buffer applied to Curve output (0x186a0 = 100_000 = 0.001% of fee precision).
/// Accounts for state drift between multicall read and eth_call execution.
/// Curve pools are susceptible to frontrunning that shifts reserves by 1-2 wei,
/// causing the "fewer coins than expected" revert.
pub(crate) const CURVE_OUTPUT_BUFFER: U256 = U256::from_limbs([0x186a0, 0, 0, 0]);
type CurveXp = SmallVec<[U256; MAX_POOL_TOKENS]>;

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
        let denominator =
            (ann_minus_p.saturating_mul(d) / A_PRECISION).saturating_add(n_plus_1 * d_p);
        if denominator.is_zero() {
            return None;
        }
        d = ((ann_s + d_p * n) * d) / denominator;
        let diff = if d > d_prev { d - d_prev } else { d_prev - d };
        if diff <= ONE_U256 {
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
        if k == j {
            continue;
        }
        let val = if k == i { x } else { *xk };
        s_ += val;
        let vn = val * n;
        if vn.is_zero() {
            return None;
        }
        c = (c * d) / vn;
    }

    c = (c * d * A_PRECISION) / (ann * n);
    let b = s_ + (d * A_PRECISION) / ann;

    let mut y = d;
    for _ in 0..MAX_ITERATIONS {
        let y_prev = y;
        let denominator = (TWO_U256 * y + b).saturating_sub(d);
        if denominator.is_zero() {
            return None;
        }
        y = (y * y + c) / denominator;
        let diff = if y > y_prev { y - y_prev } else { y_prev - y };
        if diff <= ONE_U256 {
            return Some(y);
        }
    }
    Some(y)
}

fn to_xp(balances: &[U256], rates: &[U256]) -> Option<CurveXp> {
    if !rates.is_empty() && rates.len() != balances.len() {
        return None;
    }

    let mut xp = CurveXp::with_capacity(balances.len());
    if rates.is_empty() {
        xp.extend_from_slice(balances);
    } else {
        xp.extend(
            balances
                .iter()
                .zip(rates.iter())
                .map(|(balance, rate)| (*balance * *rate) / ONE),
        );
    }
    Some(xp)
}

#[inline]
#[must_use]
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

    if state.fee >= CURVE_FEE_DENOMINATOR {
        return U256::ZERO;
    }

    let Some(xp) = to_xp(&state.balances, &state.rates) else {
        return U256::ZERO;
    };

    let d = match get_d(&xp, state.a) {
        Some(v) if !v.is_zero() => v,
        _ => return U256::ZERO,
    };

    let in_rate = state.rates.get(token_in_idx).copied().unwrap_or(ONE);
    let x = xp[token_in_idx] + (amount_in * in_rate) / ONE;
    let Some(y) = get_y(x, token_in_idx, token_out_idx, &xp, state.a, d) else {
        return U256::ZERO;
    };

    let dy = xp[token_out_idx] - y - ONE_U256;
    if dy.is_zero() {
        return U256::ZERO;
    }

    let fee_amount = (dy * state.fee) / CURVE_FEE_DENOMINATOR;
    let dy_after_fee = dy.saturating_sub(fee_amount);
    // Apply output buffer to guard against state drift between simulation and execution.
    let dy_buffered =
        dy_after_fee.saturating_sub((dy_after_fee * CURVE_OUTPUT_BUFFER) / CURVE_FEE_DENOMINATOR);
    if dy_buffered.is_zero() {
        return U256::ZERO;
    }

    let out_rate = state.rates.get(token_out_idx).copied().unwrap_or(ONE);
    if out_rate.is_zero() {
        return U256::ZERO;
    }

    (dy_buffered * ONE) / out_rate
}

#[cfg(test)]
mod tests {
    use super::*;

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
            get_curve_stable_amount_out(&state, U256::ZERO, 0, 1),
            U256::ZERO
        );
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
        fn output_bounded_by_reserve(
            amount_in in non_zero(),
            a in (1u64..1_000_000u64).prop_map(U256::from),
            balance0 in non_zero(),
            balance1 in non_zero(),
        ) {
            let state = CurvePoolState {
                balances: vec![balance0, balance1],
                a,
                fee: U256::ZERO,
                rates: vec![],
                n_coins: 2,
                gamma: None,
                d: None,
            };

            let out = get_curve_stable_amount_out(&state, amount_in, 0, 1);
            if !out.is_zero() {
                prop_assert!(out <= state.balances[1],
                    "out={} exceeds balance={}", out, state.balances[1]);
            }
        }

        #[test]
        fn identical_tokens_return_zero(
            amount_in in non_zero(),
            a in (1u64..1_000_000u64).prop_map(U256::from),
            balance in non_zero(),
        ) {
            let state = CurvePoolState {
                balances: vec![balance, balance],
                a,
                fee: U256::ZERO,
                rates: vec![],
                n_coins: 2,
                gamma: None,
                d: None,
            };
            let out = get_curve_stable_amount_out(&state, amount_in, 0, 0);
            prop_assert!(out.is_zero());
        }
    }
}
