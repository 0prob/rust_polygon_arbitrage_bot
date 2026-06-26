use ruint::aliases::U256;

use crate::core::types::{BalancerPoolKind, BalancerPoolState};

use super::fixed_point::{ONE, complement, pow_down};
use super::full_math::div_rounding_up_or_zero;

const MAX_IN_RATIO: U256 = U256::from_limbs([3_000_000_000_000_000_000, 0, 0, 0]); // 0.3 * 1e18
const DEFAULT_AMP_PRECISION: U256 = U256::from_limbs([1000, 0, 0, 0]);
const MAX_ITERATIONS: u32 = 50;

pub fn balancer_swap_fee_from_pool_meta_fee(fee: u64) -> U256 {
    let raw = U256::from(fee);
    if raw < U256::from(10_000) {
        raw * U256::from_limbs([100_000_000_000_000, 0, 0, 0]) // 1e14
    } else {
        raw
    }
}

fn resolve_swap_fee(fee: U256, meta_fee: Option<u64>) -> U256 {
    if !fee.is_zero() && fee < ONE {
        return fee;
    }
    meta_fee
        .map(balancer_swap_fee_from_pool_meta_fee)
        .unwrap_or(U256::ZERO)
}

fn abs_diff(a: U256, b: U256) -> U256 {
    if a >= b { a - b } else { b - a }
}

pub fn get_balancer_weighted_amount_out(
    state: &BalancerPoolState,
    amount_in: U256,
    in_idx: usize,
    out_idx: usize,
) -> U256 {
    if amount_in.is_zero() || in_idx == out_idx {
        return U256::ZERO;
    }
    if in_idx >= state.balances.len()
        || out_idx >= state.balances.len()
        || in_idx >= state.weights.len()
        || out_idx >= state.weights.len()
    {
        return U256::ZERO;
    }

    let bal_in = state.balances[in_idx];
    let bal_out = state.balances[out_idx];
    let w_in = state.weights[in_idx];
    let w_out = state.weights[out_idx];
    let fee = resolve_swap_fee(state.fee, None);

    if bal_in.is_zero() || bal_out.is_zero() || w_in.is_zero() || w_out.is_zero() {
        return U256::ZERO;
    }
    if fee >= ONE || amount_in > (bal_in * MAX_IN_RATIO) / ONE {
        return U256::ZERO;
    }

    let fee_complement = complement(fee);
    if fee_complement.is_zero() {
        return U256::ZERO;
    }
    let amount_in_after_fee = (amount_in * fee_complement) / ONE;
    let denominator = bal_in + amount_in_after_fee;
    if denominator.is_zero() {
        return U256::ZERO;
    }

    let base = (bal_in * ONE) / denominator;
    if base.is_zero() || base > ONE {
        return U256::ZERO;
    }

    let exponent = (w_in * ONE) / w_out;
    if exponent.is_zero() {
        return U256::ZERO;
    }

    let power = pow_down(base, exponent);
    if power > ONE {
        return U256::ZERO;
    }

    let amount_out = (bal_out * (ONE - power)) / ONE;
    if amount_out.is_zero() {
        U256::ZERO
    } else {
        amount_out
    }
}

pub fn calculate_balancer_stable_invariant(
    amp: U256,
    balances: &[U256],
    amp_precision: U256,
) -> U256 {
    if balances.len() < 2 || amp.is_zero() || amp_precision.is_zero() {
        return U256::ZERO;
    }

    let num_tokens = U256::from(balances.len());
    let mut sum = U256::ZERO;
    for b in balances {
        if b.is_zero() {
            return U256::ZERO;
        }
        sum += *b;
    }
    if sum.is_zero() {
        return U256::ZERO;
    }

    let mut invariant = sum;
    let amp_times_total = amp * num_tokens;
    if amp_times_total <= amp_precision {
        return U256::ZERO;
    }

    for _ in 0..MAX_ITERATIONS {
        let mut d_p = invariant;
        for b in balances {
            d_p = (d_p * invariant) / (*b * num_tokens);
        }

        let prev = invariant;
        let numerator = ((amp_times_total * sum) / amp_precision + d_p * num_tokens) * invariant;
        let denominator = ((amp_times_total - amp_precision) * invariant) / amp_precision
            + (num_tokens + U256::from(1)) * d_p;
        if denominator.is_zero() {
            return U256::ZERO;
        }
        invariant = numerator / denominator;
        if abs_diff(invariant, prev) <= U256::from(1) {
            return invariant;
        }
    }

    invariant
}

fn token_balance_given_invariant(
    amp: U256,
    balances: &[U256],
    invariant: U256,
    token_index: usize,
    amp_precision: U256,
) -> U256 {
    let num_tokens = U256::from(balances.len());
    let amp_times_total = amp * num_tokens;
    if amp_times_total.is_zero() || invariant.is_zero() {
        return U256::ZERO;
    }

    let mut sum = balances[0];
    let mut p_d = balances[0] * num_tokens;
    for &balance in &balances[1..] {
        if balance.is_zero() {
            return U256::ZERO;
        }
        p_d = (p_d * balance * num_tokens) / invariant;
        sum += balance;
    }

    let indexed = balances[token_index];
    if indexed.is_zero() || p_d.is_zero() {
        return U256::ZERO;
    }
    sum -= indexed;

    let inv2 = invariant * invariant;
    let c = div_rounding_up_or_zero(inv2 * amp_precision * indexed, amp_times_total * p_d);
    let b = sum + (invariant * amp_precision) / amp_times_total;

    let mut token_balance = div_rounding_up_or_zero(inv2 + c, invariant + b);
    for _ in 0..MAX_ITERATIONS {
        let prev = token_balance;
        let denominator = U256::from(2) * token_balance + b - invariant;
        if denominator.is_zero() {
            return U256::ZERO;
        }
        token_balance = div_rounding_up_or_zero(token_balance * token_balance + c, denominator);
        if abs_diff(token_balance, prev) <= U256::from(1) {
            return token_balance;
        }
    }
    token_balance
}

pub fn get_balancer_stable_amount_out(
    state: &BalancerPoolState,
    amount_in: U256,
    in_idx: usize,
    out_idx: usize,
) -> U256 {
    if amount_in.is_zero() || in_idx == out_idx {
        return U256::ZERO;
    }
    if in_idx >= state.balances.len() || out_idx >= state.balances.len() {
        return U256::ZERO;
    }

    let scaling = &state.scaling_factors;
    if scaling.len() != state.balances.len() {
        return U256::ZERO;
    }

    let scaled_balances: Vec<U256> = state
        .balances
        .iter()
        .enumerate()
        .map(|(i, b)| (*b * scaling[i]) / ONE)
        .collect();

    if scaled_balances.iter().any(U256::is_zero) {
        return U256::ZERO;
    }

    let fee = resolve_swap_fee(state.fee, None);
    if fee >= ONE || scaling[in_idx].is_zero() || scaling[out_idx].is_zero() {
        return U256::ZERO;
    }

    let amount_in_after_fee = (amount_in * complement(fee)) / ONE;
    let scaled_amount_in = (amount_in_after_fee * scaling[in_idx]) / ONE;
    if scaled_amount_in.is_zero() {
        return U256::ZERO;
    }

    let amp_precision = if state.amp_precision.is_zero() {
        DEFAULT_AMP_PRECISION
    } else {
        state.amp_precision
    };

    let invariant = calculate_balancer_stable_invariant(state.amp, &scaled_balances, amp_precision);
    if invariant.is_zero() {
        return U256::ZERO;
    }

    let mut xp = scaled_balances.clone();
    xp[in_idx] += scaled_amount_in;
    let final_balance_out =
        token_balance_given_invariant(state.amp, &xp, invariant, out_idx, amp_precision);
    let original_out = scaled_balances[out_idx];
    if final_balance_out.is_zero() || final_balance_out >= original_out {
        return U256::ZERO;
    }

    let scaled_amount_out = original_out - final_balance_out - U256::from(1);
    if scaled_amount_out.is_zero() {
        return U256::ZERO;
    }
    let amount_out = (scaled_amount_out * ONE) / scaling[out_idx];
    if amount_out.is_zero() {
        U256::ZERO
    } else {
        amount_out
    }
}

pub fn simulate_balancer_swap(
    state: &BalancerPoolState,
    amount_in: U256,
    in_idx: usize,
    out_idx: usize,
) -> U256 {
    match state.pool_type {
        BalancerPoolKind::Weighted => {
            get_balancer_weighted_amount_out(state, amount_in, in_idx, out_idx)
        }
        BalancerPoolKind::Stable => {
            get_balancer_stable_amount_out(state, amount_in, in_idx, out_idx)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn swap_fee_from_meta() {
        assert_eq!(
            balancer_swap_fee_from_pool_meta_fee(30),
            U256::from(30u128) * U256::from_limbs([100_000_000_000_000, 0, 0, 0])
        );
    }

    #[test]
    fn weighted_50_50_zero_fee() {
        let bal = U256::from(100u128) * ONE;
        let amount_in = ONE;
        let state = BalancerPoolState {
            pool_id: None,
            balances: vec![bal, bal],
            weights: vec![ONE / U256::from(2), ONE / U256::from(2)],
            scaling_factors: vec![],
            amp: U256::ZERO,
            amp_precision: U256::ZERO,
            fee: U256::ZERO,
            pool_type: BalancerPoolKind::Weighted,
            bpt_index: None,
        };
        let out = get_balancer_weighted_amount_out(&state, amount_in, 0, 1);
        let expected = (bal * ONE) / (bal + amount_in);
        assert!(out >= expected - U256::from(1) && out <= expected + U256::from(1));
    }

    #[test]
    fn stable_invariant_positive() {
        let bal = U256::from(100u128) * ONE;
        let inv =
            calculate_balancer_stable_invariant(U256::from(100_000), &[bal, bal], U256::from(1000));
        assert!(inv > U256::ZERO);
    }
}
