use alloy::primitives::U256;
use smallvec::SmallVec;

use crate::core::constants::MAX_POOL_TOKENS;
use crate::core::types::{BalancerPoolKind, BalancerPoolState};

use super::div_rounding_up_or_zero;
use super::fixed_point::{ONE, complement, mul_down, pow_down};

type BalXp = SmallVec<[U256; MAX_POOL_TOKENS]>;

// Balancer WeightedMath: 30% of token-in balance (0.3e18 fixed-point).
const MAX_IN_RATIO: U256 = U256::from_limbs([300_000_000_000_000_000, 0, 0, 0]);

/// Vault rejects swaps above 30% of token-in balance (`BAL#304` / `MAX_IN_RATIO`).
#[inline]
#[must_use]
pub fn exceeds_balancer_max_in_ratio(amount_in: U256, balance_in: U256) -> bool {
    balance_in.is_zero()
        || balance_in
            .checked_mul(MAX_IN_RATIO)
            .is_none_or(|limit| amount_in > limit / ONE)
}
const DEFAULT_AMP_PRECISION: U256 = U256::from_limbs([1000, 0, 0, 0]);
const MAX_ITERATIONS: u32 = 64;

#[must_use]
pub fn balancer_swap_fee_from_pool_meta_fee(fee: u64) -> U256 {
    let raw = U256::from(fee);
    if raw < U256::from(10_000) {
        raw * U256::from(100_000_000_000_000u64)
    } else {
        raw
    }
}

fn resolve_swap_fee(fee: U256) -> U256 {
    if !fee.is_zero() && fee < ONE {
        fee
    } else {
        U256::ZERO
    }
}

#[must_use]
pub fn get_balancer_weighted_amount_out(
    state: &BalancerPoolState,
    amount_in: U256,
    in_idx: usize,
    out_idx: usize,
) -> U256 {
    if in_idx >= state.weights.len() || out_idx >= state.weights.len() {
        return U256::ZERO;
    }
    let scaling = &state.scaling_factors;
    if scaling.len() != state.balances.len()
        || scaling[in_idx].is_zero()
        || scaling[out_idx].is_zero()
    {
        return U256::ZERO;
    }

    let bal_in = state.balances[in_idx];
    let bal_out = state.balances[out_idx];
    let w_in = state.weights[in_idx];
    let w_out = state.weights[out_idx];
    let fee = resolve_swap_fee(state.fee);

    if bal_in.is_zero() || bal_out.is_zero() || w_in.is_zero() || w_out.is_zero() {
        return U256::ZERO;
    }
    if fee >= ONE || exceeds_balancer_max_in_ratio(amount_in, bal_in) {
        return U256::ZERO;
    }

    let fee_complement = complement(fee);
    if fee_complement.is_zero() {
        return U256::ZERO;
    }
    let Some(amount_in_after_fee) = amount_in.checked_mul(fee_complement).map(|v| v / ONE) else {
        return U256::ZERO;
    };
    let Some(scaled_amount_in) = amount_in_after_fee
        .checked_mul(scaling[in_idx])
        .map(|v| v / ONE)
    else {
        return U256::ZERO;
    };
    if scaled_amount_in.is_zero() {
        return U256::ZERO;
    }

    let Some(scaled_bal_in) = bal_in.checked_mul(scaling[in_idx]).map(|v| v / ONE) else {
        return U256::ZERO;
    };
    let Some(scaled_bal_out) = bal_out.checked_mul(scaling[out_idx]).map(|v| v / ONE) else {
        return U256::ZERO;
    };
    let denominator = scaled_bal_in + scaled_amount_in;
    if denominator.is_zero() {
        return U256::ZERO;
    }

    let base = (scaled_bal_in * ONE) / denominator;
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

    let Some(scaled_amount_out) = scaled_bal_out.checked_mul(ONE - power).map(|v| v / ONE) else {
        return U256::ZERO;
    };
    if scaled_amount_out.is_zero() {
        return U256::ZERO;
    }
    let amount_out = (scaled_amount_out * ONE) / scaling[out_idx];
    if amount_out.is_zero() || amount_out > bal_out {
        U256::ZERO
    } else {
        amount_out
    }
}

#[must_use]
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
        if invariant > sum {
            return sum;
        }
        if invariant.abs_diff(prev) <= U256::from(1) {
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
        if token_balance.abs_diff(prev) <= U256::from(1) {
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
    let scaling = &state.scaling_factors;
    if scaling.len() != state.balances.len() {
        return U256::ZERO;
    }

    if state.bpt_index == Some(in_idx) || state.bpt_index == Some(out_idx) {
        return U256::ZERO;
    }

    let n = state.balances.len();
    if in_idx >= n || out_idx >= n {
        return U256::ZERO;
    }
    let mut stable_in_idx = None;
    let mut stable_out_idx = None;
    let mut scaled_balances: BalXp = SmallVec::with_capacity(n);
    for (i, balance) in state.balances.iter().enumerate() {
        if state.bpt_index == Some(i) {
            continue;
        }
        let stable_idx = scaled_balances.len();
        if i == in_idx {
            stable_in_idx = Some(stable_idx);
        }
        if i == out_idx {
            stable_out_idx = Some(stable_idx);
        }
        scaled_balances.push((*balance * scaling[i]) / ONE);
    }
    let Some(stable_in_idx) = stable_in_idx else {
        return U256::ZERO;
    };
    let Some(stable_out_idx) = stable_out_idx else {
        return U256::ZERO;
    };

    if scaled_balances.iter().any(U256::is_zero) {
        return U256::ZERO;
    }

    let fee = resolve_swap_fee(state.fee);
    if fee >= ONE || scaling[in_idx].is_zero() || scaling[out_idx].is_zero() {
        return U256::ZERO;
    }
    if exceeds_balancer_max_in_ratio(amount_in, state.balances[in_idx]) {
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
    xp[stable_in_idx] += scaled_amount_in;
    let final_balance_out =
        token_balance_given_invariant(state.amp, &xp, invariant, stable_out_idx, amp_precision);
    let original_out = scaled_balances[stable_out_idx];
    if final_balance_out.is_zero() || final_balance_out >= original_out {
        return U256::ZERO;
    }

    let scaled_amount_out = original_out - final_balance_out - U256::from(1);
    if scaled_amount_out.is_zero() {
        return U256::ZERO;
    }
    let amount_out = (scaled_amount_out * ONE) / scaling[out_idx];
    let bal_out = state.balances[out_idx];
    if amount_out.is_zero() || amount_out > bal_out {
        U256::ZERO
    } else {
        amount_out
    }
}

fn linear_to_nominal(real: U256, fee: U256, lower: U256, upper: U256) -> Option<U256> {
    if real < lower {
        real.checked_sub(mul_down(lower - real, fee))
    } else if real <= upper {
        Some(real)
    } else {
        real.checked_sub(mul_down(real - upper, fee))
    }
}

fn linear_from_nominal(nominal: U256, fee: U256, lower: U256, upper: U256) -> Option<U256> {
    if nominal < lower {
        let denominator = ONE.checked_add(fee)?;
        (nominal.checked_add(mul_down(fee, lower))? * ONE).checked_div(denominator)
    } else if nominal <= upper {
        Some(nominal)
    } else {
        let denominator = ONE.checked_sub(fee)?;
        (nominal.checked_sub(mul_down(fee, upper))? * ONE).checked_div(denominator)
    }
}

#[must_use]
pub fn get_balancer_linear_amount_out(
    state: &BalancerPoolState,
    amount_in: U256,
    in_idx: usize,
    out_idx: usize,
) -> U256 {
    let Some(linear) = state.linear.as_ref() else {
        return U256::ZERO;
    };
    if state.scaling_factors.len() != state.balances.len()
        || state.scaling_factors[in_idx].is_zero()
        || state.scaling_factors[out_idx].is_zero()
        || linear.wrapped_rate.is_zero()
    {
        return U256::ZERO;
    }
    if exceeds_balancer_max_in_ratio(amount_in, state.balances[in_idx]) {
        return U256::ZERO;
    }
    let scaled_in = amount_in * state.scaling_factors[in_idx] / ONE;
    let main_balance =
        state.balances[linear.main_index] * state.scaling_factors[linear.main_index] / ONE;
    let fee = resolve_swap_fee(state.fee);
    let scaled_out = if in_idx == linear.main_index && out_idx == linear.wrapped_index {
        let Some(before) =
            linear_to_nominal(main_balance, fee, linear.lower_target, linear.upper_target)
        else {
            return U256::ZERO;
        };
        let Some(after_balance) = main_balance.checked_add(scaled_in) else {
            return U256::ZERO;
        };
        let Some(after) =
            linear_to_nominal(after_balance, fee, linear.lower_target, linear.upper_target)
        else {
            return U256::ZERO;
        };
        (after - before) * ONE / linear.wrapped_rate
    } else if in_idx == linear.wrapped_index && out_idx == linear.main_index {
        let Some(before) =
            linear_to_nominal(main_balance, fee, linear.lower_target, linear.upper_target)
        else {
            return U256::ZERO;
        };
        let delta = mul_down(scaled_in, linear.wrapped_rate);
        let Some(after_nominal) = before.checked_sub(delta) else {
            return U256::ZERO;
        };
        let Some(after_balance) =
            linear_from_nominal(after_nominal, fee, linear.lower_target, linear.upper_target)
        else {
            return U256::ZERO;
        };
        let Some(out) = main_balance.checked_sub(after_balance) else {
            return U256::ZERO;
        };
        out
    } else {
        return U256::ZERO;
    };
    let amount_out = scaled_out * ONE / state.scaling_factors[out_idx];
    if amount_out > state.balances[out_idx] {
        U256::ZERO
    } else {
        amount_out
    }
}

#[inline]
#[must_use]
pub fn simulate_balancer_swap(
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
    match state.pool_type {
        BalancerPoolKind::Weighted => {
            get_balancer_weighted_amount_out(state, amount_in, in_idx, out_idx)
        }
        BalancerPoolKind::Stable => {
            get_balancer_stable_amount_out(state, amount_in, in_idx, out_idx)
        }
        BalancerPoolKind::Linear => {
            get_balancer_linear_amount_out(state, amount_in, in_idx, out_idx)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::BalancerLinearState;

    fn linear_state(main_balance: U256, fee: U256, rate: U256) -> BalancerPoolState {
        BalancerPoolState {
            pool_id: None,
            tokens: vec![],
            balances: vec![main_balance, U256::from(100) * ONE, U256::MAX],
            weights: vec![],
            scaling_factors: vec![ONE, ONE, ONE],
            amp: U256::ZERO,
            amp_precision: U256::ZERO,
            fee,
            pool_type: BalancerPoolKind::Linear,
            linear: Some(BalancerLinearState {
                main_index: 0,
                wrapped_index: 1,
                lower_target: U256::from(50) * ONE,
                upper_target: U256::from(150) * ONE,
                wrapped_rate: rate,
            }),
            bpt_index: Some(2),
            is_updating: false,
            last_change_block: 0,
        }
    }

    #[test]
    fn test_zero_amount_returns_zero() {
        let state = BalancerPoolState {
            pool_id: None,
            tokens: vec![],
            balances: vec![],
            weights: vec![],
            scaling_factors: vec![],
            amp: U256::ZERO,
            amp_precision: U256::ZERO,
            fee: U256::ZERO,
            pool_type: BalancerPoolKind::Weighted,
            linear: None,
            bpt_index: None,
            is_updating: false,
            last_change_block: 0,
        };
        assert_eq!(simulate_balancer_swap(&state, U256::ZERO, 0, 1), U256::ZERO);
    }

    #[test]
    fn weighted_quote_does_not_drain_live_pool_balance() {
        // Given: the live weighted-pool state that produced a phantom cycle.
        let state = BalancerPoolState {
            pool_id: None,
            tokens: vec![],
            balances: vec![
                U256::from(5_470_183_738_152_410u64),
                U256::ZERO,
                U256::from(3_527_960_702_014_628u64),
            ],
            weights: vec![
                U256::from(333_400_000_000_000_000u64),
                U256::from(333_300_000_000_000_000u64),
                U256::from(333_300_000_000_000_000u64),
            ],
            scaling_factors: vec![ONE, ONE * U256::from(1_000_000_000_000u64), ONE],
            amp: U256::ZERO,
            amp_precision: U256::ZERO,
            fee: U256::from(3_000_000_000_000_000u64),
            pool_type: BalancerPoolKind::Weighted,
            linear: None,
            bpt_index: None,
            is_updating: false,
            last_change_block: 0,
        };

        // When: the observed input is quoted from token 0 to token 2.
        let amount_out = simulate_balancer_swap(&state, U256::from(1_000_000_000_000_000u64), 0, 2);
        // Then: the quote must remain strictly below the pool's output balance.
        assert!(amount_out < state.balances[2]);
    }

    #[test]
    fn linear_main_wrapped_quotes_match_reference_formulas_in_target_band() {
        let state = linear_state(U256::from(100) * ONE, U256::ZERO, U256::from(2) * ONE);

        assert_eq!(
            simulate_balancer_swap(&state, U256::from(10) * ONE, 0, 1),
            U256::from(5) * ONE
        );
        assert_eq!(
            simulate_balancer_swap(&state, U256::from(5) * ONE, 1, 0),
            U256::from(10) * ONE
        );
    }

    #[test]
    fn linear_main_in_charges_fee_above_upper_target() {
        let state = linear_state(
            U256::from(200) * ONE,
            ONE / U256::from(10),
            U256::from(2) * ONE,
        );

        assert_eq!(
            simulate_balancer_swap(&state, U256::from(10) * ONE, 0, 1),
            U256::from(45) * (ONE / U256::from(10))
        );
    }

    #[test]
    fn linear_quotes_apply_token_scaling_and_reject_bpt_pairs() {
        let mut state = linear_state(U256::from(100_000_000), U256::ZERO, U256::from(2) * ONE);
        // Balancer scaling factors are fixed point: 1e18 * 10^(18 - decimals).
        state.scaling_factors[0] = ONE * U256::from(1_000_000_000_000u64);

        assert_eq!(
            simulate_balancer_swap(&state, U256::from(1_000_000), 0, 1),
            ONE / U256::from(2)
        );
        assert_eq!(simulate_balancer_swap(&state, ONE, 0, 2), U256::ZERO);
    }

    #[test]
    fn composable_stable_quote_ignores_bpt_balance() {
        let state = BalancerPoolState {
            pool_id: None,
            tokens: vec![],
            balances: vec![
                U256::from(5) * ONE,
                U256::from(3) * ONE,
                U256::from(7) * ONE,
            ],
            weights: vec![],
            scaling_factors: vec![ONE, ONE, ONE],
            amp: U256::from(5_000u64),
            amp_precision: U256::from(1_000u64),
            fee: U256::ZERO,
            pool_type: BalancerPoolKind::Stable,
            linear: None,
            bpt_index: Some(2),
            is_updating: false,
            last_change_block: 0,
        };
        let mut huge_bpt = state.clone();
        huge_bpt.balances[2] = U256::from(2_596u64) * U256::from(10u64).pow(U256::from(30u64));

        assert_eq!(
            get_balancer_stable_amount_out(&state, ONE / U256::from(10u64), 0, 1),
            get_balancer_stable_amount_out(&huge_bpt, ONE / U256::from(10u64), 0, 1),
        );
    }
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used)]
mod proptests {
    use super::*;
    use crate::core::math::fixed_point::{MAX_POW_RELATIVE_ERROR, mul_up};
    use proptest::prelude::*;

    fn u128_nonzero() -> impl Strategy<Value = U256> {
        (1u128..=u128::MAX).prop_map(U256::from)
    }

    fn weight() -> impl Strategy<Value = U256> {
        (1u128..=1_000_000_000_000_000_000u128).prop_map(U256::from)
    }

    proptest! {
        #[test]
        fn weighted_output_bounded(
            amount_in in u128_nonzero(),
            bal_in in u128_nonzero(),
            bal_out in u128_nonzero(),
            w_in in weight(),
            w_out in weight(),
        ) {
            let w_in = w_in % ONE;
            let w_out = w_out % ONE;
            if w_in.is_zero() || w_out.is_zero() { return Ok(()); }

            let state = BalancerPoolState {
                pool_id: None,
                tokens: vec![],
                balances: vec![bal_in, bal_out],
                weights: vec![w_in, w_out],
                scaling_factors: vec![ONE, ONE],
                amp: U256::ZERO,
                amp_precision: U256::ZERO,
                fee: U256::ZERO,
                pool_type: BalancerPoolKind::Weighted,
                linear: None,
                bpt_index: None,
                is_updating: false,
                last_change_block: 0,
            };
            let out = get_balancer_weighted_amount_out(&state, amount_in, 0, 1);
            if !out.is_zero() {
                prop_assert!(out <= state.balances[1],
                    "weighted out={} exceeds balance={}", out, state.balances[1]);
            }
        }

        #[test]
        fn pow_down_identity(
            x in (1u128..=u128::MAX).prop_map(U256::from),
        ) {
            let x = x % ONE;
            if x.is_zero() { return Ok(()); }
            let result = pow_down(x, ONE);
            let max_error = mul_up(result, MAX_POW_RELATIVE_ERROR) + U256::from(1);
            let diff = if x > result { x - result } else { result - x };
            prop_assert!(diff <= max_error,
                "pow_down(x, 1) should be ≈ x: x={}, got={}", x, result);
        }

        #[test]
        fn stable_invariant_sane(
            amp in (1u64..100_000u64).prop_map(U256::from),
            bal0 in (1000u128..=u128::MAX).prop_map(U256::from),
            bal1 in (1000u128..=u128::MAX).prop_map(U256::from),
        ) {
            let balances = vec![bal0, bal1, U256::from(100) * ONE];
            let inv = calculate_balancer_stable_invariant(amp, &balances, U256::from(1000));
            if !inv.is_zero() {
                let sum = bal0 + bal1 + U256::from(100) * ONE;
                prop_assert!(inv <= sum,
                    "invariant {} exceeds sum {}", inv, sum);
            }
        }
    }
}
