use alloy::primitives::U256;

use crate::core::types::{DodoPoolState, DodoRState};

use alloy::primitives::U512;

use super::fixed_point::{ONE, ONE_U512, mul_down as mul_floor};
use crate::util::{u512_to_u256, u512_to_u256_checked};

// Exact 1e36 — DODO on-chain `_K_PRECISION`. Previous constant was an approximation.
const ONE2: U256 = {
    let bytes = 1_000_000_000_000_000_000_000_000_000_000_000_000u128;
    U256::from_limbs([bytes as u64, (bytes >> 64) as u64, 0, 0])
};

fn div_ceil(a: U256, b: U256) -> U256 {
    if b.is_zero() {
        return U256::ZERO;
    }
    let product = U512::from(a) * ONE_U512;
    let result = (product + U512::from(b) - U512::ONE) / U512::from(b);
    u512_to_u256(result)
}

fn reciprocal_floor(target: U256) -> U256 {
    if target.is_zero() {
        return U256::ZERO;
    }
    let result = U512::from(ONE2) / U512::from(target);
    u512_to_u256(result)
}

fn solve_quadratic_function_for_trade(v0: U256, v1: U256, delta: U256, i: U256, k: U256) -> U256 {
    if v0.is_zero() || v1.is_zero() || delta.is_zero() || i.is_zero() || k > ONE {
        return U256::ZERO;
    }

    if k.is_zero() {
        let linear = mul_floor(i, delta);
        return if linear > v1 { v1 } else { linear };
    }

    if k == ONE {
        let idelta = i * delta;
        let temp = if idelta.is_zero() {
            U256::ZERO
        } else {
            (idelta * v1) / (v0 * v0)
        };
        return if temp.is_zero() {
            U256::ZERO
        } else {
            (v1 * temp) / (temp + ONE)
        };
    }

    let part2 = {
        // U512 for k*v0²/v1; then checked truncation since quotient can exceed U256::MAX
        // for extreme k/v0/v1 combinations (k up to 1e18, v0 up to 1e30).
        let k_v0_v0 = U512::from(k) * U512::from(v0) * U512::from(v0) / U512::from(v1);
        let k_term = u512_to_u256_checked(k_v0_v0).unwrap_or(U256::MAX);
        k_term + mul_floor(i, delta)
    };
    let one_minus_k = ONE - k;
    let mut b_abs = one_minus_k * v1;
    let mut b_sig = false;
    if b_abs >= part2 {
        b_abs -= part2;
    } else {
        b_abs = part2 - b_abs;
        b_sig = true;
    }
    b_abs /= ONE;

    let square_root_input = mul_floor(one_minus_k * U256::from(4), mul_floor(k, v0) * v0);
    let square_root = if square_root_input.is_zero() {
        b_abs
    } else {
        (b_abs * b_abs + square_root_input).root(2)
    };

    let denominator = one_minus_k * U256::from(2);
    if denominator.is_zero() {
        return U256::ZERO;
    }

    let numerator = if b_sig {
        if square_root <= b_abs {
            return U256::ZERO;
        }
        square_root - b_abs
    } else {
        b_abs + square_root
    };

    let v2 = div_ceil(numerator, denominator);
    if v2 > v1 { U256::ZERO } else { v1 - v2 }
}

fn general_integrate(v0: U256, v1: U256, v2: U256, i: U256, k: U256) -> U256 {
    if v0.is_zero() || v1 < v2 || v2.is_zero() || i.is_zero() || k > ONE {
        return U256::ZERO;
    }
    let delta = v1 - v2;
    let fair_amount = U512::from(i) * U512::from(delta);
    if k.is_zero() {
        return u512_to_u256(fair_amount / ONE_U512);
    }
    let ratio = U512::from(ONE) * U512::from(v0) * U512::from(v0) / U512::from(v1) / U512::from(v2);
    let penalty = U512::from(k) * ratio / ONE_U512;
    let factor = U512::from(ONE - k) + penalty;
    u512_to_u256(factor * fair_amount / U512::from(ONE2))
}

#[must_use]
pub fn get_dodo_gross_amount_out(
    state: &DodoPoolState,
    amount_in: U256,
    base_to_quote: bool,
) -> U256 {
    if amount_in.is_zero() {
        return U256::ZERO;
    }

    let b = state.base_reserve;
    let q = state.quote_reserve;
    let i = state.i;
    let k = state.k;

    if i.is_zero() || k > ONE || b.is_zero() || q.is_zero() {
        return U256::ZERO;
    }

    let inverse_i = reciprocal_floor(i);
    match (state.r_state, base_to_quote) {
        (DodoRState::One, true) => solve_quadratic_function_for_trade(
            state.quote_target,
            state.quote_target,
            amount_in,
            i,
            k,
        ),
        (DodoRState::One, false) => solve_quadratic_function_for_trade(
            state.base_target,
            state.base_target,
            amount_in,
            inverse_i,
            k,
        ),
        (DodoRState::AboveOne, true) => {
            let back_in = state.base_target.saturating_sub(b);
            let back_out = q.saturating_sub(state.quote_target);
            if amount_in < back_in {
                general_integrate(state.base_target, b + amount_in, b, i, k).min(back_out)
            } else if amount_in == back_in {
                back_out
            } else {
                back_out
                    + solve_quadratic_function_for_trade(
                        state.quote_target,
                        state.quote_target,
                        amount_in - back_in,
                        i,
                        k,
                    )
            }
        }
        (DodoRState::AboveOne, false) => {
            solve_quadratic_function_for_trade(state.base_target, b, amount_in, inverse_i, k)
        }
        (DodoRState::BelowOne, true) => {
            solve_quadratic_function_for_trade(state.quote_target, q, amount_in, i, k)
        }
        (DodoRState::BelowOne, false) => {
            let back_in = state.quote_target.saturating_sub(q);
            let back_out = b.saturating_sub(state.base_target);
            if amount_in < back_in {
                general_integrate(state.quote_target, q + amount_in, q, inverse_i, k).min(back_out)
            } else if amount_in == back_in {
                back_out
            } else {
                back_out
                    + solve_quadratic_function_for_trade(
                        state.base_target,
                        state.base_target,
                        amount_in - back_in,
                        inverse_i,
                        k,
                    )
            }
        }
    }
}

#[inline]
#[must_use]
pub fn get_dodo_amount_out(state: &DodoPoolState, amount_in: U256, base_to_quote: bool) -> U256 {
    let gross = get_dodo_gross_amount_out(state, amount_in, base_to_quote);
    if gross.is_zero() {
        return U256::ZERO;
    }

    let lp = state.lp_fee_rate;
    let mt = state.mt_fee_rate;
    if lp >= ONE || mt >= ONE || lp.saturating_add(mt) >= ONE {
        return U256::ZERO;
    }

    gross - mul_floor(gross, lp) - mul_floor(gross, mt)
}

#[must_use]
pub fn estimate_dodo_hop_capacity(state: &DodoPoolState, base_to_quote: bool) -> U256 {
    let b = state.base_reserve;
    let q = state.quote_reserve;

    if b.is_zero() || q.is_zero() {
        return U256::ZERO;
    }

    let reserve_fraction = |reserve: U256| {
        let tenth = reserve / U256::from(10);
        if tenth > U256::ZERO { tenth } else { reserve }
    };

    reserve_fraction(if base_to_quote { b } else { q })
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::Address;

    #[test]
    fn test_zero_amount_returns_zero() {
        let state = DodoPoolState {
            base_reserve: U256::from(1000u64),
            quote_reserve: U256::from(1000u64),
            base_token: Address::ZERO,
            quote_token: Address::ZERO,
            base_target: U256::from(1000u64),
            quote_target: U256::from(1000u64),
            r_state: DodoRState::One,
            i: U256::from(1u64),
            k: U256::ZERO,
            lp_fee_rate: U256::ZERO,
            mt_fee_rate: U256::ZERO,
        };
        assert_eq!(get_dodo_amount_out(&state, U256::ZERO, true), U256::ZERO);
    }

    #[test]
    fn above_one_trade_to_target_returns_quote_surplus() {
        let state = DodoPoolState {
            base_reserve: U256::from(900u64) * ONE,
            quote_reserve: U256::from(1_100u64) * ONE,
            base_token: Address::ZERO,
            quote_token: Address::ZERO,
            base_target: U256::from(1_000u64) * ONE,
            quote_target: U256::from(1_000u64) * ONE,
            r_state: DodoRState::AboveOne,
            i: ONE,
            k: U256::from(100_000_000_000_000_000u64),
            lp_fee_rate: U256::ZERO,
            mt_fee_rate: U256::ZERO,
        };

        let out = get_dodo_gross_amount_out(&state, U256::from(100u64) * ONE, true);

        assert_eq!(out, U256::from(100u64) * ONE);
    }

    #[test]
    fn dodo_amount_out_applies_lp_and_mt_fee_components() {
        let gross_state = DodoPoolState {
            base_reserve: U256::from(1_000u64) * ONE,
            quote_reserve: U256::from(1_000u64) * ONE,
            base_token: Address::ZERO,
            quote_token: Address::ZERO,
            base_target: U256::from(1_000u64) * ONE,
            quote_target: U256::from(1_000u64) * ONE,
            r_state: DodoRState::One,
            i: ONE,
            k: U256::ZERO,
            lp_fee_rate: U256::ZERO,
            mt_fee_rate: U256::ZERO,
        };
        let fee_state = DodoPoolState {
            lp_fee_rate: ONE / U256::from(10u8),
            mt_fee_rate: ONE / U256::from(20u8),
            ..gross_state
        };
        let gross = get_dodo_gross_amount_out(&gross_state, U256::from(100u64) * ONE, true);
        let net = get_dodo_amount_out(&fee_state, U256::from(100u64) * ONE, true);
        assert!(gross > net);
        let expected = gross - (gross / U256::from(10u8)) - (gross / U256::from(20u8));
        assert_eq!(net, expected);
    }
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used)]
mod proptests {
    use super::*;
    use alloy::primitives::Address;
    use proptest::prelude::*;

    fn non_zero() -> impl Strategy<Value = U256> {
        (1u128..=u128::MAX).prop_map(U256::from)
    }

    proptest! {
        #[test]
        fn output_bounded_by_reserve(
            amount_in in non_zero(),
            base_reserve in non_zero(),
            quote_reserve in non_zero(),
            i in non_zero(),
            k in (0u64..1_000_000_000_000_000_000u64).prop_map(U256::from),
        ) {
            let k = k % super::super::fixed_point::ONE;
            let state = DodoPoolState {
                base_reserve,
                quote_reserve,
                base_token: Address::ZERO,
                quote_token: Address::ZERO,
                base_target: base_reserve,
                quote_target: quote_reserve,
                r_state: DodoRState::One,
                i,
                k,
                lp_fee_rate: U256::ZERO,
                mt_fee_rate: U256::ZERO,
            };

            let out = get_dodo_amount_out(&state, amount_in, true);
            if !out.is_zero() {
                prop_assert!(out <= state.quote_reserve,
                    "dodo out={} exceeds quote reserve={}", out, state.quote_reserve);
            }

            let out_rev = get_dodo_amount_out(&state, amount_in, false);
            if !out_rev.is_zero() {
                prop_assert!(out_rev <= state.base_reserve,
                    "dodo rev out={} exceeds base reserve={}", out_rev, state.base_reserve);
            }
        }
    }
}
