use alloy::primitives::U256;

use crate::core::types::DodoPoolState;

use alloy::primitives::U512;

use super::fixed_point::{ONE, mul_down as mul_floor};

// Exact 1e36 — DODO on-chain `_K_PRECISION`. Previous constant was an approximation.
const ONE2: U256 = {
    let bytes = 1_000_000_000_000_000_000_000_000_000_000_000_000u128;
    U256::from_limbs([bytes as u64, (bytes >> 64) as u64, 0, 0])
};

fn div_ceil(a: U256, b: U256) -> U256 {
    if b.is_zero() {
        return U256::ZERO;
    }
    // U512 widening prevents overflow in a * ONE
    let product = U512::from(a) * U512::from(ONE);
    let result = (product + U512::from(b) - U512::ONE) / U512::from(b);
    crate::util::u512_to_u256(result)
}

fn reciprocal_floor(target: U256) -> U256 {
    if target.is_zero() {
        return U256::ZERO;
    }
    let result = U512::from(ONE2) / U512::from(target);
    crate::util::u512_to_u256(result)
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

    let part2 = (k * v0 * v0) / v1 + mul_floor(i, delta);
    let mut b_abs = (ONE - k) * v1;
    let mut b_sig = false;
    if b_abs >= part2 {
        b_abs -= part2;
    } else {
        b_abs = part2 - b_abs;
        b_sig = true;
    }
    b_abs /= ONE;

    let mut square_root = mul_floor((ONE - k) * U256::from(4), mul_floor(k, v0) * v0);
    square_root = (b_abs * b_abs + square_root).root(2);

    let denominator = (ONE - k) * U256::from(2);
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

    if base_to_quote {
        solve_quadratic_function_for_trade(q, q, amount_in, i, k)
    } else {
        let inverse_i = reciprocal_floor(i);
        if inverse_i.is_zero() {
            return U256::ZERO;
        }
        solve_quadratic_function_for_trade(b, b, amount_in, inverse_i, k)
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
    if lp >= ONE {
        return U256::ZERO;
    }

    gross - mul_floor(gross, lp)
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
            i: U256::from(1u64),
            k: U256::ZERO,
            lp_fee_rate: U256::ZERO,
        };
        assert_eq!(get_dodo_amount_out(&state, U256::ZERO, true), U256::ZERO);
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
                i,
                k,
                lp_fee_rate: U256::ZERO,
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
