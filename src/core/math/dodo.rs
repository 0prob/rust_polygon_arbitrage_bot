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
        // DODOMath k==1 branch: temp = i·delta·V1 / (V0·V0); receive = V1·temp/(temp+1).
        // Widen intermediates — SafeMath reverts on overflow; we fail closed to 0.
        let idelta = U512::from(i) * U512::from(delta);
        let v0_sq = U512::from(v0) * U512::from(v0);
        if v0_sq.is_zero() {
            return U256::ZERO;
        }
        let Some(temp) = u512_to_u256_checked(idelta * U512::from(v1) / v0_sq) else {
            return U256::ZERO;
        };
        if temp.is_zero() {
            return U256::ZERO;
        }
        let denom = U512::from(temp) + ONE_U512;
        return u512_to_u256_checked(U512::from(v1) * U512::from(temp) / denom)
            .unwrap_or(U256::ZERO);
    }

    // DODOMath._SolveQuadraticFunctionForTrade part2:
    //   k.mul(V0).div(V1).mul(V0).add(i.mul(delta))
    // Note: `i.mul(delta)` is the *raw product*, not DecimalMath.mulFloor.
    // bAbs is divided by ONE later so both terms share 1e18-scaled amount units.
    // A prior mul_floor(i, delta) understated the idelta term by 1e18 and
    // systematically mispriced non-linear sellBase/sellQuote quotes.
    let part2 = {
        let k_v0 = U512::from(k) * U512::from(v0);
        let k_div = k_v0 / U512::from(v1);
        let Some(k_term) = u512_to_u256_checked(k_div * U512::from(v0)) else {
            return U256::ZERO;
        };
        let Some(i_delta) = u512_to_u256_checked(U512::from(i) * U512::from(delta)) else {
            return U256::ZERO;
        };
        let Some(sum) = k_term.checked_add(i_delta) else {
            return U256::ZERO;
        };
        sum
    };
    let one_minus_k = ONE - k;
    // part1 = (1-k)·V1 — widen so large reserves do not wrap before the abs step.
    let Some(mut b_abs) = u512_to_u256_checked(U512::from(one_minus_k) * U512::from(v1)) else {
        return U256::ZERO;
    };
    let mut b_sig = false;
    if b_abs >= part2 {
        b_abs -= part2;
    } else {
        b_abs = part2 - b_abs;
        b_sig = true;
    }
    b_abs /= ONE;

    // squareRoot = 4·(1-k)·mulFloor(k,V0)·V0  (DecimalMath.mulFloor outer)
    // Use U512 for mulFloor(k,V0)*V0 which is k·V0²/1e18.
    let mfl_k_v0 = mul_floor(k, v0);
    let Some(mfl_times_v0) = u512_to_u256_checked(U512::from(mfl_k_v0) * U512::from(v0)) else {
        return U256::ZERO;
    };
    let square_root_input = mul_floor(one_minus_k * U256::from(4), mfl_times_v0);
    let square_root = if square_root_input.is_zero() {
        b_abs
    } else {
        let radicand = U512::from(b_abs) * U512::from(b_abs) + U512::from(square_root_input);
        let Some(rad_u256) = u512_to_u256_checked(radicand) else {
            return U256::ZERO;
        };
        rad_u256.root(2)
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
        match b_abs.checked_add(square_root) {
            Some(n) => n,
            None => return U256::ZERO,
        }
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

/// Convert live DODO `_LP_FEE_RATE_` + `_MT_FEE_RATE_` (1e18) to edge `fee_bps`.
#[must_use]
pub fn dodo_fee_bps_from_pool(lp_fee_rate: U256, mt_fee_rate: U256) -> Option<u32> {
    let total = lp_fee_rate.saturating_add(mt_fee_rate);
    if total.is_zero() || total >= ONE {
        return None;
    }
    let bps = (total * U256::from(10_000u64)) / ONE;
    Some(bps.min(U256::from(9_999u64)).to::<u32>())
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
    fn dodo_fee_bps_from_1e18_lp_mt() {
        assert_eq!(
            dodo_fee_bps_from_pool(ONE / U256::from(1000u64), U256::ZERO),
            Some(10)
        );
        assert_eq!(
            dodo_fee_bps_from_pool(ONE / U256::from(1000u64), ONE / U256::from(2000u64)),
            Some(15)
        );
        assert_eq!(dodo_fee_bps_from_pool(U256::ZERO, U256::ZERO), None);
        assert_eq!(dodo_fee_bps_from_pool(ONE, U256::ZERO), None);
    }

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

    /// R=ONE sellBase with non-zero k exercises `_SolveQuadraticFunctionForTrade`.
    /// DODOMath uses raw `i*delta` in part2 (not DecimalMath.mulFloor). A prior
    /// mul_floor under-scaled that term by 1e18 and returned ~10 wei for a 10e18
    /// trade — lock the on-chain result here.
    #[test]
    fn r_one_sell_base_quadratic_matches_dodo_math_hand() {
        // V0=V1=Q0=1000e18, payBase=10e18, i=1e18, k=0.1e18
        // part2 = k·V0/V1·V0 + i·delta (SafeMath, raw products) = 110e36
        // part1 = (1-k)·V1 = 900e36; bAbs = (part1-part2)/1e18 = 790e18
        // squareRoot = sqrt(bAbs² + mulFloor(4(1-k), mulFloor(k,V0)·V0))
        // V2 = DecimalMath.divCeil(bAbs+sqrt, 2(1-k)); receive = V1 - V2
        let state = DodoPoolState {
            base_reserve: U256::from(1_000u64) * ONE,
            quote_reserve: U256::from(1_000u64) * ONE,
            base_token: Address::ZERO,
            quote_token: Address::ZERO,
            base_target: U256::from(1_000u64) * ONE,
            quote_target: U256::from(1_000u64) * ONE,
            r_state: DodoRState::One,
            i: ONE,
            k: U256::from(100_000_000_000_000_000u64), // 0.1e18
            lp_fee_rate: U256::ZERO,
            mt_fee_rate: U256::ZERO,
        };
        let pay = U256::from(10u64) * ONE;
        let out = get_dodo_gross_amount_out(&state, pay, true);
        // With correct raw i*delta, quote out sits just under pay on a mild k curve.
        assert!(out > U256::from(9u64) * ONE, "out={out}");
        assert!(out < pay, "out={out}");
        // Pin exact DODOMath result (mul_floor(i,delta) in part2 yields ~10 wei instead).
        assert_eq!(out, U256::from(9_989_919_447_032_049_723u64));
    }

    #[test]
    fn sell_quote_r_one_is_symmetric_at_i_one() {
        let state = DodoPoolState {
            base_reserve: U256::from(1_000u64) * ONE,
            quote_reserve: U256::from(1_000u64) * ONE,
            base_token: Address::ZERO,
            quote_token: Address::ZERO,
            base_target: U256::from(1_000u64) * ONE,
            quote_target: U256::from(1_000u64) * ONE,
            r_state: DodoRState::One,
            i: ONE,
            k: U256::from(100_000_000_000_000_000u64),
            lp_fee_rate: U256::ZERO,
            mt_fee_rate: U256::ZERO,
        };
        let pay = U256::from(10u64) * ONE;
        let sell_base = get_dodo_gross_amount_out(&state, pay, true);
        let sell_quote = get_dodo_gross_amount_out(&state, pay, false);
        // i=1 and equal targets ⇒ sellBase and sellQuote share the same curve.
        assert_eq!(sell_base, sell_quote);
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use crate::core::math::proptest_util::{u256_fp18, u256_nonzero};
    use alloy::primitives::Address;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn output_bounded_by_reserve(
            amount_in in u256_nonzero(),
            base_reserve in u256_nonzero(),
            quote_reserve in u256_nonzero(),
            i in u256_nonzero(),
            k in u256_fp18(),
        ) {
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
                prop_assert!(
                    out <= state.quote_reserve,
                    "dodo out={out} exceeds quote reserve={}",
                    state.quote_reserve
                );
            }

            let out_rev = get_dodo_amount_out(&state, amount_in, false);
            if !out_rev.is_zero() {
                prop_assert!(
                    out_rev <= state.base_reserve,
                    "dodo rev out={out_rev} exceeds base reserve={}",
                    state.base_reserve
                );
            }
        }
    }
}
