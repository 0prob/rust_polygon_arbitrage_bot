use alloy::primitives::{U256, U512};

use crate::core::types::{WoofiBaseTokenState, WoofiPoolState};

use super::fixed_point::ONE;
const WOOFI_FEE_DENOMINATOR: U256 = U256::from_limbs([100_000, 0, 0, 0]);

fn mul_div_triple(a: U256, b: U256, c: U256, d: U256, e: U256) -> U256 {
    if let Some(ab) = a.checked_mul(b)
        && let Some(abc) = ab.checked_mul(c)
        && let Some(de) = d.checked_mul(e)
        && !de.is_zero()
    {
        return abc / de;
    }
    // U512 fallback: if the division result exceeds U256::MAX, return zero.
    let result = U512::from(a) * U512::from(b) * U512::from(c) / (U512::from(d) * U512::from(e));
    if result > U512::from(U256::MAX) {
        U256::ZERO
    } else {
        crate::util::u512_to_u256(result)
    }
}

fn has_positive_swap_factor(gamma: U256, spread: U256) -> bool {
    gamma <= ONE && spread <= ONE && gamma + spread < ONE
}

fn calc_quote_amount_sell_base(
    base: &WoofiBaseTokenState,
    base_amount: U256,
    spread_override: Option<U256>,
) -> U256 {
    let spread = spread_override.unwrap_or(base.spread);
    if base_amount.is_zero()
        || base.price.is_zero()
        || base.base_dec.is_zero()
        || base.quote_dec.is_zero()
        || base.price_dec.is_zero()
    {
        return U256::ZERO;
    }

    let notional_swap = mul_div_triple(
        base_amount,
        base.price,
        base.quote_dec,
        base.base_dec,
        base.price_dec,
    );
    // WooPPV2 enforces `notionalSwap <= maxNotionalSwap` unconditionally, so a
    // zero cap rejects every positive swap (not "no cap").
    if notional_swap > base.max_notional_swap {
        return U256::ZERO;
    }

    let gamma = mul_div_triple(
        base_amount,
        base.price,
        base.coeff,
        base.price_dec,
        base.base_dec,
    );
    if gamma > base.max_gamma {
        return U256::ZERO;
    }
    if !has_positive_swap_factor(gamma, spread) {
        return U256::ZERO;
    }

    let quote_no_spread = mul_div_triple(
        base_amount,
        base.price,
        base.quote_dec,
        base.price_dec,
        U256::ONE,
    );
    (quote_no_spread * (ONE - gamma - spread)) / (ONE * base.base_dec)
}

fn calc_base_amount_sell_quote(
    base: &WoofiBaseTokenState,
    quote_amount: U256,
    spread_override: Option<U256>,
) -> U256 {
    let spread = spread_override.unwrap_or(base.spread);
    if quote_amount.is_zero()
        || base.price.is_zero()
        || base.base_dec.is_zero()
        || base.quote_dec.is_zero()
        || base.price_dec.is_zero()
    {
        return U256::ZERO;
    }

    if quote_amount > base.max_notional_swap {
        return U256::ZERO;
    }

    let gamma = (quote_amount * base.coeff) / base.quote_dec;
    if gamma > base.max_gamma {
        return U256::ZERO;
    }
    if !has_positive_swap_factor(gamma, spread) {
        return U256::ZERO;
    }

    let base_no_spread = mul_div_triple(
        quote_amount,
        base.base_dec,
        base.price_dec,
        base.price,
        U256::ONE,
    );
    (base_no_spread * (ONE - gamma - spread)) / (ONE * base.quote_dec)
}

fn apply_woofi_fee(amount: U256, fee_rate: U256) -> U256 {
    if amount.is_zero() || fee_rate >= WOOFI_FEE_DENOMINATOR {
        return U256::ZERO;
    }
    amount - (amount * fee_rate) / WOOFI_FEE_DENOMINATOR
}

fn woofi_fee_rate_to_bps(fee_rate: U256) -> u32 {
    if fee_rate >= WOOFI_FEE_DENOMINATOR {
        return 9_999;
    }
    ((fee_rate * U256::from(10_000u64)) / WOOFI_FEE_DENOMINATOR)
        .min(U256::from(9_999u64))
        .to::<u32>()
}

/// Active-leg WooFi `feeRate` (1e5 denom) → edge `fee_bps` for graph ranking.
/// Base→base is a single WooPPV2 `_swapBaseToBase` call that charges ONE fee at
/// the higher of the two legs' rates (matches the single `apply_woofi_fee` in sim).
#[must_use]
pub fn woofi_fee_bps_from_edge(
    state: &WoofiPoolState,
    token_in_idx: u8,
    token_out_idx: u8,
) -> Option<u32> {
    let quote_idx = state.base_states.len();
    let tin = token_in_idx as usize;
    let tout = token_out_idx as usize;
    if tin == tout || tin > quote_idx || tout > quote_idx {
        return None;
    }
    let total = if tout == quote_idx {
        woofi_fee_rate_to_bps(state.base_states.get(tin)?.fee_rate)
    } else if tin == quote_idx {
        woofi_fee_rate_to_bps(state.base_states.get(tout)?.fee_rate)
    } else {
        let sell = woofi_fee_rate_to_bps(state.base_states.get(tin)?.fee_rate);
        let buy = woofi_fee_rate_to_bps(state.base_states.get(tout)?.fee_rate);
        sell.max(buy)
    };
    Some(total.min(9_999))
}

/// Simulate WooFi swap by base index (0 = quote token path uses base_states[0]).
#[inline]
#[must_use]
pub fn get_woofi_amount_out(
    state: &WoofiPoolState,
    amount_in: U256,
    token_in_is_quote: bool,
    token_out_is_quote: bool,
    base_in_idx: Option<usize>,
    base_out_idx: Option<usize>,
) -> U256 {
    if amount_in.is_zero() {
        return U256::ZERO;
    }

    if token_out_is_quote {
        let Some(idx) = base_in_idx else {
            return U256::ZERO;
        };
        let Some(base) = state.base_states.get(idx) else {
            return U256::ZERO;
        };
        let gross_quote_out = calc_quote_amount_sell_base(base, amount_in, None);
        let quote_out = apply_woofi_fee(gross_quote_out, base.fee_rate);
        if state.quote_reserve.is_zero() || quote_out > state.quote_reserve {
            return U256::ZERO;
        }
        return quote_out;
    }

    if token_in_is_quote {
        let Some(idx) = base_out_idx else {
            return U256::ZERO;
        };
        let Some(base) = state.base_states.get(idx) else {
            return U256::ZERO;
        };
        let fee_adjusted = apply_woofi_fee(amount_in, base.fee_rate);
        let base_out = calc_base_amount_sell_quote(base, fee_adjusted, None);
        if base.reserve.is_zero() || base_out > base.reserve {
            return U256::ZERO;
        }
        return base_out;
    }

    let Some(sell_idx) = base_in_idx else {
        return U256::ZERO;
    };
    let Some(buy_idx) = base_out_idx else {
        return U256::ZERO;
    };
    let Some(sell) = state.base_states.get(sell_idx) else {
        return U256::ZERO;
    };
    let Some(buy) = state.base_states.get(buy_idx) else {
        return U256::ZERO;
    };
    if amount_in > sell.reserve || sell.reserve.is_zero() {
        return U256::ZERO;
    }
    // WooPPV2._swapBaseToBase is a single swap (not two quote legs): it applies
    // ONE fee at max(feeRate1, feeRate2) and runs BOTH legs on the shared spread
    // max(spread1, spread2)/2. The intermediate quote debit (`reserve - gross`)
    // Obeys `gross <= quote_reserve`, so the reserve guard runs on the gross,
    // pre-fee quote — not the post-fee amount.
    let spread = sell.spread.max(buy.spread) / U256::from(2u64);
    let fee_rate = sell.fee_rate.max(buy.fee_rate);
    let quote_gross = calc_quote_amount_sell_base(sell, amount_in, Some(spread));
    if quote_gross.is_zero() || quote_gross > state.quote_reserve {
        return U256::ZERO;
    }
    let quote_amount = apply_woofi_fee(quote_gross, fee_rate);
    let base_out = calc_base_amount_sell_quote(buy, quote_amount, Some(spread));
    if buy.reserve.is_zero() || base_out > buy.reserve {
        return U256::ZERO;
    }
    base_out
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::Address;

    fn base_state(reserve: U256) -> WoofiBaseTokenState {
        WoofiBaseTokenState {
            price: U256::from(10u128.pow(18)),
            spread: U256::ZERO,
            coeff: U256::ZERO,
            reserve,
            base_dec: U256::from(10u128.pow(18)),
            quote_dec: U256::from(10u128.pow(6)),
            price_dec: U256::from(10u128.pow(8)),
            fee_rate: U256::ZERO,
            // Live WooPPs run with a positive maxGamma and an effectively
            // unbounded maxNotionalSwap; 0 on-chain means "reject all swaps".
            max_gamma: ONE,
            max_notional_swap: U256::from(u128::MAX),
        }
    }

    #[test]
    fn fee_rate_1e5_converts_to_bps_per_active_leg() {
        let mut sell = base_state(U256::from(1_000u64));
        let mut buy = base_state(U256::from(1_000u64));
        sell.fee_rate = U256::from(25u64); // 25 / 1e5 = 2.5 bps → 2
        buy.fee_rate = U256::from(50u64); // 5 bps
        let state = WoofiPoolState {
            tokens: vec![
                Address::with_last_byte(1),
                Address::with_last_byte(2),
                Address::with_last_byte(3),
            ],
            quote_reserve: U256::from(1_000u64),
            base_states: vec![sell, buy],
            fee: U256::ZERO,
        };
        assert_eq!(woofi_fee_bps_from_edge(&state, 0, 2), Some(2)); // sell base→quote
        assert_eq!(woofi_fee_bps_from_edge(&state, 2, 1), Some(5)); // quote→buy
        assert_eq!(woofi_fee_bps_from_edge(&state, 0, 1), Some(5)); // base→base: single max
    }

    #[test]
    fn base_to_base_rejects_insufficient_quote_reserve() {
        let state = WoofiPoolState {
            tokens: vec![
                Address::with_last_byte(1),
                Address::with_last_byte(2),
                Address::with_last_byte(3),
            ],
            quote_reserve: U256::from(100u64),
            base_states: vec![
                base_state(U256::from(1_000_000u64) * U256::from(10u128.pow(18))),
                base_state(U256::from(1_000_000u64) * U256::from(10u128.pow(18))),
            ],
            fee: U256::ZERO,
        };
        let out = get_woofi_amount_out(
            &state,
            U256::from(100u64) * U256::from(10u128.pow(18)),
            false,
            false,
            Some(0),
            Some(1),
        );
        assert_eq!(out, U256::ZERO);
    }

    #[test]
    fn base_to_base_rejects_input_exceeding_sell_reserve() {
        let state = WoofiPoolState {
            tokens: vec![
                Address::with_last_byte(1),
                Address::with_last_byte(2),
                Address::with_last_byte(3),
            ],
            quote_reserve: U256::from(1_000_000_000_000u64) * U256::from(10u128.pow(6)),
            base_states: vec![
                base_state(U256::from(10u128.pow(18))),
                base_state(U256::from(1_000_000u64) * U256::from(10u128.pow(18))),
            ],
            fee: U256::ZERO,
        };
        let out = get_woofi_amount_out(
            &state,
            U256::from(5u64) * U256::from(10u128.pow(18)),
            false,
            false,
            Some(0),
            Some(1),
        );
        assert_eq!(out, U256::ZERO);
    }

    #[test]
    fn sell_base_charges_fee_on_quote_output() {
        let mut base = base_state(U256::from(1_000_000u64) * U256::from(10u128.pow(18)));
        base.coeff = U256::from(100_000_000_000_000u64);
        base.fee_rate = U256::from(100u64);
        let amount_in = U256::from(10u64) * U256::from(10u128.pow(18));
        let gross = calc_quote_amount_sell_base(&base, amount_in, None);
        let state = WoofiPoolState {
            tokens: vec![Address::with_last_byte(1), Address::with_last_byte(2)],
            quote_reserve: U256::from(1_000_000u64) * U256::from(10u128.pow(6)),
            base_states: vec![base.clone()],
            fee: U256::ZERO,
        };

        let out = get_woofi_amount_out(&state, amount_in, false, true, Some(0), None);

        assert_eq!(out, apply_woofi_fee(gross, base.fee_rate));
    }

    #[test]
    fn base_to_base_matches_single_woopp_swap_with_max_fee_and_shared_spread() {
        let mut sell = base_state(U256::from(1_000_000u64) * U256::from(10u128.pow(18)));
        sell.spread = U256::from(1_000_000_000_000_000u64);
        sell.fee_rate = U256::from(25u64);
        let mut buy = base_state(U256::from(1_000_000u64) * U256::from(10u128.pow(18)));
        buy.spread = U256::from(3_000_000_000_000_000u64);
        buy.fee_rate = U256::from(75u64);
        let amount_in = U256::from(10u64) * U256::from(10u128.pow(18));
        // WooPPV2._swapBaseToBase: one fee at max(fee1,fee2)=75, both legs run on
        // the shared spread max(spread1,spread2)/2 = 1.5e15.
        let spread = sell.spread.max(buy.spread) / U256::from(2u64);
        let fee_rate = sell.fee_rate.max(buy.fee_rate);
        let gross = calc_quote_amount_sell_base(&sell, amount_in, Some(spread));
        let quoted = apply_woofi_fee(gross, fee_rate);
        let expected = calc_base_amount_sell_quote(&buy, quoted, Some(spread));
        let state = WoofiPoolState {
            tokens: vec![
                Address::with_last_byte(1),
                Address::with_last_byte(2),
                Address::with_last_byte(3),
            ],
            quote_reserve: U256::from(1_000_000_000_000u64) * U256::from(10u128.pow(6)),
            base_states: vec![sell, buy],
            fee: U256::ZERO,
        };

        let out = get_woofi_amount_out(&state, amount_in, false, false, Some(0), Some(1));

        // Matches the single-swap contract model exactly.
        assert_eq!(out, expected);
        assert!(!out.is_zero());
        // A double-fee / per-leg-spread model would quote a strictly smaller amount.
        let wrong = {
            let g = calc_quote_amount_sell_base(&state.base_states[0], amount_in, None);
            let a = apply_woofi_fee(g, state.base_states[0].fee_rate);
            let b = apply_woofi_fee(a, state.base_states[1].fee_rate);
            calc_base_amount_sell_quote(&state.base_states[1], b, None)
        };
        assert!(out > wrong, "single-fee model must beat the old two-fee model");
    }

    #[test]
    fn zero_leg_caps_reject_positive_swap() {
        // A zero maxGamma (or maxNotionalSwap) on WooPPV2 means EVERY swap for
        // that token reverts — it must not be treated as "unlimited".
        let mut base = base_state(U256::from(1_000_000u64) * U256::from(10u128.pow(18)));
        base.coeff = U256::from(100_000_000_000_000u64);
        base.max_gamma = U256::ZERO;
        let state = WoofiPoolState {
            tokens: vec![Address::with_last_byte(1), Address::with_last_byte(2)],
            quote_reserve: U256::from(1_000_000u64) * U256::from(10u128.pow(6)),
            base_states: vec![base],
            fee: U256::ZERO,
        };
        let amount = U256::from(10u64) * U256::from(10u128.pow(18));
        assert_eq!(
            get_woofi_amount_out(&state, amount, false, true, Some(0), None),
            U256::ZERO
        );

        // A zero maxNotionalSwap likewise caps every trade.
        let mut base2 = base_state(U256::from(1_000_000u64) * U256::from(10u128.pow(18)));
        base2.max_notional_swap = U256::ZERO;
        let state2 = WoofiPoolState {
            tokens: vec![Address::with_last_byte(1), Address::with_last_byte(2)],
            quote_reserve: U256::from(1_000_000u64) * U256::from(10u128.pow(6)),
            base_states: vec![base2],
            fee: U256::ZERO,
        };
        assert_eq!(
            get_woofi_amount_out(&state2, amount, false, true, Some(0), None),
            U256::ZERO
        );
    }

    #[test]
    fn quote_path_requires_feasible_oracle_state_and_reserve() {
        let state = WoofiPoolState {
            tokens: vec![Address::with_last_byte(1), Address::with_last_byte(2)],
            quote_reserve: U256::ZERO,
            base_states: vec![base_state(
                U256::from(1_000_000u64) * U256::from(10u128.pow(18)),
            )],
            fee: U256::ZERO,
        };

        assert_eq!(
            get_woofi_amount_out(
                &state,
                U256::from(10u64) * U256::from(10u128.pow(18)),
                false,
                true,
                Some(0),
                None
            ),
            U256::ZERO
        );
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use crate::core::math::proptest_util::{U256_AMT_MAX, u256_fp18, u256_nonzero};
    use alloy::primitives::Address;
    use proptest::prelude::*;

    fn small_fee() -> impl Strategy<Value = U256> {
        (0u64..100_000u64).prop_map(U256::from)
    }

    proptest! {
        #[test]
        fn output_bounded_by_quote_reserve(
            amount in (1u128..=U256_AMT_MAX / 1_000_000).prop_map(U256::from),
            price in u256_nonzero(),
            spread in u256_fp18(),
            coeff in (0u128..=crate::core::math::proptest_util::FP18_ONE).prop_map(U256::from),
            reserve in u256_nonzero(),
            fee_rate in small_fee(),
            quote_reserve in u256_nonzero(),
            max_gamma in (0u128..=crate::core::math::proptest_util::FP18_ONE).prop_map(U256::from),
        ) {
            let base = WoofiBaseTokenState {
                price,
                spread,
                coeff,
                reserve,
                base_dec: U256::from(10u128.pow(18)),
                quote_dec: U256::from(10u128.pow(6)),
                price_dec: U256::from(10u128.pow(8)),
                fee_rate,
                max_gamma,
                max_notional_swap: U256::from(u128::MAX),
            };

            let state = WoofiPoolState {
                tokens: vec![Address::with_last_byte(1), Address::with_last_byte(2)],
                quote_reserve,
                base_states: vec![base.clone(), base],
                fee: U256::ZERO,
            };

            let out = get_woofi_amount_out(&state, amount, false, true, Some(0), None);
            if !out.is_zero() {
                prop_assert!(
                    out <= state.quote_reserve,
                    "woofi out={out} exceeds quote reserve={}",
                    state.quote_reserve
                );
            }
        }
    }
}
