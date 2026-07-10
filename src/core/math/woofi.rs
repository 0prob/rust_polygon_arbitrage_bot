use alloy::primitives::{U256, U512};

use crate::core::types::{WoofiBaseTokenState, WoofiPoolState};

use super::fixed_point::ONE;
const WOOFI_FEE_DENOMINATOR: U256 = U256::from_limbs([100_000, 0, 0, 0]);

fn mul_div_triple(a: U256, b: U256, c: U256, d: U256, e: U256) -> U256 {
    if let Some(ab) = a.checked_mul(b) {
        if let Some(abc) = ab.checked_mul(c) {
            if let Some(de) = d.checked_mul(e) {
                if !de.is_zero() {
                    return abc / de;
                }
            }
        }
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
    if !base.max_notional_swap.is_zero() && notional_swap > base.max_notional_swap {
        return U256::ZERO;
    }

    let gamma = mul_div_triple(
        base_amount,
        base.price,
        base.coeff,
        base.price_dec,
        base.base_dec,
    );
    if !base.max_gamma.is_zero() && gamma > base.max_gamma {
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

    if !base.max_notional_swap.is_zero() && quote_amount > base.max_notional_swap {
        return U256::ZERO;
    }

    let gamma = (quote_amount * base.coeff) / base.quote_dec;
    if !base.max_gamma.is_zero() && gamma > base.max_gamma {
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
    let quote_gross = calc_quote_amount_sell_base(sell, amount_in, None);
    let quote_amount = apply_woofi_fee(quote_gross, sell.fee_rate);
    if quote_amount.is_zero() || quote_amount > state.quote_reserve {
        return U256::ZERO;
    }
    let quote_after_buy_fee = apply_woofi_fee(quote_amount, buy.fee_rate);
    let base_out = calc_base_amount_sell_quote(buy, quote_after_buy_fee, None);
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
            max_gamma: U256::ZERO,
            max_notional_swap: U256::ZERO,
        }
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
    fn base_to_base_matches_two_official_quote_legs() {
        let mut sell = base_state(U256::from(1_000_000u64) * U256::from(10u128.pow(18)));
        sell.spread = U256::from(1_000_000_000_000_000u64);
        sell.fee_rate = U256::from(25u64);
        let mut buy = base_state(U256::from(1_000_000u64) * U256::from(10u128.pow(18)));
        buy.spread = U256::from(3_000_000_000_000_000u64);
        buy.fee_rate = U256::from(75u64);
        let amount_in = U256::from(10u64) * U256::from(10u128.pow(18));
        let quote_gross = calc_quote_amount_sell_base(&sell, amount_in, None);
        let quote_after_fee = apply_woofi_fee(quote_gross, sell.fee_rate);
        let expected =
            calc_base_amount_sell_quote(&buy, apply_woofi_fee(quote_after_fee, buy.fee_rate), None);
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

        assert_eq!(out, expected);
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
#[allow(clippy::panic, clippy::unwrap_used)]
mod proptests {
    use super::*;
    use alloy::primitives::Address;
    use proptest::prelude::*;

    fn non_zero_u256() -> impl Strategy<Value = U256> {
        (1u128..=u128::MAX / 2).prop_map(U256::from)
    }

    fn small_fee() -> impl Strategy<Value = U256> {
        (0u64..100_000u64).prop_map(U256::from)
    }

    proptest! {
        #[test]
        fn fee_adjusted_invariant(
            amount in (1u128..u128::MAX / 1_000_000).prop_map(U256::from),
            price in non_zero_u256(),
            spread in (0u128..=1_000_000_000_000_000_000u128).prop_map(U256::from),
            coeff in (0u128..1_000_000_000_000_000_000u128).prop_map(U256::from),
            reserve in non_zero_u256(),
            fee_rate in small_fee(),
            quote_reserve in non_zero_u256(),
            max_gamma in (0u128..=1_000_000_000_000_000_000u128).prop_map(U256::from),
        ) {
            let spread = spread % super::super::fixed_point::ONE;
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
                max_notional_swap: U256::ZERO,
            };

            let state = WoofiPoolState {
                tokens: vec![Address::with_last_byte(1), Address::with_last_byte(2)],
                quote_reserve,
                base_states: vec![base.clone(), base],
                fee: U256::ZERO,
            };

            let out = get_woofi_amount_out(&state, amount, false, true, Some(0), None);
            if !out.is_zero() {
                prop_assert!(out <= state.quote_reserve,
                    "woofi out={} exceeds quote reserve={}", out, state.quote_reserve);
            }
        }
    }
}
