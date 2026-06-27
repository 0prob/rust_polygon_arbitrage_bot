use ruint::aliases::{U256, U512};

use crate::core::types::{WoofiBaseTokenState, WoofiPoolState};

use super::fixed_point::ONE;
const WOOFI_FEE_DENOMINATOR: U256 = U256::from_limbs([100_000, 0, 0, 0]);

fn mul_div_triple(a: U256, b: U256, c: U256, d: U256, e: U256) -> U256 {
    (U512::from(a) * U512::from(b) * U512::from(c) / (U512::from(d) * U512::from(e)))
        .wrapping_to::<U256>()
}

fn mul_triple_div_single(a: U256, b: U256, c: U256, d: U256) -> U256 {
    (U512::from(a) * U512::from(b) * U512::from(c) / U512::from(d))
        .wrapping_to::<U256>()
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

    let notional_swap =
        mul_div_triple(base_amount, base.price, base.quote_dec, base.base_dec, base.price_dec);
    if !base.max_notional_swap.is_zero() && notional_swap > base.max_notional_swap {
        return U256::ZERO;
    }

    let gamma = mul_div_triple(base_amount, base.price, base.coeff, base.price_dec, base.base_dec);
    if !base.max_gamma.is_zero() && gamma > base.max_gamma {
        return U256::ZERO;
    }
    if !has_positive_swap_factor(gamma, spread) {
        return U256::ZERO;
    }

    let quote_no_spread = mul_triple_div_single(base_amount, base.price, base.quote_dec, base.price_dec);
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

    let base_no_spread = mul_triple_div_single(quote_amount, base.base_dec, base.price_dec, base.price);
    (base_no_spread * (ONE - gamma - spread)) / (ONE * base.quote_dec)
}

fn apply_woofi_fee(amount: U256, fee_rate: U256) -> U256 {
    if amount.is_zero() || fee_rate >= WOOFI_FEE_DENOMINATOR {
        return U256::ZERO;
    }
    amount - (amount * fee_rate) / WOOFI_FEE_DENOMINATOR
}

/// Simulate WooFi swap by base index (0 = quote token path uses base_states[0]).
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
        let fee_adjusted = apply_woofi_fee(amount_in, base.fee_rate);
        let quote_out = calc_quote_amount_sell_base(base, fee_adjusted, None);
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
    let shared_spread = sell.spread.max(buy.spread) / U256::from(2);
    let fee_rate = sell.fee_rate.max(buy.fee_rate);
    let fee_adjusted = apply_woofi_fee(amount_in, fee_rate);
    let quote_amount = calc_quote_amount_sell_base(sell, fee_adjusted, Some(shared_spread));
    let base_out = calc_base_amount_sell_quote(buy, quote_amount, Some(shared_spread));
    if buy.reserve.is_zero() || base_out > buy.reserve {
        return U256::ZERO;
    }
    base_out
}

