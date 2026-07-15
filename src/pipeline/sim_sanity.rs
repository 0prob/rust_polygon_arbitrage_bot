use alloy::primitives::{I256, U256};

use crate::core::constants::{
    BPS_SCALE, MAX_SANE_PROFIT_MATIC_WEI, MAX_SANE_PROFIT_RATIO_BPS, MAX_SUPPORTED_TOKEN_DECIMALS,
    MIN_ECONOMIC_VALUE_MATIC_WEI, MIN_TOKEN_TO_MATIC_RATE,
};
use crate::core::math::fixed_point::ONE;
use crate::util::ten_pow_u256_cached as ten_pow_u256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimSanityReject {
    UnsupportedTokenDecimals,
    AmountBelowEconomicFloor,
    InsaneProfitRatio,
    InsaneProfitMatic,
    OptimizerPinnedAtFloor,
}

#[derive(Debug, Clone, Copy)]
pub struct SimSanityInput {
    pub amount_in: U256,
    pub gross_profit: U256,
    pub search_low: U256,
    pub token_decimals: u8,
    pub token_to_matic_rate: U256,
}

/// Smallest borrow size that represents meaningful notional (~0.001 token or ~1 MATIC).
#[must_use]
pub fn min_economic_amount_in(token_decimals: u8, token_to_matic_rate: U256) -> U256 {
    if token_decimals > MAX_SUPPORTED_TOKEN_DECIMALS {
        return U256::MAX;
    }
    let scale = ten_pow_u256(token_decimals);
    let dust_floor = scale / U256::from(1000u64);
    let absolute_floor = U256::from(1000u64);
    if token_to_matic_rate.is_zero() {
        return dust_floor.max(absolute_floor);
    }
    let matic_floor = U256::from(MIN_ECONOMIC_VALUE_MATIC_WEI);
    let Some(economic) = matic_floor.checked_mul(scale) else {
        return U256::MAX;
    };
    let economic = economic / token_to_matic_rate;
    economic.max(dust_floor).max(absolute_floor)
}

/// Live MATIC/USD for flash-cap sizing. Returns `None` when oracle is cold — callers
/// must not invent a USD price (avoids oversizing borrows vs `max_flash_loan_usd`).
#[inline]
#[must_use]
pub fn matic_usd_for_flash_cap(matic_usd: f64) -> Option<f64> {
    (matic_usd.is_finite() && matic_usd > 0.0).then_some(matic_usd)
}

/// USD notional cap → MATIC wei from a Chainlink MATIC/USD answer (8 decimals).
#[must_use]
pub fn max_flash_loan_matic_wei_from_usd_chainlink_8(usd_cap: u64, matic_usd_answer: I256) -> Option<U256> {
    if usd_cap == 0 {
        return None;
    }
    let Ok(matic) = i128::try_from(matic_usd_answer) else {
        return None;
    };
    if matic <= 0 {
        return None;
    }
    let numer = U256::from(usd_cap)
        .checked_mul(ONE)?
        .checked_mul(U256::from(100_000_000u64))?;
    Some(numer / U256::from(matic as u128))
}

/// USD notional cap → MATIC wei (`max_flash_loan_usd` is denominated in US dollars).
#[must_use]
pub fn max_flash_loan_matic_wei_from_usd(usd_cap: u64, matic_usd: f64) -> Option<U256> {
    if usd_cap == 0 {
        return None;
    }
    let matic_usd = matic_usd_for_flash_cap(matic_usd)?;
    let matic_usd_micros = (matic_usd * 1_000_000.0).round();
    if !matic_usd_micros.is_finite() || matic_usd_micros <= 0.0 {
        return None;
    }
    let matic_usd_micros = u64::try_from(matic_usd_micros as u128).ok()?;
    let numer = U256::from(usd_cap)
        .checked_mul(ONE)?
        .checked_mul(U256::from(1_000_000u64))?;
    Some(numer / U256::from(matic_usd_micros))
}

/// Max borrow in start-token wei from the configured USD flash cap.
#[must_use]
pub fn max_flash_borrow_wei(
    max_flash_loan_usd: u64,
    token_decimals: u8,
    token_to_matic_rate: U256,
    matic_usd: f64,
    matic_usd_chainlink: Option<I256>,
) -> Option<U256> {
    if token_decimals > MAX_SUPPORTED_TOKEN_DECIMALS || token_to_matic_rate.is_zero() {
        return None;
    }
    let max_matic_wei = match matic_usd_chainlink {
        Some(raw) => max_flash_loan_matic_wei_from_usd_chainlink_8(max_flash_loan_usd, raw)?,
        None => max_flash_loan_matic_wei_from_usd(max_flash_loan_usd, matic_usd)?,
    };
    let scale = ten_pow_u256(token_decimals);
    max_matic_wei
        .checked_mul(scale)?
        .checked_div(token_to_matic_rate)
}

/// Fast rejects for Brent inner-loop evaluation (skips floor/pin checks).
#[inline]
pub fn check_sim_sanity_fast(input: SimSanityInput) -> Result<(), SimSanityReject> {
    if input.token_decimals > MAX_SUPPORTED_TOKEN_DECIMALS {
        return Err(SimSanityReject::UnsupportedTokenDecimals);
    }
    if input.amount_in.is_zero() {
        return Err(SimSanityReject::AmountBelowEconomicFloor);
    }
    let Some(max_sane_profit) = input
        .amount_in
        .checked_mul(U256::from(MAX_SANE_PROFIT_RATIO_BPS))
        .map(|value| value / BPS_SCALE)
    else {
        return Err(SimSanityReject::InsaneProfitRatio);
    };
    if input.gross_profit > max_sane_profit {
        return Err(SimSanityReject::InsaneProfitRatio);
    }
    Ok(())
}

pub fn check_sim_sanity(input: SimSanityInput) -> Result<(), SimSanityReject> {
    if input.token_decimals > MAX_SUPPORTED_TOKEN_DECIMALS {
        return Err(SimSanityReject::UnsupportedTokenDecimals);
    }
    if input.amount_in.is_zero() {
        return Err(SimSanityReject::AmountBelowEconomicFloor);
    }

    let floor = min_economic_amount_in(input.token_decimals, input.token_to_matic_rate);
    let tickless_probe = crate::pipeline::spot_price::spot_probe_for_decimals(input.token_decimals);
    let tickless_cap_trade = input.amount_in >= tickless_probe && input.amount_in < floor;
    if input.amount_in < floor && !tickless_cap_trade {
        return Err(SimSanityReject::AmountBelowEconomicFloor);
    }

    let Some(max_sane_profit) = input
        .amount_in
        .checked_mul(U256::from(MAX_SANE_PROFIT_RATIO_BPS))
        .map(|value| value / BPS_SCALE)
    else {
        return Err(SimSanityReject::InsaneProfitRatio);
    };
    if input.gross_profit > max_sane_profit {
        return Err(SimSanityReject::InsaneProfitRatio);
    }

    if input.token_to_matic_rate >= MIN_TOKEN_TO_MATIC_RATE {
        let scale = ten_pow_u256(input.token_decimals);
        if let Some(matic_profit) = input
            .gross_profit
            .checked_mul(input.token_to_matic_rate)
            .map(|v| v / scale)
            && matic_profit > U256::from(MAX_SANE_PROFIT_MATIC_WEI)
        {
            return Err(SimSanityReject::InsaneProfitMatic);
        }
    }

    // Brent pinned at the search floor with non-trivial profit → corrupt bounds/state.
    // Skip for tickless CL cap trades sized at the decimal-aware probe (below economic floor).
    if !tickless_cap_trade {
        let pin_tolerance = input.search_low / U256::from(50u64); // relaxed 2x for Balancer/Curve edge cases
        let pin_ceiling = input
            .search_low
            .saturating_add(pin_tolerance.max(U256::from(1u8)));
        if input.amount_in <= pin_ceiling
            && input.gross_profit > input.amount_in / U256::from(20u64)
        {
            return Err(SimSanityReject::OptimizerPinnedAtFloor);
        }
    }

    Ok(())
}

/// Dispatch/Brent final check: pinned-at-floor can be a real high-ROI arb on shallow pools.
/// Retry without the Brent pin heuristic (same as probe fallback).
pub fn check_sim_sanity_for_dispatch(input: SimSanityInput) -> Result<(), SimSanityReject> {
    match check_sim_sanity(input) {
        Ok(()) => Ok(()),
        Err(SimSanityReject::OptimizerPinnedAtFloor) => check_sim_sanity(SimSanityInput {
            search_low: U256::ZERO,
            ..input
        }),
        Err(reason) => Err(reason),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tickless_probe_below_economic_floor_passes_sanity() {
        use crate::pipeline::spot_price::spot_probe_for_decimals;

        let rate = U256::from(10u128.pow(18));
        let probe = spot_probe_for_decimals(18);
        let profit = probe / U256::from(200u64);
        assert!(
            check_sim_sanity(SimSanityInput {
                amount_in: probe,
                gross_profit: profit,
                search_low: probe,
                token_decimals: 18,
                token_to_matic_rate: rate,
            })
            .is_ok()
        );
    }

    #[test]
    fn unsupported_decimals_fail_closed_without_overflow() {
        let rate = U256::from(10u128.pow(18));
        assert_eq!(min_economic_amount_in(77, rate), U256::MAX);
        assert_eq!(max_flash_borrow_wei(50_000, 77, rate, 1.0, None), None);
        assert_eq!(
            check_sim_sanity_fast(SimSanityInput {
                amount_in: U256::ONE,
                gross_profit: U256::ZERO,
                search_low: U256::ONE,
                token_decimals: 77,
                token_to_matic_rate: rate,
            }),
            Err(SimSanityReject::UnsupportedTokenDecimals)
        );
    }

    #[test]
    fn overflowing_profit_ratio_bound_fails_closed() {
        assert_eq!(
            check_sim_sanity_fast(SimSanityInput {
                amount_in: U256::MAX,
                gross_profit: U256::ZERO,
                search_low: U256::ONE,
                token_decimals: 18,
                token_to_matic_rate: U256::ONE,
            }),
            Err(SimSanityReject::InsaneProfitRatio)
        );
    }

    #[test]
    fn insane_matic_profit_is_rejected() {
        let rate = U256::from(10u128.pow(18));
        let amount_in = U256::from(10u128.pow(20));
        // MAX_SANE_PROFIT_MATIC_WEI = 50 POL; use 100 POL to exceed the cap.
        let profit = U256::from(100u128 * 10u128.pow(18));
        assert!(matches!(
            check_sim_sanity(SimSanityInput {
                amount_in,
                gross_profit: profit,
                search_low: amount_in / U256::from(2u64),
                token_decimals: 18,
                token_to_matic_rate: rate,
            }),
            Err(SimSanityReject::InsaneProfitMatic)
        ));
    }

    #[test]
    fn dispatch_sanity_allows_pinned_high_roi_when_probe_fallback_would() {
        let rate = U256::from(10u128.pow(18));
        let economic = min_economic_amount_in(18, rate);
        let amount_in = economic;
        let profit = amount_in / U256::from(5u64);
        let input = SimSanityInput {
            amount_in,
            gross_profit: profit,
            search_low: economic,
            token_decimals: 18,
            token_to_matic_rate: rate,
        };
        assert!(matches!(
            check_sim_sanity(input),
            Err(SimSanityReject::OptimizerPinnedAtFloor)
        ));
        assert!(check_sim_sanity_for_dispatch(input).is_ok());
    }

    #[test]
    fn hf_search_low_must_match_dispatch_for_pinned_optimizer() {
        let rate = U256::from(10u128.pow(18));
        let economic = min_economic_amount_in(18, rate);
        let amount_in = economic;
        let profit = amount_in / U256::from(5u64);
        let hf_search_low = amount_in / U256::from(100u64);
        let input = SimSanityInput {
            amount_in,
            gross_profit: profit,
            search_low: hf_search_low,
            token_decimals: 18,
            token_to_matic_rate: rate,
        };
        assert!(check_sim_sanity(input).is_ok());
        let dispatch_mismatch = SimSanityInput {
            search_low: economic,
            ..input
        };
        assert!(matches!(
            check_sim_sanity(dispatch_mismatch),
            Err(SimSanityReject::OptimizerPinnedAtFloor)
        ));
    }

    #[test]
    fn max_flash_borrow_scales_with_rate() {
        let cap =
            max_flash_borrow_wei(50_000, 18, U256::from(10u128.pow(18)), 1.0, None).expect("cap");
        assert_eq!(cap, U256::from(50_000u64) * ONE);
        let low_rate_cap = max_flash_borrow_wei(
            50_000,
            18,
            U256::from(10u128.pow(15)),
            1.0,
            None,
        )
        .expect("cap");
        assert!(low_rate_cap > cap);
    }

    #[test]
    fn max_flash_borrow_uses_usd_not_matic_units() {
        let cap =
            max_flash_borrow_wei(50_000, 18, U256::from(10u128.pow(18)), 0.5, None).expect("cap");
        assert_eq!(cap, U256::from(100_000u64) * ONE);
    }

    #[test]
    fn max_flash_borrow_chainlink_8_matches_float_usd() {
        use alloy::primitives::I256;
        let rate = U256::from(10u128.pow(18));
        let matic_raw = I256::from(U256::from(50_000_000u64));
        let float_cap = max_flash_borrow_wei(50_000, 18, rate, 0.5, None).expect("float");
        let chain_cap = max_flash_borrow_wei(50_000, 18, rate, 0.0, Some(matic_raw)).expect("cl");
        assert_eq!(float_cap, chain_cap);
    }
}
