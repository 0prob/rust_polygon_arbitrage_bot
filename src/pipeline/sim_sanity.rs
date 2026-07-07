use alloy::primitives::U256;

use crate::core::constants::{
    BPS_SCALE, MAX_SANE_PROFIT_MATIC_WEI, MAX_SANE_PROFIT_RATIO_BPS, MIN_ECONOMIC_VALUE_MATIC_WEI,
    MIN_TOKEN_TO_MATIC_RATE,
};
use crate::core::math::fixed_point::ONE;
use crate::util::ten_pow_u256_cached as ten_pow_u256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimSanityReject {
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
    let scale = ten_pow_u256(token_decimals);
    let dust_floor = scale / U256::from(1000u64);
    let absolute_floor = U256::from(1000u64);
    if token_to_matic_rate.is_zero() {
        return dust_floor.max(absolute_floor);
    }
    let matic_floor = U256::from(MIN_ECONOMIC_VALUE_MATIC_WEI);
    let economic = (matic_floor * scale) / token_to_matic_rate;
    economic.max(dust_floor).max(absolute_floor)
}

/// Max borrow in start-token wei from the configured MATIC-notional flash cap.
#[must_use]
pub fn max_flash_borrow_wei(
    max_flash_loan_matic: u64,
    token_decimals: u8,
    token_to_matic_rate: U256,
) -> Option<U256> {
    if token_to_matic_rate.is_zero() {
        return None;
    }
    let scale = ten_pow_u256(token_decimals);
    let max_matic_wei = U256::from(max_flash_loan_matic).checked_mul(ONE)?;
    max_matic_wei
        .checked_mul(scale)?
        .checked_div(token_to_matic_rate)
}

pub fn check_sim_sanity(input: SimSanityInput) -> Result<(), SimSanityReject> {
    if input.amount_in.is_zero() {
        return Err(SimSanityReject::AmountBelowEconomicFloor);
    }

    let floor = min_economic_amount_in(input.token_decimals, input.token_to_matic_rate);
    let tickless_probe = crate::pipeline::spot_price::SPOT_PROBE;
    let tickless_cap_trade = input.amount_in >= tickless_probe && input.amount_in < floor;
    if input.amount_in < floor && !tickless_cap_trade {
        return Err(SimSanityReject::AmountBelowEconomicFloor);
    }

    let max_sane_profit = input.amount_in * U256::from(MAX_SANE_PROFIT_RATIO_BPS) / BPS_SCALE;
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
    // Skip for tickless CL cap trades sized at SPOT_PROBE (below economic floor).
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tickless_probe_below_economic_floor_passes_sanity() {
        use crate::pipeline::spot_price::SPOT_PROBE;

        let rate = U256::from(10u128.pow(18));
        let profit = SPOT_PROBE / U256::from(200u64);
        assert!(
            check_sim_sanity(SimSanityInput {
                amount_in: SPOT_PROBE,
                gross_profit: profit,
                search_low: SPOT_PROBE,
                token_decimals: 18,
                token_to_matic_rate: rate,
            })
            .is_ok()
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
    fn max_flash_borrow_scales_with_rate() {
        let cap = max_flash_borrow_wei(50_000, 18, U256::from(10u128.pow(18))).expect("cap");
        assert_eq!(cap, U256::from(50_000u64) * ONE);
        let low_rate_cap =
            max_flash_borrow_wei(50_000, 18, U256::from(10u128.pow(15))).expect("cap");
        assert!(low_rate_cap > cap);
    }
}
