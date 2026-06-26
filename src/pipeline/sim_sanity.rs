use ruint::aliases::U256;

use crate::core::constants::{BPS_SCALE, MAX_SANE_PROFIT_RATIO_BPS, MIN_ECONOMIC_VALUE_MATIC_WEI};
use crate::util::ten_pow_u256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimSanityReject {
    AmountBelowEconomicFloor,
    InsaneProfitRatio,
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

/// Minimum simulation input for profitability probes (avoids dust-level phantom profits).
pub fn profit_probe_amount(token_decimals: u8, token_to_matic_rate: U256) -> U256 {
    min_economic_amount_in(token_decimals, token_to_matic_rate)
}

pub fn check_sim_sanity(input: SimSanityInput) -> Result<(), SimSanityReject> {
    if input.amount_in.is_zero() {
        return Err(SimSanityReject::AmountBelowEconomicFloor);
    }

    let floor = min_economic_amount_in(input.token_decimals, input.token_to_matic_rate);
    if input.amount_in < floor {
        return Err(SimSanityReject::AmountBelowEconomicFloor);
    }

    let max_profit = (input.amount_in * U256::from(MAX_SANE_PROFIT_RATIO_BPS)) / BPS_SCALE;
    if input.gross_profit > max_profit {
        return Err(SimSanityReject::InsaneProfitRatio);
    }

    // Brent pinned at the search floor with non-trivial profit → corrupt bounds/state.
    let pin_tolerance = input.search_low / U256::from(100u64);
    let pin_ceiling = input
        .search_low
        .saturating_add(pin_tolerance.max(U256::from(1u8)));
    if input.amount_in <= pin_ceiling && input.gross_profit > input.amount_in / U256::from(100u64) {
        return Err(SimSanityReject::OptimizerPinnedAtFloor);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_journal_phantom_amount_in_100() {
        let rate = U256::from(10u128).pow(U256::from(18u32));
        let err = check_sim_sanity(SimSanityInput {
            amount_in: U256::from(100u64),
            gross_profit: U256::from(30_585_511_224_037_666_497_434_726u128),
            search_low: U256::from(100u64),
            token_decimals: 18,
            token_to_matic_rate: rate,
        })
        .unwrap_err();
        assert!(matches!(
            err,
            SimSanityReject::AmountBelowEconomicFloor | SimSanityReject::InsaneProfitRatio
        ));
    }

    #[test]
    fn accepts_reasonable_triangle_profit() {
        let rate = U256::from(10u128).pow(U256::from(18u32));
        let amount_in = U256::from(10u128).pow(U256::from(18u32));
        assert!(
            check_sim_sanity(SimSanityInput {
                amount_in,
                gross_profit: amount_in / U256::from(100u64),
                search_low: amount_in / U256::from(10u64),
                token_decimals: 18,
                token_to_matic_rate: rate,
            })
            .is_ok()
        );
    }

    #[test]
    fn rejects_insane_roi_at_economic_size() {
        let rate = U256::from(10u128).pow(U256::from(18u32));
        let amount_in = U256::from(10u128).pow(U256::from(18u32));
        assert_eq!(
            check_sim_sanity(SimSanityInput {
                amount_in,
                gross_profit: amount_in * U256::from(2u8),
                search_low: amount_in / U256::from(10u64),
                token_decimals: 18,
                token_to_matic_rate: rate,
            }),
            Err(SimSanityReject::InsaneProfitRatio)
        );
    }
}
