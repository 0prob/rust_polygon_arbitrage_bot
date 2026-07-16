use crate::util::u512_to_u256;
use alloy::primitives::{U256, U512};

use crate::core::constants::{DEFAULT_FEE_NUMERATOR, FEE_DENOMINATOR};
use crate::core::types::V2PoolState;

#[inline]
#[must_use]
pub fn get_amount_out(
    amount_in: U256,
    reserve_in: U256,
    reserve_out: U256,
    fee_numerator: U256,
    fee_denominator: U256,
) -> U256 {
    if amount_in.is_zero()
        || reserve_in.is_zero()
        || reserve_out.is_zero()
        || fee_numerator.is_zero()
        || fee_denominator.is_zero()
        || fee_numerator >= fee_denominator
    {
        return U256::ZERO;
    }

    // U512 widening prevents silent overflow on deep pools with large reserves.
    // amount_in * fee_numerator * reserve_out can exceed U256::MAX for
    // high-liquidity pairs (e.g. WMATIC/USDC with 10^18+ balances).
    let amount_in_with_fee = U512::from(amount_in) * U512::from(fee_numerator);
    let numerator = amount_in_with_fee * U512::from(reserve_out);
    let den_part = U512::from(reserve_in) * U512::from(fee_denominator);
    let denominator = den_part + amount_in_with_fee;
    if denominator.is_zero() {
        return U256::ZERO;
    }
    let result = numerator / denominator;
    if result.is_zero() {
        return U256::ZERO;
    }
    u512_to_u256(result)
}

#[must_use]
pub fn resolve_v2_fee_with_edge(state: &V2PoolState, edge_fee_bps: Option<u32>) -> (U256, U256) {
    if let Some(bps) = edge_fee_bps
        && bps > 0
        && bps < 10000
    {
        let num = U256::from(10_000u64 - u64::from(bps));
        return (num, U256::from(10_000u64));
    }
    if !state.fee.is_zero() && !state.fee_denominator.is_zero() && state.fee < state.fee_denominator
    {
        return (state.fee, state.fee_denominator);
    }
    (DEFAULT_FEE_NUMERATOR, FEE_DENOMINATOR)
}

#[inline]
#[must_use]
pub fn simulate_v2_swap(
    state: &V2PoolState,
    amount_in: U256,
    zero_for_one: bool,
    edge_fee_bps: Option<u32>,
) -> U256 {
    let (numerator, denominator) = resolve_v2_fee_with_edge(state, edge_fee_bps);
    let (reserve_in, reserve_out) = if zero_for_one {
        (state.reserve0, state.reserve1)
    } else {
        (state.reserve1, state.reserve0)
    };
    get_amount_out(amount_in, reserve_in, reserve_out, numerator, denominator)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::V2PoolState;

    #[test]
    fn edge_fee_bps_matches_pool_fee_for_standard_tiers() {
        let state = V2PoolState {
            reserve0: U256::from(1u64),
            reserve1: U256::from(1u64),
            fee: U256::ZERO,
            fee_denominator: U256::ZERO,
            block_timestamp_last: 0,
        };
        let (num, den) = resolve_v2_fee_with_edge(&state, Some(30));
        assert_eq!(num, U256::from(9970u64));
        assert_eq!(den, U256::from(10_000u64));
        let (num, den) = resolve_v2_fee_with_edge(&state, Some(5));
        assert_eq!(num, U256::from(9995u64));
        assert_eq!(den, U256::from(10_000u64));
    }

    #[test]
    fn test_get_amount_out_returns_nonzero() {
        let out = get_amount_out(
            U256::from(1_000_000_000_000_000_000u64),
            U256::from(10_000_000_000_000_000_000u64),
            U256::from(200_000_000u64),
            U256::from(997u64),
            U256::from(1000u64),
        );
        assert!(out > U256::ZERO);
    }
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn output_bounded_by_reserve(
            amount_in in 1u128..=u128::MAX,
            reserve_in in 1u128..=u128::MAX,
            reserve_out in 1u128..=u128::MAX,
            fee_num in 1u64..10000u64,
        ) {
            let amount_in = U256::from(amount_in);
            let reserve_in = U256::from(reserve_in);
            let reserve_out = U256::from(reserve_out);
            let fee_numerator = U256::from(fee_num);
            let fee_denominator = U256::from(10000u64);

            if fee_numerator >= fee_denominator {
                return Ok(());
            }

            let out = get_amount_out(amount_in, reserve_in, reserve_out, fee_numerator, fee_denominator);
            if !out.is_zero() {
                prop_assert!(out <= reserve_out,
                    "output {} exceeds reserve {}", out, reserve_out);
            }
        }

        #[test]
        fn zero_in_returns_zero(
            reserve_in in 1u128..=u128::MAX,
            reserve_out in 1u128..=u128::MAX,
            fee_num in 1u64..10000u64,
        ) {
            let out = get_amount_out(
                U256::ZERO,
                U256::from(reserve_in),
                U256::from(reserve_out),
                U256::from(fee_num),
                U256::from(10000u64),
            );
            prop_assert!(out.is_zero());
        }
    }
}
