use ruint::aliases::U256;

use crate::core::constants::{BPS_SCALE, DEFAULT_FEE_NUMERATOR, FEE_DENOMINATOR};
use crate::core::types::V2PoolState;

#[inline]
fn fits_u128(v: U256) -> bool {
    v.as_limbs()[2] == 0 && v.as_limbs()[3] == 0
}

#[inline]
fn to_u128(v: U256) -> u128 {
    debug_assert!(fits_u128(v));
    v.as_limbs()[0] as u128 | ((v.as_limbs()[1] as u128) << 64)
}

#[inline]
fn get_amount_out_u128(
    amount_in: u128,
    reserve_in: u128,
    reserve_out: u128,
    fee_numerator: u128,
    fee_denominator: u128,
) -> Option<u128> {
    let amount_in_with_fee = amount_in.checked_mul(fee_numerator)?;
    let numerator = amount_in_with_fee.checked_mul(reserve_out)?;
    let denominator = reserve_in
        .checked_mul(fee_denominator)?
        .checked_add(amount_in_with_fee)?;
    if denominator == 0 {
        return None;
    }
    Some(numerator / denominator)
}

#[inline]
fn get_amount_in_u128(
    amount_out: u128,
    reserve_in: u128,
    reserve_out: u128,
    fee_numerator: u128,
    fee_denominator: u128,
) -> Option<u128> {
    let numerator = reserve_in
        .checked_mul(amount_out)?
        .checked_mul(fee_denominator)?;
    let denominator = reserve_out
        .checked_sub(amount_out)?
        .checked_mul(fee_numerator)?;
    if denominator == 0 {
        return None;
    }
    Some(numerator / denominator + 1)
}

/// Fee numerator for constant-product swap: keeps (10000 - feeBps) / 10000 of input.
#[inline]
pub fn v2_fee_numerator_from_bps(fee_bps: U256, fee_denominator: U256) -> U256 {
    if fee_bps.is_zero() || fee_bps >= BPS_SCALE || fee_denominator.is_zero() {
        return U256::ZERO;
    }
    (fee_denominator * (BPS_SCALE - fee_bps)) / BPS_SCALE
}

#[inline]
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

    if fits_u128(amount_in)
        && fits_u128(reserve_in)
        && fits_u128(reserve_out)
        && fits_u128(fee_numerator)
        && fits_u128(fee_denominator)
        && let Some(out) = get_amount_out_u128(
            to_u128(amount_in),
            to_u128(reserve_in),
            to_u128(reserve_out),
            to_u128(fee_numerator),
            to_u128(fee_denominator),
        )
    {
        return U256::from(out);
    }

    let amount_in_with_fee = amount_in * fee_numerator;
    let numerator = amount_in_with_fee * reserve_out;
    let denominator = reserve_in * fee_denominator + amount_in_with_fee;
    numerator / denominator
}

#[inline]
pub fn get_amount_in(
    amount_out: U256,
    reserve_in: U256,
    reserve_out: U256,
    fee_numerator: U256,
    fee_denominator: U256,
) -> U256 {
    if amount_out.is_zero()
        || reserve_in.is_zero()
        || reserve_out.is_zero()
        || fee_numerator.is_zero()
        || fee_denominator.is_zero()
        || fee_numerator >= fee_denominator
        || amount_out >= reserve_out
    {
        return U256::ZERO;
    }

    if fits_u128(amount_out)
        && fits_u128(reserve_in)
        && fits_u128(reserve_out)
        && fits_u128(fee_numerator)
        && fits_u128(fee_denominator)
        && let Some(ain) = get_amount_in_u128(
            to_u128(amount_out),
            to_u128(reserve_in),
            to_u128(reserve_out),
            to_u128(fee_numerator),
            to_u128(fee_denominator),
        )
    {
        return U256::from(ain);
    }

    let numerator = reserve_in * amount_out * fee_denominator;
    let denominator = (reserve_out - amount_out) * fee_numerator;
    numerator / denominator + U256::from(1)
}

pub struct V2Fee {
    pub numerator: U256,
    pub denominator: U256,
}

pub fn resolve_v2_fee(state: &V2PoolState) -> V2Fee {
    if !state.fee.is_zero() && !state.fee_denominator.is_zero() {
        return V2Fee {
            numerator: state.fee,
            denominator: state.fee_denominator,
        };
    }
    V2Fee {
        numerator: DEFAULT_FEE_NUMERATOR,
        denominator: FEE_DENOMINATOR,
    }
}

pub fn resolve_v2_fee_with_edge(state: &V2PoolState, edge_fee_bps: Option<u32>) -> V2Fee {
    if let Some(bps) = edge_fee_bps {
        let num = v2_fee_numerator_from_bps(U256::from(bps), FEE_DENOMINATOR);
        if !num.is_zero() {
            return V2Fee {
                numerator: num,
                denominator: FEE_DENOMINATOR,
            };
        }
    }
    resolve_v2_fee(state)
}

pub fn simulate_v2_swap(
    state: &V2PoolState,
    amount_in: U256,
    zero_for_one: bool,
    edge_fee_bps: Option<u32>,
) -> U256 {
    let fee = resolve_v2_fee_with_edge(state, edge_fee_bps);
    let (reserve_in, reserve_out) = if zero_for_one {
        (state.reserve0, state.reserve1)
    } else {
        (state.reserve1, state.reserve0)
    };
    get_amount_out(
        amount_in,
        reserve_in,
        reserve_out,
        fee.numerator,
        fee.denominator,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fee_numerator_from_bps() {
        assert_eq!(
            v2_fee_numerator_from_bps(U256::from(30), FEE_DENOMINATOR),
            U256::from(997)
        );
        assert_eq!(
            v2_fee_numerator_from_bps(U256::from(25), FEE_DENOMINATOR),
            U256::from(997)
        );
        assert_eq!(
            v2_fee_numerator_from_bps(U256::from(20), FEE_DENOMINATOR),
            U256::from(998)
        );
    }

    #[test]
    fn constant_product_swap() {
        let state = V2PoolState {
            reserve0: U256::from(1_000_000),
            reserve1: U256::from(2_000_000),
            fee: U256::ZERO,
            fee_denominator: U256::ZERO,
        };
        let out = simulate_v2_swap(&state, U256::from(10_000), true, None);
        let expected = get_amount_out(
            U256::from(10_000),
            U256::from(1_000_000),
            U256::from(2_000_000),
            DEFAULT_FEE_NUMERATOR,
            FEE_DENOMINATOR,
        );
        assert_eq!(out, expected);
        assert!(out > U256::ZERO);
    }

    #[test]
    fn u128_fast_path_matches_u256_for_realistic_reserves() {
        let state = V2PoolState {
            reserve0: U256::from(1_000_000u128) * U256::from(10u128).pow(U256::from(18)),
            reserve1: U256::from(2_000_000u128) * U256::from(10u128).pow(U256::from(18)),
            fee: U256::ZERO,
            fee_denominator: U256::ZERO,
        };
        let amount_in = U256::from(10u128).pow(U256::from(18));
        let out = simulate_v2_swap(&state, amount_in, true, Some(30));
        assert!(out > U256::ZERO);

        let fee = resolve_v2_fee_with_edge(&state, Some(30));
        let direct = get_amount_out(
            amount_in,
            state.reserve0,
            state.reserve1,
            fee.numerator,
            fee.denominator,
        );
        assert_eq!(out, direct);
    }
}
