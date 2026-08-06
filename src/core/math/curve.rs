use alloy::primitives::U256;
use smallvec::SmallVec;

use crate::core::constants::MAX_POOL_TOKENS;
use crate::core::types::CurvePoolState;

use super::fixed_point::ONE;
pub(crate) const CURVE_FEE_DENOMINATOR: U256 = U256::from_limbs([10_000_000_000, 0, 0, 0]);
const A_PRECISION: U256 = U256::from_limbs([100, 0, 0, 0]);
const ONE_U256: U256 = U256::from_limbs([1, 0, 0, 0]);
const TWO_U256: U256 = U256::from_limbs([2, 0, 0, 0]);
const MAX_ITERATIONS: u32 = 128;
/// Safety buffer on Curve dy (5e6 / 1e10 = 0.05%).
/// Covers multicall→exec drift after NG `_dynamic_fee` (2e10 offpeg default).
pub(crate) const CURVE_OUTPUT_BUFFER: U256 = U256::from_limbs([5_000_000, 0, 0, 0]);
/// Stable-NG example `offpeg_fee_multiplier` (2e10). Decode fallback when the
/// live `offpeg_fee_multiplier()` call is unavailable.
pub(crate) const DEFAULT_OFFPEG_FEE_MULTIPLIER: U256 = U256::from_limbs([20_000_000_000, 0, 0, 0]);

/// Convert Curve `fee()` (1e10 denom) to edge `fee_bps`.
#[must_use]
pub fn curve_fee_bps_from_pool(fee: U256) -> Option<u32> {
    if fee.is_zero() || fee >= CURVE_FEE_DENOMINATOR {
        return None;
    }
    let bps = (fee * U256::from(10_000u64)) / CURVE_FEE_DENOMINATOR;
    Some(bps.min(U256::from(9_999u64)).to::<u32>())
}

/// CurveStableSwapNGViews `_dynamic_fee(xpi, xpj, fee, offpeg_fee_multiplier)`.
#[must_use]
fn dynamic_fee(xpi: U256, xpj: U256, fee: U256, offpeg_mult: U256) -> U256 {
    if offpeg_mult <= CURVE_FEE_DENOMINATOR || xpi.is_zero() || xpj.is_zero() {
        return fee;
    }
    let sum = xpi + xpj;
    let xps2 = sum.saturating_mul(sum);
    if xps2.is_zero() {
        return fee;
    }
    // (_offpeg * fee) / ((_offpeg - FEE_DENOM) * 4 * xpi * xpj / xps2 + FEE_DENOM)
    let numerator = offpeg_mult.saturating_mul(fee);
    let prod = xpi.saturating_mul(xpj).saturating_mul(U256::from(4u8));
    let imbalance = ((offpeg_mult - CURVE_FEE_DENOMINATOR).saturating_mul(prod)) / xps2;
    let denom = imbalance.saturating_add(CURVE_FEE_DENOMINATOR);
    if denom.is_zero() {
        return fee;
    }
    numerator / denom
}

type CurveXp = SmallVec<[U256; MAX_POOL_TOKENS]>;

/// Pre-compute the stable-pool invariant D for a decode-time snapshot so quote
/// hot paths skip the `get_d` loop. Mirrors exactly what
/// [`try_curve_stable_amount_out`] would compute (same `to_xp` + `get_d`).
#[must_use]
pub(crate) fn curve_stable_cache_d(
    balances: &[U256],
    rates: &[U256],
    a: U256,
) -> Option<U256> {
    get_d(&to_xp(balances, rates)?, a)
}

fn get_d(xp: &[U256], a: U256) -> Option<U256> {
    if a.is_zero() || xp.len() < 2 || xp.iter().any(U256::is_zero) {
        return None;
    }

    let n = U256::from(xp.len());
    let s: U256 = xp.iter().copied().sum();
    if s.is_zero() {
        return Some(U256::ZERO);
    }

    let ann = a * n;
    if ann <= A_PRECISION {
        return None;
    }

    let mut d = s;
    let ann_s = (ann * s) / A_PRECISION;
    let ann_minus_p = ann - A_PRECISION;
    let n_plus_1 = n + U256::from(1);

    for _ in 0..MAX_ITERATIONS {
        let mut d_p = d;
        for x in xp {
            let xn = *x * n;
            if xn.is_zero() {
                return None;
            }
            d_p = (d_p * d) / xn;
        }
        let d_prev = d;
        // EVM reverts on overflow — checked_mul propagates as None, matching
        // on-chain behavior. saturating_mul silently produces wrong D values.
        let ann_term = ann_minus_p.checked_mul(d)? / A_PRECISION;
        let hop_term = n_plus_1.checked_mul(d_p)?;
        let denominator = ann_term.checked_add(hop_term)?;
        if denominator.is_zero() {
            return None;
        }
        d = ((ann_s + d_p * n) * d) / denominator;
        let diff = if d > d_prev { d - d_prev } else { d_prev - d };
        if diff <= ONE_U256 {
            return Some(d);
        }
    }
    Some(d)
}

fn get_y(x: U256, i: usize, j: usize, xp: &[U256], a: U256, d: U256) -> Option<U256> {
    let n = U256::from(xp.len());
    let ann = a * n;
    if ann.is_zero() || d.is_zero() {
        return None;
    }

    let mut s_ = U256::ZERO;
    let mut c = d;
    for (k, xk) in xp.iter().enumerate() {
        if k == j {
            continue;
        }
        let val = if k == i { x } else { *xk };
        s_ += val;
        let vn = val * n;
        if vn.is_zero() {
            return None;
        }
        c = (c * d) / vn;
    }

    c = (c * d * A_PRECISION) / (ann * n);
    let b = s_ + (d * A_PRECISION) / ann;

    let mut y = d;
    for _ in 0..MAX_ITERATIONS {
        let y_prev = y;
        let denominator = (TWO_U256 * y + b).saturating_sub(d);
        if denominator.is_zero() {
            return None;
        }
        y = (y * y + c) / denominator;
        let diff = if y > y_prev { y - y_prev } else { y_prev - y };
        if diff <= ONE_U256 {
            return Some(y);
        }
    }
    Some(y)
}

fn to_xp(balances: &[U256], rates: &[U256]) -> Option<CurveXp> {
    if !rates.is_empty() && rates.len() != balances.len() {
        return None;
    }

    let mut xp = CurveXp::with_capacity(balances.len());
    if rates.is_empty() {
        xp.extend_from_slice(balances);
    } else {
        xp.extend(
            balances
                .iter()
                .zip(rates.iter())
                .map(|(balance, rate)| (*balance * *rate) / ONE),
        );
    }
    Some(xp)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CurveStableReject {
    ZeroAmount,
    InvalidIndices,
    FeeTooHigh,
    Xp,
    DInvariant,
    Y,
    ZeroOut,
}

pub fn try_curve_stable_amount_out(
    state: &CurvePoolState,
    amount_in: U256,
    token_in_idx: usize,
    token_out_idx: usize,
) -> Result<U256, CurveStableReject> {
    if amount_in.is_zero() {
        return Err(CurveStableReject::ZeroAmount);
    }
    if state.a.is_zero()
        || token_in_idx == token_out_idx
        || token_in_idx >= state.balances.len()
        || token_out_idx >= state.balances.len()
    {
        return Err(CurveStableReject::InvalidIndices);
    }
    if state.fee >= CURVE_FEE_DENOMINATOR {
        return Err(CurveStableReject::FeeTooHigh);
    }
    let Some(xp) = to_xp(&state.balances, &state.rates) else {
        return Err(CurveStableReject::Xp);
    };
    let d = state
        .d
        .filter(|d| !d.is_zero())
        .or_else(|| get_d(&xp, state.a))
        .filter(|d| !d.is_zero())
        .ok_or(CurveStableReject::DInvariant)?;
    let in_rate = state.rates.get(token_in_idx).copied().unwrap_or(ONE);
    let x = xp[token_in_idx] + (amount_in * in_rate) / ONE;
    let Some(y) = get_y(x, token_in_idx, token_out_idx, &xp, state.a, d) else {
        return Err(CurveStableReject::Y);
    };
    let dy = xp[token_out_idx].saturating_sub(y).saturating_sub(ONE_U256);
    if dy.is_zero() {
        return Err(CurveStableReject::ZeroOut);
    }
    // NG `_get_dy` uses `_dynamic_fee(avg_xp_i, avg_xp_j, fee)` with the live
    // `offpeg_fee_multiplier`; classic CurveStableSwap charges a static fee
    // scaled by N/(4·(N−1)). `offpeg_fee_multiplier: None` marks a classic pool.
    let n_coins = U256::from(state.balances.len());
    let fee = match state.offpeg_fee_multiplier {
        Some(offpeg) => {
            let xpi_avg = (xp[token_in_idx] + x) / TWO_U256;
            let xpj_avg = (xp[token_out_idx] + y) / TWO_U256;
            dynamic_fee(xpi_avg, xpj_avg, state.fee, offpeg)
        }
        None => {
            let static_fee = state.fee * n_coins;
            static_fee / (U256::from(4u8) * (n_coins - U256::from(1u8)))
        }
    };
    let fee_amount = (dy * fee) / CURVE_FEE_DENOMINATOR;
    let dy_after_fee = dy.saturating_sub(fee_amount);
    let dy_buffered =
        dy_after_fee.saturating_sub((dy_after_fee * CURVE_OUTPUT_BUFFER) / CURVE_FEE_DENOMINATOR);
    if dy_buffered.is_zero() {
        return Err(CurveStableReject::ZeroOut);
    }
    let out_rate = state.rates.get(token_out_idx).copied().unwrap_or(ONE);
    if out_rate.is_zero() {
        return Err(CurveStableReject::ZeroOut);
    }
    Ok((dy_buffered * ONE) / out_rate)
}

#[inline]
#[must_use]
pub fn get_curve_stable_amount_out(
    state: &CurvePoolState,
    amount_in: U256,
    token_in_idx: usize,
    token_out_idx: usize,
) -> U256 {
    try_curve_stable_amount_out(state, amount_in, token_in_idx, token_out_idx).unwrap_or(U256::ZERO)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::math::fixed_point::ONE;

    #[test]
    fn curve_fee_bps_from_1e10_denom() {
        assert_eq!(curve_fee_bps_from_pool(U256::from(5_000_000u64)), Some(5));
        assert_eq!(curve_fee_bps_from_pool(U256::ZERO), None);
        assert_eq!(curve_fee_bps_from_pool(CURVE_FEE_DENOMINATOR), None);
    }

    #[test]
    fn dynamic_fee_doubles_when_one_side_dust() {
        let fee = U256::from(5_000_000u64);
        let balanced = dynamic_fee(ONE, ONE, fee, DEFAULT_OFFPEG_FEE_MULTIPLIER);
        // At equal xp, 4*x*y/(x+y)^2 = 1 → denom = offpeg → fee unchanged.
        assert_eq!(balanced, fee);
        let skewed = dynamic_fee(
            ONE,
            ONE * U256::from(1_000_000u64),
            fee,
            DEFAULT_OFFPEG_FEE_MULTIPLIER,
        );
        assert!(skewed > fee, "skewed={skewed} fee={fee}");
        assert!(skewed <= fee * U256::from(2u8), "skewed={skewed}");
    }

    /// Classic CurveStableSwap charges `fee * N / (4 * (N - 1))` — half the
    /// pool `fee()` for 2-coin pools — while StableSwapNG uses `fee()` directly.
    #[test]
    fn classic_two_coin_charges_half_static_fee() {
        let fee = U256::from(50_000_000u64); // 0.5% → classic 0.25%, NG 0.5%.
        let classic = CurvePoolState {
            balances: vec![
                U256::from(1_000_000u64) * ONE,
                U256::from(1_000_000u64) * ONE,
            ],
            a: U256::from(1_000u64),
            fee,
            rates: vec![ONE, ONE],
            n_coins: 2,
            gamma: None,
            d: None,
            offpeg_fee_multiplier: None,
        };
        let ng = CurvePoolState {
            offpeg_fee_multiplier: Some(DEFAULT_OFFPEG_FEE_MULTIPLIER),
            ..classic.clone()
        };
        let ain = U256::from(10_000u64) * ONE;
        let out_classic = try_curve_stable_amount_out(&classic, ain, 0, 1).expect("classic");
        let out_ng = try_curve_stable_amount_out(&ng, ain, 0, 1).expect("ng");
        // Bear: classic's lower fee must yield a strictly higher output.
        assert!(out_classic > out_ng);
        // Measured fee gap is half the pool fee (~25 bps for 0.5% fee).
        let classic_fee_bps = ((out_classic - out_ng) * U256::from(10_000u64)) / out_ng;
        let half_fee_bps = (fee * U256::from(5_000u64)) / CURVE_FEE_DENOMINATOR;
        assert!(classic_fee_bps > U256::from(20u64), "gap only {classic_fee_bps} bps");
        assert!(classic_fee_bps <= half_fee_bps + U256::from(5u64), "gap too wide");
    }

    #[test]
    fn test_zero_amount_returns_zero() {
        let state = CurvePoolState {
            balances: vec![],
            a: U256::ZERO,
            fee: U256::ZERO,
            rates: vec![],
            n_coins: 0,
            gamma: None,
            d: None,
            offpeg_fee_multiplier: None,
        };
        assert_eq!(
            get_curve_stable_amount_out(&state, U256::ZERO, 0, 1),
            U256::ZERO
        );
    }

    #[test]
    fn stable_two_coin_swap_returns_positive_output_within_reserve() {
        let state = CurvePoolState {
            balances: vec![
                U256::from(1_000_000u64) * ONE,
                U256::from(1_000_000u64) * ONE,
            ],
            a: U256::from(1_000u64),
            fee: U256::from(1_000_000u64),
            rates: vec![ONE, ONE],
            n_coins: 2,
            gamma: None,
            d: None,
            offpeg_fee_multiplier: Some(crate::core::math::curve::DEFAULT_OFFPEG_FEE_MULTIPLIER),
        };
        let out = get_curve_stable_amount_out(&state, U256::from(10_000u64) * ONE, 0, 1);
        assert!(out > U256::ZERO);
        assert!(out <= state.balances[1]);
    }

    #[test]
    fn stable_ng_quote_matches_captured_wstpol_wpol_pool_magnitude() {
        let state = CurvePoolState {
            balances: vec![
                U256::from(44_342_439_882_218_174_841_778u128),
                U256::from(76_448_922_753_221_051_110_075u128),
            ],
            a: U256::from(500u64),
            fee: U256::from(1_000_000u64),
            rates: vec![U256::from(1_340_225_880_628_655_045u128), ONE],
            n_coins: 2,
            gamma: None,
            d: None,
            offpeg_fee_multiplier: Some(crate::core::math::curve::DEFAULT_OFFPEG_FEE_MULTIPLIER),
        };

        let out = get_curve_stable_amount_out(&state, U256::from(152_587_890_625_000u64), 1, 0);
        assert!(out > U256::from(100_000_000_000_000u64), "out={out}");
        assert!(out < U256::from(120_000_000_000_000u64), "out={out}");
    }

    #[test]
    fn stable_ng_dai_pool_quote_near_onchain_get_dy_with_a_precise() {
        // Live 0xd9e5… get_dy(1,0,1e17) ≈ 1.007e17. A()=1000 → A_precise=100000.
        let state = CurvePoolState {
            balances: vec![
                U256::from(56_835_580_758_005_775_099u128),
                U256::from(7_653_346_903_703_122_284u128),
            ],
            a: U256::from(100_000u64),
            fee: U256::from(5_000_000u64),
            rates: vec![ONE, ONE],
            n_coins: 2,
            gamma: None,
            d: None,
            offpeg_fee_multiplier: Some(crate::core::math::curve::DEFAULT_OFFPEG_FEE_MULTIPLIER),
        };
        let out = get_curve_stable_amount_out(&state, U256::from(100_000_000_000_000_000u64), 1, 0);
        let onchain = U256::from(100_751_662_013_279_075u64);
        let lo = onchain * U256::from(99u64) / U256::from(100u64);
        let hi = onchain * U256::from(101u64) / U256::from(100u64);
        assert!(out >= lo && out <= hi, "out={out} onchain={onchain}");
    }

    /// GHST/stGHST Stable-NG (0x4b3e…): dry-run hop2 transferAll empty-revert when Curve
    /// leaves 0 intermediate. Local quote must not wildly diverge from get_dy.
    #[test]
    fn ghst_stghst_ng_quote_near_onchain_get_dy() {
        // Live balances/A/fee; get_dy(1,0,397790001835358637)=269816550691123458
        let state = CurvePoolState {
            balances: vec![
                U256::from(913_061_441_281_512_614u128),
                U256::from(14_632_452_263_254_568_709u128),
            ],
            a: U256::from(10_000u64), // A()=100 → A_precise
            fee: U256::from(4_000_000u64),
            rates: vec![ONE, ONE],
            n_coins: 2,
            gamma: None,
            d: None,
            offpeg_fee_multiplier: Some(crate::core::math::curve::DEFAULT_OFFPEG_FEE_MULTIPLIER),
        };
        let ain = U256::from(397_790_001_835_358_637u128);
        let out = try_curve_stable_amount_out(&state, ain, 1, 0).expect("quote");
        let chain = U256::from(269_816_550_691_123_458u128);
        let lo = chain * U256::from(95u64) / U256::from(100u64);
        let hi = chain * U256::from(105u64) / U256::from(100u64);
        assert!(
            out >= lo && out <= hi,
            "local={out} chain={chain} (bps={})",
            out * U256::from(10_000u64) / chain
        );
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use crate::core::math::proptest_util::u256_nonzero;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn output_bounded_by_reserve(
            amount_in in u256_nonzero(),
            a in (1u64..1_000_000u64).prop_map(U256::from),
            balance0 in u256_nonzero(),
            balance1 in u256_nonzero(),
        ) {
            let state = CurvePoolState {
                balances: vec![balance0, balance1],
                a,
                fee: U256::ZERO,
                rates: vec![],
                n_coins: 2,
                gamma: None,
                d: None,
                offpeg_fee_multiplier: None,
            };

            let out = get_curve_stable_amount_out(&state, amount_in, 0, 1);
            if !out.is_zero() {
                prop_assert!(
                    out <= state.balances[1],
                    "out={out} exceeds balance={}",
                    state.balances[1]
                );
            }
        }

        #[test]
        fn identical_tokens_return_zero(
            amount_in in u256_nonzero(),
            a in (1u64..1_000_000u64).prop_map(U256::from),
            balance in u256_nonzero(),
        ) {
            let state = CurvePoolState {
                balances: vec![balance, balance],
                a,
                fee: U256::ZERO,
                rates: vec![],
                n_coins: 2,
                gamma: None,
                d: None,
                offpeg_fee_multiplier: None,
            };
            let out = get_curve_stable_amount_out(&state, amount_in, 0, 0);
            prop_assert!(out.is_zero());
        }
    }
}
