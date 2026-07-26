use alloy::primitives::{Address, Bytes, U256};
use alloy::sol_types::SolCall;
use std::sync::Arc;

use crate::abis::{
    IBalancerLinearPool, IBalancerPool, IBalancerVaultRead, ICurveCryptoPool, ICurvePool,
    IDodoPoolState,
};
use crate::core::math::fixed_point::ONE;

use crate::core::types::{
    BalancerLinearState, BalancerPoolKind, BalancerPoolState, CurvePoolState, DodoPoolState,
    DodoRState, PoolState, ProtocolType, V2PoolState, V3PoolState, V4PoolState,
};
use crate::core::v4_storage::{decode_v4_liquidity, decode_v4_slot0};
use crate::pipeline::abi_cache::{decode_abi_word, decode_algebra_global_state};

use super::plans::{CallKind, PoolFetchPlan};

#[inline]
fn decode_u256(bytes: &Bytes) -> Option<U256> {
    decode_abi_word(bytes)
}

#[inline]
fn decode_address(bytes: &Bytes) -> Option<Address> {
    if bytes.len() < 32 {
        return None;
    }
    // Address is last 20 bytes of the 32-byte word
    Some(Address::from_slice(&bytes[12..32]))
}

/// Zero-copy V2 getReserves decode (reserve0, reserve1, blockTimestampLast as 32-byte ABI words).
pub fn decode_v2_reserves(bytes: &[u8]) -> Option<(U256, U256, u32)> {
    if bytes.len() < 96 {
        return None;
    }
    let r0 = U256::from_be_slice(&bytes[0..32]);
    let r1 = U256::from_be_slice(&bytes[32..64]);
    let ts = U256::from_be_slice(&bytes[64..96]);
    Some((r0, r1, ts.as_limbs()[0] as u32))
}

/// Zero-copy V3 slot0 decode (sqrtPriceX96, tick, unlocked, feeProtocol, observationCardinality).
/// slot0() returns 7 ABI words (224 bytes, each field in its own 32-byte slot).
pub fn decode_v3_slot0(bytes: &[u8]) -> Option<(U256, i32, bool, u8, u16)> {
    if bytes.len() < 224 {
        return None;
    }
    let sqrt = U256::from_be_slice(&bytes[0..32]);
    let tick = crate::util::sign_extend_tick24(U256::from_be_slice(&bytes[32..64]));
    let obs_card = U256::from_be_slice(&bytes[96..128]).as_limbs()[0] as u16;
    let fee_proto = U256::from_be_slice(&bytes[160..192]).as_limbs()[0] as u8;
    let unlocked = U256::from_be_slice(&bytes[192..224]).as_limbs()[0] != 0;
    Some((sqrt, tick, unlocked, fee_proto, obs_card))
}

pub(super) fn decode_plan(plan: &PoolFetchPlan, results: &[Option<Bytes>]) -> Option<PoolState> {
    if results.len() != plan.kinds.len() {
        return None;
    }
    match plan.pool.protocol {
        ProtocolType::UniswapV2 => decode_v2(plan, results),
        ProtocolType::UniswapV3 => decode_v3(plan, results),
        ProtocolType::UniswapV4 => decode_v4(plan, results),
        ProtocolType::Dodo => decode_dodo(plan, results),
        ProtocolType::CurveStable => decode_curve_stable(plan, results),
        ProtocolType::CurveCrypto => decode_curve_crypto(plan, results),
        ProtocolType::BalancerV2 => decode_balancer(plan, results),
        ProtocolType::Woofi => None,
    }
}

fn decode_v2(plan: &PoolFetchPlan, results: &[Option<Bytes>]) -> Option<PoolState> {
    let bytes = results.first()?.as_ref()?;
    let (r0, r1, block_ts) = decode_v2_reserves(bytes)?;
    if r0.is_zero() || r1.is_zero() {
        return None;
    }
    let (fee, fee_denominator) = v2_stored_fee_from_bps(plan.pool.fee_bps);
    Some(PoolState::V2(V2PoolState {
        reserve0: r0,
        reserve1: r1,
        fee,
        fee_denominator,
        block_timestamp_last: block_ts,
    }))
}

#[inline]
fn v2_stored_fee_from_bps(fee_bps: u32) -> (U256, U256) {
    let bps = fee_bps.min(9_999);
    (
        U256::from(10_000u64 - u64::from(bps)),
        U256::from(10_000u64),
    )
}

fn decode_v3_head(
    bytes: &Bytes,
    prefer_algebra: bool,
) -> Option<(U256, i32, bool, Option<U256>, u32, u16)> {
    if prefer_algebra {
        // Do not fall back to Uni slot0 on the same globalState bytes — a ≥224B
        // Algebra payload can look like a plausible slot0 and skip the real slot0 result.
        return decode_algebra_global_state(bytes)
            .map(|(sqrt, tick, unlocked, fee)| (sqrt, tick, unlocked, Some(fee), 0, 0));
    }
    decode_v3_slot0(bytes)
        .map(|(sqrt, tick, unlocked, fee_protocol, obs_card)| {
            (
                sqrt,
                tick,
                unlocked,
                None,
                u32::from(fee_protocol),
                obs_card,
            )
        })
        .or_else(|| {
            decode_algebra_global_state(bytes)
                .map(|(sqrt, tick, unlocked, fee)| (sqrt, tick, unlocked, Some(fee), 0, 0))
        })
}

fn decode_v3(plan: &PoolFetchPlan, results: &[Option<Bytes>]) -> Option<PoolState> {
    // One pass over kinds — no repeated linear scans for head/liq/fee.
    let mut global_bytes = None;
    let mut slot0_bytes = None;
    let mut liq_bytes = None;
    let mut fee_bytes = None;
    for (result, kind) in results.iter().zip(plan.kinds.iter()) {
        match kind {
            CallKind::V3GlobalState => global_bytes = result.as_ref(),
            CallKind::V3Slot0 => slot0_bytes = result.as_ref(),
            CallKind::V3Liquidity => liq_bytes = result.as_ref(),
            CallKind::V3Fee => fee_bytes = result.as_ref(),
            _ => {}
        }
    }
    let (sqrt, tick, unlocked, algebra_fee, fee_protocol, obs_card) = global_bytes
        .and_then(|b| decode_v3_head(b, true))
        .or_else(|| slot0_bytes.and_then(|b| decode_v3_head(b, false)))?;
    let liquidity = decode_u256(liq_bytes?)?.as_limbs()[0] as u128;
    if sqrt.is_zero() || liquidity == 0 {
        return None;
    }
    let fee_pips = algebra_fee
        .or_else(|| {
            fee_bytes
                .and_then(decode_u256)
                .map(|v| U256::from(v.as_limbs()[0] as u32))
        })
        .unwrap_or_else(|| U256::from(plan.pool.fee_bps) * U256::from(100u32));
    Some(PoolState::V3(V3PoolState {
        sqrt_price_x96: sqrt,
        tick,
        liquidity,
        fee: fee_pips,
        tick_spacing: plan.pool.tick_spacing.unwrap_or(60),
        ticks: Arc::from([] as [crate::core::types::V3Tick; 0]),
        unlocked,
        fee_protocol,
        observation_cardinality: obs_card,
    }))
}

fn decode_v4(plan: &PoolFetchPlan, results: &[Option<Bytes>]) -> Option<PoolState> {
    let slot0_bytes = results.first()?.as_ref()?;
    let liq_bytes = results.get(1)?.as_ref()?;
    let slot0_raw = decode_u256(slot0_bytes)?;
    let liq_raw = decode_u256(liq_bytes)?;
    let decoded = decode_v4_slot0(slot0_raw);
    let liquidity = decode_v4_liquidity(liq_raw);
    if decoded.sqrt_price_x96.is_zero() || liquidity == 0 {
        return None;
    }
    // slot0 packs lpFee; 0 is a valid V4 zero-fee / dynamic-fee pool — do not
    // fall back to indexer fee_bps (that overcharges and kills real arbs).
    let fee = U256::from(decoded.lp_fee);
    Some(PoolState::V4(V4PoolState {
        sqrt_price_x96: decoded.sqrt_price_x96,
        tick: decoded.tick,
        liquidity,
        fee,
        tick_spacing: plan.pool.tick_spacing.unwrap_or(60),
        ticks: Arc::from([] as [crate::core::types::V3Tick; 0]),
        unlocked: true,
        fee_protocol: decoded.protocol_fee,
        observation_cardinality: 1,
    }))
}

fn decode_dodo(plan: &PoolFetchPlan, results: &[Option<Bytes>]) -> Option<PoolState> {
    let mut base_b = None;
    let mut quote_b = None;
    let mut base_token_b = None;
    let mut quote_token_b = None;
    let mut i_b = None;
    let mut k_b = None;
    let mut lp_fee_b = None;
    let mut mt_fee_b: Option<Option<&Bytes>> = None;
    let mut pmm_b = None;
    for (result, kind) in results.iter().zip(plan.kinds.iter()) {
        match kind {
            CallKind::DodoBase => base_b = result.as_ref(),
            CallKind::DodoQuote => quote_b = result.as_ref(),
            CallKind::DodoBaseToken => base_token_b = result.as_ref(),
            CallKind::DodoQuoteToken => quote_token_b = result.as_ref(),
            CallKind::DodoI => i_b = result.as_ref(),
            CallKind::DodoK => k_b = result.as_ref(),
            CallKind::DodoLpFee => lp_fee_b = result.as_ref(),
            // Distinguish "call not in plan" from "call failed" (None result).
            CallKind::DodoMtFee => mt_fee_b = Some(result.as_ref()),
            CallKind::DodoPmmState => pmm_b = result.as_ref(),
            _ => {}
        }
    }
    let base = decode_u256(base_b?)?;
    let quote = decode_u256(quote_b?)?;
    let base_token = decode_address(base_token_b?)?;
    let quote_token = decode_address(quote_token_b?)?;
    let i = decode_u256(i_b?)?;
    let k = decode_u256(k_b?)?;
    let lp_fee_rate = decode_u256(lp_fee_b?)?;
    // Polygon DVM/DPP/DSP: `_MT_FEE_RATE_()` reverts (fee lives on `_MT_FEE_RATE_MODEL_`).
    // Soft-fail → mt=0 rather than dropping the pool from the arena/TUI.
    // When the view succeeds (some private pools), keep the live rate.
    let mt_fee_rate = match mt_fee_b {
        Some(Some(bytes)) => decode_u256(bytes).unwrap_or(U256::ZERO),
        _ => U256::ZERO,
    };
    let pmm = IDodoPoolState::getPMMStateForCallCall::abi_decode_returns(pmm_b?).ok()?;
    let pmm_i = U256::from(pmm.i);
    let pmm_k = U256::from(pmm.K);
    let r_state = match pmm.R {
        r if r == U256::ZERO => DodoRState::One,
        r if r == U256::ONE => DodoRState::AboveOne,
        r if r == U256::from(2u8) => DodoRState::BelowOne,
        _ => return None,
    };
    if base.is_zero()
        || quote.is_zero()
        || base_token.is_zero()
        || quote_token.is_zero()
        || pmm_i.is_zero()
        || pmm_k > ONE
        || lp_fee_rate >= ONE
        || mt_fee_rate >= ONE
        || lp_fee_rate.saturating_add(mt_fee_rate) >= ONE
        || i != pmm_i
        || k != pmm_k
        || U256::from(pmm.B) != base
        || U256::from(pmm.Q) != quote
        || U256::from(pmm.B0).is_zero()
        || U256::from(pmm.Q0).is_zero()
    {
        return None;
    }
    Some(PoolState::Dodo(DodoPoolState {
        base_reserve: base,
        quote_reserve: quote,
        base_token,
        quote_token,
        base_target: U256::from(pmm.B0),
        quote_target: U256::from(pmm.Q0),
        r_state,
        i: pmm_i,
        k: pmm_k,
        lp_fee_rate,
        mt_fee_rate,
    }))
}

fn decode_curve_balances(plan: &PoolFetchPlan, results: &[Option<Bytes>]) -> Option<Vec<U256>> {
    let n_fetched = super::plans::curve_balance_slots(plan.pool.tokens.len());
    let mut balances = Vec::with_capacity(n_fetched);
    for i in 0..n_fetched {
        let b = results
            .get(i)
            .and_then(|r| r.as_ref())
            .and_then(decode_u256)
            .unwrap_or(U256::ZERO);
        balances.push(b);
    }
    if balances.iter().any(U256::is_zero) {
        return None;
    }
    Some(balances)
}

fn decode_curve_fee(results: &[Option<Bytes>], fee_idx: usize) -> Option<U256> {
    decode_u256(results.get(fee_idx)?.as_ref()?)
}

fn decode_curve_stored_rates(
    results: &[Option<Bytes>],
    n_fetched: usize,
    rates_idx: usize,
) -> Option<Vec<U256>> {
    let rates = results
        .get(rates_idx)
        .and_then(|b| b.as_ref())
        .and_then(|b| ICurvePool::stored_ratesCall::abi_decode_returns(b).ok())
        .map(|r| r.iter().map(|&x| U256::from(x)).collect::<Vec<_>>());
    let rates = rates?;
    if rates.len() != n_fetched || rates.iter().any(U256::is_zero) {
        return None;
    }
    Some(rates)
}

fn decode_curve_crypto_rates(
    n_fetched: usize,
    price_scale: U256,
    precisions: [U256; 2],
) -> Option<Vec<U256>> {
    if n_fetched != 2 || price_scale.is_zero() || precisions.iter().any(U256::is_zero) {
        return None;
    }
    Some(vec![
        precisions[0].checked_mul(ONE)?,
        precisions[1].checked_mul(price_scale)?,
    ])
}

fn decode_curve_stable(plan: &PoolFetchPlan, results: &[Option<Bytes>]) -> Option<PoolState> {
    let balances = decode_curve_balances(plan, results)?;
    let n_fetched = balances.len();
    let a_idx = n_fetched;
    let fee_idx = n_fetched + 1;
    let rates_idx = n_fetched + 2;
    let a_raw = decode_u256(results.get(a_idx)?.as_ref()?)?;
    let fee = decode_curve_fee(results, fee_idx)?;
    let rates = decode_curve_stored_rates(results, n_fetched, rates_idx)?;
    if a_raw.is_zero() {
        return None;
    }
    // Multicall fetches A(); StableSwap math (and our get_d/get_y) use A_precise = A * 100.
    // Storing unscaled A under-amplifies ~100× → local dy ≫ get_dy (dry-run min_dy reverts).
    const A_PRECISION: u64 = 100;
    let a = a_raw.checked_mul(U256::from(A_PRECISION))?;
    Some(PoolState::Curve(CurvePoolState {
        balances,
        a,
        fee,
        rates,
        n_coins: n_fetched as u8,
        gamma: None,
        d: None,
    }))
}

fn decode_curve_crypto(plan: &PoolFetchPlan, results: &[Option<Bytes>]) -> Option<PoolState> {
    let balances = decode_curve_balances(plan, results)?;
    let n_fetched = balances.len();
    let a_idx = n_fetched;
    let fee_idx = n_fetched + 1;
    let gamma_idx = n_fetched + 2;
    let price_scale_idx = n_fetched + 3;
    let precisions_idx = n_fetched + 4;
    let a = decode_u256(results.get(a_idx)?.as_ref()?)?;
    let fee = decode_curve_fee(results, fee_idx)?;
    let gamma = results
        .get(gamma_idx)
        .and_then(|b| b.as_ref())
        .and_then(|b| ICurvePool::gammaCall::abi_decode_returns(b).ok())
        .map(U256::from)?;
    if gamma.is_zero() || a.is_zero() {
        return None;
    }
    let price_scale = results
        .get(price_scale_idx)
        .and_then(|b| b.as_ref())
        .and_then(|b| ICurveCryptoPool::price_scaleCall::abi_decode_returns(b).ok())?;
    let precisions = results
        .get(precisions_idx)
        .and_then(|b| b.as_ref())
        .and_then(|b| ICurveCryptoPool::precisionsCall::abi_decode_returns(b).ok())?;
    let rates = decode_curve_crypto_rates(n_fetched, price_scale, precisions)?;
    Some(PoolState::Curve(CurvePoolState {
        balances,
        a,
        fee,
        rates,
        n_coins: n_fetched as u8,
        gamma: Some(gamma),
        d: None,
    }))
}

fn decode_balancer_linear(
    vault_tokens: &[Address],
    main_bytes: Option<&Bytes>,
    wrapped_bytes: Option<&Bytes>,
    targets_bytes: Option<&Bytes>,
    rate_bytes: Option<&Bytes>,
) -> Option<BalancerLinearState> {
    let main = IBalancerLinearPool::getMainTokenCall::abi_decode_returns(main_bytes?).ok()?;
    let wrapped =
        IBalancerLinearPool::getWrappedTokenCall::abi_decode_returns(wrapped_bytes?).ok()?;
    let targets = IBalancerLinearPool::getTargetsCall::abi_decode_returns(targets_bytes?).ok()?;
    let rate =
        IBalancerLinearPool::getWrappedTokenRateCall::abi_decode_returns(rate_bytes?).ok()?;
    let main_index = vault_tokens.iter().position(|t| *t == main)?;
    let wrapped_index = vault_tokens.iter().position(|t| *t == wrapped)?;
    if main_index == wrapped_index
        || U256::from(targets.upperTarget) < U256::from(targets.lowerTarget)
        || U256::from(rate).is_zero()
    {
        return None;
    }
    Some(BalancerLinearState {
        main_index,
        wrapped_index,
        lower_target: U256::from(targets.lowerTarget),
        upper_target: U256::from(targets.upperTarget),
        wrapped_rate: U256::from(rate),
    })
}

fn classify_balancer_pool(
    pool_type: Option<&str>,
    has_linear_state: bool,
    amp_valid: bool,
    weights_valid: bool,
) -> Option<BalancerPoolKind> {
    if has_linear_state {
        return Some(BalancerPoolKind::Linear);
    }
    match pool_type {
        Some("stable") if amp_valid => Some(BalancerPoolKind::Stable),
        Some("weighted") if weights_valid => Some(BalancerPoolKind::Weighted),
        // Labeled linear but probes failed, or unknown family label.
        Some(_) => None,
        None => {
            if weights_valid && !amp_valid {
                Some(BalancerPoolKind::Weighted)
            } else if amp_valid && !weights_valid {
                Some(BalancerPoolKind::Stable)
            } else if weights_valid {
                // Both probes succeeded: weighted pools must not use stable math.
                Some(BalancerPoolKind::Weighted)
            } else {
                None
            }
        }
    }
}

// Test-only helpers for isolated fee decode checks.
#[cfg(test)]
fn plan_result<'a>(
    plan: &PoolFetchPlan,
    results: &'a [Option<Bytes>],
    kind: CallKind,
) -> Option<&'a Bytes> {
    let idx = plan.kinds.iter().position(|k| *k == kind)?;
    results.get(idx)?.as_ref()
}

#[cfg(test)]
fn decode_balancer_swap_fee(plan: &PoolFetchPlan, results: &[Option<Bytes>]) -> Option<U256> {
    plan_result(plan, results, CallKind::BalancerSwapFee)
        .and_then(|b| IBalancerPool::getSwapFeePercentageCall::abi_decode_returns(b).ok())
        .map(U256::from)
}

fn decode_balancer(plan: &PoolFetchPlan, results: &[Option<Bytes>]) -> Option<PoolState> {
    // Single pass over kinds — true O(N), not N×K linear scans.
    let mut tokens_bytes = None;
    let mut swap_fee_bytes = None;
    let mut weights_bytes = None;
    let mut amp_bytes = None;
    let mut scaling_bytes = None;
    let mut linear_main = None;
    let mut linear_wrapped = None;
    let mut linear_targets = None;
    let mut linear_rate = None;
    for (result, kind) in results.iter().zip(plan.kinds.iter()) {
        match kind {
            CallKind::BalancerTokens => tokens_bytes = result.as_ref(),
            CallKind::BalancerSwapFee => swap_fee_bytes = result.as_ref(),
            CallKind::BalancerWeights => weights_bytes = result.as_ref(),
            CallKind::BalancerAmp => amp_bytes = result.as_ref(),
            CallKind::BalancerScalingFactors => scaling_bytes = result.as_ref(),
            CallKind::BalancerLinearMainToken => linear_main = result.as_ref(),
            CallKind::BalancerLinearWrappedToken => linear_wrapped = result.as_ref(),
            CallKind::BalancerLinearTargets => linear_targets = result.as_ref(),
            CallKind::BalancerLinearRate => linear_rate = result.as_ref(),
            _ => {}
        }
    }

    let tokens = IBalancerVaultRead::getPoolTokensCall::abi_decode_returns(tokens_bytes?).ok()?;
    let last_change_block = tokens.lastChangeBlock.as_limbs()[0];
    // ponytail: move decoded vecs — uint256[] already is Vec<U256>.
    let balances = tokens.balances;
    if balances.len() < 2 {
        return None;
    }
    let n = balances.len();
    let swap_fee = IBalancerPool::getSwapFeePercentageCall::abi_decode_returns(swap_fee_bytes?)
        .ok()
        .map(U256::from)?;
    let decoded_weights = weights_bytes
        .and_then(|b| IBalancerPool::getNormalizedWeightsCall::abi_decode_returns(b).ok());
    let amp_from_chain = amp_bytes
        .and_then(|b| IBalancerPool::getAmplificationParameterCall::abi_decode_returns(b).ok());
    let (amp, amp_precision, has_onchain_amp, is_updating) = match amp_from_chain {
        Some(t) => (
            U256::from(t.value),
            U256::from(t.precision),
            true,
            t.isUpdating,
        ),
        None => (U256::ZERO, U256::ZERO, false, false),
    };
    let scaling_factors =
        IBalancerPool::getScalingFactorsCall::abi_decode_returns(scaling_bytes?).ok()?;
    if scaling_factors.len() != n || scaling_factors.iter().any(U256::is_zero) {
        return None;
    }
    // Kind-keyed linear fields — independent of call order / contiguity.
    let linear = decode_balancer_linear(
        &tokens.tokens,
        linear_main,
        linear_wrapped,
        linear_targets,
        linear_rate,
    );
    let amp_valid = has_onchain_amp && !amp.is_zero() && !amp_precision.is_zero();
    let weights_valid = decoded_weights
        .as_ref()
        .is_some_and(|weights| weights.len() == n && weights.iter().all(|w| !w.is_zero()));
    let pool_type = classify_balancer_pool(
        plan.pool.pool_type.as_deref(),
        linear.is_some(),
        amp_valid,
        weights_valid,
    )?;
    let weights = decoded_weights.unwrap_or_else(|| vec![ONE; n]);
    let bpt_index = tokens.tokens.iter().position(|t| *t == plan.pool.address);
    Some(PoolState::Balancer(BalancerPoolState {
        pool_id: plan.pool.pool_id,
        tokens: tokens.tokens,
        balances,
        weights,
        scaling_factors,
        amp,
        amp_precision,
        fee: swap_fee,
        pool_type,
        linear,
        bpt_index,
        is_updating,
        last_change_block,
    }))
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use super::*;
    use alloy::sol_types::SolCall;

    fn abi_word(value: u64) -> [u8; 32] {
        let mut out = [0u8; 32];
        out[24..].copy_from_slice(&value.to_be_bytes());
        out
    }

    #[test]
    fn decode_v2_reserves_reads_packed_slots() {
        let mut bytes = Vec::with_capacity(96);
        bytes.extend_from_slice(&abi_word(1_000));
        bytes.extend_from_slice(&abi_word(2_000));
        bytes.extend_from_slice(&abi_word(42));
        let (r0, r1, ts) = decode_v2_reserves(&bytes).expect("decode");
        assert_eq!(r0, U256::from(1_000u64));
        assert_eq!(r1, U256::from(2_000u64));
        assert_eq!(ts, 42);
    }

    #[test]
    fn v2_stored_fee_from_bps_matches_edge_fee_resolution() {
        let (fee, den) = v2_stored_fee_from_bps(30);
        assert_eq!(fee, U256::from(9970u64));
        assert_eq!(den, U256::from(10_000u64));
        let state = V2PoolState {
            reserve0: U256::from(1u64),
            reserve1: U256::from(1u64),
            fee,
            fee_denominator: den,
            block_timestamp_last: 0,
        };
        let (num, denom) = crate::core::math::uniswap_v2::resolve_v2_fee_with_edge(&state, None);
        assert_eq!(num, U256::from(9970u64));
        assert_eq!(denom, U256::from(10_000u64));
    }

    #[test]
    fn decode_v3_slot0_reads_tick_and_observation_cardinality() {
        let mut bytes = vec![0u8; 224];
        bytes[0..32].copy_from_slice(&abi_word(1 << 20));
        // tick = -1 (int24 sign-extended)
        bytes[32..64].fill(0xFF);
        bytes[96..128].copy_from_slice(&abi_word(17));
        bytes[160..192].copy_from_slice(&abi_word(6));
        bytes[192..224].copy_from_slice(&abi_word(1));

        let (sqrt, tick, unlocked, fee_proto, obs_card) = decode_v3_slot0(&bytes).expect("decode");
        assert_eq!(sqrt, U256::from(1u64 << 20));
        assert_eq!(tick, -1);
        assert!(unlocked);
        assert_eq!(fee_proto, 6);
        assert_eq!(obs_card, 17);
    }

    #[test]
    fn decode_v3_keeps_slot0_protocol_fee_in_pool_state() {
        let plan = PoolFetchPlan {
            pool: super::super::plans::FetchPoolInfo {
                address: Address::ZERO,
                protocol: ProtocolType::UniswapV3,
                tokens: Vec::new(),
                fee_bps: 30,
                tick_spacing: Some(60),
                pool_id: None,
                pool_type: None,
                protocol_label: None,
            },
            calls: Vec::new(),
            kinds: vec![CallKind::V3Slot0, CallKind::V3Liquidity, CallKind::V3Fee],
        };
        let mut slot0 = vec![0u8; 224];
        slot0[0..32].copy_from_slice(&abi_word(1 << 20));
        slot0[160..192].copy_from_slice(&abi_word(0x42));
        slot0[192..224].copy_from_slice(&abi_word(1));
        let state = decode_plan(
            &plan,
            &[
                Some(Bytes::from(slot0)),
                Some(Bytes::copy_from_slice(&abi_word(1_000))),
                Some(Bytes::copy_from_slice(&abi_word(3_000))),
            ],
        )
        .expect("V3 state should decode");
        let PoolState::V3(state) = state else {
            panic!("expected V3 state");
        };
        assert_eq!(state.fee_protocol, 0x42);
    }

    #[test]
    fn algebra_prefer_path_does_not_decode_short_payload_as_slot0() {
        // globalState shorter than Uni slot0 — must fail closed so caller can use
        // the dedicated V3Slot0 multicall result instead of a garbage head.
        let short = Bytes::from(vec![0u8; 160]);
        assert!(decode_v3_head(&short, true).is_none());
    }

    #[test]
    fn decode_v2_rejects_zero_reserves() {
        let plan = PoolFetchPlan {
            pool: super::super::plans::FetchPoolInfo {
                address: Address::ZERO,
                protocol: ProtocolType::UniswapV2,
                tokens: Vec::new(),
                fee_bps: 30,
                tick_spacing: None,
                pool_id: None,
                pool_type: None,
                protocol_label: None,
            },
            calls: Vec::new(),
            kinds: vec![CallKind::V2Reserves],
        };
        let mut bytes = Vec::with_capacity(96);
        bytes.extend_from_slice(&abi_word(0));
        bytes.extend_from_slice(&abi_word(100));
        bytes.extend_from_slice(&abi_word(0));
        let results = vec![Some(Bytes::from(bytes))];
        assert!(decode_plan(&plan, &results).is_none());
    }

    #[test]
    fn decode_curve_stable_accepts_three_coin_stableswap_ng() {
        let plan = PoolFetchPlan {
            pool: super::super::plans::FetchPoolInfo {
                address: Address::ZERO,
                protocol: ProtocolType::CurveStable,
                tokens: vec![
                    Address::with_last_byte(1),
                    Address::with_last_byte(2),
                    Address::with_last_byte(3),
                ],
                fee_bps: 4,
                tick_spacing: None,
                pool_id: None,
                pool_type: Some("stable_ng".into()),
                protocol_label: None,
            },
            calls: Vec::new(),
            kinds: vec![
                CallKind::CurveBalance(0),
                CallKind::CurveBalance(1),
                CallKind::CurveBalance(2),
                CallKind::CurveA,
                CallKind::CurveFee,
                CallKind::CurveRates,
            ],
        };
        let rates = ICurvePool::stored_ratesCall::abi_encode_returns(&vec![ONE; 3]);
        let results = vec![
            Some(Bytes::copy_from_slice(&abi_word(1_000))),
            Some(Bytes::copy_from_slice(&abi_word(2_000))),
            Some(Bytes::copy_from_slice(&abi_word(3_000))),
            Some(Bytes::copy_from_slice(&abi_word(100))),
            Some(Bytes::copy_from_slice(&abi_word(4_000_000))),
            Some(Bytes::from(rates)),
        ];

        let Some(PoolState::Curve(state)) = decode_plan(&plan, &results) else {
            panic!("three-coin Curve NG pool should decode");
        };
        assert_eq!(state.n_coins, 3);
        assert_eq!(state.balances.len(), 3);
        assert_eq!(state.rates, vec![ONE; 3]);
    }

    #[test]
    fn decode_v4_keeps_zero_lp_fee_from_slot0() {
        // sqrt=2^96, tick=0, protocol=0, lpFee=0 — indexer fee_bps must not win.
        let slot0 = U256::from(1u128 << 96);
        let plan = PoolFetchPlan {
            pool: super::super::plans::FetchPoolInfo {
                address: Address::ZERO,
                protocol: ProtocolType::UniswapV4,
                tokens: Vec::new(),
                fee_bps: 30,
                tick_spacing: Some(60),
                pool_id: None,
                pool_type: None,
                protocol_label: None,
            },
            calls: Vec::new(),
            kinds: vec![CallKind::V4Slot0, CallKind::V4Liquidity],
        };
        let results = vec![
            Some(Bytes::copy_from_slice(&slot0.to_be_bytes::<32>())),
            Some(Bytes::copy_from_slice(&abi_word(1_000_000))),
        ];
        let Some(PoolState::V4(state)) = decode_plan(&plan, &results) else {
            panic!("V4 zero-fee pool should decode");
        };
        assert!(state.fee.is_zero());
        assert_eq!(state.fee_protocol, 0);
    }

    #[test]
    fn dodo_decode_soft_fails_mt_fee_when_planned_call_reverts() {
        // Live Polygon DVM/DPP/DSP: `_MT_FEE_RATE_()` reverts; pool must still decode.
        let plan = PoolFetchPlan {
            pool: super::super::plans::FetchPoolInfo {
                address: Address::ZERO,
                protocol: ProtocolType::Dodo,
                tokens: Vec::new(),
                fee_bps: 1_000,
                tick_spacing: None,
                pool_id: None,
                pool_type: None,
                protocol_label: None,
            },
            calls: Vec::new(),
            kinds: vec![
                CallKind::DodoBase,
                CallKind::DodoQuote,
                CallKind::DodoBaseToken,
                CallKind::DodoQuoteToken,
                CallKind::DodoI,
                CallKind::DodoK,
                CallKind::DodoLpFee,
                CallKind::DodoMtFee,
                CallKind::DodoPmmState,
            ],
        };
        let values = [
            2_040_000_000_000_000u64,
            5_000_000_000_000_000,
            2_628_567_256_663,
            12_288_863_768,
            2_764_216_862_899,
            12_012_067_168,
            1,
        ];
        let mut pmm = Vec::with_capacity(32 * values.len());
        for value in values {
            pmm.extend_from_slice(&abi_word(value));
        }
        let results = vec![
            Some(Bytes::copy_from_slice(&abi_word(values[2]))),
            Some(Bytes::copy_from_slice(&abi_word(values[3]))),
            Some(Bytes::copy_from_slice(&abi_word(1))),
            Some(Bytes::copy_from_slice(&abi_word(2))),
            Some(Bytes::copy_from_slice(&abi_word(values[0]))),
            Some(Bytes::copy_from_slice(&abi_word(values[1]))),
            Some(Bytes::copy_from_slice(&abi_word(10_000_000_000_000_000))),
            None, // planned MT fee call reverted (Polygon DODO)
            Some(Bytes::from(pmm)),
        ];
        let Some(PoolState::Dodo(state)) = decode_plan(&plan, &results) else {
            panic!("DODO should decode when MT fee view reverts");
        };
        assert!(state.mt_fee_rate.is_zero());
        assert_eq!(state.base_reserve, U256::from(values[2]));
    }

    #[test]
    fn balancer_classification_requires_family_specific_state() {
        assert_eq!(
            classify_balancer_pool(Some("linear"), true, false, false),
            Some(BalancerPoolKind::Linear)
        );
        assert_eq!(
            classify_balancer_pool(Some("stable"), false, true, false),
            Some(BalancerPoolKind::Stable)
        );
        assert_eq!(
            classify_balancer_pool(Some("weighted"), false, false, true),
            Some(BalancerPoolKind::Weighted)
        );
        assert_eq!(
            classify_balancer_pool(Some("linear"), false, true, true),
            None
        );
        assert_eq!(
            classify_balancer_pool(Some("stable"), false, false, true),
            None
        );
        assert_eq!(
            classify_balancer_pool(Some("weighted"), false, true, false),
            None
        );
        assert_eq!(classify_balancer_pool(None, false, false, false), None);
        assert_eq!(
            classify_balancer_pool(None, false, true, true),
            Some(BalancerPoolKind::Weighted)
        );
        assert_eq!(
            classify_balancer_pool(None, false, false, true),
            Some(BalancerPoolKind::Weighted)
        );
        assert_eq!(
            classify_balancer_pool(None, true, true, true),
            Some(BalancerPoolKind::Linear)
        );
    }

    #[test]
    fn dodo_decode_requires_all_invariant_parameters() {
        let plan = PoolFetchPlan {
            pool: super::super::plans::FetchPoolInfo {
                address: Address::ZERO,
                protocol: ProtocolType::Dodo,
                tokens: Vec::new(),
                fee_bps: 30,
                tick_spacing: None,
                pool_id: None,
                pool_type: None,
                protocol_label: None,
            },
            calls: Vec::new(),
            kinds: vec![
                CallKind::DodoBase,
                CallKind::DodoQuote,
                CallKind::DodoBaseToken,
                CallKind::DodoQuoteToken,
                CallKind::DodoI,
                CallKind::DodoK,
                CallKind::DodoLpFee,
            ],
        };
        let results = vec![
            Some(Bytes::copy_from_slice(&abi_word(100))),
            Some(Bytes::copy_from_slice(&abi_word(100))),
            Some(Bytes::copy_from_slice(&[0u8; 32])),
            Some(Bytes::copy_from_slice(&[0u8; 32])),
            None,
            Some(Bytes::copy_from_slice(&abi_word(0))),
            Some(Bytes::copy_from_slice(&abi_word(0))),
        ];
        assert!(decode_plan(&plan, &results).is_none());
    }

    #[test]
    fn dodo_decode_rejects_non_atomic_price_snapshot() {
        let plan = PoolFetchPlan {
            pool: super::super::plans::FetchPoolInfo {
                address: Address::ZERO,
                protocol: ProtocolType::Dodo,
                tokens: Vec::new(),
                fee_bps: 100,
                tick_spacing: None,
                pool_id: None,
                pool_type: None,
                protocol_label: None,
            },
            calls: Vec::new(),
            kinds: vec![
                CallKind::DodoBase,
                CallKind::DodoQuote,
                CallKind::DodoBaseToken,
                CallKind::DodoQuoteToken,
                CallKind::DodoI,
                CallKind::DodoK,
                CallKind::DodoLpFee,
                CallKind::DodoPmmState,
            ],
        };
        let mut pmm = Vec::with_capacity(32 * 7);
        for value in [20u64, 5, 100, 100, 120, 80, 1] {
            pmm.extend_from_slice(&abi_word(value));
        }
        let results = vec![
            Some(Bytes::copy_from_slice(&abi_word(100))),
            Some(Bytes::copy_from_slice(&abi_word(100))),
            Some(Bytes::copy_from_slice(&abi_word(1))),
            Some(Bytes::copy_from_slice(&abi_word(2))),
            Some(Bytes::copy_from_slice(&abi_word(10))),
            Some(Bytes::copy_from_slice(&abi_word(5))),
            Some(Bytes::copy_from_slice(&abi_word(0))),
            Some(Bytes::from(pmm)),
        ];

        assert!(decode_plan(&plan, &results).is_none());
    }

    #[test]
    fn dodo_decode_matches_captured_query_sell_base_without_inferred_mt_fee() {
        let plan = PoolFetchPlan {
            pool: super::super::plans::FetchPoolInfo {
                address: Address::ZERO,
                protocol: ProtocolType::Dodo,
                tokens: Vec::new(),
                fee_bps: 1_000,
                tick_spacing: None,
                pool_id: None,
                pool_type: None,
                protocol_label: None,
            },
            calls: Vec::new(),
            kinds: vec![
                CallKind::DodoBase,
                CallKind::DodoQuote,
                CallKind::DodoBaseToken,
                CallKind::DodoQuoteToken,
                CallKind::DodoI,
                CallKind::DodoK,
                CallKind::DodoLpFee,
                CallKind::DodoPmmState,
            ],
        };
        let values = [
            2_040_000_000_000_000u64,
            5_000_000_000_000_000,
            2_628_567_256_663,
            12_288_863_768,
            2_764_216_862_899,
            12_012_067_168,
            1,
        ];
        let mut pmm = Vec::with_capacity(32 * values.len());
        for value in values {
            pmm.extend_from_slice(&abi_word(value));
        }
        let results = vec![
            Some(Bytes::copy_from_slice(&abi_word(values[2]))),
            Some(Bytes::copy_from_slice(&abi_word(values[3]))),
            Some(Bytes::copy_from_slice(&abi_word(1))),
            Some(Bytes::copy_from_slice(&abi_word(2))),
            Some(Bytes::copy_from_slice(&abi_word(values[0]))),
            Some(Bytes::copy_from_slice(&abi_word(values[1]))),
            Some(Bytes::copy_from_slice(&abi_word(10_000_000_000_000_000))),
            Some(Bytes::from(pmm)),
        ];
        let Some(PoolState::Dodo(state)) = decode_plan(&plan, &results) else {
            panic!("DODO snapshot should decode");
        };
        assert!(state.mt_fee_rate.is_zero());

        assert_eq!(
            crate::core::math::dodo::get_dodo_amount_out(&state, U256::from(474_425u64), true),
            U256::from(959u64),
        );
    }

    #[test]
    fn dodo_decode_applies_on_chain_mt_fee_rate() {
        let plan = PoolFetchPlan {
            pool: super::super::plans::FetchPoolInfo {
                address: Address::ZERO,
                protocol: ProtocolType::Dodo,
                tokens: Vec::new(),
                fee_bps: 1_000,
                tick_spacing: None,
                pool_id: None,
                pool_type: None,
                protocol_label: None,
            },
            calls: Vec::new(),
            kinds: vec![
                CallKind::DodoBase,
                CallKind::DodoQuote,
                CallKind::DodoBaseToken,
                CallKind::DodoQuoteToken,
                CallKind::DodoI,
                CallKind::DodoK,
                CallKind::DodoLpFee,
                CallKind::DodoMtFee,
                CallKind::DodoPmmState,
            ],
        };
        let values = [
            2_040_000_000_000_000u64,
            5_000_000_000_000_000,
            2_628_567_256_663,
            12_288_863_768,
            2_764_216_862_899,
            12_012_067_168,
            1,
        ];
        let mut pmm = Vec::with_capacity(32 * values.len());
        for value in values {
            pmm.extend_from_slice(&abi_word(value));
        }
        let mt = 10_000_000_000_000_000u64; // 1%
        let results = vec![
            Some(Bytes::copy_from_slice(&abi_word(values[2]))),
            Some(Bytes::copy_from_slice(&abi_word(values[3]))),
            Some(Bytes::copy_from_slice(&abi_word(1))),
            Some(Bytes::copy_from_slice(&abi_word(2))),
            Some(Bytes::copy_from_slice(&abi_word(values[0]))),
            Some(Bytes::copy_from_slice(&abi_word(values[1]))),
            Some(Bytes::copy_from_slice(&abi_word(10_000_000_000_000_000))), // 1% LP
            Some(Bytes::copy_from_slice(&abi_word(mt))),
            Some(Bytes::from(pmm)),
        ];
        let Some(PoolState::Dodo(state)) = decode_plan(&plan, &results) else {
            panic!("DODO snapshot should decode with MT fee");
        };
        assert_eq!(state.mt_fee_rate, U256::from(mt));
        let with_mt =
            crate::core::math::dodo::get_dodo_amount_out(&state, U256::from(474_425u64), true);
        assert!(with_mt < U256::from(959u64));
        assert!(!with_mt.is_zero());
    }

    #[test]
    fn curve_crypto_rates_require_indexed_scale_and_precisions() {
        let scale = U256::from(2_112_531_811_800_907_072u128);
        let precisions = [
            U256::from(1_000_000_000_000u64),
            U256::from(1_000_000_000u64),
        ];

        assert_eq!(
            decode_curve_crypto_rates(2, scale, precisions),
            Some(vec![precisions[0] * ONE, precisions[1] * scale])
        );
        assert!(decode_curve_crypto_rates(2, U256::ZERO, precisions).is_none());
        assert!(decode_curve_crypto_rates(3, scale, precisions).is_none());
    }

    #[test]
    fn curve_fee_requires_a_successful_multicall_result() {
        assert_eq!(decode_curve_fee(&[None], 0), None);
        assert_eq!(
            decode_curve_fee(&[Some(Bytes::from_static(&[0u8; 31]))], 0),
            None
        );
        assert_eq!(
            decode_curve_fee(&[Some(Bytes::copy_from_slice(&abi_word(4_000_000)))], 0),
            Some(U256::from(4_000_000u64))
        );
    }

    #[test]
    fn stable_ng_with_more_than_two_coins_fails_closed() {
        let plan = PoolFetchPlan {
            pool: super::super::plans::FetchPoolInfo {
                address: Address::ZERO,
                protocol: ProtocolType::CurveStable,
                tokens: vec![
                    Address::ZERO,
                    Address::with_last_byte(1),
                    Address::with_last_byte(2),
                ],
                fee_bps: 4,
                tick_spacing: None,
                pool_id: None,
                pool_type: Some("stable_ng".to_string()),
                protocol_label: None,
            },
            calls: Vec::new(),
            kinds: Vec::new(),
        };
        let results = vec![
            Some(Bytes::copy_from_slice(&abi_word(1))),
            Some(Bytes::copy_from_slice(&abi_word(1))),
            Some(Bytes::copy_from_slice(&abi_word(1))),
        ];

        assert!(decode_curve_stable(&plan, &results).is_none());
    }

    #[test]
    fn curve_stored_rates_must_be_live_and_complete() {
        let missing = vec![None];
        assert_eq!(decode_curve_stored_rates(&missing, 2, 0), None);

        let zero_rates = ICurvePool::stored_ratesCall::abi_encode_returns(&vec![ONE, U256::ZERO]);
        assert_eq!(
            decode_curve_stored_rates(&[Some(Bytes::from(zero_rates))], 2, 0),
            None
        );
    }

    #[test]
    fn balancer_swap_fee_must_be_live() {
        let plan = PoolFetchPlan {
            pool: super::super::plans::FetchPoolInfo {
                address: Address::ZERO,
                protocol: ProtocolType::BalancerV2,
                tokens: Vec::new(),
                fee_bps: 30,
                tick_spacing: None,
                pool_id: None,
                pool_type: None,
                protocol_label: None,
            },
            calls: Vec::new(),
            kinds: vec![CallKind::BalancerSwapFee],
        };
        assert_eq!(decode_balancer_swap_fee(&plan, &[None]), None);
        assert_eq!(
            decode_balancer_swap_fee(
                &plan,
                &[Some(Bytes::copy_from_slice(&abi_word(
                    3_000_000_000_000_000
                )))],
            ),
            Some(U256::from(3_000_000_000_000_000u64))
        );
    }

    #[test]
    fn decode_balancer_weighted_single_pass_kind_order_independent() {
        let t0 = Address::with_last_byte(1);
        let t1 = Address::with_last_byte(2);
        let pool = Address::with_last_byte(9);
        // Deliberately non-plan order to prove kind-keyed single-pass decode.
        let kinds = vec![
            CallKind::BalancerScalingFactors,
            CallKind::BalancerSwapFee,
            CallKind::BalancerWeights,
            CallKind::BalancerTokens,
        ];
        let plan = PoolFetchPlan {
            pool: super::super::plans::FetchPoolInfo {
                address: pool,
                protocol: ProtocolType::BalancerV2,
                tokens: vec![t0, t1],
                fee_bps: 30,
                tick_spacing: None,
                pool_id: None,
                pool_type: Some("weighted".into()),
                protocol_label: None,
            },
            calls: Vec::new(),
            kinds,
        };
        let half = ONE / U256::from(2u64);
        let tokens_ret = IBalancerVaultRead::getPoolTokensCall::abi_encode_returns(
            &IBalancerVaultRead::getPoolTokensReturn {
                tokens: vec![t0, t1],
                balances: vec![U256::from(1_000u64), U256::from(2_000u64)],
                lastChangeBlock: U256::from(99u64),
            },
        );
        let fee_ret = IBalancerPool::getSwapFeePercentageCall::abi_encode_returns(&U256::from(
            3_000_000_000_000_000u64,
        ));
        let weights_ret =
            IBalancerPool::getNormalizedWeightsCall::abi_encode_returns(&vec![half, half]);
        let scaling_ret = IBalancerPool::getScalingFactorsCall::abi_encode_returns(&vec![ONE, ONE]);
        let results = vec![
            Some(Bytes::from(scaling_ret)),
            Some(Bytes::from(fee_ret)),
            Some(Bytes::from(weights_ret)),
            Some(Bytes::from(tokens_ret)),
        ];
        let Some(PoolState::Balancer(state)) = decode_plan(&plan, &results) else {
            panic!("weighted Balancer pool should decode regardless of kind order");
        };
        assert_eq!(state.pool_type, BalancerPoolKind::Weighted);
        assert_eq!(
            state.balances,
            vec![U256::from(1_000u64), U256::from(2_000u64)]
        );
        assert_eq!(state.fee, U256::from(3_000_000_000_000_000u64));
        assert_eq!(state.last_change_block, 99);
        assert!(state.linear.is_none());
        assert!(state.bpt_index.is_none());
    }

    #[test]
    fn decode_balancer_rejects_missing_swap_fee() {
        let t0 = Address::with_last_byte(1);
        let t1 = Address::with_last_byte(2);
        let plan = PoolFetchPlan {
            pool: super::super::plans::FetchPoolInfo {
                address: Address::ZERO,
                protocol: ProtocolType::BalancerV2,
                tokens: vec![t0, t1],
                fee_bps: 30,
                tick_spacing: None,
                pool_id: None,
                pool_type: Some("weighted".into()),
                protocol_label: None,
            },
            calls: Vec::new(),
            kinds: vec![
                CallKind::BalancerTokens,
                CallKind::BalancerSwapFee,
                CallKind::BalancerWeights,
                CallKind::BalancerScalingFactors,
            ],
        };
        let tokens_ret = IBalancerVaultRead::getPoolTokensCall::abi_encode_returns(
            &IBalancerVaultRead::getPoolTokensReturn {
                tokens: vec![t0, t1],
                balances: vec![U256::from(1u64), U256::from(1u64)],
                lastChangeBlock: U256::ZERO,
            },
        );
        let half = ONE / U256::from(2u64);
        let results = vec![
            Some(Bytes::from(tokens_ret)),
            None, // swap fee required live
            Some(Bytes::from(
                IBalancerPool::getNormalizedWeightsCall::abi_encode_returns(&vec![half, half]),
            )),
            Some(Bytes::from(
                IBalancerPool::getScalingFactorsCall::abi_encode_returns(&vec![ONE, ONE]),
            )),
        ];
        assert!(decode_plan(&plan, &results).is_none());
    }
}
