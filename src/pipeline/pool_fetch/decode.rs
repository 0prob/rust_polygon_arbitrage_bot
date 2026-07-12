use alloy::primitives::{Address, Bytes, U256};
use alloy::sol_types::SolCall;
use std::sync::Arc;

use crate::abis::{
    IBalancerLinearPool, IBalancerPool, IBalancerVaultRead, ICurvePool, IDodoPoolState,
};
use crate::core::math::balancer::balancer_swap_fee_from_pool_meta_fee;
use crate::core::math::fixed_point::ONE;

const DODO_FEE_RATE_SCALE: U256 = U256::from_limbs([100_000_000_000_000, 0, 0, 0]); // 1e14
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
    let fee_bps = U256::from(plan.pool.fee_bps);
    Some(PoolState::V2(V2PoolState {
        reserve0: r0,
        reserve1: r1,
        fee: fee_bps,
        fee_denominator: U256::from(10_000u64),
        block_timestamp_last: block_ts,
    }))
}

fn decode_v3_head(
    bytes: &Bytes,
    prefer_algebra: bool,
) -> Option<(U256, i32, bool, Option<U256>, u16)> {
    if prefer_algebra {
        return decode_algebra_global_state(bytes)
            .map(|(sqrt, tick, unlocked, fee)| (sqrt, tick, unlocked, Some(fee), 0u16))
            .or_else(|| {
                decode_v3_slot0(bytes).map(|(sqrt, tick, unlocked, _, obs_card)| {
                    (sqrt, tick, unlocked, None, obs_card)
                })
            });
    }
    decode_v3_slot0(bytes)
        .map(|(sqrt, tick, unlocked, _, obs_card)| (sqrt, tick, unlocked, None, obs_card))
        .or_else(|| {
            decode_algebra_global_state(bytes)
                .map(|(sqrt, tick, unlocked, fee)| (sqrt, tick, unlocked, Some(fee), 0u16))
        })
}

fn decode_v3_head_from_results(
    plan: &PoolFetchPlan,
    results: &[Option<Bytes>],
) -> Option<(U256, i32, bool, Option<U256>, u16)> {
    let mut global_bytes = None;
    let mut slot0_bytes = None;
    for (result, kind) in results.iter().zip(plan.kinds.iter()) {
        match kind {
            CallKind::V3GlobalState => global_bytes = result.as_ref(),
            CallKind::V3Slot0 => slot0_bytes = result.as_ref(),
            _ => {}
        }
    }
    if let Some(bytes) = global_bytes
        && let Some(head) = decode_v3_head(bytes, true)
    {
        return Some(head);
    }
    slot0_bytes.and_then(|bytes| decode_v3_head(bytes, false))
}

fn decode_v3(plan: &PoolFetchPlan, results: &[Option<Bytes>]) -> Option<PoolState> {
    let (sqrt, tick, unlocked, algebra_fee, obs_card) = decode_v3_head_from_results(plan, results)?;
    let liq_idx = plan
        .kinds
        .iter()
        .position(|k| *k == CallKind::V3Liquidity)
        .unwrap_or(1);
    let liq_bytes = results.get(liq_idx)?.as_ref()?;
    let liquidity = decode_u256(liq_bytes)?.as_limbs()[0] as u128;
    if sqrt.is_zero() || liquidity == 0 {
        return None;
    }
    let fee_idx = plan.kinds.iter().position(|k| *k == CallKind::V3Fee);
    let fee_pips = algebra_fee
        .or_else(|| {
            fee_idx
                .and_then(|idx| results.get(idx))
                .and_then(|b| b.as_ref())
                .and_then(|b| decode_u256(b).map(|v| v.as_limbs()[0] as u32))
                .map(U256::from)
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
        fee_protocol: 0,
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
    let fee = if decoded.lp_fee > 0 {
        U256::from(decoded.lp_fee)
    } else {
        U256::from(plan.pool.fee_bps) * U256::from(100u32)
    };
    Some(PoolState::V4(V4PoolState {
        sqrt_price_x96: decoded.sqrt_price_x96,
        tick: decoded.tick,
        liquidity,
        fee,
        tick_spacing: plan.pool.tick_spacing.unwrap_or(60),
        ticks: Arc::from([] as [crate::core::types::V3Tick; 0]),
        unlocked: true,
        fee_protocol: 0,
        observation_cardinality: 1,
    }))
}

fn decode_dodo(plan: &PoolFetchPlan, results: &[Option<Bytes>]) -> Option<PoolState> {
    let base = decode_u256(results.first()?.as_ref()?)?;
    let quote = decode_u256(results.get(1)?.as_ref()?)?;
    let base_token = decode_address(results.get(2)?.as_ref()?)?;
    let quote_token = decode_address(results.get(3)?.as_ref()?)?;
    let i = decode_u256(results.get(4)?.as_ref()?)?;
    let k = decode_u256(results.get(5)?.as_ref()?)?;
    let lp_fee_rate = decode_u256(results.get(6)?.as_ref()?)?;
    let pmm = IDodoPoolState::getPMMStateForCallCall::abi_decode_returns(results.get(7)?.as_ref()?)
        .ok()?;
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
        || i.is_zero()
        || k > ONE
        || lp_fee_rate >= ONE
        || U256::from(pmm.B) != base
        || U256::from(pmm.Q) != quote
        || U256::from(pmm.B0).is_zero()
        || U256::from(pmm.Q0).is_zero()
    {
        return None;
    }
    let mt_fee_rate = dodo_mt_fee_from_indexer_bps(plan.pool.fee_bps, lp_fee_rate);
    Some(PoolState::Dodo(DodoPoolState {
        base_reserve: base,
        quote_reserve: quote,
        base_token,
        quote_token,
        base_target: U256::from(pmm.B0),
        quote_target: U256::from(pmm.Q0),
        r_state,
        i,
        k,
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

fn curve_pool_requires_stored_rates(pool_type: Option<&str>) -> bool {
    pool_type.is_some_and(|t| {
        let b = t.as_bytes();
        b.windows(7).any(|w| w.eq_ignore_ascii_case(b"stable_ng"))
            || b.windows(4).any(|w| w.eq_ignore_ascii_case(b"meta"))
    })
}

fn decode_curve_stored_rates(
    plan: &PoolFetchPlan,
    results: &[Option<Bytes>],
    n_fetched: usize,
    rates_idx: usize,
) -> Option<Vec<U256>> {
    let decoded = results
        .get(rates_idx)
        .and_then(|b| b.as_ref())
        .and_then(|b| ICurvePool::stored_ratesCall::abi_decode_returns(b).ok())
        .map(|r| r.iter().map(|&x| U256::from(x)).collect::<Vec<_>>())?;
    if decoded.len() == n_fetched && !decoded.iter().any(U256::is_zero) {
        return Some(decoded);
    }
    if curve_pool_requires_stored_rates(plan.pool.pool_type.as_deref()) {
        return None;
    }
    Some(vec![ONE; n_fetched])
}

fn decode_curve_crypto_rates(n_fetched: usize, price_scale: Option<U256>) -> Vec<U256> {
    let mut rates = vec![ONE; n_fetched];
    if n_fetched >= 2
        && let Some(scale) = price_scale.filter(|scale| !scale.is_zero())
    {
        rates[1] = scale;
    }
    rates
}

fn decode_curve_stable(plan: &PoolFetchPlan, results: &[Option<Bytes>]) -> Option<PoolState> {
    let balances = decode_curve_balances(plan, results)?;
    let n_fetched = balances.len();
    let a_idx = n_fetched;
    let fee_idx = n_fetched + 1;
    let rates_idx = n_fetched + 2;
    let a = decode_u256(results.get(a_idx)?.as_ref()?)?;
    let fee = decode_u256(results.get(fee_idx)?.as_ref()?).unwrap_or(U256::from(4_000_000u64));
    let rates = decode_curve_stored_rates(plan, results, n_fetched, rates_idx)?;
    if a.is_zero() {
        return None;
    }
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
    let a = decode_u256(results.get(a_idx)?.as_ref()?)?;
    let fee = decode_u256(results.get(fee_idx)?.as_ref()?).unwrap_or(U256::from(4_000_000u64));
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
        .and_then(|b| ICurvePool::price_scaleCall::abi_decode_returns(b).ok())
        .map(U256::from);
    let rates = decode_curve_crypto_rates(n_fetched, price_scale);
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

fn dodo_mt_fee_from_indexer_bps(fee_bps: u32, lp_fee_rate: U256) -> U256 {
    if fee_bps == 0 {
        return U256::ZERO;
    }
    let total = U256::from(fee_bps) * DODO_FEE_RATE_SCALE;
    if total > lp_fee_rate && total < ONE {
        total - lp_fee_rate
    } else {
        U256::ZERO
    }
}

fn decode_balancer_linear(
    vault_tokens: &[Address],
    linear_results: Option<&[Option<Bytes>]>,
) -> Option<BalancerLinearState> {
    let results = linear_results?;
    let main =
        IBalancerLinearPool::getMainTokenCall::abi_decode_returns(results.first()?.as_ref()?)
            .ok()?;
    let wrapped =
        IBalancerLinearPool::getWrappedTokenCall::abi_decode_returns(results.get(1)?.as_ref()?)
            .ok()?;
    let targets =
        IBalancerLinearPool::getTargetsCall::abi_decode_returns(results.get(2)?.as_ref()?).ok()?;
    let rate =
        IBalancerLinearPool::getWrappedTokenRateCall::abi_decode_returns(results.get(3)?.as_ref()?)
            .ok()?;
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
        Some("linear") | Some("stable") | Some("weighted") if !has_linear_state => {
            match pool_type {
                Some("stable") if amp_valid => Some(BalancerPoolKind::Stable),
                Some("weighted") if weights_valid => Some(BalancerPoolKind::Weighted),
                _ => None,
            }
        }
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
        _ => None,
    }
}

fn decode_balancer(plan: &PoolFetchPlan, results: &[Option<Bytes>]) -> Option<PoolState> {
    let tokens_bytes = results.first()?.as_ref()?;
    let tokens = IBalancerVaultRead::getPoolTokensCall::abi_decode_returns(tokens_bytes).ok()?;
    let last_change_block = tokens.lastChangeBlock.as_limbs()[0];
    let balances: Vec<U256> = tokens.balances.iter().map(|&b| U256::from(b)).collect();
    if balances.len() < 2 {
        return None;
    }
    let n = balances.len();
    let swap_fee = results
        .get(1)
        .and_then(|b| b.as_ref())
        .and_then(|b| IBalancerPool::getSwapFeePercentageCall::abi_decode_returns(b).ok())
        .map_or_else(
            || balancer_swap_fee_from_pool_meta_fee(plan.pool.fee_bps as u64),
            U256::from,
        );
    let decoded_weights = results
        .get(2)
        .and_then(|b| b.as_ref())
        .and_then(|b| IBalancerPool::getNormalizedWeightsCall::abi_decode_returns(b).ok())
        .map(|w| w.iter().map(|&x| U256::from(x)).collect::<Vec<_>>());
    let amp_from_chain = results
        .get(3)
        .and_then(|b| b.as_ref())
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
    let scaling_factors = results
        .get(4)
        .and_then(|b| b.as_ref())
        .and_then(|b| IBalancerPool::getScalingFactorsCall::abi_decode_returns(b).ok())
        .map(|sf| sf.iter().map(|&x| U256::from(x)).collect::<Vec<_>>())?;
    if scaling_factors.len() != n || scaling_factors.iter().any(U256::is_zero) {
        return None;
    }
    let probe_linear = plan.pool.pool_type.as_deref() == Some("linear")
        || (plan.pool.pool_type.is_none() && n >= 3);
    let linear = if probe_linear {
        decode_balancer_linear(&tokens.tokens, results.get(5..))
    } else {
        None
    };
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
        tokens: tokens.tokens.clone(),
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
mod tests {
    use super::*;

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
}
