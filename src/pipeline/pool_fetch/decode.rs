use alloy::primitives::{Bytes, U256};
use alloy::sol_types::SolCall;

use crate::abis::{
    IBalancerPool, IBalancerVaultRead, ICurvePool, IDodoPoolState, IUniswapV4PoolManager,
};
use crate::core::math::balancer::balancer_swap_fee_from_pool_meta_fee;
use crate::core::types::{
    BalancerPoolKind, BalancerPoolState, CurvePoolState, DodoPoolState, PoolState, ProtocolType,
    V2PoolState, V3PoolState, V4PoolState,
};
use crate::core::utils::v4_storage::{decode_v4_liquidity, decode_v4_slot0};

use super::plans::PoolFetchPlan;

fn decode_u256(bytes: &Bytes) -> Option<U256> {
    if bytes.len() < 32 {
        return None;
    }
    Some(U256::from_be_slice(&bytes[bytes.len() - 32..]))
}

/// Zero-copy V2 getReserves decode (reserve0, reserve1 as 32-byte ABI words).
pub fn decode_v2_reserves(bytes: &[u8]) -> Option<(U256, U256)> {
    if bytes.len() < 64 {
        return None;
    }
    Some((
        U256::from_be_slice(&bytes[0..32]),
        U256::from_be_slice(&bytes[32..64]),
    ))
}

/// Zero-copy V3 slot0 decode (sqrtPriceX96, tick).
pub fn decode_v3_slot0(bytes: &[u8]) -> Option<(U256, i32)> {
    if bytes.len() < 64 {
        return None;
    }
    let sqrt = U256::from_be_slice(&bytes[0..32]);
    let tick_word = U256::from_be_slice(&bytes[32..64]);
    let tick_raw = (tick_word & U256::from(0xFF_FFFFu64)).as_limbs()[0] as u32;
    let tick = if tick_raw & 0x80_0000 != 0 {
        (tick_raw | !0xFF_FFFF) as i32
    } else {
        tick_raw as i32
    };
    Some((sqrt, tick))
}

fn decode_u128_word(bytes: &[u8]) -> Option<u128> {
    if bytes.len() < 32 {
        return None;
    }
    let v = U256::from_be_slice(&bytes[bytes.len() - 32..]);
    Some(v.as_limbs()[0] as u128)
}

fn decode_u24_fee(bytes: &[u8]) -> Option<u32> {
    if bytes.len() < 32 {
        return None;
    }
    let v = U256::from_be_slice(&bytes[bytes.len() - 32..]);
    Some(v.as_limbs()[0] as u32)
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
    let (r0, r1) = decode_v2_reserves(bytes)?;
    if r0.is_zero() || r1.is_zero() {
        return None;
    }
    let fee_bps = U256::from(plan.pool.fee_bps);
    Some(PoolState::V2(V2PoolState {
        reserve0: r0,
        reserve1: r1,
        fee: fee_bps,
        fee_denominator: U256::from(10_000u64),
    }))
}

fn decode_v3(plan: &PoolFetchPlan, results: &[Option<Bytes>]) -> Option<PoolState> {
    let slot0_bytes = results.first()?.as_ref()?;
    let liq_bytes = results.get(1)?.as_ref()?;
    let (sqrt, tick) = decode_v3_slot0(slot0_bytes)?;
    let liquidity = decode_u128_word(liq_bytes)?;
    if sqrt.is_zero() || liquidity == 0 {
        return None;
    }
    let fee_pips = results
        .get(2)
        .and_then(|b| b.as_ref())
        .and_then(|b| decode_u24_fee(b))
        .map(U256::from)
        .unwrap_or_else(|| U256::from(plan.pool.fee_bps) * U256::from(100u32));
    Some(PoolState::V3(V3PoolState {
        sqrt_price_x96: sqrt,
        tick,
        liquidity,
        fee: fee_pips,
        tick_spacing: plan.pool.tick_spacing.unwrap_or(60),
        ticks: Box::new([]),
    }))
}

fn decode_v4(plan: &PoolFetchPlan, results: &[Option<Bytes>]) -> Option<PoolState> {
    let slot0_bytes = results.first()?.as_ref()?;
    let liq_bytes = results.get(1)?.as_ref()?;
    let slot0_raw = IUniswapV4PoolManager::extsloadCall::abi_decode_returns(slot0_bytes).ok()?;
    let liq_raw = IUniswapV4PoolManager::extsloadCall::abi_decode_returns(liq_bytes).ok()?;
    let decoded = decode_v4_slot0(U256::from_be_bytes(slot0_raw.0));
    let liquidity = decode_v4_liquidity(U256::from_be_bytes(liq_raw.0));
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
        ticks: Box::new([]),
    }))
}

fn decode_dodo(_plan: &PoolFetchPlan, results: &[Option<Bytes>]) -> Option<PoolState> {
    let base = decode_u256(results.first()?.as_ref()?)?;
    let quote = decode_u256(results.get(1)?.as_ref()?)?;
    if base.is_zero() || quote.is_zero() {
        return None;
    }
    let r_status = results
        .get(2)
        .and_then(|b| b.as_ref())
        .and_then(|b| IDodoPoolState::_R_STATUS_Call::abi_decode_returns(b).ok())
        .unwrap_or(0);
    Some(PoolState::Dodo(DodoPoolState {
        base_reserve: base,
        quote_reserve: quote,
        base_target: decode_u256(results.get(3)?.as_ref()?).unwrap_or(base),
        quote_target: decode_u256(results.get(4)?.as_ref()?).unwrap_or(quote),
        i: decode_u256(results.get(5)?.as_ref()?).unwrap_or(U256::from(10u128).pow(U256::from(18))),
        k: decode_u256(results.get(6)?.as_ref()?).unwrap_or(U256::ZERO),
        r_status,
        lp_fee_rate: decode_u256(results.get(7)?.as_ref()?).unwrap_or(U256::ZERO),
        mt_fee_rate: decode_u256(results.get(8)?.as_ref()?).unwrap_or(U256::ZERO),
    }))
}

fn decode_curve_stable(plan: &PoolFetchPlan, results: &[Option<Bytes>]) -> Option<PoolState> {
    let n_fetched = plan.pool.tokens.len();
    let n_coins = n_fetched;
    let mut balances = Vec::with_capacity(n_fetched);
    for i in 0..n_fetched {
        let b = decode_u256(results.get(i)?.as_ref()?)?;
        balances.push(b);
    }
    let a_idx = n_fetched;
    let fee_idx = n_fetched + 1;
    let rates_idx = n_fetched + 2;
    let a = decode_u256(results.get(a_idx)?.as_ref()?)?;
    let fee = decode_u256(results.get(fee_idx)?.as_ref()?).unwrap_or(U256::from(4_000_000u64));
    let rates = results
        .get(rates_idx)
        .and_then(|b| b.as_ref())
        .and_then(|b| ICurvePool::stored_ratesCall::abi_decode_returns(b).ok())
        .map(|r| r.iter().map(|&x| U256::from(x)).collect())
        .unwrap_or_else(|| vec![U256::from(10u128).pow(U256::from(18)); n_fetched]);
    Some(PoolState::Curve(CurvePoolState {
        balances,
        a,
        fee,
        rates,
        n_coins: n_coins as u8,
        gamma: None,
        d: None,
    }))
}

fn decode_curve_crypto(plan: &PoolFetchPlan, results: &[Option<Bytes>]) -> Option<PoolState> {
    let mut state = decode_curve_stable(plan, results)?;
    if let PoolState::Curve(ref mut c) = state {
        let n_fetched = plan.pool.tokens.len();
        let gamma_idx = n_fetched + 3;
        c.gamma = results
            .get(gamma_idx)
            .and_then(|b| b.as_ref())
            .and_then(|b| ICurvePool::gammaCall::abi_decode_returns(b).ok())
            .map(U256::from);
    }
    Some(state)
}

fn decode_balancer(plan: &PoolFetchPlan, results: &[Option<Bytes>]) -> Option<PoolState> {
    let tokens_bytes = results.first()?.as_ref()?;
    let tokens = IBalancerVaultRead::getPoolTokensCall::abi_decode_returns(tokens_bytes).ok()?;
    let balances: Vec<U256> = tokens.balances.iter().map(|&b| U256::from(b)).collect();
    if balances.len() < 2 {
        return None;
    }
    let n = balances.len();
    let swap_fee = results
        .get(1)
        .and_then(|b| b.as_ref())
        .and_then(|b| IBalancerPool::getSwapFeePercentageCall::abi_decode_returns(b).ok())
        .map(U256::from)
        .unwrap_or_else(|| balancer_swap_fee_from_pool_meta_fee(plan.pool.fee_bps as u64));
    let weights = results
        .get(2)
        .and_then(|b| b.as_ref())
        .and_then(|b| IBalancerPool::getNormalizedWeightsCall::abi_decode_returns(b).ok())
        .map(|w| w.iter().map(|&x| U256::from(x)).collect())
        .unwrap_or_else(|| vec![U256::from(10u128).pow(U256::from(18)); n]);
    let amp_from_chain = results
        .get(3)
        .and_then(|b| b.as_ref())
        .and_then(|b| IBalancerPool::getAmplificationParameterCall::abi_decode_returns(b).ok());
    let (amp, amp_precision, has_onchain_amp) = match amp_from_chain {
        Some(t) => (U256::from(t.value), U256::from(t.precision), true),
        None => (U256::ZERO, U256::ZERO, false),
    };
    let scaling_factors = results
        .get(4)
        .and_then(|b| b.as_ref())
        .and_then(|b| IBalancerPool::getScalingFactorsCall::abi_decode_returns(b).ok())
        .map(|sf| sf.iter().map(|&x| U256::from(x)).collect())
        .unwrap_or_else(|| vec![U256::from(10u128).pow(U256::from(18)); n]);
    let pool_type = if has_onchain_amp {
        if amp > U256::ZERO && weights.iter().any(|w| *w != weights[0]) {
            BalancerPoolKind::Weighted
        } else if amp > U256::ZERO {
            BalancerPoolKind::Stable
        } else {
            BalancerPoolKind::Weighted
        }
    } else if plan
        .pool
        .pool_type
        .as_deref()
        .is_some_and(|t| t.contains("stable"))
    {
        BalancerPoolKind::Stable
    } else {
        BalancerPoolKind::Weighted
    };
    let bpt_index = tokens.tokens.iter().position(|t| *t == plan.pool.address);
    Some(PoolState::Balancer(BalancerPoolState {
        pool_id: plan.pool.pool_id,
        balances,
        weights,
        scaling_factors,
        amp,
        amp_precision,
        fee: swap_fee,
        pool_type,
        bpt_index,
    }))
}
