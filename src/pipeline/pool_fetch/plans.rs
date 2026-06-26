use alloy::primitives::{Address, Bytes, FixedBytes, U256};

use crate::abis::{
    IBalancerPool, IBalancerVaultRead, ICurvePool, IDodoPoolState, IUniswapV2Pair, IUniswapV3Pool,
    IUniswapV4PoolManager,
};
use crate::core::constants::{BALANCER_VAULT, UNISWAP_V4_POOL_MANAGER};
use crate::core::types::ProtocolType;
use crate::core::utils::v4_storage::{V4_LIQUIDITY_OFFSET, compute_v4_pool_field_slot};
use crate::pipeline::multicall::{MulticallItem, encode_call};
use crate::services::discovery::DiscoveredPool;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CallKind {
    V2Reserves,
    V3Slot0,
    V3Liquidity,
    V3Fee,
    V4Slot0,
    V4Liquidity,
    DodoBase,
    DodoQuote,
    DodoRStatus,
    DodoBaseTarget,
    DodoQuoteTarget,
    DodoI,
    DodoK,
    DodoLpFee,
    DodoMtFee,
    CurveBalance(usize),
    CurveA,
    CurveFee,
    CurveRates,
    BalancerTokens,
    BalancerSwapFee,
    BalancerWeights,
    BalancerAmp,
    BalancerScalingFactors,
    CurveGamma,
}

#[derive(Debug, Clone)]
pub(super) struct FetchPoolInfo {
    pub address: Address,
    pub protocol: ProtocolType,
    pub tokens: Vec<Address>,
    pub fee_bps: u32,
    pub tick_spacing: Option<i32>,
    pub pool_id: Option<FixedBytes<32>>,
    pub pool_type: Option<String>,
}

impl From<&DiscoveredPool> for FetchPoolInfo {
    fn from(p: &DiscoveredPool) -> Self {
        Self {
            address: p.address,
            protocol: p.protocol,
            tokens: p.tokens.clone(),
            fee_bps: p.fee_bps,
            tick_spacing: p.tick_spacing,
            pool_id: p.pool_id,
            pool_type: p.pool_type.clone(),
        }
    }
}

pub(super) struct PoolFetchPlan {
    pub pool: FetchPoolInfo,
    pub calls: Vec<MulticallItem>,
    pub kinds: Vec<CallKind>,
}

fn push_call(plan: &mut PoolFetchPlan, target: Address, data: Bytes, kind: CallKind) {
    plan.calls.push(MulticallItem { target, data });
    plan.kinds.push(kind);
}

fn build_v2_plan(plan: &mut PoolFetchPlan) {
    push_call(
        plan,
        plan.pool.address,
        encode_call(&IUniswapV2Pair::getReservesCall {}),
        CallKind::V2Reserves,
    );
}

fn build_v3_plan(plan: &mut PoolFetchPlan) {
    push_call(
        plan,
        plan.pool.address,
        encode_call(&IUniswapV3Pool::slot0Call {}),
        CallKind::V3Slot0,
    );
    push_call(
        plan,
        plan.pool.address,
        encode_call(&IUniswapV3Pool::liquidityCall {}),
        CallKind::V3Liquidity,
    );
    push_call(
        plan,
        plan.pool.address,
        encode_call(&IUniswapV3Pool::feeCall {}),
        CallKind::V3Fee,
    );
}

fn build_v4_plan(plan: &mut PoolFetchPlan) -> bool {
    let Some(pool_id) = plan.pool.pool_id else {
        tracing::warn!(addr = %plan.pool.address, "V4 pool missing pool_id, skipping");
        return false;
    };
    let manager = UNISWAP_V4_POOL_MANAGER;
    let slot0_key = compute_v4_pool_field_slot(&pool_id, 0);
    let liq_key = compute_v4_pool_field_slot(&pool_id, V4_LIQUIDITY_OFFSET);
    push_call(
        plan,
        manager,
        encode_call(&IUniswapV4PoolManager::extsloadCall { slot: slot0_key }),
        CallKind::V4Slot0,
    );
    push_call(
        plan,
        manager,
        encode_call(&IUniswapV4PoolManager::extsloadCall { slot: liq_key }),
        CallKind::V4Liquidity,
    );
    true
}

fn build_dodo_plan(plan: &mut PoolFetchPlan) {
    let addr = plan.pool.address;
    push_call(
        plan,
        addr,
        encode_call(&IDodoPoolState::_BASE_RESERVE_Call {}),
        CallKind::DodoBase,
    );
    push_call(
        plan,
        addr,
        encode_call(&IDodoPoolState::_QUOTE_RESERVE_Call {}),
        CallKind::DodoQuote,
    );
    push_call(
        plan,
        addr,
        encode_call(&IDodoPoolState::_R_STATUS_Call {}),
        CallKind::DodoRStatus,
    );
    push_call(
        plan,
        addr,
        encode_call(&IDodoPoolState::_BASE_TARGET_Call {}),
        CallKind::DodoBaseTarget,
    );
    push_call(
        plan,
        addr,
        encode_call(&IDodoPoolState::_QUOTE_TARGET_Call {}),
        CallKind::DodoQuoteTarget,
    );
    push_call(
        plan,
        addr,
        encode_call(&IDodoPoolState::_I_Call {}),
        CallKind::DodoI,
    );
    push_call(
        plan,
        addr,
        encode_call(&IDodoPoolState::_K_Call {}),
        CallKind::DodoK,
    );
    push_call(
        plan,
        addr,
        encode_call(&IDodoPoolState::_LP_FEE_RATE_Call {}),
        CallKind::DodoLpFee,
    );
    push_call(
        plan,
        addr,
        encode_call(&IDodoPoolState::_MT_FEE_RATE_Call {}),
        CallKind::DodoMtFee,
    );
}

fn build_curve_plan(plan: &mut PoolFetchPlan) {
    let n = plan.pool.tokens.len();
    for i in 0..n {
        push_call(
            plan,
            plan.pool.address,
            encode_call(&ICurvePool::balancesCall { i: U256::from(i) }),
            CallKind::CurveBalance(i),
        );
    }
    push_call(
        plan,
        plan.pool.address,
        encode_call(&ICurvePool::ACall {}),
        CallKind::CurveA,
    );
    push_call(
        plan,
        plan.pool.address,
        encode_call(&ICurvePool::feeCall {}),
        CallKind::CurveFee,
    );
    push_call(
        plan,
        plan.pool.address,
        encode_call(&ICurvePool::stored_ratesCall {}),
        CallKind::CurveRates,
    );
    push_call(
        plan,
        plan.pool.address,
        encode_call(&ICurvePool::gammaCall {}),
        CallKind::CurveGamma,
    );
}

fn build_balancer_plan(plan: &mut PoolFetchPlan) -> bool {
    let Some(pool_id) = plan.pool.pool_id else {
        tracing::warn!(
            addr = %plan.pool.address,
            "balancer pool missing pool_id — skipping vault fetch"
        );
        return false;
    };
    let vault = BALANCER_VAULT;
    push_call(
        plan,
        vault,
        encode_call(&IBalancerVaultRead::getPoolTokensCall { poolId: pool_id }),
        CallKind::BalancerTokens,
    );
    push_call(
        plan,
        plan.pool.address,
        encode_call(&IBalancerPool::getSwapFeePercentageCall {}),
        CallKind::BalancerSwapFee,
    );
    push_call(
        plan,
        plan.pool.address,
        encode_call(&IBalancerPool::getNormalizedWeightsCall {}),
        CallKind::BalancerWeights,
    );
    push_call(
        plan,
        plan.pool.address,
        encode_call(&IBalancerPool::getAmplificationParameterCall {}),
        CallKind::BalancerAmp,
    );
    push_call(
        plan,
        plan.pool.address,
        encode_call(&IBalancerPool::getScalingFactorsCall {}),
        CallKind::BalancerScalingFactors,
    );
    true
}

pub(super) fn build_plan(pool: &DiscoveredPool) -> Option<PoolFetchPlan> {
    let info = FetchPoolInfo::from(pool);
    let mut plan = PoolFetchPlan {
        pool: info,
        calls: Vec::new(),
        kinds: Vec::new(),
    };
    match pool.protocol {
        ProtocolType::UniswapV2 => build_v2_plan(&mut plan),
        ProtocolType::UniswapV3 => build_v3_plan(&mut plan),
        ProtocolType::UniswapV4 => {
            if !build_v4_plan(&mut plan) {
                return None;
            }
        }
        ProtocolType::Dodo => build_dodo_plan(&mut plan),
        ProtocolType::CurveStable | ProtocolType::CurveCrypto => build_curve_plan(&mut plan),
        ProtocolType::BalancerV2 => {
            if !build_balancer_plan(&mut plan) {
                return None;
            }
        }
        ProtocolType::Woofi => return None,
    }
    if plan.calls.is_empty() {
        None
    } else {
        Some(plan)
    }
}
