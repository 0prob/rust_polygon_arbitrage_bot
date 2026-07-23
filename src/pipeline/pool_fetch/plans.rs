use alloy::primitives::{Address, Bytes, FixedBytes};

use crate::core::constants::{BALANCER_VAULT, UNISWAP_V4_POOL_MANAGER};
use crate::core::types::ProtocolType;
use crate::core::v4_storage::{V4_LIQUIDITY_OFFSET, compute_v4_pool_field_slot};
use crate::pipeline::abi_cache::{
    ALGEBRA_GLOBAL_STATE, BALANCER_AMP, BALANCER_LINEAR_MAIN, BALANCER_LINEAR_RATE,
    BALANCER_LINEAR_TARGETS, BALANCER_LINEAR_WRAPPED, BALANCER_SCALING, BALANCER_SWAP_FEE,
    BALANCER_WEIGHTS, CURVE_A, CURVE_BALANCES, CURVE_CRYPTO_PRECISIONS, CURVE_CRYPTO_PRICE_SCALE,
    CURVE_FEE, CURVE_GAMMA, CURVE_STORED_RATES, DODO_BASE_RESERVE, DODO_BASE_TOKEN, DODO_I, DODO_K,
    DODO_LP_FEE, DODO_MT_FEE, DODO_PMM_STATE, DODO_QUOTE_RESERVE, DODO_QUOTE_TOKEN,
    V2_GET_RESERVES, V3_FEE, V3_LIQUIDITY, V3_SLOT0, encode_balancer_pool_tokens, encode_extsload,
};
use crate::pipeline::multicall::MulticallItem;
use crate::services::discovery::{DiscoveredPool, resolve_v4_pool_id};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CallKind {
    V2Reserves,
    V3Slot0,
    V3GlobalState,
    V3Liquidity,
    V3Fee,
    V4Slot0,
    V4Liquidity,
    DodoBase,
    DodoQuote,
    DodoBaseToken,
    DodoQuoteToken,
    DodoI,
    DodoK,
    DodoLpFee,
    DodoMtFee,
    DodoPmmState,
    CurveBalance(usize),
    CurveA,
    CurveFee,
    CurveRates,
    CurveCryptoPriceScale,
    CurveCryptoPrecisions,
    BalancerTokens,
    BalancerSwapFee,
    BalancerWeights,
    BalancerAmp,
    BalancerScalingFactors,
    BalancerLinearMainToken,
    BalancerLinearWrappedToken,
    BalancerLinearTargets,
    BalancerLinearRate,
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
    pub protocol_label: Option<String>,
}

impl From<&DiscoveredPool> for FetchPoolInfo {
    fn from(p: &DiscoveredPool) -> Self {
        Self {
            address: p.address,
            protocol: p.protocol,
            tokens: p.tokens.clone(),
            fee_bps: p.fee_bps,
            tick_spacing: p.tick_spacing,
            pool_id: if p.protocol == ProtocolType::UniswapV4 {
                resolve_v4_pool_id(p)
            } else {
                p.pool_id
            },
            pool_type: p.pool_type.clone(),
            protocol_label: Some(p.protocol_label.clone()),
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
        V2_GET_RESERVES.clone(),
        CallKind::V2Reserves,
    );
}

fn build_v3_plan(plan: &mut PoolFetchPlan) {
    let is_algebra = crate::core::protocol::is_algebra_protocol_label(
        plan.pool.protocol_label.as_deref().unwrap_or(""),
    );
    if is_algebra {
        push_call(
            plan,
            plan.pool.address,
            ALGEBRA_GLOBAL_STATE.clone(),
            CallKind::V3GlobalState,
        );
        // Fallback when globalState reverts on mislabeled or hybrid pools.
        push_call(plan, plan.pool.address, V3_SLOT0.clone(), CallKind::V3Slot0);
    } else {
        push_call(plan, plan.pool.address, V3_SLOT0.clone(), CallKind::V3Slot0);
    }
    push_call(
        plan,
        plan.pool.address,
        V3_LIQUIDITY.clone(),
        CallKind::V3Liquidity,
    );
    if !is_algebra {
        push_call(plan, plan.pool.address, V3_FEE.clone(), CallKind::V3Fee);
    }
}

fn build_v4_plan(plan: &mut PoolFetchPlan) -> bool {
    let Some(pool_id) = plan.pool.pool_id else {
        return false;
    };
    let manager = UNISWAP_V4_POOL_MANAGER;
    let slot0_key = compute_v4_pool_field_slot(&pool_id, 0);
    let liq_key = compute_v4_pool_field_slot(&pool_id, V4_LIQUIDITY_OFFSET);
    push_call(plan, manager, encode_extsload(slot0_key), CallKind::V4Slot0);
    push_call(
        plan,
        manager,
        encode_extsload(liq_key),
        CallKind::V4Liquidity,
    );
    true
}

fn build_dodo_plan(plan: &mut PoolFetchPlan) {
    let addr = plan.pool.address;
    push_call(plan, addr, DODO_BASE_RESERVE.clone(), CallKind::DodoBase);
    push_call(plan, addr, DODO_QUOTE_RESERVE.clone(), CallKind::DodoQuote);
    push_call(plan, addr, DODO_BASE_TOKEN.clone(), CallKind::DodoBaseToken);
    push_call(
        plan,
        addr,
        DODO_QUOTE_TOKEN.clone(),
        CallKind::DodoQuoteToken,
    );
    push_call(plan, addr, DODO_I.clone(), CallKind::DodoI);
    push_call(plan, addr, DODO_K.clone(), CallKind::DodoK);
    push_call(plan, addr, DODO_LP_FEE.clone(), CallKind::DodoLpFee);
    push_call(plan, addr, DODO_MT_FEE.clone(), CallKind::DodoMtFee);
    push_call(plan, addr, DODO_PMM_STATE.clone(), CallKind::DodoPmmState);
}

pub(super) fn curve_balance_slots(token_count: usize) -> usize {
    token_count.clamp(2, 8)
}

fn build_curve_plan(plan: &mut PoolFetchPlan) {
    let n = curve_balance_slots(plan.pool.tokens.len());
    for i in 0..n {
        push_call(
            plan,
            plan.pool.address,
            CURVE_BALANCES[i].clone(),
            CallKind::CurveBalance(i),
        );
    }
    push_call(plan, plan.pool.address, CURVE_A.clone(), CallKind::CurveA);
    push_call(
        plan,
        plan.pool.address,
        CURVE_FEE.clone(),
        CallKind::CurveFee,
    );
    if plan.pool.protocol == ProtocolType::CurveCrypto {
        push_call(
            plan,
            plan.pool.address,
            CURVE_GAMMA.clone(),
            CallKind::CurveGamma,
        );
        push_call(
            plan,
            plan.pool.address,
            CURVE_CRYPTO_PRICE_SCALE.clone(),
            CallKind::CurveCryptoPriceScale,
        );
        push_call(
            plan,
            plan.pool.address,
            CURVE_CRYPTO_PRECISIONS.clone(),
            CallKind::CurveCryptoPrecisions,
        );
    } else {
        push_call(
            plan,
            plan.pool.address,
            CURVE_STORED_RATES.clone(),
            CallKind::CurveRates,
        );
    }
}

fn build_balancer_plan(plan: &mut PoolFetchPlan) -> bool {
    let Some(pool_id) = plan.pool.pool_id else {
        return false;
    };
    let vault = BALANCER_VAULT;
    let addr = plan.pool.address;
    push_call(
        plan,
        vault,
        encode_balancer_pool_tokens(pool_id),
        CallKind::BalancerTokens,
    );
    push_call(
        plan,
        addr,
        BALANCER_SWAP_FEE.clone(),
        CallKind::BalancerSwapFee,
    );
    // Known family: skip probes that always revert (weighted has no amp, etc.).
    let known = plan.pool.pool_type.as_deref();
    let need_weights = matches!(known, Some("weighted") | None);
    let need_amp = matches!(known, Some("stable") | None);
    let need_linear = known == Some("linear") || (known.is_none() && plan.pool.tokens.len() >= 3);
    if need_weights {
        push_call(
            plan,
            addr,
            BALANCER_WEIGHTS.clone(),
            CallKind::BalancerWeights,
        );
    }
    if need_amp {
        push_call(plan, addr, BALANCER_AMP.clone(), CallKind::BalancerAmp);
    }
    push_call(
        plan,
        addr,
        BALANCER_SCALING.clone(),
        CallKind::BalancerScalingFactors,
    );
    if need_linear {
        push_call(
            plan,
            addr,
            BALANCER_LINEAR_MAIN.clone(),
            CallKind::BalancerLinearMainToken,
        );
        push_call(
            plan,
            addr,
            BALANCER_LINEAR_WRAPPED.clone(),
            CallKind::BalancerLinearWrappedToken,
        );
        push_call(
            plan,
            addr,
            BALANCER_LINEAR_TARGETS.clone(),
            CallKind::BalancerLinearTargets,
        );
        push_call(
            plan,
            addr,
            BALANCER_LINEAR_RATE.clone(),
            CallKind::BalancerLinearRate,
        );
    }
    true
}

fn plan_call_capacity(
    protocol: ProtocolType,
    token_count: usize,
    pool_type: Option<&str>,
) -> usize {
    match protocol {
        ProtocolType::UniswapV2 => 1,
        // Algebra: globalState + slot0 fallback + liquidity; UniV3: slot0 + liquidity + fee.
        ProtocolType::UniswapV3 => 3,
        ProtocolType::UniswapV4 => 2,
        ProtocolType::Dodo => 9,
        ProtocolType::CurveStable => curve_balance_slots(token_count).saturating_add(3),
        ProtocolType::CurveCrypto => curve_balance_slots(token_count).saturating_add(5),
        ProtocolType::BalancerV2 => {
            // tokens + fee + scaling, plus optional weights/amp/linear probes.
            let mut n = 3usize;
            if matches!(pool_type, Some("weighted") | None) {
                n += 1;
            }
            if matches!(pool_type, Some("stable") | None) {
                n += 1;
            }
            if pool_type == Some("linear") || (pool_type.is_none() && token_count >= 3) {
                n += 4;
            }
            n
        }
        ProtocolType::Woofi => 0,
    }
}

pub(super) fn build_plan_with_pool_id(
    pool: &DiscoveredPool,
    pool_id: Option<FixedBytes<32>>,
) -> Option<PoolFetchPlan> {
    let mut info = FetchPoolInfo::from(pool);
    if pool.protocol == ProtocolType::BalancerV2 {
        info.pool_id = pool_id;
    }
    let cap = plan_call_capacity(pool.protocol, pool.tokens.len(), pool.pool_type.as_deref());
    let mut plan = PoolFetchPlan {
        pool: info,
        calls: Vec::with_capacity(cap),
        kinds: Vec::with_capacity(cap),
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
    Some(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{Address, FixedBytes};

    fn balancer_pool(pool_type: Option<&str>) -> DiscoveredPool {
        DiscoveredPool {
            pool_key: "0x0000000000000000000000000000010000000003".into(),
            address: Address::from([3u8; 20]),
            protocol: ProtocolType::BalancerV2,
            protocol_label: "BALANCER_V2".into(),
            tokens: vec![Address::from([1u8; 20]), Address::from([2u8; 20])],
            fee_bps: 30,
            tick_spacing: None,
            pool_id: Some(FixedBytes::from([0x11; 32])),
            pool_id_verified: true,
            hooks: None,
            pool_type: pool_type.map(str::to_string),
            created_block: 1,
        }
    }

    #[test]
    fn known_weighted_balancer_skips_amp_and_linear_probes() {
        let pool = balancer_pool(Some("weighted"));
        let plan = build_plan_with_pool_id(&pool, pool.pool_id).expect("plan");
        assert!(plan.kinds.contains(&CallKind::BalancerWeights));
        assert!(!plan.kinds.contains(&CallKind::BalancerAmp));
        assert!(!plan.kinds.contains(&CallKind::BalancerLinearMainToken));
        assert!(plan.kinds.contains(&CallKind::BalancerScalingFactors));
    }

    #[test]
    fn known_stable_balancer_skips_weights_and_linear_probes() {
        let pool = balancer_pool(Some("stable"));
        let plan = build_plan_with_pool_id(&pool, pool.pool_id).expect("plan");
        assert!(plan.kinds.contains(&CallKind::BalancerAmp));
        assert!(!plan.kinds.contains(&CallKind::BalancerWeights));
        assert!(!plan.kinds.contains(&CallKind::BalancerLinearMainToken));
    }

    #[test]
    fn unknown_balancer_probes_weights_and_amp() {
        let pool = balancer_pool(None);
        let plan = build_plan_with_pool_id(&pool, pool.pool_id).expect("plan");
        assert!(plan.kinds.contains(&CallKind::BalancerWeights));
        assert!(plan.kinds.contains(&CallKind::BalancerAmp));
    }
}
