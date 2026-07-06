use alloy::primitives::{Address, Bytes, FixedBytes, U256};
use rustc_hash::{FxHashMap, FxHasher};

use crate::core::types::{EvaluatedRoute, FlashLoanSource, PoolIndex, RouteSimulationResult};
use crate::pipeline::arena::StateArena;
use crate::pipeline::types::PoolMeta;
use crate::services::execution::calldata::{
    ExecutorEntrypoint, RouteEncodeConfig, build_arb_calldata, build_calldata_hops, encode_route,
    hops_are_balancer_only,
};
use crate::services::execution::gas::buffer_gas_limit;
use crate::services::execution::profit::on_chain_min_profit;

#[derive(Debug, Clone)]
pub struct CandidateExecution {
    pub route_fingerprint: u64,
    pub calldata: Bytes,
    pub target_address: Address,
    pub value: U256,
    pub profit_token: Address,
    pub expected_profit_matic_wei: U256,
    pub gas_limit: Option<U256>,
    pub simulated_gas: u32,
    pub route_hash: FixedBytes<32>,
    /// Fields used to re-assess profitability after dry-run gas is known.
    pub gross_profit: U256,
    pub amount_in: U256,
    pub token_decimals: u8,
    pub token_to_matic_rate: U256,
    pub slippage_bps: u64,
    pub flash_loan_source: FlashLoanSource,
    pub min_profit_matic_wei: U256,
    pub min_profit_roi_bps: u64,
    pub hop_count: u32,
    pub safety_multiplier_bps: u64,
    /// State-cache generation used to build and simulate this candidate.
    pub state_generation: u64,
}

pub struct CandidateBuildConfig {
    pub executor_address: Address,
    pub slippage_bps: u64,
    pub flash_loan_source: FlashLoanSource,
    pub deadline_secs_from_now: u64,
    pub min_profit_matic_wei: U256,
    pub min_profit_roi_bps: u64,
    pub token_decimals: u8,
    pub token_to_matic_rate: U256,
    pub safety_multiplier_bps: u64,
    pub state_generation: u64,
    pub route_fingerprint: u64,
}

#[must_use]
fn balancer_batch_direct_eligible(
    hops: &[crate::services::execution::calldata::CalldataHop],
) -> bool {
    hops_are_balancer_only(hops)
        && hops.len() <= crate::pipeline::route_calls::MAX_BALANCER_BATCH_HOPS
}

#[must_use]
fn resolve_executor_entrypoint(
    flash_source: FlashLoanSource,
    hops: &[crate::services::execution::calldata::CalldataHop],
) -> ExecutorEntrypoint {
    match flash_source {
        FlashLoanSource::Direct if balancer_batch_direct_eligible(hops) => {
            ExecutorEntrypoint::Direct
        }
        FlashLoanSource::Balancer if hops_are_balancer_only(hops) => {
            ExecutorEntrypoint::BalancerFlash
        }
        FlashLoanSource::Direct | FlashLoanSource::AaveV3 | FlashLoanSource::Balancer => {
            ExecutorEntrypoint::AaveFlash
        }
    }
}

/// Align flash-loan source with executor entrypoint (mixed Balancer hops cannot use Balancer flash).
#[must_use]
fn resolve_dispatch_flash_source(
    flash_source: FlashLoanSource,
    hops: &[crate::services::execution::calldata::CalldataHop],
) -> FlashLoanSource {
    if (!hops_are_balancer_only(hops)
        && matches!(
            flash_source,
            FlashLoanSource::Balancer | FlashLoanSource::Direct
        ))
        || (flash_source == FlashLoanSource::Direct && !balancer_batch_direct_eligible(hops))
    {
        FlashLoanSource::AaveV3
    } else {
        flash_source
    }
}

pub fn build_execution_candidate(
    arena: &StateArena,
    evaluated: &EvaluatedRoute,
    config: &CandidateBuildConfig,
    pool_metas_by_pool: &FxHashMap<PoolIndex, &PoolMeta>,
) -> anyhow::Result<CandidateExecution> {
    let assessment = evaluated
        .assessment
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("missing profit assessment"))?;

    let start_token = arena
        .token_address(evaluated.cycle.start_token)
        .ok_or_else(|| anyhow::anyhow!("missing start token address"))?;

    let hops = build_calldata_hops(
        arena,
        &evaluated.cycle.edges,
        &evaluated.result.hop_amounts,
        pool_metas_by_pool,
    )
    .ok_or_else(|| anyhow::anyhow!("failed to build calldata hops"))?;

    let deadline = U256::from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(config.deadline_secs_from_now, |d| {
                d.as_secs() + config.deadline_secs_from_now
            }),
    );

    let dispatch_flash_source = resolve_dispatch_flash_source(config.flash_loan_source, &hops);
    let encode_cfg = RouteEncodeConfig {
        slippage_bps: config.slippage_bps,
        deadline,
    };
    let executor_calls = encode_route(
        arena,
        &hops,
        config.executor_address,
        encode_cfg,
        dispatch_flash_source,
    )?;
    // The assessment already applies route slippage and the selected flash-loan
    // fee. Encode that same executable profit basis instead of gross AMM output.
    let min_profit = on_chain_min_profit(assessment.net_profit)
        .ok_or_else(|| anyhow::anyhow!("invalid or overflowing on-chain profit calculation"))?;

    let entrypoint = resolve_executor_entrypoint(dispatch_flash_source, &hops);
    let built = build_arb_calldata(
        config.executor_address,
        start_token,
        start_token,
        evaluated.result.amount_in,
        min_profit,
        deadline,
        executor_calls,
        entrypoint,
    )?;

    Ok(CandidateExecution {
        route_fingerprint: config.route_fingerprint,
        calldata: built.data,
        target_address: built.to,
        value: built.value,
        profit_token: start_token,
        expected_profit_matic_wei: assessment.net_profit_after_gas_matic_wei,
        gas_limit: buffer_gas_limit(evaluated.result.total_gas),
        simulated_gas: evaluated.result.total_gas,
        route_hash: built.route_hash,
        gross_profit: evaluated.result.profit,
        amount_in: evaluated.result.amount_in,
        token_decimals: config.token_decimals,
        token_to_matic_rate: config.token_to_matic_rate,
        slippage_bps: config.slippage_bps,
        flash_loan_source: config.flash_loan_source,
        min_profit_matic_wei: config.min_profit_matic_wei,
        min_profit_roi_bps: config.min_profit_roi_bps,
        hop_count: evaluated.cycle.hop_count,
        safety_multiplier_bps: config.safety_multiplier_bps,
        state_generation: config.state_generation,
    })
}

#[must_use]
pub fn evaluated_from_sim(
    cycle: crate::core::types::FoundCycle,
    result: RouteSimulationResult,
    assessment: crate::core::types::ProfitAssessment,
    effective_slippage_bps: u64,
) -> EvaluatedRoute {
    EvaluatedRoute {
        cycle,
        result,
        assessment: Some(assessment),
        effective_slippage_bps,
    }
}

#[must_use]
pub fn hash_cycle_edges(edges: &[crate::core::types::Edge]) -> u64 {
    use std::hash::Hasher;
    let mut h = FxHasher::default();
    for e in edges {
        let p_in = ((e.pool_index.0 as u64) << 32) | (e.token_in.0 as u64);
        let out_z = ((e.token_out.0 as u64) << 32) | (e.zero_for_one as u64);
        h.write_u64(p_in);
        h.write_u64(out_z);
    }
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{Edge, ProtocolType, TokenIndex};
    use crate::services::execution::calldata::CalldataHop;

    fn hop(protocol: ProtocolType) -> CalldataHop {
        CalldataHop {
            edge: Edge {
                pool_index: PoolIndex(0),
                token_in: TokenIndex(0),
                token_out: TokenIndex(1),
                token_in_idx: 0,
                token_out_idx: 1,
                protocol,
                fee_bps: 0,
                zero_for_one: true,
            },
            pool_address: Address::ZERO,
            token_in: Address::ZERO,
            token_out: Address::repeat_byte(1),
            amount_in: U256::from(1u8),
            amount_out: U256::from(1u8),
            pool_id: None,
            protocol_label: None,
            pool_type: None,
            router: None,
            hooks: None,
        }
    }

    #[test]
    fn mixed_balancer_hops_force_aave_dispatch() {
        let hops = vec![hop(ProtocolType::BalancerV2), hop(ProtocolType::UniswapV3)];
        assert_eq!(
            resolve_dispatch_flash_source(FlashLoanSource::Balancer, &hops),
            FlashLoanSource::AaveV3
        );
        assert_eq!(
            resolve_executor_entrypoint(FlashLoanSource::Balancer, &hops),
            ExecutorEntrypoint::AaveFlash
        );
    }

    #[test]
    fn pure_balancer_hops_keep_direct_or_balancer_flash() {
        let hops = vec![hop(ProtocolType::BalancerV2)];
        assert_eq!(
            resolve_dispatch_flash_source(FlashLoanSource::Direct, &hops),
            FlashLoanSource::Direct
        );
        assert_eq!(
            resolve_executor_entrypoint(FlashLoanSource::Direct, &hops),
            ExecutorEntrypoint::Direct
        );
        assert_eq!(
            resolve_executor_entrypoint(FlashLoanSource::Balancer, &hops),
            ExecutorEntrypoint::BalancerFlash
        );
    }

    #[test]
    fn long_balancer_routes_force_aave_instead_of_direct_batch() {
        let hops = vec![
            hop(ProtocolType::BalancerV2),
            hop(ProtocolType::BalancerV2),
            hop(ProtocolType::BalancerV2),
            hop(ProtocolType::BalancerV2),
            hop(ProtocolType::BalancerV2),
        ];
        assert_eq!(
            resolve_dispatch_flash_source(FlashLoanSource::Direct, &hops),
            FlashLoanSource::AaveV3
        );
        assert_eq!(
            resolve_executor_entrypoint(FlashLoanSource::Direct, &hops),
            ExecutorEntrypoint::AaveFlash
        );
    }
}
