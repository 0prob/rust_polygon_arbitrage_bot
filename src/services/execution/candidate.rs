use alloy::primitives::{Address, Bytes, FixedBytes, U256};
use tracing::instrument;

use crate::core::types::{EvaluatedRoute, FlashLoanSource, RouteSimulationResult};
use crate::pipeline::arena::StateArena;
use crate::pipeline::types::PoolMeta;
use crate::services::execution::calldata::{
    RouteEncodeConfig, build_arb_calldata, build_calldata_hops, encode_route,
};
use crate::services::execution::gas::buffer_gas_limit;
use crate::services::execution::profit::{on_chain_min_profit_for_route, slippage_adjusted};

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
}

#[instrument(
    skip(arena, evaluated, config, pool_metas),
    fields(
        route_fingerprint = crate::pipeline::types::route_fingerprint(&evaluated.cycle.edges),
        hop_count = evaluated.cycle.hop_count,
        amount_in = %evaluated.result.amount_in,
        expected_profit_matic_wei = tracing::field::Empty,
    )
)]
pub fn build_execution_candidate(
    arena: &StateArena,
    evaluated: &EvaluatedRoute,
    config: &CandidateBuildConfig,
    pool_metas: &[PoolMeta],
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
        pool_metas,
    )
    .ok_or_else(|| anyhow::anyhow!("failed to build calldata hops"))?;

    let deadline = U256::from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() + config.deadline_secs_from_now)
            .unwrap_or(config.deadline_secs_from_now),
    );

    let encode_cfg = RouteEncodeConfig {
        slippage_bps: config.slippage_bps,
        deadline,
    };
    let executor_calls = encode_route(arena, &hops, config.executor_address, encode_cfg)?;
    let profit_basis = slippage_adjusted(evaluated.result.profit, config.slippage_bps)
        .unwrap_or(evaluated.result.profit);
    let min_profit = on_chain_min_profit_for_route(profit_basis, config.slippage_bps);

    let use_aave = matches!(config.flash_loan_source, FlashLoanSource::AaveV3);
    let built = build_arb_calldata(
        config.executor_address,
        start_token,
        start_token,
        evaluated.result.amount_in,
        min_profit,
        deadline,
        executor_calls,
        use_aave,
    );

    tracing::Span::current().record(
        "expected_profit_matic_wei",
        tracing::field::display(&assessment.net_profit_after_gas_matic_wei),
    );

    Ok(CandidateExecution {
        route_fingerprint: crate::pipeline::types::route_fingerprint(&evaluated.cycle.edges),
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
    })
}

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
