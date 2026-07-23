use alloy::primitives::{Address, Bytes, FixedBytes, U256};
use rustc_hash::FxHashMap;

use crate::core::types::{
    EvaluatedRoute, FlashLoanSource, PoolIndex, ProtocolType, RouteSimulationResult,
};
use crate::pipeline::arena::StateArena;
use crate::pipeline::types::PoolMeta;
use crate::services::execution::calldata::{
    ExecutorEntrypoint, RouteEncodeConfig, build_arb_calldata, build_calldata_hops, encode_route,
    hops_are_balancer_only,
};
use crate::services::execution::flash_liquidity::{
    TokenFlashLiquidity, align_flash_source_for_dispatch, dodo_base_flash_pool_for_cycle,
};
use crate::services::execution::gas::buffer_gas_limit;
use crate::services::execution::profit::AssessProfitInput;
use crate::services::execution::profit::on_chain_min_profit_from_assessment;

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
    pub state_block: u64,
    pub state_hash: Option<alloy::primitives::B256>,
    /// Human-readable hop trace for dry-run / submit failure logs.
    pub route_trace: String,
    pub adaptive_flash_cap_bound: bool,
    pub adaptive_flash_loan_usd_limit: u64,
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
    pub state_block: u64,
    pub state_hash: Option<alloy::primitives::B256>,
    pub route_fingerprint: u64,
    pub flash_liquidity: TokenFlashLiquidity,
    pub has_dodo_pool: bool,
    /// Skip liquidity re-alignment when `prepare_evaluated_route` already validated the plan.
    pub trust_prepared_flash: bool,
    pub adaptive_flash_cap_bound: bool,
    pub adaptive_flash_loan_usd_limit: u64,
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
        FlashLoanSource::Balancer => ExecutorEntrypoint::BalancerFlash,
        FlashLoanSource::Dodo => ExecutorEntrypoint::DodoFlash,
        FlashLoanSource::AaveV3 | FlashLoanSource::Direct => ExecutorEntrypoint::AaveFlash,
    }
}

/// Direct→Aave when the route cannot use `executeArbDirect` batch.
#[must_use]
fn structural_flash_source(
    flash_source: FlashLoanSource,
    hops: &[crate::services::execution::calldata::CalldataHop],
) -> FlashLoanSource {
    if flash_source == FlashLoanSource::Direct && !balancer_batch_direct_eligible(hops) {
        FlashLoanSource::AaveV3
    } else {
        flash_source
    }
}

/// Resolve flash source + entrypoint. When `liquidity` is `None`, trust the prepared
/// structural source (HF already aligned); otherwise re-check live liquidity.
fn resolve_dispatch(
    flash_source: FlashLoanSource,
    hops: &[crate::services::execution::calldata::CalldataHop],
    liquidity: Option<&TokenFlashLiquidity>,
    has_dodo_pool: bool,
) -> anyhow::Result<(FlashLoanSource, ExecutorEntrypoint)> {
    let source = structural_flash_source(flash_source, hops);
    if source == FlashLoanSource::Dodo {
        if !crate::services::execution::profit::DODO_EXTERNAL_FLASH_ENABLED {
            anyhow::bail!("DODO flash disabled until external (non-route) lenders are wired");
        }
        if !has_dodo_pool {
            anyhow::bail!("DODO flash requires a DODO pool in the route");
        }
    }
    let dispatch_flash_source = if let Some(liquidity) = liquidity {
        let balancer_only = hops_are_balancer_only(hops);
        let route_uses_balancer_vault = hops
            .iter()
            .any(|h| h.edge.protocol == ProtocolType::BalancerV2);
        align_flash_source_for_dispatch(
            source,
            liquidity,
            balancer_only,
            has_dodo_pool,
            route_uses_balancer_vault,
        )
        .ok_or_else(|| anyhow::anyhow!("no viable flash source for route"))?
    } else {
        source
    };
    Ok((
        dispatch_flash_source,
        resolve_executor_entrypoint(dispatch_flash_source, hops),
    ))
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
    .map_err(|reason| anyhow::anyhow!("failed to build calldata hops: {reason}"))?;

    let deadline = U256::from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(config.deadline_secs_from_now, |d| {
                d.as_secs() + config.deadline_secs_from_now
            }),
    );

    let (dispatch_flash_source, entrypoint) = resolve_dispatch(
        config.flash_loan_source,
        &hops,
        (!config.trust_prepared_flash).then_some(&config.flash_liquidity),
        config.has_dodo_pool,
    )?;
    // Non-DODO flash credits `start_token`; hop0 must spend that same ERC-20.
    // (DODO packs the lending pool address into the flash_token field.)
    if entrypoint != ExecutorEntrypoint::DodoFlash
        && hops.first().is_none_or(|h| h.token_in != start_token)
    {
        anyhow::bail!(
            "flash/hop0 token mismatch: start={start_token} hop0_in={}",
            hops.first().map(|h| h.token_in).unwrap_or_default()
        );
    }
    if evaluated.result.amount_in.is_zero() {
        anyhow::bail!("flash amount_in is zero");
    }
    let encode_cfg = RouteEncodeConfig {
        // Per-hop config only — hop minOut must not re-apply full-route depth haircut.
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
    // minProfit must match the route-level assessment (already compound+depth slip + flash fee).
    // Rebuilding via per-hop `on_chain_min_profit_for_route` double-compounds slip and can
    // set a floor above what prepare/reassess modeled — dry-run InsufficientProfit ghosts.
    let min_profit = on_chain_min_profit_from_assessment(assessment)
        .ok_or_else(|| anyhow::anyhow!("invalid or overflowing on-chain profit calculation"))?;

    // DODO flash loan: packRoute flash_token field must be the DODO pool address,
    // not the token address — the Huff contract calls flashLoan on it directly.
    let flash_token = if entrypoint == ExecutorEntrypoint::DodoFlash {
        dodo_base_flash_pool_for_cycle(arena, &evaluated.cycle).ok_or_else(|| {
            anyhow::anyhow!("DODO flash entrypoint but no base-compatible DODO pool in route")
        })?
    } else {
        start_token
    };

    let route_trace = hops
        .iter()
        .map(|h| {
            let label = h
                .protocol_label
                .as_deref()
                .map(str::to_owned)
                .unwrap_or_else(|| format!("{:?}", h.edge.protocol));
            format!("{label}@{:#x}", h.pool_address)
        })
        .collect::<Vec<_>>()
        .join("->");

    let built = build_arb_calldata(
        config.executor_address,
        flash_token,
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
        flash_loan_source: dispatch_flash_source,
        min_profit_matic_wei: config.min_profit_matic_wei,
        min_profit_roi_bps: config.min_profit_roi_bps,
        hop_count: evaluated.cycle.edge_hops(),
        safety_multiplier_bps: config.safety_multiplier_bps,
        state_generation: config.state_generation,
        state_block: config.state_block,
        state_hash: config.state_hash,
        route_trace,
        adaptive_flash_cap_bound: config.adaptive_flash_cap_bound,
        adaptive_flash_loan_usd_limit: config.adaptive_flash_loan_usd_limit,
    })
}

impl CandidateExecution {
    #[must_use]
    pub fn profit_assessment_input(
        &self,
        gas_units: u32,
        gas_price_wei: U256,
        min_profit_matic_wei: U256,
    ) -> AssessProfitInput {
        AssessProfitInput {
            gross_profit: self.gross_profit,
            amount_in: self.amount_in,
            gas_units,
            gas_price_wei,
            token_to_matic_rate: self.token_to_matic_rate,
            token_decimals: self.token_decimals,
            hop_count: self.hop_count,
            min_profit_matic_wei,
            min_profit_roi_bps: self.min_profit_roi_bps,
            slippage_bps: self.slippage_bps,
            flash_loan_source: self.flash_loan_source,
            safety_multiplier_bps: self.safety_multiplier_bps,
            profit_priority_alpha_bps: 0,
        }
    }
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
    crate::pipeline::cycle_filter::cycle_key(edges)
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

    fn aave_liquidity() -> TokenFlashLiquidity {
        TokenFlashLiquidity {
            balancer: U256::ZERO,
            aave: U256::from(1_000u64),
            aave_listed: true,
            dodo: U256::ZERO,
        }
    }

    #[test]
    fn mixed_balancer_hops_force_aave_dispatch() {
        let hops = vec![hop(ProtocolType::BalancerV2), hop(ProtocolType::UniswapV3)];
        let (source, entry) = resolve_dispatch(
            FlashLoanSource::Balancer,
            &hops,
            Some(&aave_liquidity()),
            false,
        )
        .expect("aave available");
        assert_eq!(source, FlashLoanSource::AaveV3);
        assert_eq!(entry, ExecutorEntrypoint::AaveFlash);
    }

    #[test]
    fn mixed_balancer_hops_skip_when_aave_unavailable() {
        let hops = vec![hop(ProtocolType::BalancerV2), hop(ProtocolType::UniswapV3)];
        let liquidity = TokenFlashLiquidity::default();
        assert!(
            resolve_dispatch(FlashLoanSource::Balancer, &hops, Some(&liquidity), false).is_err()
        );
    }

    #[test]
    fn pure_balancer_hops_keep_direct_or_balancer_flash() {
        let hops = vec![hop(ProtocolType::BalancerV2)];
        let (source, entry) = resolve_dispatch(
            FlashLoanSource::Direct,
            &hops,
            Some(&aave_liquidity()),
            false,
        )
        .expect("direct");
        assert_eq!(source, FlashLoanSource::Direct);
        assert_eq!(entry, ExecutorEntrypoint::Direct);
        assert_eq!(
            resolve_executor_entrypoint(FlashLoanSource::Balancer, &hops),
            ExecutorEntrypoint::BalancerFlash
        );
    }

    #[test]
    fn candidate_stores_dispatch_flash_source_not_config_source() {
        let hops = vec![hop(ProtocolType::BalancerV2), hop(ProtocolType::UniswapV3)];
        let (dispatch, _) = resolve_dispatch(
            FlashLoanSource::Balancer,
            &hops,
            Some(&aave_liquidity()),
            false,
        )
        .expect("mixed Balancer/V3 route should dispatch through Aave");
        assert_eq!(dispatch, FlashLoanSource::AaveV3);
        assert_ne!(dispatch, FlashLoanSource::Balancer);
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
        let (source, entry) = resolve_dispatch(
            FlashLoanSource::Direct,
            &hops,
            Some(&aave_liquidity()),
            false,
        )
        .expect("aave");
        assert_eq!(source, FlashLoanSource::AaveV3);
        assert_eq!(entry, ExecutorEntrypoint::AaveFlash);
    }

    #[test]
    fn trust_prepared_skips_liquidity_realign() {
        let hops = vec![hop(ProtocolType::BalancerV2)];
        let (source, _) =
            resolve_dispatch(FlashLoanSource::Direct, &hops, None, false).expect("prepared");
        assert_eq!(source, FlashLoanSource::Direct);
    }
}
