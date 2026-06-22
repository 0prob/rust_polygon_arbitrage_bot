use std::collections::HashMap;
use std::sync::LazyLock;

use alloy::primitives::Address;
use ruint::aliases::U256;
use rustc_hash::FxHashMap;

use crate::core::types::{
    FlashLoanSource, FoundCycle, ProfitAssessment, RouteSimulationResult, TokenIndex,
};
use crate::pipeline::arena::StateArena;
use crate::pipeline::local_sim::simulate_route_detailed;
use crate::pipeline::ternary::optimize_cycle;
use crate::pipeline::types::OptimizationResult;
use crate::services::execution::impact_slippage::{depth_impact_slippage_bps, effective_slippage_bps};
use crate::services::execution::profit::{
    ProfitEvalContext, ProfitThresholds, RouteProfitParams, assess_profit, build_assess_input,
};

fn build_hf_eval_pool() -> rayon::ThreadPool {
    let preferred = num_cpus::get().max(2);
    rayon::ThreadPoolBuilder::new()
        .num_threads(preferred)
        .thread_name(|i| format!("hf-eval-{i}"))
        .build()
        .unwrap_or_else(|e| {
            tracing::warn!(error = %e, n = preferred, "hf eval thread pool failed — using single-thread fallback");
            rayon::ThreadPoolBuilder::new()
                .num_threads(1)
                .build()
                .expect("single-thread pool with num_threads=1 cannot fail")
        })
}

static HF_EVAL_POOL: LazyLock<rayon::ThreadPool> = LazyLock::new(build_hf_eval_pool);

pub struct HfEvalInput<'a> {
    pub arena: &'a StateArena,
    pub token_to_matic_rates: &'a FxHashMap<TokenIndex, U256>,
    pub token_decimals: &'a HashMap<Address, u8>,
    pub brent_iters: u32,
    pub min_profit_matic: U256,
    pub min_profit_roi_bps: u64,
    pub gas_price: U256,
    pub slippage_bps: u64,
    pub flash_source: FlashLoanSource,
    pub max_flash_loan_usd: u64,
    pub safety_multiplier_bps: u64,
}

/// Owned eval context for off-thread CPU work.
#[derive(Clone)]
pub struct HfEvalInputOwned {
    pub arena: StateArena,
    pub token_to_matic_rates: FxHashMap<TokenIndex, U256>,
    pub token_decimals: HashMap<Address, u8>,
    pub brent_iters: u32,
    pub min_profit_matic: U256,
    pub min_profit_roi_bps: u64,
    pub gas_price: U256,
    pub slippage_bps: u64,
    pub flash_source: FlashLoanSource,
    pub max_flash_loan_usd: u64,
    pub safety_multiplier_bps: u64,
}

impl HfEvalInputOwned {
    pub fn from_input(input: &HfEvalInput<'_>, arena: StateArena) -> Self {
        Self {
            arena,
            token_to_matic_rates: input.token_to_matic_rates.clone(),
            token_decimals: input.token_decimals.clone(),
            brent_iters: input.brent_iters,
            min_profit_matic: input.min_profit_matic,
            min_profit_roi_bps: input.min_profit_roi_bps,
            gas_price: input.gas_price,
            slippage_bps: input.slippage_bps,
            flash_source: input.flash_source,
            max_flash_loan_usd: input.max_flash_loan_usd,
            safety_multiplier_bps: input.safety_multiplier_bps,
        }
    }
}

pub struct HfEvalResult {
    pub cycle: FoundCycle,
    pub opt: OptimizationResult,
    pub sim: RouteSimulationResult,
    pub assessment: ProfitAssessment,
    pub effective_slippage_bps: u64,
}

pub fn evaluate_cycles_parallel(
    cycles: &[FoundCycle],
    input: &HfEvalInput<'_>,
) -> Vec<HfEvalResult> {
    cycles
        .par_iter()
        .filter_map(|cycle| evaluate_one(cycle, input))
        .collect()
}

/// Run cycle evaluation on a dedicated rayon pool so Tokio workers stay responsive.
pub async fn evaluate_cycles_parallel_async(
    cycles: Vec<FoundCycle>,
    input: HfEvalInputOwned,
) -> anyhow::Result<Vec<HfEvalResult>> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    HF_EVAL_POOL.spawn(move || {
        let eval = HfEvalInput {
            arena: &input.arena,
            token_to_matic_rates: &input.token_to_matic_rates,
            token_decimals: &input.token_decimals,
            brent_iters: input.brent_iters,
            min_profit_matic: input.min_profit_matic,
            min_profit_roi_bps: input.min_profit_roi_bps,
            gas_price: input.gas_price,
            slippage_bps: input.slippage_bps,
            flash_source: input.flash_source,
            max_flash_loan_usd: input.max_flash_loan_usd,
            safety_multiplier_bps: input.safety_multiplier_bps,
        };
        let _ = tx.send(evaluate_cycles_parallel(&cycles, &eval));
    });
    rx.await
        .map_err(|e| anyhow::anyhow!("hf eval task failed: {e}"))
}

fn evaluate_one(cycle: &FoundCycle, input: &HfEvalInput<'_>) -> Option<HfEvalResult> {
    let probe = crate::pipeline::local_sim::simulate_route_minimal(
        input.arena,
        &cycle.edges,
        crate::pipeline::spot_price::SPOT_PROBE,
    );
    if probe.as_ref().is_none_or(|s| s.profit.is_zero()) {
        return None;
    }

    let base_slippage = effective_slippage_bps(input.slippage_bps, 0);
    let profit_ctx = ProfitEvalContext::with_safety_multiplier(
        cycle.start_token,
        input.arena,
        input.token_to_matic_rates,
        input.token_decimals,
        input.gas_price,
        base_slippage,
        input.flash_source,
        input.safety_multiplier_bps,
    );

    let opt = optimize_cycle(
        input.arena,
        cycle,
        input.token_to_matic_rates,
        input.token_decimals,
        Some(input.max_flash_loan_usd),
        Some(input.brent_iters),
        None,
        Some(&profit_ctx),
    )?;

    let sim = simulate_route_detailed(input.arena, &cycle.edges, opt.optimal_input)?;
    if sim.profit.is_zero() {
        return None;
    }

    let depth_bps = depth_impact_slippage_bps(input.arena, &cycle.edges, opt.optimal_input);
    let slippage_bps = effective_slippage_bps(input.slippage_bps, depth_bps);

    let assessment = best_assessment_for_cycle(input, &sim, cycle, slippage_bps)?;

    Some(HfEvalResult {
        cycle: cycle.clone(),
        opt,
        sim,
        assessment,
        effective_slippage_bps: slippage_bps,
    })
}

fn best_assessment_for_cycle(
    input: &HfEvalInput<'_>,
    sim: &RouteSimulationResult,
    cycle: &FoundCycle,
    slippage_bps: u64,
) -> Option<ProfitAssessment> {
    Some(assess_profit(build_assess_input(
        cycle.start_token,
        input.arena,
        RouteProfitParams {
            gross_profit: sim.profit,
            amount_in: sim.amount_in,
            gas_units: sim.total_gas,
            hop_count: cycle.hop_count,
            slippage_bps,
            flash_loan_source: input.flash_source,
        },
        input.token_to_matic_rates,
        input.token_decimals,
        input.gas_price,
        ProfitThresholds {
            min_profit_matic_wei: input.min_profit_matic,
            min_profit_roi_bps: input.min_profit_roi_bps,
            safety_multiplier_bps: input.safety_multiplier_bps,
        },
    )))
}

use rayon::prelude::*;
