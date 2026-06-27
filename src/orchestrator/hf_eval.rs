use std::collections::HashMap;
use std::sync::Arc;
use std::sync::LazyLock;

use alloy::primitives::Address;
use ruint::aliases::U256;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::core::types::{
    FlashLoanSource, FoundCycle, ProfitAssessment, RouteSimulationResult, TokenIndex,
};
use crate::pipeline::arena::StateArena;
use crate::pipeline::cycle_filter::graph_negative_rescue_cap;
use crate::pipeline::local_sim::{self, simulate_route_detailed, simulate_route_minimal};
use crate::pipeline::sim_sanity::{SimSanityInput, check_sim_sanity, profit_probe_amount};
use crate::pipeline::ternary::optimize_cycle;
use crate::pipeline::types::OptimizationResult;
use crate::services::execution::impact_slippage::{
    depth_impact_slippage_bps_with_base, effective_slippage_bps,
};
use crate::services::execution::profit::{
    ProfitEvalContext, ProfitThresholds, RouteProfitParams, assess_route_profit,
    net_profit_after_gas_from_sim,
};
use crate::services::execution::flash_liquidity::{
    FlashLiquidityCache, balancer_route_flash_feasible, prefer_aave_flash_start,
};
use crate::services::execution::gas_oracle::GasOracle;
use crate::services::oracle::{
    has_reliable_matic_rate, resolve_token_decimals_for_index, resolve_token_to_matic_rate,
};
use crate::pipeline::types::{MinimalSimResult, compare_cycle_score, route_fingerprint};

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
    pub gas_oracle: &'a GasOracle,
    pub brent_iters: u32,
    pub min_profit_matic: U256,
    pub min_profit_roi_bps: u64,
    pub gas_price: U256,
    pub slippage_bps: u64,
    pub flash_source: FlashLoanSource,
    pub max_flash_loan_usd: u64,
    pub safety_multiplier_bps: u64,
    pub flash_liquidity: &'a FlashLiquidityCache,
}
#[derive(Clone)]
pub struct HfEvalInputOwned {
    pub arena: StateArena,
    pub token_to_matic_rates: Arc<FxHashMap<TokenIndex, U256>>,
    pub token_decimals: Arc<HashMap<Address, u8>>,
    pub gas_oracle: Arc<GasOracle>,
    pub brent_iters: u32,
    pub min_profit_matic: U256,
    pub min_profit_roi_bps: u64,
    pub gas_price: U256,
    pub slippage_bps: u64,
    pub flash_source: FlashLoanSource,
    pub max_flash_loan_usd: u64,
    pub safety_multiplier_bps: u64,
    pub flash_liquidity: Arc<FlashLiquidityCache>,
}

impl HfEvalInputOwned {
    pub fn with_shared_maps(
        arena: StateArena,
        token_to_matic_rates: Arc<FxHashMap<TokenIndex, U256>>,
        token_decimals: Arc<HashMap<Address, u8>>,
        gas_oracle: Arc<GasOracle>,
        brent_iters: u32,
        min_profit_matic: U256,
        min_profit_roi_bps: u64,
        gas_price: U256,
        slippage_bps: u64,
        flash_source: FlashLoanSource,
        max_flash_loan_usd: u64,
        safety_multiplier_bps: u64,
        flash_liquidity: Arc<FlashLiquidityCache>,
    ) -> Self {
        Self {
            arena,
            token_to_matic_rates,
            token_decimals,
            gas_oracle,
            brent_iters,
            min_profit_matic,
            min_profit_roi_bps,
            gas_price,
            slippage_bps,
            flash_source,
            max_flash_loan_usd,
            safety_multiplier_bps,
            flash_liquidity,
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

/// Probe-simulate cycles, drop net-unprofitable routes, rank by net profit, cap to `max_keep`.
/// Graph-negative rescue candidates consume a separate budget so they do not displace sim slots.
pub fn rank_cycles_by_probe_net(
    arena: &StateArena,
    cycles: Vec<FoundCycle>,
    token_to_matic_rates: &FxHashMap<TokenIndex, U256>,
    token_decimals: &HashMap<Address, u8>,
    gas_price: U256,
    slippage_bps: u64,
    flash_source: FlashLoanSource,
    max_keep: usize,
    gas_oracle: &GasOracle,
    flash_liquidity: &FlashLiquidityCache,
    safety_multiplier_bps: u64,
) -> (Vec<FoundCycle>, FxHashMap<u64, MinimalSimResult>) {
    if cycles.is_empty() || max_keep == 0 {
        return (Vec::new(), FxHashMap::default());
    }

    let rescue_cap = graph_negative_rescue_cap(max_keep);
    let sim_budget = max_keep.saturating_sub(rescue_cap);
    let base_slippage = effective_slippage_bps(slippage_bps, 0);
    let mut profitable_ranked: Vec<(U256, FoundCycle)> = Vec::new();
    let mut rescue: Vec<FoundCycle> = Vec::new();
    let mut probe_sims = FxHashMap::default();

    for cycle in cycles {
        if !has_reliable_matic_rate(cycle.start_token, token_to_matic_rates) {
            continue;
        }
        if !balancer_route_flash_feasible(&cycle, arena, flash_liquidity) {
            continue;
        }
        let start_decimals = resolve_token_decimals_for_index(
            cycle.start_token,
            arena,
            token_decimals,
        );
        let rate = resolve_token_to_matic_rate(cycle.start_token, arena, token_to_matic_rates);
        let probe_amount = profit_probe_amount(start_decimals, rate);
        let probe = match simulate_route_minimal(arena, &cycle.edges, probe_amount) {
            Some(sim) if !sim.profit.is_zero() => sim,
            None if cycle.score < 0.0 => {
                rescue.push(cycle);
                continue;
            }
            _ => continue,
        };

        probe_sims.insert(route_fingerprint(&cycle.edges), probe.clone());

        let route_fp = route_fingerprint(&cycle.edges);
        let ctx = ProfitEvalContext::with_safety_multiplier(
            cycle.start_token,
            arena,
            token_to_matic_rates,
            token_decimals,
            gas_price,
            base_slippage,
            flash_source,
            safety_multiplier_bps,
        );
        let mut calibrated_probe = probe.clone();
        calibrated_probe.total_gas =
            gas_oracle.route_gas_or_heuristic(route_fp, probe.total_gas);
        let net = net_profit_after_gas_from_sim(&calibrated_probe, probe_amount, &ctx);
        if net.is_zero() {
            continue;
        }
        profitable_ranked.push((net, cycle));
    }

    profitable_ranked.sort_by(|a, b| {
        b.0.cmp(&a.0).then_with(|| {
            a.1.score
                .partial_cmp(&b.1.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
    });
    let mut kept: Vec<FoundCycle> = profitable_ranked
        .into_iter()
        .take(sim_budget)
        .map(|(_, cycle)| cycle)
        .collect();

    rescue.sort_by(compare_cycle_score);
    for cycle in rescue.into_iter().take(rescue_cap.min(max_keep.saturating_sub(kept.len()))) {
        kept.push(cycle);
    }

    let kept_fps: FxHashSet<u64> = kept.iter().map(|c| route_fingerprint(&c.edges)).collect();
    probe_sims.retain(|fp, _| kept_fps.contains(fp));
    (kept, probe_sims)
}

pub fn evaluate_cycles_parallel(
    cycles: &[FoundCycle],
    input: &HfEvalInput<'_>,
    probe_sims: &FxHashMap<u64, MinimalSimResult>,
) -> Vec<HfEvalResult> {
    cycles
        .par_iter()
        .filter_map(|cycle| {
            let fp = route_fingerprint(&cycle.edges);
            if !probe_sims.contains_key(&fp) {
                return None;
            }
            evaluate_one(cycle, input, probe_sims)
        })
        .collect()
}

/// Rescore, rank, and evaluate cycles on the dedicated rayon pool (keeps Tokio workers free).
pub async fn rescore_rank_and_evaluate_async(
    mut cycles: Vec<FoundCycle>,
    input: HfEvalInputOwned,
    sim_cap: usize,
) -> anyhow::Result<Vec<HfEvalResult>> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    HF_EVAL_POOL.spawn(move || {
        let rates = input.token_to_matic_rates.as_ref();
        let decimals = input.token_decimals.as_ref();
        let mut spot_table = crate::pipeline::spot_price::SpotTable::new(input.arena.pool_count());
        crate::pipeline::spot_price::rescore_cycles_with_table_and_gas(
            &input.arena,
            &mut spot_table,
            &mut cycles,
            Some(input.gas_price),
            Some(rates),
            Some(decimals),
            Some(input.flash_source),
        );
        let (cycles, probe_sims) = rank_cycles_by_probe_net(
            &input.arena,
            cycles,
            rates,
            decimals,
            input.gas_price,
            input.slippage_bps,
            input.flash_source,
            sim_cap,
            &input.gas_oracle,
            input.flash_liquidity.as_ref(),
            input.safety_multiplier_bps,
        );
        let eval = HfEvalInput {
            arena: &input.arena,
            token_to_matic_rates: rates,
            token_decimals: decimals,
            gas_oracle: &input.gas_oracle,
            brent_iters: input.brent_iters,
            min_profit_matic: input.min_profit_matic,
            min_profit_roi_bps: input.min_profit_roi_bps,
            gas_price: input.gas_price,
            slippage_bps: input.slippage_bps,
            flash_source: input.flash_source,
            max_flash_loan_usd: input.max_flash_loan_usd,
            safety_multiplier_bps: input.safety_multiplier_bps,
            flash_liquidity: input.flash_liquidity.as_ref(),
        };
        let _ = tx.send(evaluate_cycles_parallel(&cycles, &eval, &probe_sims));
    });
    rx.await
        .map_err(|e| anyhow::anyhow!("hf eval task failed: {e}"))
}

fn evaluate_one(
    cycle: &FoundCycle,
    input: &HfEvalInput<'_>,
    probe_sims: &FxHashMap<u64, MinimalSimResult>,
) -> Option<HfEvalResult> {
    let route_fp_orig = route_fingerprint(&cycle.edges);
    let cycle = prefer_aave_flash_start(cycle, input.arena, input.flash_liquidity);
    let route_fp = route_fingerprint(&cycle.edges);
    if !balancer_route_flash_feasible(&cycle, input.arena, input.flash_liquidity) {
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

    let seed_sims: Option<Vec<(U256, MinimalSimResult)>> = if route_fp == route_fp_orig {
        probe_sims
            .get(&route_fp)
            .or_else(|| probe_sims.get(&route_fp_orig))
            .map(|sim| {
                let start_decimals = resolve_token_decimals_for_index(
                    cycle.start_token,
                    input.arena,
                    input.token_decimals,
                );
                let rate = resolve_token_to_matic_rate(
                    cycle.start_token,
                    input.arena,
                    input.token_to_matic_rates,
                );
                let probe_amount = profit_probe_amount(start_decimals, rate);
                vec![(probe_amount, sim.clone())]
            })
    } else {
        None
    };
    let seed_slice = seed_sims.as_deref();

    let mut opt = optimize_cycle(
        input.arena,
        &cycle,
        input.token_to_matic_rates,
        input.token_decimals,
        Some(input.max_flash_loan_usd),
        Some(input.brent_iters),
        None,
        &profit_ctx,
        seed_slice,
    )?;

    let mut sim = simulate_route_detailed(input.arena, &cycle.edges, opt.optimal_input)?;
    if !local_sim::route_cl_fidelity_ok(
        input.arena,
        &cycle.edges,
        opt.optimal_input,
        crate::pipeline::spot_price::SPOT_PROBE,
    ) {
        return None;
    }
    if sim.profit.is_zero() {
        return None;
    }
    if sim_sanity_reject(&cycle, input, &sim, opt.search_low).is_some() {
        return None;
    }

    let mut depth_bps = depth_impact_slippage_bps_with_base(
        input.arena,
        &cycle.edges,
        opt.optimal_input,
        Some(&MinimalSimResult {
            profit: sim.profit,
            amount_out: sim.amount_out,
            total_gas: sim.total_gas,
        }),
    );
    let mut slippage_bps = effective_slippage_bps(input.slippage_bps, depth_bps);
    if slippage_bps > base_slippage {
        let depth_ctx = ProfitEvalContext::with_safety_multiplier(
            cycle.start_token,
            input.arena,
            input.token_to_matic_rates,
            input.token_decimals,
            input.gas_price,
            slippage_bps,
            input.flash_source,
            input.safety_multiplier_bps,
        );
        if let Some(reopt) = optimize_cycle(
            input.arena,
            &cycle,
            input.token_to_matic_rates,
            input.token_decimals,
            Some(input.max_flash_loan_usd),
            Some(input.brent_iters),
            None,
            &depth_ctx,
            seed_slice,
        ) {
            opt = reopt;
            sim = simulate_route_detailed(input.arena, &cycle.edges, opt.optimal_input)?;
            if !local_sim::route_cl_fidelity_ok(
                input.arena,
                &cycle.edges,
                opt.optimal_input,
                crate::pipeline::spot_price::SPOT_PROBE,
            ) {
                return None;
            }
            if sim.profit.is_zero() || sim_sanity_reject(&cycle, input, &sim, opt.search_low).is_some() {
                return None;
            }
            depth_bps = depth_impact_slippage_bps_with_base(
                input.arena,
                &cycle.edges,
                opt.optimal_input,
                Some(&MinimalSimResult {
                    profit: sim.profit,
                    amount_out: sim.amount_out,
                    total_gas: sim.total_gas,
                }),
            );
            slippage_bps = effective_slippage_bps(input.slippage_bps, depth_bps);
        }
    }

    let assessment = assess_route_for_cycle(input, &sim, &cycle, slippage_bps)?;

    Some(HfEvalResult {
        cycle: cycle.clone(),
        opt,
        sim,
        assessment,
        effective_slippage_bps: slippage_bps,
    })
}

fn assess_route_for_cycle(
    input: &HfEvalInput<'_>,
    sim: &RouteSimulationResult,
    cycle: &FoundCycle,
    slippage_bps: u64,
) -> Option<ProfitAssessment> {
    let route = RouteProfitParams {
        gross_profit: sim.profit,
        amount_in: sim.amount_in,
        gas_units: input
            .gas_oracle
            .route_gas_or_heuristic(route_fingerprint(&cycle.edges), sim.total_gas),
        hop_count: cycle.hop_count,
        slippage_bps,
        flash_loan_source: input.flash_source,
    };
    let thresholds = ProfitThresholds {
        min_profit_matic_wei: input.min_profit_matic,
        min_profit_roi_bps: input.min_profit_roi_bps,
        safety_multiplier_bps: input.safety_multiplier_bps,
    };
    Some(assess_route_profit(
        cycle.start_token,
        input.arena,
        &route,
        input.token_to_matic_rates,
        input.token_decimals,
        input.gas_price,
        &thresholds,
    ))
}

fn sim_sanity_reject(
    cycle: &FoundCycle,
    input: &HfEvalInput<'_>,
    sim: &RouteSimulationResult,
    search_low: U256,
) -> Option<crate::pipeline::sim_sanity::SimSanityReject> {
    let token_to_matic_rate =
        resolve_token_to_matic_rate(cycle.start_token, input.arena, input.token_to_matic_rates);
    let token_decimals = resolve_token_decimals_for_index(
        cycle.start_token,
        input.arena,
        input.token_decimals,
    );
    check_sim_sanity(SimSanityInput {
        amount_in: sim.amount_in,
        gross_profit: sim.profit,
        search_low,
        token_decimals,
        token_to_matic_rate,
    })
    .err()
}

use rayon::prelude::*;
