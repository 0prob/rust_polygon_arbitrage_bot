use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use alloy::primitives::Address;
use alloy::primitives::U256;
use anyhow::Context;
use rayon::prelude::*;
use rustc_hash::FxHashMap;

use crate::core::types::{
    FlashLoanSource, FoundCycle, ProfitAssessment, RouteSimulationResult, TokenIndex,
};
use crate::pipeline::arena::StateArena;
use crate::pipeline::cycle_filter::graph_negative_rescue_cap;
use crate::pipeline::local_sim::{
    self, MinimalSimFailure, simulate_route_detailed, simulate_route_minimal,
};
use crate::pipeline::route_calls::route_fits_executor;
use crate::pipeline::sim_sanity::{
    FlashBorrowCapParams, SimSanityInput, check_sim_sanity, check_sim_sanity_fast,
    check_sim_sanity_for_dispatch, min_economic_amount_in,
};
use crate::pipeline::spot_price::for_each_rank_probe_amount;
use crate::pipeline::ternary::{BRENT_SEED_CACHE_SLOTS, RouteGasCosting, optimize_cycle};
use crate::pipeline::types::OptimizationResult;
use crate::pipeline::types::{MinimalSimResult, compare_cycle_score};
use crate::services::execution::candidate::hash_cycle_edges;
use crate::services::execution::flash_liquidity::{
    FlashLiquidityCache, FlashLoanDiagnostics, balancer_route_flash_feasible,
    build_cycle_flash_context, flash_reject_reason, prefer_aave_flash_start,
    resolve_flash_source_for_cycle, resolve_flash_source_with_context,
};
use crate::services::execution::flash_policy::FlashLoanPolicy;
use crate::services::execution::gas_oracle::{GasOracle, RouteGasLookup};
use crate::services::execution::impact_slippage::{
    depth_impact_slippage_bps_with_base, effective_slippage_bps,
};
use crate::services::execution::profit::{
    AssessmentGas, ProfitEvalContext, RouteAssessRequest, assess_route_from_sim,
    net_profit_matic_from_sim, route_profit_thresholds,
};
use crate::services::execution::service::ExecutionService;
use crate::services::oracle::{
    cycle_tokens_have_known_decimals, has_reliable_matic_rate, resolve_token_decimals_for_index,
    resolve_token_to_matic_rate,
};

#[derive(Default)]
struct SkipCounters {
    rate: u32,
    flash: u32,
    flash_source: u32,
    missing_decimals: u32,
    minimal_no_sim: u32,
    minimal_zero_profit: u32,
    minimal_sanity: u32,
    net: u32,
    executor_budget: u32,
}

static MINIMAL_SIM_DIAGNOSTICS: AtomicU32 = AtomicU32::new(0);

impl SkipCounters {
    fn merge(&mut self, other: SkipCounters) {
        self.rate += other.rate;
        self.flash += other.flash;
        self.flash_source += other.flash_source;
        self.missing_decimals += other.missing_decimals;
        self.minimal_no_sim += other.minimal_no_sim;
        self.minimal_zero_profit += other.minimal_zero_profit;
        self.minimal_sanity += other.minimal_sanity;
        self.net += other.net;
        self.executor_budget += other.executor_budget;
    }

    fn probe(&self) -> u32 {
        self.missing_decimals + self.minimal_sim()
    }

    fn minimal_sim(&self) -> u32 {
        self.minimal_no_sim + self.minimal_zero_profit + self.minimal_sanity
    }
}

#[derive(Default)]
struct MinimalSimReasonCounts {
    invalid_route: u32,
    missing_pool: u32,
    non_tradable: u32,
    shallow_cl: u32,
    v2_reserve_exhausted: u32,
    math: u32,
    unsupported_state: u32,
    zero_output: u32,
}

impl MinimalSimReasonCounts {
    fn merge(&mut self, other: Self) {
        self.invalid_route += other.invalid_route;
        self.missing_pool += other.missing_pool;
        self.non_tradable += other.non_tradable;
        self.shallow_cl += other.shallow_cl;
        self.v2_reserve_exhausted += other.v2_reserve_exhausted;
        self.math += other.math;
        self.unsupported_state += other.unsupported_state;
        self.zero_output += other.zero_output;
    }

    fn record(&mut self, reason: MinimalSimFailure) {
        match reason {
            MinimalSimFailure::InvalidRoute => self.invalid_route += 1,
            MinimalSimFailure::MissingPool { .. } => self.missing_pool += 1,
            MinimalSimFailure::NonTradable { .. } => self.non_tradable += 1,
            MinimalSimFailure::ShallowCl { .. } => self.shallow_cl += 1,
            MinimalSimFailure::V2ReserveExhausted { .. } => self.v2_reserve_exhausted += 1,
            MinimalSimFailure::Math { .. } => self.math += 1,
            MinimalSimFailure::UnsupportedState { .. } => self.unsupported_state += 1,
            MinimalSimFailure::ZeroOutput { .. } => self.zero_output += 1,
        }
    }
}

#[derive(Clone, Copy)]
enum MinimalProbeReject {
    NoSimulation,
    ZeroProfit,
    SanityReject,
}

impl MinimalProbeReject {
    fn record(self, skip: &mut SkipCounters) {
        match self {
            Self::NoSimulation => skip.minimal_no_sim += 1,
            Self::ZeroProfit => skip.minimal_zero_profit += 1,
            Self::SanityReject => skip.minimal_sanity += 1,
        }
    }
}

#[derive(Default)]
struct ProbeRankPartial {
    profitable: Vec<(u64, U256, FoundCycle)>,
    rescue: Vec<(u64, FoundCycle)>,
    near_net: Vec<(u64, U256, FoundCycle)>,
    seeds: FxHashMap<u64, (U256, MinimalSimResult)>,
    skip: SkipCounters,
    minimal_sim_reasons: MinimalSimReasonCounts,
    flash_diag: Option<String>,
    flash_loan: FlashLoanDiagnostics,
}

impl ProbeRankPartial {
    fn merge(mut self, other: Self) -> Self {
        self.profitable.extend(other.profitable);
        self.rescue.extend(other.rescue);
        self.near_net.extend(other.near_net);
        self.seeds.extend(other.seeds);
        self.skip.merge(other.skip);
        self.minimal_sim_reasons.merge(other.minimal_sim_reasons);
        if self.flash_diag.is_none() {
            self.flash_diag = other.flash_diag;
        }
        self.flash_loan.merge(other.flash_loan);
        self
    }
}

pub struct HfEvalInput<'a> {
    pub arena: &'a StateArena,
    pub token_to_matic_rates: &'a FxHashMap<TokenIndex, U256>,
    pub token_decimals: &'a FxHashMap<Address, u8>,
    pub gas_oracle: &'a GasOracle,
    pub route_gas: &'a RouteGasLookup,
    pub state_generation: u64,
    pub brent_iters: u32,
    pub min_profit_matic: U256,
    pub min_profit_roi_bps: u64,
    pub gas_price: U256,
    pub slippage_bps: u64,
    pub flash_policy: FlashLoanPolicy,
    pub max_flash_loan_usd: u64,
    pub matic_usd: f64,
    pub matic_usd_chainlink: Option<alloy::primitives::I256>,
    pub safety_multiplier_bps: u64,
    pub profit_priority_alpha_bps: u64,
    pub flash_liquidity: &'a FlashLiquidityCache,
    pub flash_ttl: Duration,
    pub execution: &'a ExecutionService,
}

#[derive(Clone)]
pub struct HfEvalInputOwned {
    pub arena: Arc<StateArena>,
    pub token_to_matic_rates: Arc<FxHashMap<TokenIndex, U256>>,
    pub token_decimals: Arc<FxHashMap<Address, u8>>,
    pub gas_oracle: Arc<GasOracle>,
    pub state_generation: u64,
    pub brent_iters: u32,
    pub min_profit_matic: U256,
    pub min_profit_roi_bps: u64,
    pub gas_price: U256,
    pub slippage_bps: u64,
    pub flash_policy: FlashLoanPolicy,
    pub max_flash_loan_usd: u64,
    pub matic_usd: f64,
    pub matic_usd_chainlink: Option<alloy::primitives::I256>,
    pub safety_multiplier_bps: u64,
    pub profit_priority_alpha_bps: u64,
    pub flash_liquidity: Arc<FlashLiquidityCache>,
    pub execution: Arc<ExecutionService>,
}

impl HfEvalInputOwned {
    pub fn as_eval_input<'a>(&'a self, route_gas: &'a RouteGasLookup) -> HfEvalInput<'a> {
        HfEvalInput {
            arena: &self.arena,
            token_to_matic_rates: self.token_to_matic_rates.as_ref(),
            token_decimals: self.token_decimals.as_ref(),
            gas_oracle: self.gas_oracle.as_ref(),
            route_gas,
            state_generation: self.state_generation,
            brent_iters: self.brent_iters,
            min_profit_matic: self.min_profit_matic,
            min_profit_roi_bps: self.min_profit_roi_bps,
            gas_price: self.gas_price,
            slippage_bps: self.slippage_bps,
            flash_policy: self.flash_policy,
            max_flash_loan_usd: self.max_flash_loan_usd,
            matic_usd: self.matic_usd,
            matic_usd_chainlink: self.matic_usd_chainlink,
            safety_multiplier_bps: self.safety_multiplier_bps,
            profit_priority_alpha_bps: self.profit_priority_alpha_bps,
            flash_liquidity: self.flash_liquidity.as_ref(),
            flash_ttl: self.flash_liquidity.ttl(),
            execution: self.execution.as_ref(),
        }
    }
}

#[derive(Clone)]
pub struct HfEvalResult {
    pub route_fingerprint: u64,
    pub cycle: FoundCycle,
    pub opt: OptimizationResult,
    pub sim: RouteSimulationResult,
    pub assessment: ProfitAssessment,
    pub effective_slippage_bps: u64,
    /// Flash source used for the assessment (and intended dispatch path).
    pub flash_source: FlashLoanSource,
    /// Set when `filter_balancer_onchain_verified` already ran `queryBatchSwap`.
    pub balancer_batch_verified: bool,
}

/// Economic probe first, then decimal-aware spot probe for tickless CL routes.
fn try_rank_probe_minimal(
    arena: &StateArena,
    cycle: &FoundCycle,
    start_decimals: u8,
    rate: U256,
) -> Result<(U256, MinimalSimResult), MinimalProbeReject> {
    minimal_rank_probe(arena, cycle, start_decimals, rate, true)
}

/// Shared minimal probe used by rank + simulatable checks (`search_low` differs for rank vs filter).
fn minimal_rank_probe(
    arena: &StateArena,
    cycle: &FoundCycle,
    start_decimals: u8,
    rate: U256,
    rank_amount_as_search_low: bool,
) -> Result<(U256, MinimalSimResult), MinimalProbeReject> {
    let mut best: Option<(U256, MinimalSimResult)> = None;
    let mut saw_simulation = false;
    let mut saw_profit = false;
    for_each_rank_probe_amount(start_decimals, rate, |amount| {
        let Some(sim) = simulate_route_minimal(arena, &cycle.edges, amount) else {
            return;
        };
        saw_simulation = true;
        if sim.profit.is_zero() {
            return;
        }
        saw_profit = true;
        let search_low = if rank_amount_as_search_low {
            amount
        } else {
            U256::ZERO
        };
        if check_sim_sanity(SimSanityInput {
            amount_in: amount,
            gross_profit: sim.profit,
            search_low,
            token_decimals: start_decimals,
            token_to_matic_rate: rate,
        })
        .is_err()
        {
            return;
        }
        if best.as_ref().is_none_or(|(_, b)| sim.profit > b.profit) {
            best = Some((amount, sim));
        }
    });
    best.ok_or({
        if !saw_simulation {
            MinimalProbeReject::NoSimulation
        } else if !saw_profit {
            MinimalProbeReject::ZeroProfit
        } else {
            MinimalProbeReject::SanityReject
        }
    })
}

/// Routes that cannot minimal-sim at probe or spot size waste Brent work.
/// Caller must pass a flash-rotated cycle (same as `rank_cycles_by_probe_net`).
fn cycle_simulatable(
    arena: &StateArena,
    cycle: &FoundCycle,
    token_decimals: &FxHashMap<Address, u8>,
    token_to_matic_rates: &FxHashMap<TokenIndex, U256>,
) -> bool {
    if !cycle_tokens_have_known_decimals(cycle, arena, token_decimals)
        || !has_reliable_matic_rate(cycle.start_token, token_to_matic_rates)
    {
        return false;
    }
    let decimals = resolve_token_decimals_for_index(cycle.start_token, arena, token_decimals);
    let rate = resolve_token_to_matic_rate(cycle.start_token, token_to_matic_rates);
    minimal_rank_probe(arena, cycle, decimals, rate, false).is_ok()
}

fn cycle_flash_evaluable(
    cycle: &FoundCycle,
    arena: &StateArena,
    flash: &crate::services::execution::flash_liquidity::FlashLiquiditySnapshot,
    flash_ttl: Duration,
    flash_policy: FlashLoanPolicy,
    token_decimals: &FxHashMap<Address, u8>,
    token_to_matic_rates: &FxHashMap<TokenIndex, U256>,
) -> bool {
    if !cycle_tokens_have_known_decimals(cycle, arena, token_decimals) {
        return false;
    }
    let decimals = resolve_token_decimals_for_index(cycle.start_token, arena, token_decimals);
    let rate = resolve_token_to_matic_rate(cycle.start_token, token_to_matic_rates);
    let economic = min_economic_amount_in(decimals, rate);
    balancer_route_flash_feasible(cycle, arena, flash, flash_ttl)
        && resolve_flash_source_for_cycle(cycle, arena, flash, flash_ttl, flash_policy, economic)
            .is_some()
}

/// Score-ranked fallback when probe ranking yields nothing simulatable at Brent size.
fn simulatable_score_fallback(
    scanned: &[Arc<FoundCycle>],
    arena: &StateArena,
    token_decimals: &FxHashMap<Address, u8>,
    token_to_matic_rates: &FxHashMap<TokenIndex, U256>,
    flash_liquidity: &FlashLiquidityCache,
    flash_policy: FlashLoanPolicy,
    max_keep: usize,
) -> Vec<FoundCycle> {
    let flash_ttl = flash_liquidity.ttl();
    let flash = flash_liquidity.load();
    let fallback_limit = scanned.len().min(max_keep.saturating_mul(2).max(max_keep));
    let mut fallback: Vec<FoundCycle> = scanned
        .iter()
        .take(fallback_limit)
        .filter_map(|cycle| {
            let ready =
                prefer_aave_flash_start(cycle, arena, &flash, flash_ttl, token_to_matic_rates);
            if cycle_simulatable(arena, &ready, token_decimals, token_to_matic_rates)
                && cycle_flash_evaluable(
                    &ready,
                    arena,
                    &flash,
                    flash_ttl,
                    flash_policy,
                    token_decimals,
                    token_to_matic_rates,
                )
            {
                Some(ready.into_owned())
            } else {
                None
            }
        })
        .collect();
    fallback.sort_by(compare_cycle_score);
    fallback.truncate(max_keep);
    fallback
}

fn select_probe_survivors(
    mut profitable: Vec<(u64, U256, FoundCycle)>,
    mut rescue: Vec<(u64, FoundCycle)>,
    max_keep: usize,
    rescue_cap: usize,
) -> Vec<(u64, FoundCycle)> {
    profitable.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| compare_cycle_score(&a.2, &b.2)));
    let mut kept: Vec<(u64, FoundCycle)> = profitable
        .into_iter()
        .take(max_keep)
        .map(|(fp, _, cycle)| (fp, cycle))
        .collect();

    if kept.len() < max_keep {
        rescue.sort_by(|a, b| compare_cycle_score(&a.1, &b.1));
        let remaining = max_keep - kept.len();
        kept.extend(rescue.into_iter().take(rescue_cap.min(remaining)));
    }
    kept
}

/// Cycle count worth gas-rescoring and probe-ranking (2× Brent cap).
#[inline]
#[must_use]
pub fn probe_rank_window(max_keep: usize, total: usize) -> usize {
    max_keep.saturating_mul(2).min(total)
}

#[allow(clippy::too_many_arguments)]
fn rank_one_cycle_probe(
    cycle_arc: &Arc<FoundCycle>,
    arena: &StateArena,
    token_to_matic_rates: &FxHashMap<TokenIndex, U256>,
    token_decimals: &FxHashMap<Address, u8>,
    gas_price: U256,
    slippage_bps: u64,
    flash_policy: FlashLoanPolicy,
    gas_oracle: &GasOracle,
    route_gas: &RouteGasLookup,
    flash: &crate::services::execution::flash_liquidity::FlashLiquiditySnapshot,
    flash_ttl: Duration,
    safety_multiplier_bps: u64,
    profit_priority_alpha_bps: u64,
) -> ProbeRankPartial {
    let mut out = ProbeRankPartial::default();
    let cycle = prefer_aave_flash_start(cycle_arc, arena, flash, flash_ttl, token_to_matic_rates);
    let fp = hash_cycle_edges(&cycle.edges);
    if !route_fits_executor(&cycle.edges) {
        out.skip.executor_budget = 1;
        return out;
    }
    if !has_reliable_matic_rate(cycle.start_token, token_to_matic_rates) {
        out.skip.rate = 1;
        return out;
    }
    if !cycle_tokens_have_known_decimals(&cycle, arena, token_decimals) {
        out.skip.missing_decimals = 1;
        return out;
    }
    if !balancer_route_flash_feasible(&cycle, arena, flash, flash_ttl) {
        out.skip.flash = 1;
        out.flash_loan.mixed_no_aave += 1;
        if out.flash_diag.is_none() {
            out.flash_diag = Some(format!(
                "fp={fp:#x} hops={} mixed_balancer_no_aave",
                cycle.edges.len(),
            ));
        }
        return out;
    }
    let Some(flash_ctx) = build_cycle_flash_context(&cycle, arena, flash, flash_ttl) else {
        out.skip.flash_source = 1;
        return out;
    };
    let start_decimals = resolve_token_decimals_for_index(cycle.start_token, arena, token_decimals);
    let rate = resolve_token_to_matic_rate(cycle.start_token, token_to_matic_rates);
    let (probe_amount, probe) = match try_rank_probe_minimal(arena, &cycle, start_decimals, rate) {
        Ok(probe) => probe,
        Err(reject) => {
            if matches!(reject, MinimalProbeReject::NoSimulation) {
                let mut first_failure = None;
                for_each_rank_probe_amount(start_decimals, rate, |amount| {
                    if first_failure.is_none() {
                        first_failure = local_sim::minimal_sim_failure(arena, &cycle.edges, amount);
                    }
                });
                if let Some(reason) = first_failure {
                    out.minimal_sim_reasons.record(reason);
                }
            }
            if matches!(reject, MinimalProbeReject::NoSimulation)
                && MINIMAL_SIM_DIAGNOSTICS.fetch_add(1, Ordering::Relaxed) < 3
            {
                let mut attempts = Vec::with_capacity(2);
                for_each_rank_probe_amount(start_decimals, rate, |amount| {
                    attempts.push((
                        amount,
                        local_sim::minimal_sim_failure(arena, &cycle.edges, amount),
                    ));
                });
                let route: Vec<_> = cycle
                    .edges
                    .iter()
                    .map(|edge| (edge.pool_index, edge.protocol))
                    .collect();
                crate::info!(
                    "hf minimal-sim reject: fp={fp:#x} route={route:?} attempts={attempts:?}"
                );
            }
            if cycle.score < 0.0 {
                out.rescue.push((fp, cycle.into_owned()));
            } else {
                reject.record(&mut out.skip);
            }
            return out;
        }
    };
    let Some(flash_source) =
        resolve_flash_source_with_context(&flash_ctx, flash_policy, probe_amount)
    else {
        out.skip.flash_source = 1;
        if let Some(reason) = flash_reject_reason(&flash_ctx, flash_policy, probe_amount) {
            out.flash_loan.record_reject(reason);
        }
        if out.flash_diag.is_none() {
            out.flash_diag = Some(format!(
                "fp={fp:#x} hops={} flash_source_reject start={} probe_amt={probe_amount}",
                cycle.edges.len(),
                flash_ctx.start_addr,
            ));
        }
        return out;
    };

    let depth_bps =
        depth_impact_slippage_bps_with_base(arena, &cycle.edges, probe_amount, Some(&probe));
    let effective_slip =
        effective_slippage_bps(slippage_bps, cycle.edges.len() as u32, depth_bps);

    let mut ctx = ProfitEvalContext::with_safety_multiplier(
        cycle.start_token,
        arena,
        token_to_matic_rates,
        token_decimals,
        gas_price,
        effective_slip,
        flash_source,
        safety_multiplier_bps,
    );
    ctx.gas_scale_bps = 10_000;
    ctx.hop_count = cycle.edges.len() as u32;
    ctx.profit_priority_alpha_bps = profit_priority_alpha_bps;
    let mut ranked_probe = probe;
    ranked_probe.total_gas = route_gas.route_gas_or_heuristic(gas_oracle, fp, probe.total_gas);
    let net_matic = net_profit_matic_from_sim(&ranked_probe, probe_amount, &ctx);
    out.seeds.insert(fp, (probe_amount, probe));
    if net_matic.is_zero() {
        let hop_count = cycle.edges.len();
        if !probe.profit.is_zero()
            && cycle_simulatable(arena, &cycle, token_decimals, token_to_matic_rates)
        {
            out.near_net.push((fp, probe.profit, cycle.into_owned()));
        }
        out.skip.net = 1;
        if probe.profit.is_zero() {
            crate::trace!(
                "probe net=0 (zero sim profit): fp={fp:#x} hops={hop_count} probe_amt={probe_amount} gas={} rate={rate} dec={start_decimals}",
                ranked_probe.total_gas,
            );
        } else {
            crate::trace!(
                "probe net=0 (gas eats profit): fp={fp:#x} hops={hop_count} sim_profit={} probe_amt={probe_amount} gas={} gas_price_wei={} rate={rate} dec={start_decimals}",
                probe.profit,
                ranked_probe.total_gas,
                ctx.gas_price,
            );
        }
        return out;
    }
    let hop_count = cycle.edges.len();
    crate::debug!(
        "probe matic>0: fp={fp:#x} hops={hop_count} net_matic={net_matic} probe_amt={probe_amount} sim_profit={} gas={} rate={rate} dec={start_decimals}",
        probe.profit,
        ranked_probe.total_gas,
    );
    out.profitable.push((fp, net_matic, cycle.into_owned()));
    out
}

#[allow(clippy::too_many_arguments)]
pub fn rank_cycles_by_probe_net(
    arena: &StateArena,
    cycles: Vec<Arc<FoundCycle>>,
    token_to_matic_rates: &FxHashMap<TokenIndex, U256>,
    token_decimals: &FxHashMap<Address, u8>,
    gas_price: U256,
    slippage_bps: u64,
    flash_policy: FlashLoanPolicy,
    max_keep: usize,
    gas_oracle: &GasOracle,
    route_gas: &RouteGasLookup,
    flash_liquidity: &FlashLiquidityCache,
    safety_multiplier_bps: u64,
    profit_priority_alpha_bps: u64,
) -> (Vec<FoundCycle>, FxHashMap<u64, (U256, MinimalSimResult)>) {
    if cycles.is_empty() || max_keep == 0 {
        return (Vec::new(), FxHashMap::default());
    }

    let rescue_cap = graph_negative_rescue_cap(max_keep);
    let probe_stop_at = probe_rank_window(max_keep, cycles.len());
    let mut scanned = cycles;
    scanned.truncate(probe_stop_at);
    let flash_ttl = flash_liquidity.ttl();
    let flash = flash_liquidity.load();
    let partial = if crate::util::should_use_rayon(scanned.len()) {
        scanned
            .par_iter()
            .map(|cycle_arc| {
                rank_one_cycle_probe(
                    cycle_arc,
                    arena,
                    token_to_matic_rates,
                    token_decimals,
                    gas_price,
                    slippage_bps,
                    flash_policy,
                    gas_oracle,
                    route_gas,
                    &flash,
                    flash_ttl,
                    safety_multiplier_bps,
                    profit_priority_alpha_bps,
                )
            })
            .reduce(ProbeRankPartial::default, ProbeRankPartial::merge)
    } else {
        scanned
            .iter()
            .map(|cycle_arc| {
                rank_one_cycle_probe(
                    cycle_arc,
                    arena,
                    token_to_matic_rates,
                    token_decimals,
                    gas_price,
                    slippage_bps,
                    flash_policy,
                    gas_oracle,
                    route_gas,
                    &flash,
                    flash_ttl,
                    safety_multiplier_bps,
                    profit_priority_alpha_bps,
                )
            })
            .fold(ProbeRankPartial::default(), ProbeRankPartial::merge)
    };
    let ProbeRankPartial {
        profitable: profitable_ranked,
        mut rescue,
        seeds: mut probe_seeds,
        skip,
        minimal_sim_reasons,
        mut near_net,
        flash_diag,
        mut flash_loan,
    } = partial;
    // Rescue holds spot-negative cycles that already failed `try_rank_probe_minimal`;
    // re-running full minimal sim here duplicated work. Flash feasibility is enough
    // before Brent / probe-fallback tries larger sizes.
    rescue.retain(|cycle| {
        cycle_flash_evaluable(
            &cycle.1,
            arena,
            &flash,
            flash_ttl,
            flash_policy,
            token_decimals,
            token_to_matic_rates,
        )
    });
    let rescue_len = rescue.len();
    let had_net_ranked = !profitable_ranked.is_empty();
    let mut kept = if had_net_ranked {
        select_probe_survivors(profitable_ranked, rescue, max_keep, rescue_cap)
    } else {
        Vec::new()
    };
    let mut seen: rustc_hash::FxHashSet<u64> = kept.iter().map(|(fp, _)| *fp).collect();
    let near_net_count = near_net.len();
    if kept.len() < max_keep && !scanned.is_empty() {
        if near_net_count > 0 {
            near_net.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| compare_cycle_score(&a.2, &b.2)));
            for (fp, _, cycle) in near_net.drain(..) {
                if kept.len() >= max_keep {
                    break;
                }
                if seen.insert(fp)
                    && cycle_flash_evaluable(
                        &cycle,
                        arena,
                        &flash,
                        flash_ttl,
                        flash_policy,
                        token_decimals,
                        token_to_matic_rates,
                    )
                {
                    kept.push((fp, cycle));
                }
            }
        }
        if kept.len() < max_keep {
            let fallback = simulatable_score_fallback(
                &scanned,
                arena,
                token_decimals,
                token_to_matic_rates,
                flash_liquidity,
                flash_policy,
                max_keep,
            );
            for cycle in fallback {
                if kept.len() >= max_keep {
                    break;
                }
                let fp = hash_cycle_edges(&cycle.edges);
                if seen.insert(fp) {
                    kept.push((fp, cycle));
                }
            }
        }
        if had_net_ranked || near_net_count > 0 {
            crate::debug!(
                "probe rank backfill: kept={} scanned={} skip_rate={} skip_probe={} (missing_decimals={} minimal_sim={} no_sim={} zero_profit={} sanity={} probe_fail_reasons=(invalid={} missing_pool={} non_tradable={} shallow_cl={} v2_reserve={} math={} unsupported={} zero_output={})) skip_net={} near_net={near_net_count} rescue={rescue_len}",
                kept.len(),
                scanned.len(),
                skip.rate,
                skip.probe(),
                skip.missing_decimals,
                skip.minimal_sim(),
                skip.minimal_no_sim,
                skip.minimal_zero_profit,
                skip.minimal_sanity,
                minimal_sim_reasons.invalid_route,
                minimal_sim_reasons.missing_pool,
                minimal_sim_reasons.non_tradable,
                minimal_sim_reasons.shallow_cl,
                minimal_sim_reasons.v2_reserve_exhausted,
                minimal_sim_reasons.math,
                minimal_sim_reasons.unsupported_state,
                minimal_sim_reasons.zero_output,
                skip.net,
            );
        }
    }
    probe_seeds.retain(|fingerprint, _| seen.contains(fingerprint));

    if kept.is_empty() && !scanned.is_empty() {
        let sample = flash_diag.as_deref().unwrap_or("none");
        crate::debug!(
            "probe rank empty: scanned={} skip_rate={} skip_flash={} skip_flash_source={} skip_probe={} (missing_decimals={} minimal_sim={} no_sim={} zero_profit={} sanity={}) skip_net={} rescue={rescue_len} sample={sample}",
            scanned.len(),
            skip.rate,
            skip.flash,
            skip.flash_source,
            skip.probe(),
            skip.missing_decimals,
            skip.minimal_sim(),
            skip.minimal_no_sim,
            skip.minimal_zero_profit,
            skip.minimal_sanity,
            skip.net,
        );
    } else if kept.len() * 4 < scanned.len() {
        crate::info!(
            "route probe rank: kept={} scanned={} skip_rate={} skip_executor={} skip_flash={} skip_flash_source={} skip_probe={} (missing_decimals={} minimal_sim={} no_sim={} zero_profit={} sanity={} probe_fail_reasons=(invalid={} missing_pool={} non_tradable={} shallow_cl={} v2_reserve={} math={} unsupported={} zero_output={})) skip_net={} near_net={}",
            kept.len(),
            scanned.len(),
            skip.rate,
            skip.executor_budget,
            skip.flash,
            skip.flash_source,
            skip.probe(),
            skip.missing_decimals,
            skip.minimal_sim(),
            skip.minimal_no_sim,
            skip.minimal_zero_profit,
            skip.minimal_sanity,
            minimal_sim_reasons.invalid_route,
            minimal_sim_reasons.missing_pool,
            minimal_sim_reasons.non_tradable,
            minimal_sim_reasons.shallow_cl,
            minimal_sim_reasons.v2_reserve_exhausted,
            minimal_sim_reasons.math,
            minimal_sim_reasons.unsupported_state,
            minimal_sim_reasons.zero_output,
            skip.net,
            near_net_count,
        );
    }
    flash_loan.cache_generation = flash.generation();
    flash_loan.log_summary("probe_rank");

    let kept_cycles = kept.into_iter().map(|(_, cycle)| cycle).collect();
    (kept_cycles, probe_seeds)
}

#[derive(Default)]
struct EvalFailStats {
    quarantine: AtomicU32,
    executor_budget: AtomicU32,
    flash: AtomicU32,
    flash_source: AtomicU32,
    opt_none: AtomicU32,
    detailed_none: AtomicU32,
    fallback_none: AtomicU32,
    probe_sim_none: AtomicU32,
    probe_zero_profit: AtomicU32,
    probe_fidelity: AtomicU32,
    probe_sanity: AtomicU32,
}

impl EvalFailStats {
    fn log_assess_summary(&self, in_count: usize, out_count: usize) {
        if in_count == 0 {
            return;
        }
        crate::info!(
            "route assess: in={in_count} ok={out_count} quarantine={} executor_budget={} flash={} flash_source={} opt_none={} detailed_none={} fallback_none={} probe_fail(sim_none={} zero={} fidelity={} sanity={})",
            load(&self.quarantine),
            load(&self.executor_budget),
            load(&self.flash),
            load(&self.flash_source),
            load(&self.opt_none),
            load(&self.detailed_none),
            load(&self.fallback_none),
            load(&self.probe_sim_none),
            load(&self.probe_zero_profit),
            load(&self.probe_fidelity),
            load(&self.probe_sanity),
        );
    }
}

fn inc(c: &AtomicU32) {
    c.fetch_add(1, Ordering::Relaxed);
}

fn add(dst: &AtomicU32, src: u32) {
    dst.fetch_add(src, Ordering::Relaxed);
}

fn load(c: &AtomicU32) -> u32 {
    c.load(Ordering::Relaxed)
}

#[must_use]
pub fn evaluate_cycles_parallel(
    cycles: &[FoundCycle],
    input: &HfEvalInput<'_>,
    probe_seeds: &FxHashMap<u64, (U256, MinimalSimResult)>,
) -> Vec<HfEvalResult> {
    let stats = EvalFailStats::default();
    let in_count = cycles.len();
    let results: Vec<HfEvalResult> = if crate::util::should_use_rayon(cycles.len()) {
        cycles
            .par_iter()
            .filter_map(|cycle| evaluate_one(cycle, input, probe_seeds, &stats))
            .collect()
    } else {
        cycles
            .iter()
            .filter_map(|cycle| evaluate_one(cycle, input, probe_seeds, &stats))
            .collect()
    };
    if in_count > 0 && (results.len() < in_count || results.is_empty()) {
        stats.log_assess_summary(in_count, results.len());
        crate::pipeline::curve_sim::log_curve_sim_summary();
        crate::pipeline::brent_diag::log_brent_summary();
        crate::pipeline::ternary_diag::log_ternary_summary();
    }
    results
}

pub async fn rescore_rank_and_evaluate_async(
    mut cycles: Vec<Arc<FoundCycle>>,
    input: Arc<HfEvalInputOwned>,
    sim_cap: usize,
) -> anyhow::Result<(Vec<HfEvalResult>, Arc<StateArena>, usize)> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    crate::util::cpu_pool().spawn(move || {
        let result = crate::util::run_cpu(|| {
            let rates = input.token_to_matic_rates.as_ref();
            let decimals = input.token_decimals.as_ref();
            let probe_window = probe_rank_window(sim_cap, cycles.len());
            let mut spot_table =
                crate::pipeline::spot_price::SpotTable::new(input.arena.pool_count());
            if probe_window > 0 {
                crate::pipeline::spot_price::rescore_arc_cycles_with_table_and_gas(
                    &input.arena,
                    &mut spot_table,
                    &mut cycles[..probe_window],
                    Some(input.gas_price),
                    Some(rates),
                    Some(decimals),
                    None,
                );
                cycles[..probe_window].sort_by(|a, b| compare_cycle_score(a.as_ref(), b.as_ref()));
            }
            let route_gas = RouteGasLookup::for_fingerprints(
                &input.gas_oracle,
                cycles
                    .iter()
                    .take(probe_window)
                    .map(|c| hash_cycle_edges(&c.as_ref().edges)),
            );
            let (cycles, probe_seeds) = rank_cycles_by_probe_net(
                &input.arena,
                cycles,
                rates,
                decimals,
                input.gas_price,
                input.slippage_bps,
                input.flash_policy,
                sim_cap,
                &input.gas_oracle,
                &route_gas,
                input.flash_liquidity.as_ref(),
                input.safety_multiplier_bps,
                input.profit_priority_alpha_bps,
            );
            if !cycles.is_empty() {
                crate::debug!("probe rank kept {} cycles for Brent", cycles.len());
            }
            let eval = input.as_eval_input(&route_gas);
            let eval_results = evaluate_cycles_parallel(&cycles, &eval, &probe_seeds);
            input
                .execution
                .route_sim_cache
                .debug_log_if_active("hf_eval");
            let probe_kept = cycles.len();
            (eval_results, Arc::clone(&input.arena), probe_kept)
        });
        if tx.send(result).is_err() {
            crate::debug!("hf eval result channel closed before send");
        }
    });
    rx.await.context("hf eval task failed")
}

fn probe_fallback_amounts(
    cycle: &FoundCycle,
    input: &HfEvalInput<'_>,
    probe_seed: Option<(U256, MinimalSimResult)>,
) -> [U256; 3] {
    let flash_cap = flash_cap_for_cycle(input, cycle);
    let dec = flash_cap.token_decimals;
    let rate = flash_cap.token_to_matic_rate;
    let economic = min_economic_amount_in(dec, rate);
    let spot = crate::pipeline::spot_price::spot_probe_for_decimals(dec);
    let seed = probe_seed.map(|(a, _)| a).unwrap_or(economic);
    // Try economic first, then the probe seed amount, then the decimal-aware spot probe.
    let mut amounts = [U256::ZERO; 3];
    let mut n = 0usize;
    for candidate in [economic, seed, spot] {
        if candidate.is_zero() {
            continue;
        }
        let duplicate = (0..n).any(|i| amounts[i] == candidate);
        if duplicate {
            continue;
        }
        if candidate == spot && !flash_cap.amount_within_cap(spot) {
            continue;
        }
        amounts[n] = candidate;
        n += 1;
    }
    amounts
}

fn probe_fallback_opt(
    cycle: &FoundCycle,
    input: &HfEvalInput<'_>,
    probe_seed: Option<(U256, MinimalSimResult)>,
    stats: &EvalFailStats,
    _fp: u64,
) -> Option<(OptimizationResult, RouteSimulationResult)> {
    let rate = resolve_token_to_matic_rate(cycle.start_token, input.token_to_matic_rates);
    let decimals =
        resolve_token_decimals_for_index(cycle.start_token, input.arena, input.token_decimals);
    let (mut psn, mut pzp, mut pf, mut ps) = (0u32, 0u32, 0u32, 0u32);
    let mut best: Option<(OptimizationResult, RouteSimulationResult)> = None;
    for amount in probe_fallback_amounts(cycle, input, probe_seed) {
        if amount.is_zero() {
            continue;
        }
        let Some(sim) = simulate_route_detailed(input.arena, &cycle.edges, amount) else {
            psn += 1;
            continue;
        };
        if sim.profit.is_zero() {
            pzp += 1;
            continue;
        }
        if !local_sim::route_hop_fidelity_ok_after_walk(input.arena, &cycle.edges, &sim.hop_amounts)
        {
            pf += 1;
            continue;
        }
        // search_low=ZERO so check_sim_sanity's OptimizerPinnedAtFloor check
        // doesn't false-positive: this is a static probe, not a Brent search
        // where the solver got stuck at the floor.
        if let Err(reason) = check_sim_sanity(SimSanityInput {
            amount_in: amount,
            gross_profit: sim.profit,
            search_low: U256::ZERO,
            token_decimals: decimals,
            token_to_matic_rate: rate,
        }) {
            crate::debug!(
                "probe fallback sanity reject: fp={_fp:#x} start_token={} hops={} pool_addrs={:?} edges={:?} amount={amount} profit={} rate={rate} dec={decimals} reason={reason:?}",
                cycle.start_token.0,
                cycle.hop_count,
                cycle
                    .edges
                    .iter()
                    .filter_map(|edge| input.arena.pool_address(edge.pool_index))
                    .collect::<Vec<_>>(),
                cycle.edges,
                sim.profit,
            );
            ps += 1;
            continue;
        }
        let candidate = (
            OptimizationResult {
                optimal_input: amount,
                expected_gross: sim.amount_out,
                net_profit: sim.profit,
                total_gas: sim.total_gas,
                search_low: U256::ZERO,
            },
            sim,
        );
        let replace = best
            .as_ref()
            .is_none_or(|(best_opt, _)| candidate.0.net_profit > best_opt.net_profit);
        if replace {
            best = Some(candidate);
        }
    }
    let s = stats;
    add(&s.probe_sim_none, psn);
    add(&s.probe_zero_profit, pzp);
    add(&s.probe_fidelity, pf);
    add(&s.probe_sanity, ps);
    best
}

/// Economic + spot probe sims for Brent warm-start (deduped, sanity-filtered).
fn build_brent_probe_seeds(
    arena: &StateArena,
    cycle: &FoundCycle,
    start_decimals: u8,
    start_rate: U256,
    probe_seed: Option<(U256, MinimalSimResult)>,
) -> Vec<(U256, MinimalSimResult)> {
    let mut seeds = Vec::with_capacity(2);
    if let Some(pair) = probe_seed {
        seeds.push(pair);
    }
    for_each_rank_probe_amount(start_decimals, start_rate, |amount| {
        if seeds.len() >= BRENT_SEED_CACHE_SLOTS {
            return;
        }
        if seeds.iter().any(|(a, _)| *a == amount) {
            return;
        }
        let Some(sim) = simulate_route_minimal(arena, &cycle.edges, amount) else {
            return;
        };
        if sim.profit.is_zero() {
            return;
        }
        if check_sim_sanity_fast(SimSanityInput {
            amount_in: amount,
            gross_profit: sim.profit,
            search_low: U256::ZERO,
            token_decimals: start_decimals,
            token_to_matic_rate: start_rate,
        })
        .is_err()
        {
            return;
        }
        seeds.push((amount, sim));
    });
    seeds
}

fn evaluate_one(
    cycle: &FoundCycle,
    input: &HfEvalInput<'_>,
    probe_seeds: &FxHashMap<u64, (U256, MinimalSimResult)>,
    stats: &EvalFailStats,
) -> Option<HfEvalResult> {
    // Cycles from rank_cycles_by_probe_net are already dispatch-ready (Aave start rotation).
    let fp = hash_cycle_edges(&cycle.edges);
    let route_state_revision = input.arena.route_state_revision(&cycle.edges);
    if !route_fits_executor(&cycle.edges) {
        inc(&stats.executor_budget);
        return None;
    }
    if input.execution.is_route_quarantined(fp) {
        inc(&stats.quarantine);
        return None;
    }
    if !cycle_tokens_have_known_decimals(cycle, input.arena, input.token_decimals) {
        inc(&stats.opt_none);
        return None;
    }
    let flash = input.flash_liquidity.load();
    let flash_ttl = input.flash_ttl;
    if !balancer_route_flash_feasible(cycle, input.arena, &flash, flash_ttl) {
        inc(&stats.flash);
        return None;
    }
    let probe_seed = probe_seeds.get(&fp).map(|(amount, sim)| (*amount, *sim));
    let base_slippage = input.slippage_bps;
    let start_decimals =
        resolve_token_decimals_for_index(cycle.start_token, input.arena, input.token_decimals);
    let start_rate = resolve_token_to_matic_rate(cycle.start_token, input.token_to_matic_rates);
    let brent_probe_amount = probe_seed
        .map(|(amount, _)| amount)
        .unwrap_or_else(|| min_economic_amount_in(start_decimals, start_rate));
    let Some(flash_ctx) = build_cycle_flash_context(cycle, input.arena, &flash, flash_ttl) else {
        inc(&stats.flash_source);
        return None;
    };
    let Some(flash_source_brent) =
        resolve_flash_source_with_context(&flash_ctx, input.flash_policy, brent_probe_amount)
    else {
        inc(&stats.flash_source);
        return None;
    };
    let hop_count = cycle.edges.len() as u32;
    let brent_slippage = probe_seed
        .map(|(amount, sim)| {
            let depth =
                depth_impact_slippage_bps_with_base(input.arena, &cycle.edges, amount, Some(&sim));
            effective_slippage_bps(input.slippage_bps, hop_count, depth)
        })
        .unwrap_or_else(|| effective_slippage_bps(base_slippage, hop_count, 0));
    let mut profit_ctx = ProfitEvalContext::with_safety_multiplier(
        cycle.start_token,
        input.arena,
        input.token_to_matic_rates,
        input.token_decimals,
        input.gas_price,
        brent_slippage,
        flash_source_brent,
        input.safety_multiplier_bps,
    );
    // Match probe ranking: pre-resolve route gas, do not scale twice in Brent.
    profit_ctx.gas_scale_bps = 10_000;
    profit_ctx.hop_count = cycle.edges.len() as u32;
    profit_ctx.profit_priority_alpha_bps = input.profit_priority_alpha_bps;
    let route_gas_costing = RouteGasCosting {
        lookup: input.route_gas,
        oracle: input.gas_oracle,
        fingerprint: fp,
    };
    let brent_seeds =
        build_brent_probe_seeds(input.arena, cycle, start_decimals, start_rate, probe_seed);
    let brent_seed_slice = (!brent_seeds.is_empty()).then_some(brent_seeds.as_slice());

    let (mut opt, mut sim, probe_only) = match optimize_cycle(
        input.arena,
        cycle,
        input.token_to_matic_rates,
        input.token_decimals,
        Some(input.max_flash_loan_usd),
        input.matic_usd,
        input.matic_usd_chainlink,
        Some(input.brent_iters),
        None,
        &profit_ctx,
        brent_seed_slice,
        Some(route_gas_costing),
        route_state_revision
            .map(|revision| (input.execution.route_sim_cache.as_ref(), revision, fp)),
    ) {
        Some(opt) => {
            let Some(sim) = simulate_route_detailed(input.arena, &cycle.edges, opt.optimal_input)
            else {
                crate::trace!(
                    "evaluate_one detailed_none: fp={fp:#x} opt_input={}",
                    opt.optimal_input
                );
                inc(&stats.detailed_none);
                return None;
            };
            if validate_optimized_sim(input, cycle, &sim, opt.optimal_input, opt.search_low) {
                (opt, sim, false)
            } else {
                crate::trace!(
                    "evaluate_one validate_failed -> probe_fallback: fp={fp:#x} search_low={}",
                    opt.search_low
                );
                let pair =
                    probe_fallback_opt(cycle, input, probe_seed, stats, fp).or_else(|| {
                        inc(&stats.fallback_none);
                        None
                    })?;
                (pair.0, pair.1, true)
            }
        }
        None => {
            crate::trace!("evaluate_one opt_none -> probe_fallback: fp={fp:#x}");
            let pair = probe_fallback_opt(cycle, input, probe_seed, stats, fp).or_else(|| {
                inc(&stats.opt_none);
                None
            })?;
            (pair.0, pair.1, true)
        }
    };

    let Some(flash_source) =
        resolve_flash_source_with_context(&flash_ctx, input.flash_policy, opt.optimal_input)
    else {
        inc(&stats.flash_source);
        return None;
    };
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
    let mut slippage_bps = effective_slippage_bps(input.slippage_bps, hop_count, depth_bps);
    let config_route_slip = effective_slippage_bps(base_slippage, hop_count, 0);
    let mut assessment = if slippage_bps > config_route_slip {
        let depth_assessment =
            assess_route_for_cycle(input, &sim, cycle, fp, slippage_bps, flash_source)?;
        if depth_assessment.should_execute {
            depth_assessment
        } else {
            let mut depth_ctx = ProfitEvalContext::with_safety_multiplier(
                cycle.start_token,
                input.arena,
                input.token_to_matic_rates,
                input.token_decimals,
                input.gas_price,
                slippage_bps,
                flash_source,
                input.safety_multiplier_bps,
            );
            depth_ctx.gas_scale_bps = 10_000;
            depth_ctx.hop_count = cycle.edges.len() as u32;
            depth_ctx.profit_priority_alpha_bps = input.profit_priority_alpha_bps;
            if let Some(reopt) = optimize_cycle(
                input.arena,
                cycle,
                input.token_to_matic_rates,
                input.token_decimals,
                Some(input.max_flash_loan_usd),
                input.matic_usd,
                input.matic_usd_chainlink,
                Some(input.brent_iters),
                None,
                &depth_ctx,
                None,
                Some(route_gas_costing),
                route_state_revision
                    .map(|revision| (input.execution.route_sim_cache.as_ref(), revision, fp)),
            ) {
                opt = reopt;
                sim = simulate_route_detailed(input.arena, &cycle.edges, opt.optimal_input)?;
                if !validate_optimized_sim(input, cycle, &sim, opt.optimal_input, opt.search_low) {
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
                slippage_bps = effective_slippage_bps(input.slippage_bps, hop_count, depth_bps);
            }
            assess_route_for_cycle(input, &sim, cycle, fp, slippage_bps, flash_source)?
        }
    } else {
        assess_route_for_cycle(input, &sim, cycle, fp, slippage_bps, flash_source)?
    };
    if probe_only && !assessment.should_execute {
        assessment.reject_reason = assessment
            .reject_reason
            .or_else(|| Some("Brent did not converge; probe-only assessment".into()));
    } else if probe_only {
        crate::info!(
            "hf probe-dispatch: fp={fp} input={} net_matic={} (Brent fallback, probe sizing validated)",
            opt.optimal_input,
            assessment.net_profit_after_gas_matic_wei,
        );
    }

    Some(HfEvalResult {
        route_fingerprint: fp,
        cycle: cycle.clone(),
        opt,
        sim,
        assessment,
        effective_slippage_bps: slippage_bps,
        flash_source,
        balancer_batch_verified: false,
    })
}

/// Recompute profitability after `sim` was updated (resim, on-chain verify, etc.).
#[must_use]
pub fn reassess_hf_eval_result(
    result: &HfEvalResult,
    input: &HfEvalInput<'_>,
    flash_source: FlashLoanSource,
) -> Option<ProfitAssessment> {
    // `effective_slippage_bps` is already route-level (config compounded + depth).
    assess_route_for_cycle(
        input,
        &result.sim,
        &result.cycle,
        result.route_fingerprint,
        result.effective_slippage_bps,
        flash_source,
    )
}

fn assess_route_for_cycle(
    input: &HfEvalInput<'_>,
    sim: &RouteSimulationResult,
    cycle: &FoundCycle,
    fp: u64,
    slippage_bps: u64,
    flash_source: FlashLoanSource,
) -> Option<ProfitAssessment> {
    let risk_bps = input.execution.route_risk_multiplier_bps(fp);
    let thresholds = route_profit_thresholds(
        input.min_profit_matic,
        input.min_profit_roi_bps,
        input.safety_multiplier_bps,
        input.profit_priority_alpha_bps,
        risk_bps,
    );
    Some(assess_route_from_sim(&RouteAssessRequest {
        cycle_start: cycle.start_token,
        arena: input.arena,
        gross_profit: sim.profit,
        amount_in: sim.amount_in,
        simulated_gas: sim.total_gas,
        hop_count: cycle.hop_count,
        slippage_bps,
        flash_source,
        gas: AssessmentGas::TickRoute {
            lookup: input.route_gas,
            oracle: input.gas_oracle,
            route_fp: fp,
        },
        thresholds,
        token_to_matic_rates: input.token_to_matic_rates,
        token_decimals: input.token_decimals,
        gas_price: input.gas_price,
    }))
}

fn flash_cap_for_cycle(input: &HfEvalInput<'_>, cycle: &FoundCycle) -> FlashBorrowCapParams {
    FlashBorrowCapParams {
        max_flash_loan_usd: input.max_flash_loan_usd,
        token_decimals: resolve_token_decimals_for_index(
            cycle.start_token,
            input.arena,
            input.token_decimals,
        ),
        token_to_matic_rate: resolve_token_to_matic_rate(
            cycle.start_token,
            input.token_to_matic_rates,
        ),
        matic_usd: input.matic_usd,
        matic_usd_chainlink: input.matic_usd_chainlink,
    }
}

fn validate_optimized_sim(
    input: &HfEvalInput<'_>,
    cycle: &FoundCycle,
    sim: &RouteSimulationResult,
    optimal_input: U256,
    search_low: U256,
) -> bool {
    let flash_cap = flash_cap_for_cycle(input, cycle);
    let token_to_matic_rate = flash_cap.token_to_matic_rate;
    let token_decimals = flash_cap.token_decimals;

    sim.amount_in == optimal_input
        && local_sim::route_hop_fidelity_ok_after_walk(input.arena, &cycle.edges, &sim.hop_amounts)
        && !sim.profit.is_zero()
        && flash_cap.amount_within_cap(sim.amount_in)
        && check_sim_sanity_for_dispatch(SimSanityInput {
            amount_in: sim.amount_in,
            gross_profit: sim.profit,
            search_low,
            token_decimals,
            token_to_matic_rate,
        })
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{CycleEdges, TokenIndex};

    fn cycle(id: u32) -> FoundCycle {
        FoundCycle {
            start_token: TokenIndex(id),
            edges: CycleEdges::new(),
            hop_count: 2,
            log_weight: -(f64::from(id)),
            cumulative_fee_bps: 0,
            score: -(f64::from(id)),
            cycle_ratio: U256::ZERO,
        }
    }

    #[test]
    fn profitable_probe_routes_fill_full_cap_before_rescues() {
        let profitable = (0..8)
            .map(|id| (u64::from(id), U256::from(100u32 - id), cycle(id)))
            .collect();
        let kept = select_probe_survivors(profitable, vec![(99, cycle(99))], 8, 2);
        assert_eq!(kept.len(), 8);
        assert!(
            kept.iter()
                .all(|(_, cycle)| cycle.start_token != TokenIndex(99))
        );
    }

    #[test]
    fn probe_rank_window_scales_with_sim_cap() {
        assert_eq!(probe_rank_window(75, 1_000), 150);
        assert_eq!(probe_rank_window(75, 100), 100);
        assert_eq!(probe_rank_window(0, 100), 0);
    }

    #[test]
    fn rescue_routes_only_backfill_unused_capacity() {
        let profitable = vec![
            (1, U256::from(100u8), cycle(1)),
            (2, U256::from(90u8), cycle(2)),
        ];
        let kept = select_probe_survivors(
            profitable,
            vec![(10, cycle(10)), (11, cycle(11)), (12, cycle(12))],
            4,
            2,
        );
        assert_eq!(kept.len(), 4);
        assert_eq!(kept[0].1.start_token, TokenIndex(1));
        assert_eq!(kept[1].1.start_token, TokenIndex(2));
    }

    #[test]
    fn probe_skip_counters_keep_missing_decimals_distinct_from_simulation_rejects() {
        let mut aggregate = SkipCounters::default();
        aggregate.merge(SkipCounters {
            missing_decimals: 2,
            minimal_no_sim: 3,
            ..SkipCounters::default()
        });

        assert_eq!(aggregate.missing_decimals, 2);
        assert_eq!(aggregate.minimal_sim(), 3);
        assert_eq!(aggregate.probe(), 5);
    }

    #[test]
    fn minimal_probe_rejects_preserve_their_reason() {
        let mut skip = SkipCounters::default();
        MinimalProbeReject::NoSimulation.record(&mut skip);
        MinimalProbeReject::ZeroProfit.record(&mut skip);
        MinimalProbeReject::SanityReject.record(&mut skip);

        assert_eq!(skip.minimal_no_sim, 1);
        assert_eq!(skip.minimal_zero_profit, 1);
        assert_eq!(skip.minimal_sanity, 1);
        assert_eq!(skip.minimal_sim(), 3);
    }

    #[test]
    fn probe_skip_counter_merge_does_not_double_count_reduced_partials() {
        let mut reduced = SkipCounters::default();
        reduced.merge(SkipCounters {
            missing_decimals: 2,
            ..SkipCounters::default()
        });

        let mut aggregate = SkipCounters::default();
        aggregate.merge(reduced);

        assert_eq!(aggregate.probe(), 2);
    }
}
