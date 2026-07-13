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
use crate::pipeline::local_sim::{self, simulate_route_detailed, simulate_route_minimal};
use crate::pipeline::sim_sanity::{
    SimSanityInput, check_sim_sanity, check_sim_sanity_for_dispatch, max_flash_borrow_wei,
    min_economic_amount_in,
};
use crate::pipeline::spot_price::spot_probe_for_decimals;
use crate::pipeline::ternary::{RouteGasCosting, optimize_cycle};
use crate::pipeline::types::OptimizationResult;
use crate::pipeline::types::{MinimalSimResult, compare_cycle_score};
use crate::services::execution::candidate::hash_cycle_edges;
use crate::services::execution::flash_liquidity::{
    FlashLiquidityCache, balancer_route_flash_feasible, prefer_aave_flash_start,
    resolve_flash_source_for_cycle,
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
    probe: u32,
    net: u32,
}

impl SkipCounters {
    fn merge(&mut self, other: SkipCounters) {
        self.rate += other.rate;
        self.flash += other.flash;
        self.flash_source += other.flash_source;
        self.probe += other.probe;
        self.net += other.net;
    }
}

#[derive(Default)]
struct ProbeRankPartial {
    profitable: Vec<(u64, U256, FoundCycle)>,
    rescue: Vec<(u64, FoundCycle)>,
    near_net: Vec<(u64, U256, FoundCycle)>,
    seeds: FxHashMap<u64, (U256, MinimalSimResult)>,
    skip: SkipCounters,
    flash_diag: Option<String>,
}

impl ProbeRankPartial {
    fn merge(mut self, other: Self) -> Self {
        self.profitable.extend(other.profitable);
        self.rescue.extend(other.rescue);
        self.near_net.extend(other.near_net);
        self.seeds.extend(other.seeds);
        self.skip.merge(other.skip);
        if self.flash_diag.is_none() {
            self.flash_diag = other.flash_diag;
        }
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
    /// Set when `filter_balancer_onchain_verified` already ran `queryBatchSwap`.
    pub balancer_batch_verified: bool,
}

/// Economic probe first, then decimal-aware spot probe for tickless CL routes.
fn try_rank_probe_minimal(
    arena: &StateArena,
    cycle: &FoundCycle,
    start_decimals: u8,
    rate: U256,
) -> Option<(U256, MinimalSimResult)> {
    minimal_rank_probe(arena, cycle, start_decimals, rate, true)
}

/// Shared minimal probe used by rank + simulatable checks (`search_low` differs for rank vs filter).
fn minimal_rank_probe(
    arena: &StateArena,
    cycle: &FoundCycle,
    start_decimals: u8,
    rate: U256,
    rank_amount_as_search_low: bool,
) -> Option<(U256, MinimalSimResult)> {
    let economic = min_economic_amount_in(start_decimals, rate);
    let spot_probe = spot_probe_for_decimals(start_decimals);
    for amount in [economic, spot_probe] {
        let Some(sim) = simulate_route_minimal(arena, &cycle.edges, amount) else {
            continue;
        };
        if sim.profit.is_zero() {
            continue;
        }
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
        .is_ok()
        {
            return Some((amount, sim));
        }
    }
    None
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
    minimal_rank_probe(arena, cycle, decimals, rate, false).is_some()
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
            let ready = prefer_aave_flash_start(cycle, arena, &flash, flash_ttl);
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
    let cycle = prefer_aave_flash_start(cycle_arc, arena, flash, flash_ttl);
    let fp = hash_cycle_edges(&cycle.edges);
    if !has_reliable_matic_rate(cycle.start_token, token_to_matic_rates) {
        out.skip.rate = 1;
        return out;
    }
    if !cycle_tokens_have_known_decimals(&cycle, arena, token_decimals) {
        out.skip.probe = 1;
        return out;
    }
    if !balancer_route_flash_feasible(&cycle, arena, flash, flash_ttl) {
        out.skip.flash = 1;
        if out.flash_diag.is_none() {
            out.flash_diag = Some(format!(
                "fp={fp:#x} hops={} mixed_balancer_no_aave",
                cycle.edges.len(),
            ));
        }
        return out;
    }
    let start_decimals = resolve_token_decimals_for_index(cycle.start_token, arena, token_decimals);
    let rate = resolve_token_to_matic_rate(cycle.start_token, token_to_matic_rates);
    let Some((probe_amount, probe)) = try_rank_probe_minimal(arena, &cycle, start_decimals, rate)
    else {
        if cycle.score < 0.0 {
            out.rescue.push((fp, cycle.into_owned()));
        } else {
            out.skip.probe = 1;
        }
        return out;
    };
    let Some(flash_source) =
        resolve_flash_source_for_cycle(&cycle, arena, flash, flash_ttl, flash_policy, probe_amount)
    else {
        out.skip.flash_source = 1;
        if out.flash_diag.is_none() {
            let start_addr = arena
                .token_address(cycle.start_token)
                .map(|a| format!("{a}"))
                .unwrap_or_else(|| "?".into());
            out.flash_diag = Some(format!(
                "fp={fp:#x} hops={} flash_source_reject start={start_addr} probe_amt={probe_amount}",
                cycle.edges.len(),
            ));
        }
        return out;
    };

    let depth_bps =
        depth_impact_slippage_bps_with_base(arena, &cycle.edges, probe_amount, Some(&probe));
    let effective_slip = effective_slippage_bps(slippage_bps, depth_bps);

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
                "probe net=0 (gas eats profit): fp={fp:#x} hops={hop_count} sim_profit={} probe_amt={probe_amount} gas={} gas_cost_in_token={} rate={rate} dec={start_decimals}",
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
    let profitable_ranked = partial.profitable;
    let mut rescue = partial.rescue;
    let mut probe_seeds = partial.seeds;
    let skip = partial.skip;
    let mut near_net = partial.near_net;
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
                "probe rank backfill: kept={} scanned={} skip_rate={} skip_probe={} skip_net={} near_net={near_net_count} rescue={rescue_len}",
                kept.len(),
                scanned.len(),
                skip.rate,
                skip.probe,
                skip.net,
            );
        }
    }
    probe_seeds.retain(|fingerprint, _| seen.contains(fingerprint));

    if kept.is_empty() && !scanned.is_empty() {
        let sample = partial.flash_diag.as_deref().unwrap_or("none");
        crate::debug!(
            "probe rank empty: scanned={} skip_rate={} skip_flash={} skip_flash_source={} skip_probe={} skip_net={} rescue={rescue_len} sample={sample}",
            scanned.len(),
            skip.rate,
            skip.flash,
            skip.flash_source,
            skip.probe,
            skip.net,
        );
    } else if kept.len() * 4 < scanned.len() {
        crate::debug!(
            "probe rank thin: kept={} scanned={} skip_rate={} skip_flash={} skip_flash_source={} skip_probe={} skip_net={} near_net={}",
            kept.len(),
            scanned.len(),
            skip.rate,
            skip.flash,
            skip.flash_source,
            skip.probe,
            skip.net,
            near_net_count,
        );
    }

    let kept_cycles = kept.into_iter().map(|(_, cycle)| cycle).collect();
    (kept_cycles, probe_seeds)
}

#[derive(Default)]
struct EvalFailStats {
    quarantine: AtomicU32,
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
    if in_count > 0 {
        let out = results.len();
        if out == 0 {
            crate::debug!(
                "hf assess failed: routes={in_count} quarantine={} flash={} flash_source={} opt_none={} detailed_none={} fallback_none={} probe(sim_none={} zero={} fidelity={} sanity={})",
                load(&stats.quarantine),
                load(&stats.flash),
                load(&stats.flash_source),
                load(&stats.opt_none),
                load(&stats.detailed_none),
                load(&stats.fallback_none),
                load(&stats.probe_sim_none),
                load(&stats.probe_zero_profit),
                load(&stats.probe_fidelity),
                load(&stats.probe_sanity),
            );
        } else if out < in_count {
            crate::debug!(
                "hf assess partial: ok={out}/{in_count} quarantine={} flash={} flash_source={} opt_none={} fallback_none={}",
                load(&stats.quarantine),
                load(&stats.flash),
                load(&stats.flash_source),
                load(&stats.opt_none),
                load(&stats.fallback_none),
            );
        }
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
    let dec =
        resolve_token_decimals_for_index(cycle.start_token, input.arena, input.token_decimals);
    let rate = resolve_token_to_matic_rate(cycle.start_token, input.token_to_matic_rates);
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
        // Skip spot probe if it exceeds the flash loan cap for this token.
        if candidate == spot
            && let Some(cap) =
                max_flash_borrow_wei(input.max_flash_loan_usd, dec, rate, input.matic_usd)
            && spot > cap
        {
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
        if !local_sim::route_hop_fidelity_ok(input.arena, &cycle.edges, &sim.hop_amounts) {
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
                "probe fallback sanity reject: fp={_fp:#x} amount={amount} profit={} rate={rate} dec={decimals} reason={reason:?}",
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

fn evaluate_one(
    cycle: &FoundCycle,
    input: &HfEvalInput<'_>,
    probe_seeds: &FxHashMap<u64, (U256, MinimalSimResult)>,
    stats: &EvalFailStats,
) -> Option<HfEvalResult> {
    // Cycles from rank_cycles_by_probe_net are already dispatch-ready (Aave start rotation).
    let fp = hash_cycle_edges(&cycle.edges);
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
    let Some(flash_source_brent) = resolve_flash_source_for_cycle(
        cycle,
        input.arena,
        &flash,
        flash_ttl,
        input.flash_policy,
        brent_probe_amount,
    ) else {
        inc(&stats.flash_source);
        return None;
    };
    let brent_slippage = probe_seed
        .map(|(amount, sim)| {
            let depth =
                depth_impact_slippage_bps_with_base(input.arena, &cycle.edges, amount, Some(&sim));
            effective_slippage_bps(input.slippage_bps, depth)
        })
        .unwrap_or(base_slippage);
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

    let (mut opt, mut sim, probe_only) = match optimize_cycle(
        input.arena,
        cycle,
        input.token_to_matic_rates,
        input.token_decimals,
        Some(input.max_flash_loan_usd),
        input.matic_usd,
        Some(input.brent_iters),
        None,
        &profit_ctx,
        probe_seed.as_ref().map(std::slice::from_ref),
        Some(route_gas_costing),
        Some((
            input.execution.route_sim_cache.as_ref(),
            input.state_generation,
            fp,
        )),
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

    let Some(flash_source) = resolve_flash_source_for_cycle(
        cycle,
        input.arena,
        &flash,
        flash_ttl,
        input.flash_policy,
        opt.optimal_input,
    ) else {
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
    let mut slippage_bps = effective_slippage_bps(input.slippage_bps, depth_bps);
    let mut assessment = if slippage_bps > base_slippage {
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
                Some(input.brent_iters),
                None,
                &depth_ctx,
                None,
                Some(route_gas_costing),
                Some((
                    input.execution.route_sim_cache.as_ref(),
                    input.state_generation,
                    fp,
                )),
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
                slippage_bps = effective_slippage_bps(input.slippage_bps, depth_bps);
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
    assess_route_for_cycle(
        input,
        &result.sim,
        &result.cycle,
        result.route_fingerprint,
        result.effective_slippage_bps.max(input.slippage_bps),
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

fn validate_optimized_sim(
    input: &HfEvalInput<'_>,
    cycle: &FoundCycle,
    sim: &RouteSimulationResult,
    optimal_input: U256,
    search_low: U256,
) -> bool {
    let token_to_matic_rate =
        resolve_token_to_matic_rate(cycle.start_token, input.token_to_matic_rates);
    let token_decimals =
        resolve_token_decimals_for_index(cycle.start_token, input.arena, input.token_decimals);
    let within_flash_cap = max_flash_borrow_wei(
        input.max_flash_loan_usd,
        token_decimals,
        token_to_matic_rate,
        input.matic_usd,
    )
    .is_none_or(|cap| sim.amount_in <= cap);

    sim.amount_in == optimal_input
        && local_sim::route_hop_fidelity_ok(input.arena, &cycle.edges, &sim.hop_amounts)
        && !sim.profit.is_zero()
        && within_flash_cap
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
}
