use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use alloy::primitives::Address;
use alloy::primitives::U256;
use anyhow::Context;
use rayon::prelude::*;
use rustc_hash::FxHashMap;

use crate::core::types::{
    CycleEdges, FlashLoanSource, FoundCycle, ProfitAssessment, RouteSimulationResult, TokenIndex,
};
use crate::pipeline::arena::StateArena;
use crate::pipeline::cycle_filter::graph_negative_rescue_cap;
use crate::pipeline::local_sim::{
    self, MinimalSimFailure, first_protocol_state_mismatch, first_v2_hop_below_reserve,
    precompute_route_shallow_caps, simulate_route_detailed, simulate_route_detailed_with_caps,
    simulate_route_minimal_with_caps,
};
use crate::pipeline::route_calls::route_fits_executor;
use crate::pipeline::sim_sanity::{
    FlashBorrowCapParams, SimSanityInput, SimSanityReject, check_sim_sanity, check_sim_sanity_fast,
    check_sim_sanity_for_dispatch, min_economic_amount_in, required_profit_matic_wei,
};
use crate::pipeline::spot_price::for_each_rank_probe_amount;
use crate::pipeline::ternary::{BRENT_SEED_CACHE_SLOTS, RouteGasCosting, optimize_cycle};
use crate::pipeline::types::OptimizationResult;
use crate::pipeline::types::{MinimalSimResult, compare_cycle_execution};
use crate::services::execution::candidate::hash_cycle_edges;
use crate::services::execution::flash_liquidity::{
    FlashLiquidityCache, FlashLoanDiagnostics, FlashRejectReason, balancer_route_flash_feasible,
    build_cycle_flash_context, flash_reject_reason, prefer_aave_flash_start,
    resolve_flash_source_for_cycle, resolve_flash_source_with_context,
};
use crate::services::execution::flash_policy::FlashLoanPolicy;
use crate::services::execution::gas_oracle::{GasOracle, RouteGasLookup};
use crate::services::execution::impact_slippage::{
    depth_impact_slippage_bps_with_base, effective_slippage_bps_for_flash,
};
use crate::services::execution::profit::{
    ProfitEvalContext, RouteAssessRequest, assess_route_from_sim, assessment_gas_for_edges,
    assessment_gas_units, brent_score_matic_from_sim, cover_matic_from_sim, flash_loan_fee_amount,
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

/// Coarse probe-rank reject buckets (ops signal; not per-hop debug dumps).
#[derive(Default)]
struct MinimalSimReasonCounts {
    invalid_route: u32,
    missing_pool: u32,
    non_tradable: u32,
    shallow_cl: u32,
    cl_tickless: u32,
    cl_cap: u32,
    v2_reserve_exhausted: u32,
    token_mismatch: u32,
    math: u32,
    unsupported_state: u32,
    bal_max_in: u32,
    zero_output: u32,
    sanity_ratio: u32,
    sanity_matic: u32,
    sanity_floor: u32,
    sanity_decimals: u32,
    sanity_pin: u32,
}

impl MinimalSimReasonCounts {
    fn merge(&mut self, other: Self) {
        self.invalid_route += other.invalid_route;
        self.missing_pool += other.missing_pool;
        self.non_tradable += other.non_tradable;
        self.shallow_cl += other.shallow_cl;
        self.cl_tickless += other.cl_tickless;
        self.cl_cap += other.cl_cap;
        self.v2_reserve_exhausted += other.v2_reserve_exhausted;
        self.token_mismatch += other.token_mismatch;
        self.math += other.math;
        self.unsupported_state += other.unsupported_state;
        self.bal_max_in += other.bal_max_in;
        self.zero_output += other.zero_output;
        self.sanity_ratio += other.sanity_ratio;
        self.sanity_matic += other.sanity_matic;
        self.sanity_floor += other.sanity_floor;
        self.sanity_decimals += other.sanity_decimals;
        self.sanity_pin += other.sanity_pin;
    }

    fn record_sanity(&mut self, reason: SimSanityReject) {
        match reason {
            SimSanityReject::InsaneProfitRatio => self.sanity_ratio += 1,
            SimSanityReject::InsaneProfitMatic => self.sanity_matic += 1,
            SimSanityReject::AmountBelowEconomicFloor => self.sanity_floor += 1,
            SimSanityReject::UnsupportedTokenDecimals => self.sanity_decimals += 1,
            SimSanityReject::OptimizerPinnedAtFloor => self.sanity_pin += 1,
        }
    }

    fn record(&mut self, reason: MinimalSimFailure) {
        match reason {
            MinimalSimFailure::InvalidRoute => self.invalid_route += 1,
            MinimalSimFailure::MissingPool { .. } => self.missing_pool += 1,
            MinimalSimFailure::NonTradable { .. } => self.non_tradable += 1,
            MinimalSimFailure::ClTickless { .. } => self.cl_tickless += 1,
            MinimalSimFailure::ClCapExceeded { .. } => self.cl_cap += 1,
            // Synthetic incomplete-hydrate fills are encode-refused like shallow.
            MinimalSimFailure::ClSynthetic { .. } | MinimalSimFailure::ShallowCl { .. } => {
                self.shallow_cl += 1
            }
            MinimalSimFailure::V2ReserveExhausted { .. } => self.v2_reserve_exhausted += 1,
            MinimalSimFailure::TokenMismatch { .. } => self.token_mismatch += 1,
            MinimalSimFailure::Math { .. } => self.math += 1,
            MinimalSimFailure::UnsupportedState { .. } => self.unsupported_state += 1,
            MinimalSimFailure::BalancerMaxInRatio { .. } => self.bal_max_in += 1,
            MinimalSimFailure::ZeroOutput { .. } => self.zero_output += 1,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum MinimalProbeReject {
    NoSimulation,
    ZeroProfit,
    SanityReject(SimSanityReject),
}

/// Whether a probe reject should enter the graph-negative rescue list for Brent.
fn should_rescue_probe_reject(
    reject: MinimalProbeReject,
    _attempt_failures: &[Option<local_sim::MinimalSimFailure>],
) -> bool {
    match reject {
        // ZeroProfit: every ladder size walked with gross==0. Rescuing them filled
        // probe_kept with seedless cycles → evaluated=0 / all opt_none. Gas-underwater
        // routes with gross>0 already land in near_net (not this reject).
        // Sanity rejects are phantoms (ROI/MATIC caps); rescuing them only yields opt_none.
        // NoSimulation: every ladder size failed to walk — Brent cannot invent a size.
        MinimalProbeReject::ZeroProfit
        | MinimalProbeReject::SanityReject(_)
        | MinimalProbeReject::NoSimulation => false,
    }
}

impl MinimalProbeReject {
    fn record(self, skip: &mut SkipCounters) {
        match self {
            Self::NoSimulation => skip.minimal_no_sim += 1,
            Self::ZeroProfit => skip.minimal_zero_profit += 1,
            Self::SanityReject(_) => skip.minimal_sanity += 1,
        }
    }
}

#[derive(Default)]
struct ProbeRankPartial {
    profitable: Vec<(u64, U256, Arc<FoundCycle>)>,
    rescue: Vec<(u64, Arc<FoundCycle>)>,
    near_net: Vec<(u64, U256, Arc<FoundCycle>)>,
    seeds: ProbeSeedMap,
    skip: SkipCounters,
    minimal_sim_reasons: MinimalSimReasonCounts,
    flash_diag: Option<String>,
    flash_loan: FlashLoanDiagnostics,
}

type ProbeSeedMap = FxHashMap<CycleEdges, (U256, MinimalSimResult)>;

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
    pub route_sim_base_revision: u64,
    pub brent_iters: u32,
    pub min_profit_matic: U256,
    pub min_profit_roi_bps: u64,
    pub gas_price: U256,
    /// Priority already inside `gas_price` (oracle tip floored at min).
    pub charged_priority_fee_per_gas: U256,
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
    pub route_sim_base_revision: u64,
    pub brent_iters: u32,
    pub min_profit_matic: U256,
    pub min_profit_roi_bps: u64,
    pub gas_price: U256,
    pub charged_priority_fee_per_gas: U256,
    pub slippage_bps: u64,
    pub flash_policy: FlashLoanPolicy,
    pub max_flash_loan_usd: u64,
    pub matic_usd: f64,
    pub matic_usd_chainlink: Option<alloy::primitives::I256>,
    pub safety_multiplier_bps: u64,
    pub profit_priority_alpha_bps: u64,
    pub flash_liquidity: Arc<FlashLiquidityCache>,
    pub execution: Arc<ExecutionService>,
    /// Arena-synced metas for V4 pool_id / hydrate-exhausted phantom cool.
    pub pool_metas: Arc<Vec<crate::pipeline::types::PoolMeta>>,
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
            route_sim_base_revision: self.route_sim_base_revision,
            brent_iters: self.brent_iters,
            min_profit_matic: self.min_profit_matic,
            min_profit_roi_bps: self.min_profit_roi_bps,
            gas_price: self.gas_price,
            charged_priority_fee_per_gas: self.charged_priority_fee_per_gas,
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
    pub cycle: Arc<FoundCycle>,
    pub opt: OptimizationResult,
    pub sim: RouteSimulationResult,
    pub assessment: ProfitAssessment,
    pub effective_slippage_bps: u64,
    /// Flash source used for the assessment (and intended dispatch path).
    pub flash_source: FlashLoanSource,
    /// Set when `filter_balancer_onchain_verified` already ran `queryBatchSwap`.
    pub balancer_batch_verified: bool,
    pub adaptive_flash_cap_bound: bool,
}

/// Economic / spot / size-ladder probes for ranking. Deliberate probe sizes must not use
/// `search_low=amount` — that makes every >5% ROI look like OptimizerPinnedAtFloor.
fn try_rank_probe_minimal(
    arena: &StateArena,
    cycle: &FoundCycle,
    start_decimals: u8,
    rate: U256,
    route_sim_cache: Option<(&crate::pipeline::route_sim_cache::RouteSimCache, u64, u64)>,
) -> Result<(U256, MinimalSimResult), MinimalProbeReject> {
    minimal_rank_probe(arena, cycle, start_decimals, rate, route_sim_cache)
}

/// Shared minimal probe used by rank + simulatable checks.
fn minimal_rank_probe(
    arena: &StateArena,
    cycle: &FoundCycle,
    start_decimals: u8,
    rate: U256,
    route_sim_cache: Option<(&crate::pipeline::route_sim_cache::RouteSimCache, u64, u64)>,
) -> Result<(U256, MinimalSimResult), MinimalProbeReject> {
    let mut best: Option<(U256, MinimalSimResult)> = None;
    let mut saw_simulation = false;
    let mut saw_profit = false;
    let mut last_sanity: Option<SimSanityReject> = None;
    let economic_floor = min_economic_amount_in(start_decimals, rate);
    let spot_probe = crate::pipeline::spot_price::spot_probe_for_decimals(start_decimals);
    // One shallow-cap table for the whole ladder (was rebuilt on every amount).
    let shallow_caps = precompute_route_shallow_caps(arena, &cycle.edges);
    for_each_rank_probe_amount(start_decimals, rate, |amount| {
        // Ladder includes micro/spot below economic floor for thin V2 attribution.
        // Those dust hits were poisoning empty ranks as sanity_why(floor=…) (live
        // clfloor: floor=166) when economic+ sizes simply failed to sim/profit.
        // Keep tickless CL spot-cap trades (spot ≤ amount < floor); skip other dust.
        let tickless_cap_trade = amount >= spot_probe && amount < economic_floor;
        if amount < economic_floor && !tickless_cap_trade {
            return;
        }
        let sim = route_sim_cache
            .and_then(|(cache, revision, fp)| cache.get(revision, fp, &cycle.edges, amount))
            .or_else(|| {
                let sim = simulate_route_minimal_with_caps(
                    arena,
                    &cycle.edges,
                    amount,
                    shallow_caps.as_ref(),
                )?;
                if let Some((cache, revision, fp)) = route_sim_cache {
                    cache.insert(revision, fp, &cycle.edges, amount, sim);
                }
                Some(sim)
            });
        let Some(sim) = sim else {
            return;
        };
        saw_simulation = true;
        if sim.profit.is_zero() {
            return;
        }
        saw_profit = true;
        // search_low=0: pin heuristic is for Brent bounds, not static probe amounts.
        if let Err(reason) = check_sim_sanity(SimSanityInput {
            amount_in: amount,
            gross_profit: sim.profit,
            search_low: U256::ZERO,
            token_decimals: start_decimals,
            token_to_matic_rate: rate,
        }) {
            last_sanity = Some(reason);
            return;
        }
        // Prefer larger absolute gross among ladder sizes (token units; cross-token
        // ranking happens later via brent_score in rank_one_cycle_probe).
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
            MinimalProbeReject::SanityReject(
                last_sanity.unwrap_or(SimSanityReject::InsaneProfitRatio),
            )
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
    minimal_rank_probe(arena, cycle, decimals, rate, None).is_ok()
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
    fallback.sort_by(compare_cycle_execution);
    fallback.truncate(max_keep);
    fallback
}

fn select_probe_survivors(
    mut profitable: Vec<(u64, U256, Arc<FoundCycle>)>,
    mut rescue: Vec<(u64, Arc<FoundCycle>)>,
    max_keep: usize,
    rescue_cap: usize,
) -> Vec<(u64, Arc<FoundCycle>)> {
    // Primary: cover MATIC; secondary: gas-aware execution rank (not raw ratio).
    profitable.sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then_with(|| compare_cycle_execution(&a.2, &b.2))
    });
    let mut seen = rustc_hash::FxHashSet::<CycleEdges>::default();
    let mut kept: Vec<(u64, Arc<FoundCycle>)> = profitable
        .into_iter()
        .filter(|(_, _, cycle)| seen.insert(cycle.edges.clone()))
        .take(max_keep)
        .map(|(fp, _, cycle)| (fp, cycle))
        .collect();

    if kept.len() < max_keep {
        rescue.sort_by(|a, b| compare_cycle_execution(&a.1, &b.1));
        let remaining = max_keep - kept.len();
        kept.extend(
            rescue
                .into_iter()
                .filter(|(_, cycle)| seen.insert(cycle.edges.clone()))
                .take(rescue_cap.min(remaining)),
        );
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
    charged_priority_fee_per_gas: U256,
    slippage_bps: u64,
    flash_policy: FlashLoanPolicy,
    gas_oracle: &GasOracle,
    route_gas: &RouteGasLookup,
    flash: &crate::services::execution::flash_liquidity::FlashLiquiditySnapshot,
    flash_ttl: Duration,
    safety_multiplier_bps: u64,
    profit_priority_alpha_bps: u64,
    execution: &ExecutionService,
    pool_metas: &[crate::pipeline::types::PoolMeta],
    route_sim_base_revision: u64,
) -> ProbeRankPartial {
    let mut out = ProbeRankPartial::default();
    let rotated = prefer_aave_flash_start(cycle_arc, arena, flash, flash_ttl, token_to_matic_rates);
    // Aave start-rotation must not admit hop-broken edges into InvalidRoute.
    let cycle = if local_sim::first_hop_continuity_break_in_arena(arena, &rotated.edges).is_some()
        && local_sim::first_hop_continuity_break_in_arena(arena, &cycle_arc.edges).is_none()
    {
        std::borrow::Cow::Borrowed(cycle_arc.as_ref())
    } else {
        rotated
    };
    let cycle = match cycle {
        std::borrow::Cow::Borrowed(_) => Arc::clone(cycle_arc),
        std::borrow::Cow::Owned(cycle) => Arc::new(cycle),
    };
    let fp = hash_cycle_edges(&cycle.edges);
    let route_state_revision =
        arena.route_state_revision_with_base(&cycle.edges, route_sim_base_revision);
    if !route_fits_executor(&cycle.edges) {
        out.skip.executor_budget = 1;
        return out;
    }
    // Rotation-aware: underwater cools all start-rotations; single-fp miss let
    // Aave-rotated starts refill probe_kept (live iter13: assess quarantine ×224).
    if execution.cycle_edges_quarantined(&cycle.edges) {
        return out;
    }
    // FoT / TransferFailed cool-down — any hop, not only start (live: Wrapped SOL
    // mid-hop TransferFailed still ranked while start was WMATIC).
    if execution.cycle_has_quarantined_token(arena, &cycle.edges) {
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
    // Tickless CL hops may still rank at spot-probe size (see simulate_hop); do not
    // hard-skip the whole cycle — that forced fake tick seeding and phantom Brent depth.
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
    // Micro ladder floor — skip dead V2 pools before burning the full probe ladder.
    let micro_probe = if start_decimals >= 6 {
        crate::util::ten_pow_u256_cached(start_decimals - 6)
    } else {
        U256::from(1u64)
    };
    if let Some(hop) = first_v2_hop_below_reserve(arena, &cycle.edges, micro_probe) {
        out.minimal_sim_reasons
            .record(MinimalSimFailure::V2ReserveExhausted { hop });
        MinimalProbeReject::NoSimulation.record(&mut out.skip);
        return out;
    }
    // V2-edge/V3-state (and kin) never simulate — skip before the probe ladder.
    if let Some((hop, expected, actual)) = first_protocol_state_mismatch(arena, &cycle.edges) {
        out.minimal_sim_reasons
            .record(MinimalSimFailure::UnsupportedState {
                hop,
                expected,
                actual,
            });
        MinimalProbeReject::NoSimulation.record(&mut out.skip);
        return out;
    }
    let rank_cache = Some((execution.route_sim_cache.as_ref(), route_state_revision, fp));
    let (probe_amount, probe) = match try_rank_probe_minimal(
        arena,
        &cycle,
        start_decimals,
        rate,
        rank_cache,
    ) {
        Ok(probe) => probe,
        Err(reject) => {
            let mut attempt_failures: Vec<Option<local_sim::MinimalSimFailure>> = Vec::new();
            if matches!(reject, MinimalProbeReject::NoSimulation) {
                for_each_rank_probe_amount(start_decimals, rate, |amount| {
                    attempt_failures.push(local_sim::minimal_sim_failure(
                        arena,
                        &cycle.edges,
                        amount,
                    ));
                });
                if let Some(reason) = attempt_failures.iter().flatten().next().copied() {
                    out.minimal_sim_reasons.record(reason);
                }
            }
            // Keep sample short — long rate/route strings truncated to `sample=fp=0`.
            if out.flash_diag.is_none()
                && matches!(
                    reject,
                    MinimalProbeReject::ZeroProfit | MinimalProbeReject::NoSimulation
                )
            {
                let protos: String = cycle
                    .edges
                    .iter()
                    .map(|e| format!("{:?}", e.protocol))
                    .collect::<Vec<_>>()
                    .join(">");
                out.flash_diag = Some(format!(
                    "fp={fp:#x} hops={} rej={reject:?} protos={protos} ratio={} score={:.3} start={} dec={start_decimals}",
                    cycle.edges.len(),
                    cycle.cycle_ratio,
                    cycle.score,
                    cycle.start_token.0,
                ));
            }
            // Spot-positive / walk-zero phantoms refill every tick (live: zero_profit
            // emptied probe_kept). Cool all start-rotations so select skips them.
            // ClTickless-only NoSimulation: arm hop-level probe-miss then always
            // stale-cool the route. Waiting on hydrate-exhausted was a chicken-egg
            // (live iter19: same fp re-probed every ~2s; hops never entered cooldown
            // after hydrate gap / pool_cap truncation → exhausted stayed false).
            let cool_phantoms = match reject {
                MinimalProbeReject::ZeroProfit => true,
                MinimalProbeReject::NoSimulation => {
                    let fails: Vec<_> = attempt_failures.iter().flatten().copied().collect();
                    if fails.is_empty() {
                        false
                    } else if fails.iter().all(|f| {
                        matches!(
                            f,
                            local_sim::MinimalSimFailure::ClTickless { .. }
                                | local_sim::MinimalSimFailure::ShallowCl { .. }
                                | local_sim::MinimalSimFailure::ClSynthetic { .. }
                        )
                    }) {
                        // ClTickless: arm hop miss for drain. ShallowCl: longer cool
                        // (live iter20/21: 30s stale let fp 0x21f58… recycle ×7).
                        if fails
                            .iter()
                            .any(|f| matches!(f, local_sim::MinimalSimFailure::ClTickless { .. }))
                        {
                            crate::orchestrator::hf_execute::mark_cycle_tickless_cl_probe_miss(
                                arena, &cycle, pool_metas,
                            );
                        }
                        true
                    } else {
                        true
                    }
                }
                MinimalProbeReject::SanityReject(_) => false,
            };
            if cool_phantoms {
                let n = cycle.edges.len();
                if n > 0 {
                    // ShallowCl-only: ticks exist but never walk — 30s stale recycled
                    // the same fp every cooldown window (iter21: 0x21f58… ×7 / ~5min).
                    let nosim_fails: Vec<_> = attempt_failures.iter().flatten().copied().collect();
                    let shallow_only = matches!(reject, MinimalProbeReject::NoSimulation)
                        && !nosim_fails.is_empty()
                        && nosim_fails.iter().all(|f| {
                            matches!(
                                f,
                                local_sim::MinimalSimFailure::ShallowCl { .. }
                                    | local_sim::MinimalSimFailure::ClSynthetic { .. }
                            )
                        });
                    ExecutionService::for_each_edge_rotation(&cycle.edges, |rotated| {
                        if shallow_only {
                            execution.quarantine_probe_below_dispatch_floor(rotated);
                        } else {
                            execution.quarantine_stale_route(rotated);
                        }
                    });
                }
            }
            if should_rescue_probe_reject(reject, &attempt_failures) {
                out.rescue.push((fp, Arc::clone(&cycle)));
            } else {
                if let MinimalProbeReject::SanityReject(reason) = &reject {
                    out.minimal_sim_reasons.record_sanity(*reason);
                }
                reject.record(&mut out.skip);
            }
            return out;
        }
    };
    let Some(flash_source) =
        resolve_flash_source_with_context(&flash_ctx, flash_policy, probe_amount)
    else {
        out.skip.flash_source = 1;
        let reason = flash_reject_reason(&flash_ctx, flash_policy, probe_amount);
        if let Some(reason) = reason {
            out.flash_loan.record_reject(reason);
            // ponytail: do not quarantine ColdCache — flash prefetch/background
            // warms hubs within a tick; ROUTE_COOLDOWN emptied ranks for 30s on
            // cold-start (live: skip_flash_source=12 then sticky cools).
            // Proven ZeroLiquidity (fresh cache, no borrowable cash, rotation
            // already tried) — cool all start-rotations so select stops
            // re-feeding the same dead start (live iter22: fresh=true bal=0).
            if matches!(reason, FlashRejectReason::ZeroLiquidity) && flash_ctx.start_fresh {
                let n = cycle.edges.len();
                if n > 0 {
                    ExecutionService::for_each_edge_rotation(&cycle.edges, |rotated| {
                        execution.quarantine_stale_route(rotated);
                    });
                }
            }
        }
        if out.flash_diag.is_none() {
            out.flash_diag = Some(format!(
                "fp={fp:#x} hops={} flash_source_reject reason={reason:?} start={} probe_amt={probe_amount} fresh={} bal={} aave={} listed={} forbid_bal={}",
                cycle.edges.len(),
                flash_ctx.start_addr,
                flash_ctx.start_fresh,
                flash_ctx.liquidity.balancer,
                flash_ctx.liquidity.aave,
                flash_ctx.liquidity.aave_listed,
                flash_ctx.forbid_balancer_flash,
            ));
        }
        return out;
    };

    let depth_bps =
        depth_impact_slippage_bps_with_base(arena, &cycle.edges, probe_amount, Some(&probe));
    // 10000 = zero/unknown base profit only (+5% miss is 5000 now).
    if depth_bps >= 10_000 {
        out.skip.net = 1;
        return out;
    }
    let effective_slip =
        effective_slippage_bps_for_flash(slippage_bps, cycle.edge_hops(), depth_bps, flash_source);

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
    ctx.hop_count = cycle.edge_hops();
    ctx.profit_priority_alpha_bps = profit_priority_alpha_bps;
    ctx.charged_priority_fee_per_gas = charged_priority_fee_per_gas;
    let mut ranked_probe = probe;
    // Direct batch seeds are live-calibrated all-in; do not apply mixed-route sim_scale.
    ranked_probe.total_gas = assessment_gas_units(
        probe.total_gas,
        &assessment_gas_for_edges(&cycle.edges, Some(route_gas), gas_oracle),
    );
    let net_matic = net_profit_matic_from_sim(&ranked_probe, probe_amount, &ctx);
    out.seeds.insert(cycle.edges.clone(), (probe_amount, probe));
    if net_matic.is_zero() {
        let hop_count = cycle.edges.len();
        if !probe.profit.is_zero()
            && cycle_simulatable(arena, &cycle, token_decimals, token_to_matic_rates)
        {
            // Prefer absolute MATIC cover over brent shortfall so low-gas dust
            // (tiny gross, cheap gas) does not crowd out larger-edge near-misses.
            let cover_matic = cover_matic_from_sim(&ranked_probe, probe_amount, &ctx);
            let economic_floor = min_economic_amount_in(start_decimals, rate);
            // Tickless/micro phantoms only "profit" below the economic floor; Brent
            // then pins at floor with zero sim profit (live: same fps ×10k/hour).
            // Economic-sized underwater edges still go to near_net so Brent can
            // size up (deep pool, thin probe ROI is not a dust reject).
            // Zero MATIC cover at economic size: Brent cannot grow an edge (live
            // iter7: after chronic uq of cover~500–2271, cover_matic=0 monopolized
            // near_net → skip_net=1 / peak_avail=0). Stale-cool rotations (30s) —
            // not chronic uq (MIN_COVER_BPS=25 guards that cascade).
            if probe_amount < economic_floor || cover_matic.is_zero() {
                let n = cycle.edges.len();
                if n > 0 {
                    ExecutionService::for_each_edge_rotation(&cycle.edges, |rotated| {
                        execution.quarantine_stale_route(rotated);
                    });
                }
            } else {
                out.near_net.push((fp, cover_matic, Arc::clone(&cycle)));
            }
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
    out.profitable.push((fp, net_matic, cycle));
    out
}

type RankedProbeSeeds = (Vec<(u64, Arc<FoundCycle>)>, ProbeSeedMap);

#[allow(clippy::too_many_arguments)]
pub fn rank_cycles_by_probe_net(
    arena: &StateArena,
    cycles: Vec<Arc<FoundCycle>>,
    token_to_matic_rates: &FxHashMap<TokenIndex, U256>,
    token_decimals: &FxHashMap<Address, u8>,
    gas_price: U256,
    charged_priority_fee_per_gas: U256,
    slippage_bps: u64,
    flash_policy: FlashLoanPolicy,
    max_keep: usize,
    gas_oracle: &GasOracle,
    route_gas: &RouteGasLookup,
    flash_liquidity: &FlashLiquidityCache,
    safety_multiplier_bps: u64,
    profit_priority_alpha_bps: u64,
    execution: &ExecutionService,
    pool_metas: &[crate::pipeline::types::PoolMeta],
    route_sim_base_revision: u64,
) -> RankedProbeSeeds {
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
                    charged_priority_fee_per_gas,
                    slippage_bps,
                    flash_policy,
                    gas_oracle,
                    route_gas,
                    &flash,
                    flash_ttl,
                    safety_multiplier_bps,
                    profit_priority_alpha_bps,
                    execution,
                    pool_metas,
                    route_sim_base_revision,
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
                    charged_priority_fee_per_gas,
                    slippage_bps,
                    flash_policy,
                    gas_oracle,
                    route_gas,
                    &flash,
                    flash_ttl,
                    safety_multiplier_bps,
                    profit_priority_alpha_bps,
                    execution,
                    pool_metas,
                    route_sim_base_revision,
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
    // Drop cycles cooled by a sibling during parallel rank (rotation-aware).
    let profitable_ranked: Vec<_> = profitable_ranked
        .into_iter()
        .filter(|(_, _, cycle)| !execution.cycle_edges_quarantined(&cycle.edges))
        .collect();
    let had_net_ranked = !profitable_ranked.is_empty();
    // Fill order: profitable → near_net (simulatable underwater) → rescue → score fallback.
    // Rescue must not run before near_net: when profitable is empty, admitting rescue_cap
    // first crowded out cover-ranked near-misses and produced probe_kept>0 with evaluated=0.
    let mut kept = select_probe_survivors(profitable_ranked, Vec::new(), max_keep, 0);
    let mut seen: rustc_hash::FxHashSet<CycleEdges> =
        kept.iter().map(|(_, cycle)| cycle.edges.clone()).collect();
    let near_net_count = near_net.len();
    if kept.len() < max_keep && !scanned.is_empty() {
        if near_net_count > 0 {
            near_net.sort_by(|a, b| {
                b.1.cmp(&a.1)
                    .then_with(|| compare_cycle_execution(&a.2, &b.2))
            });
            // When nothing clears gas yet, only a few best near-misses deserve Brent.
            // Filling max_keep with underwater dust starved real stream/active routes
            // (live: probe_kept=15 skip_net=12 near_net=12 had_net=false every tick).
            let near_net_slots = if had_net_ranked {
                max_keep
            } else {
                max_keep.clamp(1, 4)
            };
            let mut near_net_kept = 0usize;
            for (fp, _, cycle) in near_net.drain(..) {
                if kept.len() >= max_keep || near_net_kept >= near_net_slots {
                    break;
                }
                // Re-check after parallel rank (rotation + FoT token cool).
                if execution.cycle_edges_quarantined(&cycle.edges)
                    || execution.cycle_has_quarantined_token(arena, &cycle.edges)
                {
                    continue;
                }
                if seen.insert(cycle.edges.clone())
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
                    near_net_kept += 1;
                }
            }
        }
        if kept.len() < max_keep && !rescue.is_empty() {
            rescue.sort_by(|a, b| compare_cycle_execution(&a.1, &b.1));
            let remaining = max_keep - kept.len();
            // Spot-negative rescue must not flood Brent when near_net already holds
            // real underwater edges (live iter10: near_net_slots=4 then rescue≈30 →
            // probe_kept≈32, sticky DODO monopolized best-eval after chronic-uq).
            let rescue_take = if had_net_ranked {
                rescue_cap.min(remaining)
            } else if near_net_count > 0 {
                0
            } else {
                rescue_cap.min(remaining).min(4)
            };
            for (fp, cycle) in rescue.into_iter().take(rescue_take) {
                if execution.cycle_edges_quarantined(&cycle.edges) {
                    continue;
                }
                if seen.insert(cycle.edges.clone()) {
                    kept.push((fp, cycle));
                }
            }
        }
        if kept.len() < max_keep {
            // Without any gas-clearing probe, cap total admissions so score-fallback
            // cannot re-flood Brent with the same underwater dust (live: kept=34 of
            // 35 with near_net_slots=4 then fallback filled the rest).
            // When near_net is also empty, score-fallback used to force total_cap≥1
            // (live iter16: kept=1 near_net=0 → assess quarantine / evaluated=0).
            let total_cap = if had_net_ranked {
                max_keep
            } else if near_net_count > 0 {
                max_keep.min(6).max(kept.len().max(1))
            } else {
                kept.len()
            };
            let fallback = simulatable_score_fallback(
                &scanned,
                arena,
                token_decimals,
                token_to_matic_rates,
                flash_liquidity,
                flash_policy,
                total_cap,
            );
            for cycle in fallback {
                if kept.len() >= total_cap {
                    break;
                }
                let cycle = Arc::new(cycle);
                let fp = hash_cycle_edges(&cycle.edges);
                let route_edges = cycle.edges.clone();
                if execution.cycle_edges_quarantined(&cycle.edges)
                    || execution.cycle_has_quarantined_token(arena, &cycle.edges)
                    || !seen.insert(route_edges.clone())
                {
                    continue;
                }
                // Score-fallback used to admit seedless cycles → assess all opt_none
                // (probe_kept>0 evaluated=0). Only keep when a non-zero minimal seed exists.
                let start_decimals =
                    resolve_token_decimals_for_index(cycle.start_token, arena, token_decimals);
                let rate = resolve_token_to_matic_rate(cycle.start_token, token_to_matic_rates);
                match try_rank_probe_minimal(arena, &cycle, start_decimals, rate, None) {
                    Ok((amount, sim)) if !sim.profit.is_zero() => {
                        let economic_floor = min_economic_amount_in(start_decimals, rate);
                        // Same dust reject as near_net — below-floor seeds only feed
                        // AmountBelowEconomicFloor / zero-at-floor Brent loops.
                        if amount < economic_floor {
                            seen.remove(&route_edges);
                            continue;
                        }
                        probe_seeds.insert(route_edges, (amount, sim));
                        kept.push((fp, cycle));
                    }
                    _ => {
                        seen.remove(&route_edges);
                    }
                }
            }
        }
        // Rate-limited INFO when probe drops cycles (live: kept=4 of selected=6–13).
        if kept.len() < scanned.len() {
            static LAST_PROBE_BACKFILL_LOG_MS: std::sync::atomic::AtomicU64 =
                std::sync::atomic::AtomicU64::new(0);
            let now = crate::util::now_ms();
            let prev = LAST_PROBE_BACKFILL_LOG_MS.load(std::sync::atomic::Ordering::Relaxed);
            if now.saturating_sub(prev) >= 2_000
                && LAST_PROBE_BACKFILL_LOG_MS
                    .compare_exchange(
                        prev,
                        now,
                        std::sync::atomic::Ordering::Relaxed,
                        std::sync::atomic::Ordering::Relaxed,
                    )
                    .is_ok()
            {
                crate::info!(
                    "probe rank backfill: kept={} scanned={} skip_rate={} skip_probe={} minimal_sim={} (no_sim={} zero_profit={} sanity={}) reasons(invalid={} missing={} non_tradable={} cl_tickless={} cl_cap={} shallow_cl={} v2={} mismatch={} math={} unsupported={} bal_max_in={} zero_out={} sanity(ratio={} matic={} floor={} dec={} pin={})) skip_net={} near_net={near_net_count} rescue={rescue_len} had_net={had_net_ranked}",
                    kept.len(),
                    scanned.len(),
                    skip.rate,
                    skip.probe(),
                    skip.minimal_sim(),
                    skip.minimal_no_sim,
                    skip.minimal_zero_profit,
                    skip.minimal_sanity,
                    minimal_sim_reasons.invalid_route,
                    minimal_sim_reasons.missing_pool,
                    minimal_sim_reasons.non_tradable,
                    minimal_sim_reasons.cl_tickless,
                    minimal_sim_reasons.cl_cap,
                    minimal_sim_reasons.shallow_cl,
                    minimal_sim_reasons.v2_reserve_exhausted,
                    minimal_sim_reasons.token_mismatch,
                    minimal_sim_reasons.math,
                    minimal_sim_reasons.unsupported_state,
                    minimal_sim_reasons.bal_max_in,
                    minimal_sim_reasons.zero_output,
                    minimal_sim_reasons.sanity_ratio,
                    minimal_sim_reasons.sanity_matic,
                    minimal_sim_reasons.sanity_floor,
                    minimal_sim_reasons.sanity_decimals,
                    minimal_sim_reasons.sanity_pin,
                    skip.net,
                );
            }
        }
    }
    probe_seeds.retain(|fingerprint, _| seen.contains(fingerprint));

    if kept.is_empty() && !scanned.is_empty() {
        let sample = flash_diag.as_deref().unwrap_or("none");
        // Empty ranks used to be debug-only; rate-limit INFO so probe_kept=0 ticks are visible.
        static LAST_EMPTY_PROBE_RANK_LOG_MS: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(0);
        let now = crate::util::now_ms();
        let prev = LAST_EMPTY_PROBE_RANK_LOG_MS.load(std::sync::atomic::Ordering::Relaxed);
        if now.saturating_sub(prev) >= 2_000
            && LAST_EMPTY_PROBE_RANK_LOG_MS
                .compare_exchange(
                    prev,
                    now,
                    std::sync::atomic::Ordering::Relaxed,
                    std::sync::atomic::Ordering::Relaxed,
                )
                .is_ok()
        {
            crate::info!(
                "probe rank empty: scanned={} skip_rate={} skip_flash={} skip_flash_source={} skip_probe={} minimal_sim={} (no_sim={} zero_profit={} sanity={}) reasons(invalid={} missing={} non_tradable={} cl_tickless={} cl_cap={} shallow_cl={} v2={} mismatch={} math={} unsupported={} bal_max_in={} zero_out={} sanity(ratio={} matic={} floor={} dec={} pin={})) skip_net={} rescue={rescue_len} sample={sample}",
                scanned.len(),
                skip.rate,
                skip.flash,
                skip.flash_source,
                skip.probe(),
                skip.minimal_sim(),
                skip.minimal_no_sim,
                skip.minimal_zero_profit,
                skip.minimal_sanity,
                minimal_sim_reasons.invalid_route,
                minimal_sim_reasons.missing_pool,
                minimal_sim_reasons.non_tradable,
                minimal_sim_reasons.cl_tickless,
                minimal_sim_reasons.cl_cap,
                minimal_sim_reasons.shallow_cl,
                minimal_sim_reasons.v2_reserve_exhausted,
                minimal_sim_reasons.token_mismatch,
                minimal_sim_reasons.math,
                minimal_sim_reasons.unsupported_state,
                minimal_sim_reasons.bal_max_in,
                minimal_sim_reasons.zero_output,
                minimal_sim_reasons.sanity_ratio,
                minimal_sim_reasons.sanity_matic,
                minimal_sim_reasons.sanity_floor,
                minimal_sim_reasons.sanity_decimals,
                minimal_sim_reasons.sanity_pin,
                skip.net,
            );
        } else {
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
        }
    } else if kept.len() * 4 < scanned.len() {
        // HF may rank hundreds of times per second; keep the signal without
        // flooding INFO (was ~70% of orchestrator log volume in live runs).
        static LAST_PROBE_RANK_LOG_MS: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(0);
        let now = crate::util::now_ms();
        let prev = LAST_PROBE_RANK_LOG_MS.load(std::sync::atomic::Ordering::Relaxed);
        if now.saturating_sub(prev) >= 2_000
            && LAST_PROBE_RANK_LOG_MS
                .compare_exchange(
                    prev,
                    now,
                    std::sync::atomic::Ordering::Relaxed,
                    std::sync::atomic::Ordering::Relaxed,
                )
                .is_ok()
        {
            crate::info!(
                "route probe rank: kept={} scanned={} skip_rate={} skip_flash={} skip_probe={} minimal_sim={} (no_sim={} zero_profit={} sanity={}) reasons(invalid={} missing={} non_tradable={} cl_tickless={} cl_cap={} shallow_cl={} v2={} mismatch={} math={} unsupported={} bal_max_in={} zero_out={} sanity(ratio={} matic={} floor={} dec={} pin={})) skip_net={} near_net={}",
                kept.len(),
                scanned.len(),
                skip.rate,
                skip.flash,
                skip.probe(),
                skip.minimal_sim(),
                skip.minimal_no_sim,
                skip.minimal_zero_profit,
                skip.minimal_sanity,
                minimal_sim_reasons.invalid_route,
                minimal_sim_reasons.missing_pool,
                minimal_sim_reasons.non_tradable,
                minimal_sim_reasons.cl_tickless,
                minimal_sim_reasons.cl_cap,
                minimal_sim_reasons.shallow_cl,
                minimal_sim_reasons.v2_reserve_exhausted,
                minimal_sim_reasons.token_mismatch,
                minimal_sim_reasons.math,
                minimal_sim_reasons.unsupported_state,
                minimal_sim_reasons.bal_max_in,
                minimal_sim_reasons.zero_output,
                minimal_sim_reasons.sanity_ratio,
                minimal_sim_reasons.sanity_matic,
                minimal_sim_reasons.sanity_floor,
                minimal_sim_reasons.sanity_decimals,
                minimal_sim_reasons.sanity_pin,
                skip.net,
                near_net_count,
            );
        }
    }
    flash_loan.cache_generation = flash.generation();
    flash_loan.log_summary("probe_rank");

    (kept, probe_seeds)
}

#[derive(Default)]
struct EvalFailStats {
    quarantine: AtomicU32,
    executor_budget: AtomicU32,
    flash: AtomicU32,
    flash_source: AtomicU32,
    missing_decimals: AtomicU32,
    opt_none: AtomicU32,
    detailed_none: AtomicU32,
    fallback_none: AtomicU32,
    probe_sim_none: AtomicU32,
    probe_zero_profit: AtomicU32,
    probe_fidelity: AtomicU32,
    probe_sanity: AtomicU32,
    /// depth_impact returned ≥10000 (unknown / collapsed +5% probe).
    depth_unknown: AtomicU32,
}

impl EvalFailStats {
    fn log_assess_summary(&self, in_count: usize, out_count: usize) {
        if in_count == 0 {
            return;
        }
        // INFO when every assessed route dies — otherwise DEBUG (hf tick already INFO).
        let msg = format!(
            "route assess: in={in_count} ok={out_count} quarantine={} executor_budget={} flash={} flash_source={} missing_decimals={} opt_none={} detailed_none={} fallback_none={} depth_unknown={} probe_fail(sim_none={} zero={} fidelity={} sanity={})",
            load(&self.quarantine),
            load(&self.executor_budget),
            load(&self.flash),
            load(&self.flash_source),
            load(&self.missing_decimals),
            load(&self.opt_none),
            load(&self.detailed_none),
            load(&self.fallback_none),
            load(&self.depth_unknown),
            load(&self.probe_sim_none),
            load(&self.probe_zero_profit),
            load(&self.probe_fidelity),
            load(&self.probe_sanity),
        );
        if out_count == 0 {
            crate::info!("{msg}");
        } else {
            crate::debug!("{msg}");
        }
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
    cycles: &[(u64, Arc<FoundCycle>)],
    input: &HfEvalInput<'_>,
    probe_seeds: &ProbeSeedMap,
) -> Vec<HfEvalResult> {
    let stats = EvalFailStats::default();
    let in_count = cycles.len();
    let results: Vec<HfEvalResult> = if crate::util::should_use_rayon(cycles.len()) {
        cycles
            .par_iter()
            .filter_map(|(fp, cycle)| evaluate_one(*fp, cycle, input, probe_seeds, &stats))
            .collect()
    } else {
        cycles
            .iter()
            .filter_map(|(fp, cycle)| evaluate_one(*fp, cycle, input, probe_seeds, &stats))
            .collect()
    };
    if in_count > 0 && (results.len() < in_count || results.is_empty()) {
        stats.log_assess_summary(in_count, results.len());
        crate::pipeline::curve_sim::log_curve_sim_summary();
    }
    // Always emit when Brent/ternary work ran — full assess success used to skip
    // these and hide shallow/cl_depth_clamp (live cldepth capture had attempts>0
    // but no `brent:` line).
    crate::pipeline::brent_diag::log_brent_summary();
    results
}

pub async fn rescore_rank_and_evaluate_async(
    mut cycles: Vec<Arc<FoundCycle>>,
    input: Arc<HfEvalInputOwned>,
    sim_cap: usize,
) -> anyhow::Result<(Vec<HfEvalResult>, Arc<StateArena>, usize)> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    // spawn alone binds pool TLS for nested par_iter (no nested install).
    crate::util::cpu_pool().spawn(move || {
        let rates = input.token_to_matic_rates.as_ref();
        let decimals = input.token_decimals.as_ref();
        let probe_window = probe_rank_window(sim_cap, cycles.len());
        let spot_table = crate::pipeline::spot_price::SpotTable::new(input.arena.pool_count());
        if probe_window > 0 {
            crate::pipeline::spot_price::rescore_arc_cycles_with_table_and_gas(
                &input.arena,
                &spot_table,
                &mut cycles[..probe_window],
                Some(input.gas_price),
                Some(rates),
                Some(decimals),
                None,
            );
            // Gas-aware score primary among profitable (not raw ratio).
            cycles[..probe_window].sort_by(|a, b| compare_cycle_execution(a.as_ref(), b.as_ref()));
        }
        let route_gas = RouteGasLookup::for_routes(
            &input.gas_oracle,
            cycles
                .iter()
                .take(probe_window)
                .map(|c| c.as_ref().edges.as_slice()),
        );
        let (cycles, probe_seeds) = rank_cycles_by_probe_net(
            &input.arena,
            cycles,
            rates,
            decimals,
            input.gas_price,
            input.charged_priority_fee_per_gas,
            input.slippage_bps,
            input.flash_policy,
            sim_cap,
            &input.gas_oracle,
            &route_gas,
            input.flash_liquidity.as_ref(),
            input.safety_multiplier_bps,
            input.profit_priority_alpha_bps,
            input.execution.as_ref(),
            input.pool_metas.as_ref(),
            input.route_sim_base_revision,
        );
        // Drop cooled routes/tokens and claim exclusive assess (live iter15: concurrent
        // HF ticks → assess_q 693; iter16: try_claim passed but evaluate_one still hit
        // quarantine=1 via FoT token cool — same counter, not checked at claim).
        let cycles: Vec<_> = cycles
            .into_iter()
            .filter(|(_, cycle)| {
                !input
                    .execution
                    .cycle_has_quarantined_token(&input.arena, &cycle.edges)
                    && input.execution.try_claim_route_assess(&cycle.edges)
            })
            .collect();
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
        let result = (eval_results, Arc::clone(&input.arena), probe_kept);
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
) -> Vec<U256> {
    let flash_cap = flash_cap_for_cycle(input, cycle);
    let dec = flash_cap.token_decimals;
    let rate = flash_cap.token_to_matic_rate;
    let mut amounts = Vec::with_capacity(5);
    let push = |amounts: &mut Vec<U256>, candidate: U256| {
        if candidate.is_zero() || amounts.contains(&candidate) {
            return;
        }
        if !flash_cap.amount_within_cap(candidate) {
            return;
        }
        amounts.push(candidate);
    };
    if let Some((seed, _)) = probe_seed {
        push(&mut amounts, seed);
    }
    for_each_rank_probe_amount(dec, rate, |candidate| push(&mut amounts, candidate));
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
    let flash = input.flash_liquidity.load();
    // Fail closed: inventing Balancer (0 fee) under-costs Aave routes and mis-models Direct.
    let flash_source = resolve_flash_source_for_cycle(
        cycle,
        input.arena,
        &flash,
        input.flash_ttl,
        input.flash_policy,
        min_economic_amount_in(decimals, rate),
    )?;
    let fallback_depth = probe_seed
        .as_ref()
        .map(|(amount, sim)| {
            depth_impact_slippage_bps_with_base(input.arena, &cycle.edges, *amount, Some(sim))
        })
        .unwrap_or(0);
    let mut profit_ctx = ProfitEvalContext::with_safety_multiplier(
        cycle.start_token,
        input.arena,
        input.token_to_matic_rates,
        input.token_decimals,
        input.gas_price,
        effective_slippage_bps_for_flash(
            input.slippage_bps,
            cycle.edge_hops(),
            fallback_depth,
            flash_source,
        ),
        flash_source,
        input.safety_multiplier_bps,
    );
    profit_ctx.gas_scale_bps = 10_000;
    profit_ctx.hop_count = cycle.edge_hops();
    profit_ctx.profit_priority_alpha_bps = input.profit_priority_alpha_bps;
    profit_ctx.charged_priority_fee_per_gas = input.charged_priority_fee_per_gas;
    let (mut psn, mut pzp, mut pf, mut ps) = (0u32, 0u32, 0u32, 0u32);
    let mut best: Option<(OptimizationResult, RouteSimulationResult, U256)> = None;
    let route_shallow_caps = precompute_route_shallow_caps(input.arena, &cycle.edges);
    // Sanity rejects amount < economic floor unless in the tickless probe band
    // [spot_probe, floor). Micro (10^(dec-6)) is always below that for 18-dec tokens
    // — sim-then-reject burned ~35k DEBUG lines / HF eval CPU per run.
    let economic_floor = min_economic_amount_in(decimals, rate);
    let tickless_probe = crate::pipeline::spot_price::spot_probe_for_decimals(decimals);
    for amount in probe_fallback_amounts(cycle, input, probe_seed) {
        if amount.is_zero() {
            continue;
        }
        if amount < economic_floor && amount < tickless_probe {
            ps += 1;
            continue;
        }
        let seed_backed = probe_seed
            .as_ref()
            .is_some_and(|(seed_amt, seed_sim)| *seed_amt == amount && !seed_sim.profit.is_zero());
        let Some(sim) = simulate_route_detailed_with_caps(
            input.arena,
            &cycle.edges,
            amount,
            route_shallow_caps.as_ref(),
        )
        .or_else(|| {
            if seed_backed {
                simulate_route_detailed(input.arena, &cycle.edges, amount)
            } else {
                None
            }
        }) else {
            psn += 1;
            continue;
        };
        if sim.profit.is_zero() {
            pzp += 1;
            continue;
        }
        if !local_sim::route_hop_fidelity_ok_after_walk(input.arena, &cycle.edges, &sim.hop_amounts)
            && !seed_backed
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
                cycle.edge_hops(),
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
        let minimal = MinimalSimResult {
            profit: sim.profit,
            amount_out: sim.amount_out,
            total_gas: sim.total_gas,
        };
        let score = brent_score_matic_from_sim(&minimal, amount, &profit_ctx);
        let candidate = (
            OptimizationResult {
                optimal_input: amount,
                expected_gross: sim.amount_out,
                net_profit: sim.profit,
                total_gas: sim.total_gas,
                search_low: U256::ZERO,
            },
            sim,
            score,
        );
        let replace = best
            .as_ref()
            .is_none_or(|(_, _, best_score)| score > *best_score);
        if replace {
            best = Some(candidate);
        }
    }
    let s = stats;
    add(&s.probe_sim_none, psn);
    add(&s.probe_zero_profit, pzp);
    add(&s.probe_fidelity, pf);
    add(&s.probe_sanity, ps);
    best.map(|(opt, sim, _)| (opt, sim))
}

/// Economic + spot probe sims for Brent warm-start (deduped, sanity-filtered).
fn build_brent_probe_seeds(
    arena: &StateArena,
    cycle: &FoundCycle,
    start_decimals: u8,
    start_rate: U256,
    probe_seed: Option<(U256, MinimalSimResult)>,
    route_sim_cache: Option<(&crate::pipeline::route_sim_cache::RouteSimCache, u64, u64)>,
) -> Vec<(U256, MinimalSimResult)> {
    let mut seeds = Vec::with_capacity(2);
    if let Some(pair) = probe_seed {
        seeds.push(pair);
    }
    let shallow_caps = precompute_route_shallow_caps(arena, &cycle.edges);
    for_each_rank_probe_amount(start_decimals, start_rate, |amount| {
        if seeds.len() >= BRENT_SEED_CACHE_SLOTS {
            return;
        }
        if seeds.iter().any(|(a, _)| *a == amount) {
            return;
        }
        let sim = route_sim_cache
            .and_then(|(cache, revision, fp)| cache.get(revision, fp, &cycle.edges, amount))
            .or_else(|| {
                let sim = simulate_route_minimal_with_caps(
                    arena,
                    &cycle.edges,
                    amount,
                    shallow_caps.as_ref(),
                )?;
                if let Some((cache, revision, fp)) = route_sim_cache {
                    cache.insert(revision, fp, &cycle.edges, amount, sim);
                }
                Some(sim)
            });
        let Some(sim) = sim else {
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
    fp: u64,
    cycle: &Arc<FoundCycle>,
    input: &HfEvalInput<'_>,
    probe_seeds: &ProbeSeedMap,
    stats: &EvalFailStats,
) -> Option<HfEvalResult> {
    // Cycles from rank_cycles_by_probe_net are already dispatch-ready (Aave start rotation).
    let route_state_revision = input
        .arena
        .route_state_revision_with_base(&cycle.edges, input.route_sim_base_revision);
    if !route_fits_executor(&cycle.edges) {
        inc(&stats.executor_budget);
        return None;
    }
    if input.execution.cycle_edges_quarantined(&cycle.edges) {
        inc(&stats.quarantine);
        return None;
    }
    if input
        .execution
        .cycle_has_quarantined_token(input.arena, &cycle.edges)
    {
        inc(&stats.quarantine);
        return None;
    }
    if !cycle_tokens_have_known_decimals(cycle, input.arena, input.token_decimals) {
        inc(&stats.missing_decimals);
        return None;
    }
    let flash = input.flash_liquidity.load();
    let flash_ttl = input.flash_ttl;
    if !balancer_route_flash_feasible(cycle, input.arena, &flash, flash_ttl) {
        inc(&stats.flash);
        return None;
    }
    let probe_seed = probe_seeds
        .get(&cycle.edges)
        .map(|(amount, sim)| (*amount, *sim));
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
    let hop_count = cycle.edge_hops();
    let brent_slippage = probe_seed
        .map(|(amount, sim)| {
            let depth =
                depth_impact_slippage_bps_with_base(input.arena, &cycle.edges, amount, Some(&sim));
            effective_slippage_bps_for_flash(
                input.slippage_bps,
                hop_count,
                depth,
                flash_source_brent,
            )
        })
        .unwrap_or_else(|| {
            effective_slippage_bps_for_flash(base_slippage, hop_count, 0, flash_source_brent)
        });
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
    profit_ctx.hop_count = cycle.edge_hops();
    profit_ctx.profit_priority_alpha_bps = input.profit_priority_alpha_bps;
    profit_ctx.charged_priority_fee_per_gas = input.charged_priority_fee_per_gas;
    let route_gas_costing = RouteGasCosting {
        lookup: input.route_gas,
        edges: &cycle.edges,
        calibrated_seed: crate::pipeline::route_calls::balancer_direct_batch_eligible(&cycle.edges)
            || crate::pipeline::route_calls::dodo_flash_batch_eligible(&cycle.edges),
    };
    let brent_seeds = build_brent_probe_seeds(
        input.arena,
        cycle,
        start_decimals,
        start_rate,
        probe_seed,
        Some((
            input.execution.route_sim_cache.as_ref(),
            route_state_revision,
            fp,
        )),
    );
    let brent_seed_slice = (!brent_seeds.is_empty()).then_some(brent_seeds.as_slice());
    // Shared CL shallow caps for post-Brent detailed walks.
    let route_shallow_caps = precompute_route_shallow_caps(input.arena, &cycle.edges);

    let max_flash_usd = input
        .execution
        .adaptive_flash_loan_usd(&cycle.edges, input.max_flash_loan_usd);
    let route_cache = Some((
        input.execution.route_sim_cache.as_ref(),
        route_state_revision,
        fp,
    ));
    let (mut opt, mut sim, probe_only) = match optimize_cycle(
        input.arena,
        cycle,
        input.token_to_matic_rates,
        input.token_decimals,
        Some(max_flash_usd),
        input.matic_usd,
        input.matic_usd_chainlink,
        Some(input.brent_iters),
        None,
        &profit_ctx,
        brent_seed_slice,
        Some(route_gas_costing),
        route_cache,
    ) {
        Some(opt) => {
            let Some(sim) = simulate_route_detailed_with_caps(
                input.arena,
                &cycle.edges,
                opt.optimal_input,
                route_shallow_caps.as_ref(),
            ) else {
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

    let Some(mut flash_source) =
        resolve_flash_source_with_context(&flash_ctx, input.flash_policy, opt.optimal_input)
    else {
        inc(&stats.flash_source);
        return None;
    };

    // Probe-size flash plan is often free Balancer; economic/optimal size often
    // needs Aave (5 bps). Brent under the wrong fee oversizes → prepare/dry-run miss.
    // One re-size under the true fee (+ provider liquidity hard-cap when known).
    let flash_fee_changed =
        flash_fee_changed_for_amount(flash_source, flash_source_brent, opt.optimal_input)?;
    if !probe_only && flash_fee_changed {
        profit_ctx.flash_source = flash_source;
        let liq_cap = match flash_source {
            FlashLoanSource::AaveV3 if flash_ctx.liquidity.aave_listed => {
                Some(flash_ctx.liquidity.aave).filter(|c| !c.is_zero())
            }
            FlashLoanSource::Balancer => {
                Some(flash_ctx.liquidity.balancer).filter(|c| !c.is_zero())
            }
            FlashLoanSource::Dodo => Some(flash_ctx.liquidity.dodo).filter(|c| !c.is_zero()),
            FlashLoanSource::Direct | FlashLoanSource::AaveV3 => None,
        };
        if let Some(new_opt) = optimize_cycle(
            input.arena,
            cycle,
            input.token_to_matic_rates,
            input.token_decimals,
            Some(max_flash_usd),
            input.matic_usd,
            input.matic_usd_chainlink,
            Some(input.brent_iters),
            liq_cap,
            &profit_ctx,
            brent_seed_slice,
            Some(route_gas_costing),
            route_cache,
        ) && let Some(new_sim) = simulate_route_detailed_with_caps(
            input.arena,
            &cycle.edges,
            new_opt.optimal_input,
            route_shallow_caps.as_ref(),
        ) && validate_optimized_sim(
            input,
            cycle,
            &new_sim,
            new_opt.optimal_input,
            new_opt.search_low,
        ) && let Some(src) =
            resolve_flash_source_with_context(&flash_ctx, input.flash_policy, new_opt.optimal_input)
        {
            crate::debug!(
                "evaluate_one flash-fee reopt: fp={fp:#x} {flash_source_brent:?}->{src:?} input {}->{}",
                opt.optimal_input,
                new_opt.optimal_input,
            );
            opt = new_opt;
            sim = new_sim;
            flash_source = src;
        }
    }

    let depth_bps = depth_impact_slippage_bps_with_base(
        input.arena,
        &cycle.edges,
        opt.optimal_input,
        Some(&MinimalSimResult {
            profit: sim.profit,
            amount_out: sim.amount_out,
            total_gas: sim.total_gas,
        }),
    );
    if depth_bps >= 10_000 {
        inc(&stats.depth_unknown);
        return None;
    }
    let slippage_bps =
        effective_slippage_bps_for_flash(input.slippage_bps, hop_count, depth_bps, flash_source);
    let mut assessment = assess_route_for_cycle(input, &sim, cycle, slippage_bps, flash_source)?;
    if probe_only
        && assessment.reject_reason.as_deref() == Some(DISPATCH_BELOW_ECONOMIC_FLOOR)
        && input
            .execution
            .quarantine_probe_below_dispatch_floor(&cycle.edges)
    {
        crate::info!(
            "hf probe-dispatch blocked (quarantined 300s): fp={fp} input={} floor={}",
            opt.optimal_input,
            dispatch_floor_for_cycle(input, cycle),
        );
    }
    if probe_only && !assessment.should_execute {
        assessment.reject_reason = assessment
            .reject_reason
            .or_else(|| Some("Brent did not converge; probe-only assessment".into()));
    } else if probe_only && assessment.should_execute {
        crate::info!(
            "hf probe-dispatch: fp={fp} input={} net_matic={} (Brent fallback, probe sizing validated)",
            opt.optimal_input,
            assessment.net_profit_after_gas_matic_wei,
        );
    }

    let adaptive_flash_cap_bound = flash_cap_for_cycle(input, cycle)
        .cap_wei()
        .is_some_and(|cap| opt.optimal_input == cap);

    Some(HfEvalResult {
        route_fingerprint: fp,
        cycle: Arc::clone(cycle),
        opt,
        sim,
        assessment,
        effective_slippage_bps: slippage_bps,
        flash_source,
        balancer_batch_verified: false,
        adaptive_flash_cap_bound,
    })
}

fn flash_fee_changed_for_amount(
    selected_source: FlashLoanSource,
    probe_source: FlashLoanSource,
    amount: U256,
) -> Option<bool> {
    Some(
        flash_loan_fee_amount(selected_source, amount)?
            != flash_loan_fee_amount(probe_source, amount)?,
    )
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
        result.effective_slippage_bps,
        flash_source,
    )
}

fn assess_route_for_cycle(
    input: &HfEvalInput<'_>,
    sim: &RouteSimulationResult,
    cycle: &FoundCycle,
    slippage_bps: u64,
    flash_source: FlashLoanSource,
) -> Option<ProfitAssessment> {
    let risk_bps = input.execution.route_risk_multiplier_bps(&cycle.edges);
    let thresholds = route_profit_thresholds(
        required_profit_matic_wei(
            input.min_profit_matic,
            input.matic_usd,
            input.matic_usd_chainlink,
        )
        .unwrap_or(U256::MAX),
        input.min_profit_roi_bps,
        input.safety_multiplier_bps,
        input.profit_priority_alpha_bps,
        risk_bps,
        input.charged_priority_fee_per_gas,
    );
    // Global MAX_SANE_PROFIT_MATIC_WEI already applied in assess_route_from_sim.
    let assessment = assess_route_from_sim(&RouteAssessRequest {
        cycle_start: cycle.start_token,
        arena: input.arena,
        gross_profit: sim.profit,
        amount_in: sim.amount_in,
        simulated_gas: sim.total_gas,
        hop_count: cycle.edge_hops(),
        slippage_bps,
        flash_source,
        gas: assessment_gas_for_edges(&cycle.edges, Some(input.route_gas), input.gas_oracle),
        thresholds,
        token_to_matic_rates: input.token_to_matic_rates,
        token_decimals: input.token_decimals,
        gas_price: input.gas_price,
    });
    Some(apply_dispatch_gate(input, cycle, sim, assessment))
}

const DISPATCH_BELOW_ECONOMIC_FLOOR: &str = "below economic floor for dispatch";
const DISPATCH_MISSING_HOP_AMOUNTS: &str = "missing hop amounts for dispatch";
const DISPATCH_HOP_FIDELITY_FAILED: &str = "hop fidelity failed for dispatch";

fn dispatch_input_floor(token_decimals: u8, economic_floor: U256) -> U256 {
    if token_decimals <= 8 {
        economic_floor.max(crate::util::ten_pow_u256(token_decimals))
    } else {
        economic_floor
    }
}

fn dispatch_floor_for_cycle(input: &HfEvalInput<'_>, cycle: &FoundCycle) -> U256 {
    let start_decimals =
        resolve_token_decimals_for_index(cycle.start_token, input.arena, input.token_decimals);
    dispatch_input_floor(
        start_decimals,
        min_economic_amount_in(
            start_decimals,
            resolve_token_to_matic_rate(cycle.start_token, input.token_to_matic_rates),
        ),
    )
}

fn dispatch_reject_reason(
    input: &HfEvalInput<'_>,
    cycle: &FoundCycle,
    sim: &RouteSimulationResult,
) -> Option<&'static str> {
    if sim.amount_in < dispatch_floor_for_cycle(input, cycle) {
        return Some(DISPATCH_BELOW_ECONOMIC_FLOOR);
    }
    if !sim.hop_amounts.iter().any(|amount| !amount.is_zero()) {
        return Some(DISPATCH_MISSING_HOP_AMOUNTS);
    }
    (!local_sim::route_hop_fidelity_ok_after_walk(input.arena, &cycle.edges, &sim.hop_amounts))
        .then_some(DISPATCH_HOP_FIDELITY_FAILED)
}

fn apply_dispatch_gate(
    input: &HfEvalInput<'_>,
    cycle: &FoundCycle,
    sim: &RouteSimulationResult,
    mut assessment: ProfitAssessment,
) -> ProfitAssessment {
    if assessment.should_execute
        && let Some(reason) = dispatch_reject_reason(input, cycle, sim)
    {
        assessment.should_execute = false;
        assessment.reject_reason = Some(reason.into());
    }
    assessment
}

fn flash_cap_for_cycle(input: &HfEvalInput<'_>, cycle: &FoundCycle) -> FlashBorrowCapParams {
    FlashBorrowCapParams {
        max_flash_loan_usd: input
            .execution
            .adaptive_flash_loan_usd(&cycle.edges, input.max_flash_loan_usd),
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
    use crate::core::types::{CycleEdges, Edge, FoundCycle, ProtocolType, TokenIndex};
    use crate::pipeline::route_sim_cache::RouteSimCache;
    use crate::test_support::FixtureBuilder;
    use std::sync::atomic::Ordering;

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

    fn arc_edge_cycle(id: u32) -> Arc<FoundCycle> {
        let mut cycle = cycle(id);
        cycle.edges.push(Edge {
            pool_index: crate::core::types::PoolIndex(id),
            token_in: TokenIndex(id),
            token_out: TokenIndex(id + 1),
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: true,
        });
        Arc::new(cycle)
    }

    #[test]
    fn dispatch_floor_requires_one_whole_low_decimal_token() {
        assert_eq!(
            dispatch_input_floor(6, U256::from(8_000u64)),
            U256::from(1_000_000u64)
        );
        assert_eq!(
            dispatch_input_floor(18, U256::from(8_000u64)),
            U256::from(8_000u64)
        );
    }

    #[test]
    fn aave_economic_size_reoptimizes_a_free_balancer_probe_on_assess_basis() {
        use crate::services::execution::profit::{
            AssessProfitInput, assess_profit, set_aave_flash_loan_fee_bps,
            set_balancer_flash_loan_fee_pct,
        };

        set_balancer_flash_loan_fee_pct(0);
        set_aave_flash_loan_fee_bps(5);
        let amount = U256::from(1_000u64);
        assert_eq!(
            flash_fee_changed_for_amount(
                FlashLoanSource::AaveV3,
                FlashLoanSource::Balancer,
                amount
            ),
            Some(true)
        );
        let assessment = |flash_loan_source| {
            assess_profit(&AssessProfitInput {
                gross_profit: U256::from(100u64),
                amount_in: amount,
                gas_units: 0,
                gas_price_wei: U256::ZERO,
                charged_priority_fee_per_gas:
                    crate::services::execution::gas::MIN_PRIORITY_FEE_PER_GAS,
                token_to_matic_rate: crate::core::math::fixed_point::ONE,
                token_decimals: 18,
                hop_count: 2,
                min_profit_matic_wei: U256::ZERO,
                min_profit_roi_bps: 0,
                slippage_bps: 0,
                flash_loan_source,
                safety_multiplier_bps: 0,
                profit_priority_alpha_bps: 0,
            })
        };
        let balancer_probe = assessment(FlashLoanSource::Balancer);
        let aave_economic = assessment(FlashLoanSource::AaveV3);
        assert_eq!(
            balancer_probe
                .net_profit
                .saturating_sub(aave_economic.net_profit),
            U256::ONE
        );
    }

    #[test]
    fn profitable_probe_routes_fill_full_cap_before_rescues() {
        let profitable = (0..8)
            .map(|id| (u64::from(id), U256::from(100u32 - id), arc_edge_cycle(id)))
            .collect();
        let kept = select_probe_survivors(profitable, vec![(99, arc_edge_cycle(99))], 8, 2);
        assert_eq!(kept.len(), 8);
        assert!(
            kept.iter()
                .all(|(_, cycle)| cycle.start_token != TokenIndex(99))
        );
    }

    #[test]
    fn probe_survivor_keeps_shared_cycle() {
        let cycle = arc_edge_cycle(1);
        let kept = select_probe_survivors(
            vec![(1, U256::from(100u8), Arc::clone(&cycle))],
            Vec::new(),
            1,
            0,
        );

        assert!(Arc::ptr_eq(&kept[0].1, &cycle));
    }

    #[test]
    fn probe_survivors_use_full_edges_when_fingerprints_collide() {
        let first = arc_edge_cycle(1);
        let second = arc_edge_cycle(2);
        let duplicate = Arc::clone(&first);
        let kept = select_probe_survivors(
            vec![
                (0, U256::from(100u8), first),
                (0, U256::from(90u8), second),
                (1, U256::from(80u8), duplicate),
            ],
            Vec::new(),
            3,
            0,
        );

        assert_eq!(kept.len(), 2);
        assert_ne!(kept[0].1.edges, kept[1].1.edges);
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
            (1, U256::from(100u8), arc_edge_cycle(1)),
            (2, U256::from(90u8), arc_edge_cycle(2)),
        ];
        let kept = select_probe_survivors(
            profitable,
            vec![
                (10, arc_edge_cycle(10)),
                (11, arc_edge_cycle(11)),
                (12, arc_edge_cycle(12)),
            ],
            4,
            2,
        );
        assert_eq!(kept.len(), 4);
        assert_eq!(kept[0].1.start_token, TokenIndex(1));
        assert_eq!(kept[1].1.start_token, TokenIndex(2));
    }

    #[test]
    fn rescues_fill_when_no_profitable_probes() {
        let kept = select_probe_survivors(
            Vec::new(),
            vec![
                (10, arc_edge_cycle(10)),
                (11, arc_edge_cycle(11)),
                (12, arc_edge_cycle(12)),
            ],
            4,
            2,
        );
        assert_eq!(kept.len(), 2);
        // Lower (more negative) graph score sorts first when cycle_ratio is zero.
        assert_eq!(kept[0].1.start_token, TokenIndex(12));
        assert_eq!(kept[1].1.start_token, TokenIndex(11));
    }

    #[test]
    fn rescue_skips_zero_profit_and_no_simulation() {
        use MinimalSimFailure::{ClCapExceeded, V2ReserveExhausted};
        assert!(!should_rescue_probe_reject(
            MinimalProbeReject::NoSimulation,
            &[Some(ClCapExceeded { hop: 0 })],
        ));
        assert!(!should_rescue_probe_reject(
            MinimalProbeReject::NoSimulation,
            &[Some(V2ReserveExhausted { hop: 0 })],
        ));
        // ZeroProfit rescue only produced seedless opt_none (kept>0 evaluated=0).
        assert!(!should_rescue_probe_reject(
            MinimalProbeReject::ZeroProfit,
            &[Some(V2ReserveExhausted { hop: 0 })],
        ));
        assert!(!should_rescue_probe_reject(
            MinimalProbeReject::SanityReject(SimSanityReject::InsaneProfitRatio),
            &[],
        ));
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
        MinimalProbeReject::SanityReject(SimSanityReject::InsaneProfitRatio).record(&mut skip);

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

    #[test]
    fn rank_probe_reuses_route_cache_for_unchanged_revision() {
        let mut fixture = FixtureBuilder::new();
        let a = fixture.token(1);
        let b = fixture.token(2);
        let first = fixture.v2_pool(
            3,
            ProtocolType::UniswapV2,
            a,
            b,
            U256::from(1_100u64) * U256::from(10u128.pow(18)),
            U256::from(900u64) * U256::from(10u128.pow(18)),
            30,
        );
        let second = fixture.v2_pool(
            4,
            ProtocolType::UniswapV2,
            b,
            a,
            U256::from(900u64) * U256::from(10u128.pow(18)),
            U256::from(1_200u64) * U256::from(10u128.pow(18)),
            30,
        );
        let cycle = FoundCycle {
            start_token: a,
            edges: CycleEdges::from_slice(&[
                Edge {
                    pool_index: first,
                    token_in: a,
                    token_out: b,
                    token_in_idx: 0,
                    token_out_idx: 1,
                    protocol: ProtocolType::UniswapV2,
                    fee_bps: 30,
                    zero_for_one: true,
                },
                Edge {
                    pool_index: second,
                    token_in: b,
                    token_out: a,
                    token_in_idx: 0,
                    token_out_idx: 1,
                    protocol: ProtocolType::UniswapV2,
                    fee_bps: 30,
                    zero_for_one: true,
                },
            ]),
            hop_count: 2,
            log_weight: 0.0,
            cumulative_fee_bps: 60,
            score: 0.0,
            cycle_ratio: U256::ZERO,
        };
        let cache = RouteSimCache::new();
        let fp = hash_cycle_edges(&cycle.edges);
        let revision = fixture
            .arena
            .route_state_revision_with_base(&cycle.edges, 1);

        let _ = minimal_rank_probe(
            &fixture.arena,
            &cycle,
            18,
            U256::from(10u128.pow(18)),
            Some((&cache, revision, fp)),
        );
        let hits_before = cache.stats.hits.load(Ordering::Relaxed);
        let _ = minimal_rank_probe(
            &fixture.arena,
            &cycle,
            18,
            U256::from(10u128.pow(18)),
            Some((&cache, revision, fp)),
        );

        assert!(cache.stats.hits.load(Ordering::Relaxed) > hits_before);
    }
}
