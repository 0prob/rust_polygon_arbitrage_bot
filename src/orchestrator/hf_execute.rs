use alloy::network::Ethereum;
use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use rustc_hash::FxHashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::task::JoinSet;

use crate::core::constants::AAVE_V3_POOL;
use crate::core::types::{FlashLoanSource, FoundCycle, PoolIndex, PoolState, V3Tick};
use crate::infra::rpc::RpcPool;
use crate::orchestrator::hf::HfContext;
use crate::orchestrator::hf_eval::{
    HfEvalInput, HfEvalInputOwned, HfEvalResult, reassess_hf_eval_result,
};
use crate::pipeline::arena::StateArena;
use crate::pipeline::local_sim::{self, simulate_route_detailed_with_caps};
use crate::pipeline::sim_sanity::required_profit_matic_wei;
use crate::pipeline::types::MinimalSimResult;

#[cfg(test)]
use crate::pipeline::tick_fetch::is_cl_tick_on_hydrate_cooldown;
use crate::pipeline::tick_fetch::{
    collect_v3_pool_addresses, collect_v4_tick_targets, hydrate_cl_ticks_with_rpc_fallback,
    is_empty_tick_on_cooldown, is_empty_v4_tick_on_cooldown, is_probe_narrow_miss_on_cooldown,
    is_probe_narrow_miss_v4_on_cooldown, mark_probe_narrow_miss_v4,
    mark_tick_hydrate_timeout_cooldown, mark_v4_tick_hydrate_timeout_cooldown, still_tickless_v3,
    still_tickless_v4,
};
use crate::services::execution::aave::{
    AaveReserveStatus, aave_flash_reserve_status_live, record_aave_prepare_skip_inactive,
};
use crate::services::execution::flash_liquidity::resolve_flash_source_for_cycle;
use crate::services::execution::flash_liquidity::{
    collect_flash_tokens_for_cycle, dodo_base_flash_pool_for_cycle,
};
use crate::services::execution::gas_oracle::RouteGasLookup;

use crate::services::execution::balancer_verify::{
    BalancerBatchReject, BatchQueryOutcome, BatchQueryVerdict,
    confirm_direct_batch_realized_profit, evaluate_batch_query, log_balancer_batch_filter_summary,
    log_balancer_prepare_gate_summary, query_balancer_batch_profit, record_balancer_batch_reject,
    record_balancer_filter_accept, record_balancer_filter_window, record_balancer_prepare_skip,
};
use crate::services::execution::calldata::build_calldata_hops;
use crate::services::execution::flash_liquidity::route_is_balancer_only;
use crate::services::execution::impact_slippage::{
    depth_impact_slippage_bps_with_base, effective_slippage_bps_for_flash,
};
use crate::services::execution::profit::on_chain_min_profit_from_assessment;
use crate::services::execution::{
    CandidateBuildConfig, ExecutionOutcome, PrepareDispatchInput, build_execution_candidate,
    prepare_evaluated_route,
};
use crate::services::oracle::resolve_matic_usd_for_flash_dispatch;
use crate::services::oracle::resolve_token_to_matic_rate_or_bootstrap;
use crate::services::state_refresh::{HF_POOL_STATE_FRESH, PoolRefreshResult};

enum RoutePoolRefreshAbort {
    NotIndexed {
        pool_count: usize,
    },
    /// Fetch ran but no pool state was written (cache was cleared pre-refresh).
    NoUpdates {
        pool_count: usize,
    },
    Rpc(anyhow::Error),
}

/// After `cache.remove`, a refresh attempt that updates zero pools cannot dispatch safely.
#[must_use]
fn route_pool_refresh_failed(result: &PoolRefreshResult) -> bool {
    result.attempted && result.updated == 0
}

fn effective_slippage_after_resim(
    arena: &StateArena,
    edges: &[crate::core::types::Edge],
    sim: &crate::core::types::RouteSimulationResult,
    configured_per_hop_bps: u64,
    flash_source: FlashLoanSource,
) -> u64 {
    let depth_bps = depth_impact_slippage_bps_with_base(
        arena,
        edges,
        sim.amount_in,
        Some(&MinimalSimResult {
            profit: sim.profit,
            amount_out: sim.amount_out,
            total_gas: sim.total_gas,
        }),
    );
    effective_slippage_after_resim_depth(
        configured_per_hop_bps,
        sim.hop_count,
        depth_bps,
        flash_source,
    )
}

fn effective_slippage_after_resim_depth(
    configured_per_hop_bps: u64,
    hop_count: u32,
    depth_bps: u64,
    flash_source: FlashLoanSource,
) -> u64 {
    effective_slippage_bps_for_flash(
        configured_per_hop_bps,
        hop_count,
        if depth_bps >= 10_000 {
            3_000
        } else {
            depth_bps
        },
        flash_source,
    )
}

/// Pure Balancer routes use Direct `batchSwap` (no per-hop minOut); mixed use multi-call.
#[inline]
fn resim_flash_source_for_slip(cycle: &FoundCycle) -> FlashLoanSource {
    if route_is_balancer_only(cycle) {
        FlashLoanSource::Direct
    } else {
        FlashLoanSource::AaveV3
    }
}

async fn refresh_route_pools_into_arena(
    refresh: &crate::services::state_refresh::StateRefreshService,
    cache: &crate::services::state_cache::StateCache,
    arena: &mut StateArena,
    pools: &[Address],
) -> Result<(bool, u64), RoutePoolRefreshAbort> {
    if pools.is_empty() {
        return Ok((false, cache.generation()));
    }
    let pool_count = pools.len();
    // Keep tradable entries fresher than ~1.5s (same HF tick / prefetch). Blind
    // remove + 429 left dispatch with tickless arena and aborted the candidate.
    // Fresh Invalid is not kept — `is_fresh_within` requires tradable.
    for pool in pools {
        if !cache.is_fresh_within(pool, HF_POOL_STATE_FRESH) {
            cache.remove(pool);
        }
    }
    let result = refresh
        .refresh_pool_states_for(pools, pool_count)
        .await
        .map_err(RoutePoolRefreshAbort::Rpc)?;
    if !result.can_use_cached_state() {
        return Err(RoutePoolRefreshAbort::NotIndexed { pool_count });
    }
    if route_pool_refresh_failed(&result) {
        return Err(RoutePoolRefreshAbort::NoUpdates {
            pool_count: result.matched,
        });
    }
    if result.updated < result.matched {
        crate::warn!(
            "route pool refresh partial: {}/{} pools updated — continuing with subset",
            result.updated,
            result.matched
        );
    }
    let generation = arena.apply_hot_cache(cache, pools);
    let fetched = result.updated > 0;
    if !fetched {
        log_route_refresh_cache_only(result, pool_count);
    }
    Ok((fetched, generation))
}

fn log_route_refresh_cache_only(result: PoolRefreshResult, pool_count: usize) {
    if result.attempted {
        crate::debug!(
            "route pool refresh: 0/{pool_count} updated after fetch — using cached state"
        );
    } else {
        crate::debug!("route pool refresh: {pool_count} pools already fresh — using cached state");
    }
}

pub(crate) struct DispatchInputs<'a> {
    pub(crate) pool_metas: &'a [crate::pipeline::types::PoolMeta],
    pub(crate) token_to_matic_rates:
        &'a rustc_hash::FxHashMap<crate::core::types::TokenIndex, U256>,
    pub(crate) token_decimals: &'a rustc_hash::FxHashMap<alloy::primitives::Address, u8>,
    pub(crate) state_generation: u64,
    pub(crate) state_block: u64,
    pub(crate) state_hash: Option<B256>,
    pub(crate) skip_dispatch_refresh: bool,
    /// HF tick flash-cap USD price; skips redundant oracle RPC on dispatch when set.
    pub(crate) matic_usd: Option<f64>,
}

#[derive(Default)]
struct SkipCounts {
    quarantine: AtomicU32,
    cooldown: AtomicU32,
    resim_fail: AtomicU32,
    resim_drift: AtomicU32,
    hop_fidelity: AtomicU32,
    prepare_meta: AtomicU32,
    prepare_plan: AtomicU32,
    prepare_aave: AtomicU32,
    prepare_balancer: AtomicU32,
    build: AtomicU32,
}

impl SkipCounts {
    fn record(&self, key: &'static str) {
        match key {
            "quarantine" => self.quarantine.fetch_add(1, Ordering::Relaxed),
            "cooldown" => self.cooldown.fetch_add(1, Ordering::Relaxed),
            "resim_fail" => self.resim_fail.fetch_add(1, Ordering::Relaxed),
            "resim_drift" => self.resim_drift.fetch_add(1, Ordering::Relaxed),
            "hop_fidelity" => self.hop_fidelity.fetch_add(1, Ordering::Relaxed),
            "prepare_meta" => self.prepare_meta.fetch_add(1, Ordering::Relaxed),
            "prepare_plan" => self.prepare_plan.fetch_add(1, Ordering::Relaxed),
            "prepare_aave" => self.prepare_aave.fetch_add(1, Ordering::Relaxed),
            "prepare_balancer" => self.prepare_balancer.fetch_add(1, Ordering::Relaxed),
            "build" => self.build.fetch_add(1, Ordering::Relaxed),
            _ => 0,
        };
    }

    fn log_dispatch_gates(&self, candidates: u32, pools_refreshed: bool) {
        if candidates == 0 {
            return;
        }
        let quarantine = self.quarantine.load(Ordering::Relaxed);
        let cooldown = self.cooldown.load(Ordering::Relaxed);
        let resim_fail = self.resim_fail.load(Ordering::Relaxed);
        let resim_drift = self.resim_drift.load(Ordering::Relaxed);
        let hop_fidelity = self.hop_fidelity.load(Ordering::Relaxed);
        let prepare_meta = self.prepare_meta.load(Ordering::Relaxed);
        let prepare_plan = self.prepare_plan.load(Ordering::Relaxed);
        let prepare_aave = self.prepare_aave.load(Ordering::Relaxed);
        let prepare_balancer = self.prepare_balancer.load(Ordering::Relaxed);
        let build = self.build.load(Ordering::Relaxed);
        let skipped = quarantine
            + cooldown
            + resim_fail
            + resim_drift
            + hop_fidelity
            + prepare_meta
            + prepare_plan
            + prepare_aave
            + prepare_balancer
            + build;
        if skipped == 0 && build == 0 {
            return;
        }
        crate::info!(
            "dispatch gate: candidates={candidates} pools_refreshed={pools_refreshed} skipped={skipped} \
             quarantine={quarantine} cooldown={cooldown} resim_fail={resim_fail} resim_drift={resim_drift} \
             hop_fidelity={hop_fidelity} prepare_meta={prepare_meta} prepare_plan={prepare_plan} \
             prepare_aave={prepare_aave} prepare_balancer={prepare_balancer} build_fail={build}",
        );
    }
}

pub(crate) async fn dispatch_profitable_candidates(
    ctx: &HfContext,
    arena: &mut StateArena,
    profitable: Vec<HfEvalResult>,
    inputs: DispatchInputs<'_>,
) {
    if profitable.is_empty() || *ctx.shutdown.borrow() {
        return;
    }

    let Some(executor) = ctx.config.execution.executor_address else {
        crate::warn!("dispatch skip: EXECUTOR_ADDRESS not configured");
        return;
    };

    // ponytail: skip executor bytecode check in dry-run mode — no on-chain txs to sign
    let sim_provider = if ctx.config.is_dry_run() {
        match ctx.rpc.connect_simulation() {
            Ok(p) => p,
            Err(e) => {
                crate::warn!("dispatch skip: simulation RPC unavailable: {e:#}");
                return;
            }
        }
    } else {
        match ctx.rpc.connect_simulation_checked(executor).await {
            Ok(p) => p,
            Err(e) => {
                let msg = format!("{e:#}");
                if msg.contains("no executor bytecode") {
                    crate::debug!("dispatch skip: {msg}");
                } else {
                    crate::warn!("dispatch skip: simulation RPC/executor check failed: {msg}");
                }
                return;
            }
        }
    };
    let pool_metas_by_pool: FxHashMap<
        crate::core::types::PoolIndex,
        &crate::pipeline::types::PoolMeta,
    > = inputs
        .pool_metas
        .iter()
        .map(|meta| (meta.pool_index, meta))
        .collect();

    let operator = ctx.wallet.operator_address(executor);

    dispatch_with_provider(
        ctx,
        arena,
        profitable,
        &sim_provider,
        operator,
        executor,
        inputs.pool_metas,
        &pool_metas_by_pool,
        inputs.token_to_matic_rates,
        inputs.token_decimals,
        inputs.state_generation,
        inputs.state_block,
        inputs.state_hash,
        inputs.skip_dispatch_refresh,
        inputs.matic_usd,
    )
    .await;

    if !ctx.config.is_dry_run() {
        ctx.execution.shutdown_resync(&sim_provider, operator).await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_with_provider<P: Provider<Ethereum> + Clone + Send + 'static>(
    ctx: &HfContext,
    arena: &mut StateArena,
    profitable: Vec<HfEvalResult>,
    sim_provider: &P,
    operator: alloy::primitives::Address,
    executor: alloy::primitives::Address,
    pool_metas: &[crate::pipeline::types::PoolMeta],
    pool_metas_by_pool: &FxHashMap<
        crate::core::types::PoolIndex,
        &crate::pipeline::types::PoolMeta,
    >,
    token_to_matic_rates: &rustc_hash::FxHashMap<crate::core::types::TokenIndex, U256>,
    token_decimals: &rustc_hash::FxHashMap<alloy::primitives::Address, u8>,
    state_generation: u64,
    state_block: u64,
    state_hash: Option<B256>,
    skip_dispatch_refresh: bool,
    matic_usd_hint: Option<f64>,
) {
    if ctx.execution.global_is_quarantined() {
        crate::warn!("dispatch skip: execution circuit breaker active");
        return;
    }
    let flash_policy = ctx.config.flash_policy;
    let Some(gas_price) = ctx.gas_oracle.conservative_gas_price_for_live_submit() else {
        let age = ctx.gas_oracle.snapshot_age_ms();
        crate::warn!("dispatch skip: gas fee snapshot missing or stale (age_ms={age:?})");
        return;
    };
    let brent_iters = ctx.config.routing.brent_search_iterations;
    let base_slippage_bps = ctx.config.execution.slippage_bps;
    let min_profit_roi_bps = ctx.config.execution.min_profit_roi_bps;
    let max_flash_loan_usd = ctx.config.execution.max_flash_loan_usd;
    let Some(matic_usd) =
        resolve_matic_usd_for_flash_dispatch(&ctx.price_oracle, matic_usd_hint, sim_provider).await
    else {
        crate::warn!("dispatch skip: MATIC/USD oracle unavailable for flash loan cap");
        return;
    };
    let min_profit_matic = required_profit_matic_wei(
        ctx.config.min_profit_matic,
        matic_usd,
        ctx.price_oracle.fresh_matic_usd_chainlink_raw(),
    )
    .unwrap_or(U256::MAX);
    // ponytail: one line per dispatch batch — verify $0.01 floor + wired executor
    crate::info!(
        "dispatch floor: min_profit_matic_wei={min_profit_matic} matic_usd={matic_usd:.6} executor={executor} candidates={}",
        profitable.len()
    );
    let deadline_secs = ctx.config.execution.deadline_secs;

    let profitable: Vec<_> = profitable
        .into_iter()
        .filter(|r| {
            // Rotation-aware: underwater/stale cool all starts; single-fp let a
            // rotated profitable candidate leak into submit (parity with evaluate_one).
            !ctx.execution.cycle_edges_quarantined(&r.cycle.edges)
                && !ctx
                    .execution
                    .is_route_on_cooldown(r.route_fingerprint, &ctx.config)
        })
        .collect();
    if profitable.is_empty() {
        return;
    }

    let mut seen_fp = rustc_hash::FxHashSet::default();
    let profitable: Vec<_> = profitable
        .into_iter()
        .filter(|r| seen_fp.insert(r.route_fingerprint))
        .collect();

    let mut flash_seen = rustc_hash::FxHashSet::default();
    let mut flash_tokens = Vec::new();
    for route in &profitable {
        collect_flash_tokens_for_cycle(arena, &route.cycle, &mut flash_seen, &mut flash_tokens);
    }
    let dispatch_pools = collect_route_pool_addresses(arena, &profitable);
    let dispatch_cycles: Vec<&FoundCycle> = profitable.iter().map(|r| &r.cycle).collect();
    let mut dispatch_state_generation = state_generation;
    let mut dispatch_state_block = state_block;
    let mut dispatch_state_hash = state_hash;
    let refresh_required = !skip_dispatch_refresh && !dispatch_pools.is_empty();
    let pools_refreshed = if !refresh_required {
        false
    } else {
        match refresh_route_pools_into_arena(&ctx.refresh, &ctx.cache, arena, &dispatch_pools).await
        {
            Ok((fetched, generation)) => {
                dispatch_state_generation = generation;
                if fetched {
                    let tick_block = ctx.refresh.last_state_block();
                    if tick_block == 0 {
                        crate::warn!("dispatch aborted: refreshed route state has no pinned block");
                        return;
                    }
                    dispatch_state_block = tick_block;
                    dispatch_state_hash = ctx.refresh.last_state_hash();
                    enrich_dispatch_cl_ticks(
                        ctx.rpc.as_ref(),
                        arena,
                        &dispatch_cycles,
                        pool_metas,
                        ctx.config.oracle.tick_word_range,
                        (tick_block > 0).then_some(tick_block),
                    )
                    .await;
                    true
                } else {
                    false
                }
            }
            Err(RoutePoolRefreshAbort::NotIndexed { pool_count }) => {
                crate::warn!(
                    "dispatch aborted: route pools not in discovery index ({pool_count} addresses)"
                );
                return;
            }
            Err(RoutePoolRefreshAbort::NoUpdates { pool_count }) => {
                crate::warn!(
                    "dispatch aborted: route pool refresh returned 0/{pool_count} updates"
                );
                return;
            }
            Err(RoutePoolRefreshAbort::Rpc(e)) => {
                crate::warn!("dispatch aborted: route pool refresh failed ({e:#})");
                return;
            }
        }
    };

    // HF tick already prefetches stale flash tokens (2.5s budget); avoid a second blocking refresh here.
    if !flash_tokens.is_empty() {
        ctx.execution
            .flash_liquidity
            .spawn_refresh_if_stale(Arc::clone(&ctx.rpc), &flash_tokens);
    }

    let skipped = Arc::new(SkipCounts::default());
    let dispatch_candidates = u32::try_from(profitable.len()).unwrap_or(u32::MAX);
    let shutdown = ctx.shutdown.clone();
    let arena_ref: &StateArena = &*arena;
    // Bound head RPC — hung eth_blockNumber blocked the entire dispatch window.
    let chain_head_hint = tokio::time::timeout(
        std::time::Duration::from_millis(1_500),
        sim_provider.get_block_number(),
    )
    .await
    .ok()
    .and_then(Result::ok);

    for evaluated in profitable {
        if *shutdown.borrow() {
            break;
        }
        let Some(outcome) = dispatch_one_candidate(
            ctx,
            arena_ref,
            evaluated,
            sim_provider,
            operator,
            executor,
            min_profit_matic,
            pool_metas_by_pool,
            token_to_matic_rates,
            token_decimals,
            dispatch_state_generation,
            dispatch_state_block,
            dispatch_state_hash,
            pools_refreshed,
            flash_policy,
            gas_price,
            brent_iters,
            base_slippage_bps,
            min_profit_roi_bps,
            max_flash_loan_usd,
            matic_usd,
            deadline_secs,
            &skipped,
            chain_head_hint,
        )
        .await
        else {
            continue;
        };
        if matches!(
            outcome,
            ExecutionOutcome::SkippedCircuitBreaker
                | ExecutionOutcome::SkippedShutdown
                | ExecutionOutcome::SkippedInsufficientBalance
        ) {
            break;
        }
        if !ctx.config.is_dry_run()
            && matches!(
                outcome,
                ExecutionOutcome::Confirmed { .. }
                    | ExecutionOutcome::Reverted { .. }
                    | ExecutionOutcome::ReceiptTimeout { .. }
            )
        {
            break;
        }
    }

    skipped.log_dispatch_gates(dispatch_candidates, pools_refreshed);
    crate::services::execution::aave::log_aave_gate_summary(dispatch_candidates);
    log_balancer_prepare_gate_summary(dispatch_candidates);
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_one_candidate<P: Provider<Ethereum> + Clone + Send + 'static>(
    ctx: &HfContext,
    arena: &StateArena,
    mut evaluated: HfEvalResult,
    sim_provider: &P,
    operator: alloy::primitives::Address,
    executor: alloy::primitives::Address,
    min_profit_matic: U256,
    pool_metas_by_pool: &FxHashMap<
        crate::core::types::PoolIndex,
        &crate::pipeline::types::PoolMeta,
    >,
    token_to_matic_rates: &rustc_hash::FxHashMap<crate::core::types::TokenIndex, U256>,
    token_decimals: &rustc_hash::FxHashMap<alloy::primitives::Address, u8>,
    dispatch_state_generation: u64,
    state_block: u64,
    state_hash: Option<B256>,
    pools_refreshed: bool,
    flash_policy: crate::services::execution::flash_policy::FlashLoanPolicy,
    gas_price: U256,
    brent_iters: u32,
    base_slippage_bps: u64,
    min_profit_roi_bps: u64,
    max_flash_loan_usd: u64,
    matic_usd: f64,
    deadline_secs: u64,
    skipped: &Arc<SkipCounts>,
    chain_head_hint: Option<u64>,
) -> Option<ExecutionOutcome> {
    let fp = evaluated.route_fingerprint;
    let balancer_batch_verified = evaluated.balancer_batch_verified;

    let Some(start_token_addr) = arena.token_address(evaluated.cycle.start_token) else {
        skipped.record("prepare_meta");
        return None;
    };
    let Some(resolved_token_decimals) = token_decimals.get(&start_token_addr).copied() else {
        skipped.record("prepare_meta");
        return None;
    };
    let Some(token_to_matic_rate) =
        resolve_token_to_matic_rate_or_bootstrap(evaluated.cycle.start_token, token_to_matic_rates)
    else {
        skipped.record("prepare_meta");
        return None;
    };

    let hop_fidelity_caps = local_sim::precompute_route_shallow_caps(arena, &evaluated.cycle.edges);
    // Keep HF assessment when sim profit is unchanged so prepare can skip a full reassess.
    // Dropped after a successful refresh resim (profit/gas may move).
    let mut prior_assessment = Some(evaluated.assessment.clone());

    let sim = if pools_refreshed {
        let amount_in = evaluated.sim.amount_in;
        let Some(refreshed) = simulate_route_detailed_with_caps(
            arena,
            &evaluated.cycle.edges,
            amount_in,
            hop_fidelity_caps.as_ref(),
        ) else {
            skipped.record("resim_fail");
            crate::debug!("dispatch skip: fp={fp} resim failed after pool refresh");
            return None;
        };
        let mut resim_profile = local_sim::ResimFidelityProfile::default();
        if let Some(reason) = local_sim::route_resim_fidelity_reject_profiled(
            &evaluated.sim,
            &refreshed,
            &mut resim_profile,
        ) {
            skipped.record("resim_drift");
            crate::info!(
                "dispatch gate resim: fp={fp} reason={reason} baseline_profit={} refreshed_profit={} profit_drift_bps={} max_hop_drift_bps={}",
                evaluated.sim.profit,
                refreshed.profit,
                resim_profile.profit_drift_bps,
                resim_profile.max_hop_drift_bps,
            );
            return None;
        }
        let mut hop_profile = local_sim::HopFidelityProfile::default();
        if let Some(reject) = local_sim::route_hop_fidelity_reject_profiled(
            arena,
            &evaluated.cycle.edges,
            &refreshed.hop_amounts,
            Some(&mut hop_profile),
            true,
            hop_fidelity_caps.as_ref(),
        ) {
            skipped.record("hop_fidelity");
            let hop = match reject {
                local_sim::HopFidelityReject::MissingPool(i)
                | local_sim::HopFidelityReject::PoolLocked(i)
                | local_sim::HopFidelityReject::ShallowCl(i)
                | local_sim::HopFidelityReject::V2ReserveExhausted(i) => i,
            };
            let hop_amount = refreshed
                .hop_amounts
                .get(hop)
                .copied()
                .unwrap_or(U256::ZERO);
            crate::debug!(
                "dispatch skip: fp={fp} hop fidelity failed: {reject:?} hop={hop} amount={hop_amount} hops_checked={} cl_depth_sims={}",
                hop_profile.hops_checked,
                hop_profile.cl_depth_sims,
            );
            return None;
        }
        if hop_profile.cl_depth_sims > 0 {
            crate::debug!(
                "dispatch resim ok: fp={fp} profit_drift_bps={} max_hop_drift_bps={} cl_depth_sims={}",
                resim_profile.profit_drift_bps,
                resim_profile.max_hop_drift_bps,
                hop_profile.cl_depth_sims,
            );
        }
        // Resim changed the economics — force prepare to reassess with planned flash source.
        prior_assessment = None;
        evaluated.effective_slippage_bps = effective_slippage_after_resim(
            arena,
            &evaluated.cycle.edges,
            &refreshed,
            base_slippage_bps,
            resim_flash_source_for_slip(&evaluated.cycle),
        );
        refreshed
    } else {
        evaluated.sim
    };
    evaluated.sim = sim;

    let liquidity = ctx.execution.flash_liquidity.snapshot(start_token_addr);
    // Route-level for profit/prepare; config per-hop for calldata minOut (do not push
    // full-route depth haircut into every hop's minOut). Floor matches encode mins.
    let route_slippage_bps = evaluated.effective_slippage_bps.max(base_slippage_bps);
    let calldata_slippage_bps =
        base_slippage_bps.max(crate::core::constants::EXECUTION_MIN_SLIPPAGE_BPS);
    let search_low = evaluated.opt.search_low;
    let adaptive_flash_cap_bound = evaluated.adaptive_flash_cap_bound;
    let evaluated = crate::services::execution::candidate::evaluated_from_sim(
        evaluated.cycle,
        evaluated.sim,
        evaluated.assessment,
        route_slippage_bps,
    );
    // prepare re-validates flash fee vs planned source before reuse.
    let log_prepare_skip = ctx.execution.should_log_prepare_skip(fp);
    let Some(prepared) = prepare_evaluated_route(&PrepareDispatchInput {
        evaluated: &evaluated,
        arena,
        liquidity,
        policy: flash_policy,
        token_to_matic_rates,
        token_decimals,
        brent_iters,
        min_profit_matic,
        min_profit_roi_bps,
        gas_price,
        slippage_bps: route_slippage_bps,
        max_flash_loan_usd: ctx
            .execution
            .adaptive_flash_loan_usd(fp, max_flash_loan_usd),
        matic_usd,
        matic_usd_chainlink: ctx.price_oracle.fresh_matic_usd_chainlink_raw(),
        safety_multiplier_bps: ctx.config.execution.profit_safety_multiplier_bps,
        profit_priority_alpha_bps: ctx.config.execution.profit_priority_fee_alpha_bps,
        route_fingerprint: fp,
        gas_oracle: &ctx.gas_oracle,
        search_low,
        risk_multiplier_bps: ctx.execution.route_risk_multiplier_bps(fp),
        existing_assessment: prior_assessment,
        log_skips: log_prepare_skip,
        adaptive_flash_cap_bound,
    }) else {
        skipped.record("prepare_plan");
        if log_prepare_skip {
            ctx.execution.record_prepare_skip(fp);
        }
        return None;
    };

    if prepared.flash_source == FlashLoanSource::AaveV3 {
        let cache_viable = ctx
            .execution
            .flash_liquidity
            .aave_viable_for_dispatch(start_token_addr);
        if !cache_viable {
            let status =
                aave_flash_reserve_status_live(sim_provider, AAVE_V3_POOL, start_token_addr).await;
            if status != AaveReserveStatus::Viable {
                skipped.record("prepare_aave");
                record_aave_prepare_skip_inactive();
                ctx.execution
                    .flash_liquidity
                    .mark_aave_inactive(start_token_addr);
                if log_prepare_skip {
                    crate::info!(
                        "aave: prepare_skip fp={fp} token={start_token_addr} status={status:?}"
                    );
                } else {
                    crate::debug!(
                        "aave: dispatch_skip fp={fp} token={start_token_addr} status={status:?}"
                    );
                }
                return None;
            }
        }
    }

    if prepared.flash_source == FlashLoanSource::Direct
        && route_is_balancer_only(&prepared.evaluated.cycle)
        && !balancer_batch_verified
    {
        let hops = match build_calldata_hops(
            arena,
            &prepared.evaluated.cycle.edges,
            &prepared.evaluated.result.hop_amounts,
            pool_metas_by_pool,
        ) {
            Ok(h) => h,
            Err(reason) => {
                crate::warn!("balancer prepare calldata_build_failed: fp={fp} {reason}");
                skipped.record("prepare_balancer");
                record_balancer_prepare_skip();
                record_balancer_batch_reject(BalancerBatchReject::CalldataBuildFailed);
                return None;
            }
        };
        if !crate::services::execution::balancer_verify::balancer_batch_within_max_in_ratio(
            arena, &hops,
        ) {
            skipped.record("prepare_balancer");
            record_balancer_prepare_skip();
            record_balancer_batch_reject(BalancerBatchReject::MaxInRatio);
            ctx.execution.quarantine_batch_query_failure(fp);
            if log_prepare_skip {
                crate::info!("balancer: prepare_skip fp={fp} reason=max_in_ratio");
            } else {
                crate::debug!("balancer: dispatch_skip fp={fp} reason=max_in_ratio");
            }
            return None;
        }
        let query_block = (state_block > 0).then_some(state_block);
        let outcome = query_balancer_batch_profit(
            sim_provider,
            executor,
            &hops,
            start_token_addr,
            query_block,
        )
        .await;
        match evaluate_batch_query(
            outcome,
            prepared.evaluated.result.amount_in,
            route_slippage_bps,
        ) {
            BatchQueryVerdict::Accepted(_) => {}
            BatchQueryVerdict::Rejected(reason) => {
                skipped.record("prepare_balancer");
                record_balancer_prepare_skip();
                record_balancer_batch_reject(reason);
                ctx.execution.quarantine_batch_query_failure(fp);
                if log_prepare_skip {
                    crate::info!(
                        "balancer: prepare_skip fp={fp} reason={reason:?} modeled={}",
                        prepared.evaluated.result.profit,
                    );
                } else {
                    crate::debug!(
                        "balancer: dispatch_skip fp={fp} reason={reason:?} modeled={}",
                        prepared.evaluated.result.profit,
                    );
                }
                return None;
            }
        }
    }

    let build_cfg = CandidateBuildConfig {
        executor_address: executor,
        // Per-hop config for calldata minOut + on-chain minProfit compounding.
        slippage_bps: calldata_slippage_bps,
        flash_loan_source: prepared.flash_source,
        deadline_secs_from_now: deadline_secs,
        min_profit_matic_wei: min_profit_matic,
        min_profit_roi_bps,
        token_decimals: resolved_token_decimals,
        token_to_matic_rate,
        safety_multiplier_bps: ctx.config.execution.profit_safety_multiplier_bps,
        state_generation: dispatch_state_generation,
        state_block,
        state_hash,
        route_fingerprint: fp,
        flash_liquidity: liquidity,
        has_dodo_pool: dodo_base_flash_pool_for_cycle(arena, &prepared.evaluated.cycle).is_some(),
        trust_prepared_flash: true,
        adaptive_flash_cap_bound: prepared.adaptive_flash_cap_bound,
        adaptive_flash_loan_usd_limit: max_flash_loan_usd,
    };

    let mut candidate =
        match build_execution_candidate(arena, &prepared.evaluated, &build_cfg, pool_metas_by_pool)
        {
            Ok(c) => c,
            Err(e) => {
                crate::warn!("dispatch build failed: fp={fp}: {e:#}");
                skipped.record("build");
                // Structural encode rejects (phantom V2 tokens, zfo, …) won't heal next tick.
                ctx.execution.quarantine_batch_query_failure(fp);
                return None;
            }
        };
    // Profit reassess uses route-level slip (config compounded + depth), not per-hop alone.
    candidate.slippage_bps = route_slippage_bps;

    if ctx
        .execution
        .is_route_hash_quarantined(&candidate.route_hash)
    {
        skipped.record("quarantine");
        return None;
    }

    if ctx
        .execution
        .should_log_dispatch(fp, candidate.expected_profit_matic_wei)
    {
        crate::info!(
            "dispatch candidate: fp={}, hops={}, sim_gas={}, profit_matic={}",
            fp,
            candidate.hop_count,
            candidate.simulated_gas,
            candidate.expected_profit_matic_wei
        );
    }

    let outcome = ctx
        .execution
        .process_candidate(
            sim_provider,
            ctx.rpc.as_ref(),
            ctx.wallet.as_ref(),
            &ctx.config,
            &candidate,
            operator,
            &ctx.gas_oracle,
            &ctx.cache,
            state_block,
            state_hash,
            Some(&ctx.ui_hook),
            Some(&ctx.shutdown),
            None,
            chain_head_hint,
        )
        .await;
    Some(outcome)
}

fn dispatch_cl_tick_snapshot(
    arena: &StateArena,
    tick_pools: &[Address],
    v4_targets: &[(PoolIndex, B256)],
) -> Vec<(PoolIndex, Arc<[V3Tick]>)> {
    let mut snapshots = Vec::with_capacity(tick_pools.len().saturating_add(v4_targets.len()));
    for pool in tick_pools {
        let Some(index) = arena.address_to_pool().get(pool).copied() else {
            continue;
        };
        let Some(PoolState::V3(state)) = arena.pool_state(index) else {
            continue;
        };
        snapshots.push((index, Arc::clone(&state.ticks)));
    }
    for &(index, _) in v4_targets {
        let Some(PoolState::V4(state)) = arena.pool_state(index) else {
            continue;
        };
        snapshots.push((index, Arc::clone(&state.ticks)));
    }
    snapshots
}

fn restore_dispatch_cl_ticks(arena: &mut StateArena, snapshots: Vec<(PoolIndex, Arc<[V3Tick]>)>) {
    // Only restore pools that are still empty — keep any family that hydrated.
    for (index, ticks) in snapshots {
        match arena.pool_state_mut(index) {
            Some(PoolState::V3(state)) | Some(PoolState::V4(state)) if state.ticks.is_empty() => {
                state.ticks = ticks;
            }
            _ => {}
        }
    }
}

/// Hard cap on tick RPC targets per probe-tick hydrate.
/// Live TickLens finishes in ~100–250ms; cap=3 left v3_total=7–8 with
/// cycles_tickless stuck (iter1: 221 cl_tickless → NoSimulation).
const HF_PROBE_TICK_POOL_CAP: usize = 6;
/// ms per tickless pool for budget→cap scaling.
///
/// Live probe-tick (word_range≤4): often 44–120ms/pool. The prior 250ms floor
/// forced `cap=1` under residual prep so multi-hop CL cycles stayed
/// `cycles_tickless=N→N` after a successful `v3_loaded=1`.
pub(crate) const HF_PROBE_TICK_MS_PER_POOL: u64 = 120;

/// Scale the probe hydrate pool set to residual prep budget so we finish more
/// often instead of cooling a large pending set on timeout.
#[must_use]
pub(crate) fn probe_tick_pool_cap_for_budget(budget: std::time::Duration) -> usize {
    let ms = budget.as_millis() as u64;
    // Do not `.max(1)` — a sub-pool budget always times out and cools the target.
    if ms < HF_PROBE_TICK_MS_PER_POOL {
        return 0;
    }
    ((ms / HF_PROBE_TICK_MS_PER_POOL) as usize).min(HF_PROBE_TICK_POOL_CAP)
}

#[cfg(test)]
fn cl_pool_on_hydrate_cooldown(
    arena: &StateArena,
    pool_metas: &[crate::pipeline::types::PoolMeta],
    pool_index: PoolIndex,
    protocol: crate::core::types::ProtocolType,
) -> bool {
    let Some(addr) = arena.pool_address(pool_index) else {
        return false;
    };
    if protocol == crate::core::types::ProtocolType::UniswapV4 {
        let v4_pool_id = crate::pipeline::types::pool_meta_at(pool_metas, pool_index)
            .and_then(|meta| meta.pool_id);
        // Missing pool_id: TickLens cannot target the hop — treat as stuck so
        // selection/drain does not keep burning probe on permanently tickless V4.
        if v4_pool_id.is_none() {
            return true;
        }
        return is_cl_tick_on_hydrate_cooldown(addr, v4_pool_id);
    }
    is_cl_tick_on_hydrate_cooldown(addr, None)
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ProbeTickHydrateStats {
    pub v3_total: usize,
    pub v3_needed: usize,
    pub v3_loaded: usize,
    pub v3_empty: usize,
    pub v3_incomplete: usize,
    pub v3_algebra_loaded: usize,
    pub v4_total: usize,
    pub v4_needed: usize,
    pub v4_loaded: usize,
    pub cycles_tickless_before: usize,
    pub cycles_tickless_after: usize,
}

fn cycle_has_tickless_cl(arena: &StateArena, cycle: &FoundCycle) -> bool {
    for edge in &cycle.edges {
        match (arena.pool_state(edge.pool_index), edge.protocol) {
            (Some(PoolState::V3(s)), crate::core::types::ProtocolType::UniswapV3)
            | (Some(PoolState::V4(s)), crate::core::types::ProtocolType::UniswapV4)
                if s.ticks.is_empty() =>
            {
                return true;
            }
            _ => {}
        }
    }
    false
}

fn count_tickless_cl_cycles<C: AsRef<FoundCycle>>(arena: &StateArena, cycles: &[C]) -> usize {
    cycles
        .iter()
        .filter(|c| cycle_has_tickless_cl(arena, c.as_ref()))
        .count()
}

/// True when every tickless V3 hop is on HF-exhausted cooldown (empty-tick **or**
/// probe_narrow_miss). Used to stale-cool ClTickless-only `NoSimulation` phantoms
/// that otherwise refill probe every tick after narrow hydrate skipped them
/// (live iter8: cl_tickless monopolized ranks while `near_net≈0`).
///
/// Prefer [`cycle_tickless_cl_hf_hydrate_exhausted`] when `pool_metas` is available
/// (covers V4/mixed residue; live iter18: V3-only left V4 phantoms re-probing).
///
/// Does **not** consult `pool_metas` / V4 pool_id — LF wide hydrate is unaffected
/// (probe_narrow_miss is HF-only and does not arm shared EMPTY).
#[cfg(test)]
#[must_use]
pub(crate) fn cycle_v3_tickless_hf_hydrate_exhausted(
    arena: &StateArena,
    cycle: &FoundCycle,
) -> bool {
    let mut saw_tickless = false;
    for edge in &cycle.edges {
        if edge.protocol != crate::core::types::ProtocolType::UniswapV3 {
            continue;
        }
        let tickless = matches!(
            arena.pool_state(edge.pool_index),
            Some(PoolState::V3(s)) if s.ticks.is_empty()
        );
        if !tickless {
            continue;
        }
        saw_tickless = true;
        let Some(addr) = arena.pool_address(edge.pool_index) else {
            return false;
        };
        if !is_empty_tick_on_cooldown(addr) && !is_probe_narrow_miss_on_cooldown(addr) {
            return false;
        }
    }
    saw_tickless
}

/// V3+V4 HF hydrate exhausted: every tickless CL hop is on empty-tick **or**
/// probe_narrow_miss (V4: empty_v4 / narrow_v4 / missing pool_id).
///
/// Used to stale-cool ClTickless-only `NoSimulation` phantoms after hydrate can
/// no longer unlock the route this cooldown window.
#[must_use]
pub(crate) fn cycle_tickless_cl_hf_hydrate_exhausted(
    arena: &StateArena,
    cycle: &FoundCycle,
    pool_metas: &[crate::pipeline::types::PoolMeta],
) -> bool {
    let mut saw_tickless = false;
    for edge in &cycle.edges {
        match edge.protocol {
            crate::core::types::ProtocolType::UniswapV3 => {
                let tickless = matches!(
                    arena.pool_state(edge.pool_index),
                    Some(PoolState::V3(s)) if s.ticks.is_empty()
                );
                if !tickless {
                    continue;
                }
                saw_tickless = true;
                let Some(addr) = arena.pool_address(edge.pool_index) else {
                    return false;
                };
                if !is_empty_tick_on_cooldown(addr) && !is_probe_narrow_miss_on_cooldown(addr) {
                    return false;
                }
            }
            crate::core::types::ProtocolType::UniswapV4 => {
                let tickless = matches!(
                    arena.pool_state(edge.pool_index),
                    Some(PoolState::V4(s)) if s.ticks.is_empty()
                );
                if !tickless {
                    continue;
                }
                saw_tickless = true;
                let Some(addr) = arena.pool_address(edge.pool_index) else {
                    return false;
                };
                let Some(pool_id) =
                    crate::pipeline::types::pool_meta_at(pool_metas, edge.pool_index)
                        .and_then(|meta| meta.pool_id)
                else {
                    // No pool_id → TickLens cannot target; treat hop as permanently stuck.
                    continue;
                };
                if !is_empty_v4_tick_on_cooldown(pool_id)
                    && !is_probe_narrow_miss_v4_on_cooldown(pool_id)
                    && !is_empty_tick_on_cooldown(addr)
                {
                    return false;
                }
            }
            _ => {}
        }
    }
    saw_tickless
}

/// Arm probe-narrow-miss on every currently tickless V3/V4 hop.
///
/// Closes the chicken-egg where ClTickless `NoSimulation` waited on
/// [`cycle_tickless_cl_hf_hydrate_exhausted`] but hops never entered cooldown
/// (hydrate gap / pool_cap truncation — live iter19: same fp every ~2s).
pub(crate) fn mark_cycle_tickless_cl_probe_miss(
    arena: &StateArena,
    cycle: &FoundCycle,
    pool_metas: &[crate::pipeline::types::PoolMeta],
) -> usize {
    let mut v3: Vec<Address> = Vec::new();
    let mut v4: Vec<alloy::primitives::FixedBytes<32>> = Vec::new();
    for edge in &cycle.edges {
        match edge.protocol {
            crate::core::types::ProtocolType::UniswapV3 => {
                let tickless = matches!(
                    arena.pool_state(edge.pool_index),
                    Some(PoolState::V3(s)) if s.ticks.is_empty()
                );
                if !tickless {
                    continue;
                }
                if let Some(addr) = arena.pool_address(edge.pool_index) {
                    v3.push(addr);
                }
            }
            crate::core::types::ProtocolType::UniswapV4 => {
                let tickless = matches!(
                    arena.pool_state(edge.pool_index),
                    Some(PoolState::V4(s)) if s.ticks.is_empty()
                );
                if !tickless {
                    continue;
                }
                if let Some(pool_id) =
                    crate::pipeline::types::pool_meta_at(pool_metas, edge.pool_index)
                        .and_then(|meta| meta.pool_id)
                {
                    v4.push(pool_id);
                }
            }
            _ => {}
        }
    }
    let n = v3.len() + v4.len();
    if !v3.is_empty() {
        crate::pipeline::tick_fetch::mark_probe_narrow_miss(v3);
    }
    if !v4.is_empty() {
        mark_probe_narrow_miss_v4(v4);
    }
    n
}

/// True when any V3/V4 hop's pool is on tick-miss cooldown.
///
/// Previously used at HF selection (over-pruned to `selected=0`); select now uses
/// [`cycle_tickless_cl_hf_hydrate_exhausted`].
#[cfg(test)]
pub(crate) fn cycle_has_cl_pool_on_miss_cooldown(
    arena: &StateArena,
    cycle: &FoundCycle,
    pool_metas: &[crate::pipeline::types::PoolMeta],
) -> bool {
    for edge in &cycle.edges {
        if !matches!(
            edge.protocol,
            crate::core::types::ProtocolType::UniswapV3
                | crate::core::types::ProtocolType::UniswapV4
        ) {
            continue;
        }
        if cl_pool_on_hydrate_cooldown(arena, pool_metas, edge.pool_index, edge.protocol) {
            return true;
        }
    }
    false
}

/// Drop cycles stuck on cooldown-empty CL ticks so probe/Brent budget goes to tradeable routes.
/// Returns how many cycles were removed. Empty remaining is fine (tick handles 0 cycles).
pub(crate) fn drain_cooldown_stuck_tickless_cycles(
    arena: &StateArena,
    cycles: &mut Vec<std::sync::Arc<FoundCycle>>,
    pool_metas: &[crate::pipeline::types::PoolMeta],
) -> usize {
    if cycles.is_empty() {
        return 0;
    }
    let before = cycles.len();
    // Empty-tick **or** probe-narrow-miss (iter20: ClTickless phantom cool arms
    // narrow-miss; empty-only drain left those cycles re-selected every tick).
    cycles.retain(|c| !cycle_tickless_cl_hf_hydrate_exhausted(arena, c.as_ref(), pool_metas));
    // Previously refused to drain when *every* cycle was stuck (kept.is_empty →
    // return 0), which left HF evaluating only dust-only phantoms for the tick.
    // Empty remaining is fine — the tick already handles cycles_considered=0.
    before.saturating_sub(cycles.len())
}

/// Collect tickless V3 addresses ranked by how many selected cycles they **fully
/// unlock** (sole remaining fetchable hop, no blocked siblings), then by cycle
/// membership, then liquidity.
///
/// Prior membership-only ranking hydrated hubs that left sibling hops empty →
/// live `v3_loaded=1` with `cycles_tickless=1→1` and probe `cl_tickless`.
fn tickless_v3_addresses_prioritized<C: AsRef<FoundCycle>>(
    arena: &StateArena,
    cycles: &[C],
    cap: usize,
) -> (usize, Vec<Address>) {
    use rustc_hash::FxHashMap;
    // addr → (sole_unlocks, membership_hits, liquidity)
    let mut freq: FxHashMap<Address, (u32, u32, u128)> = FxHashMap::default();
    for cycle in cycles {
        let mut fetchable: Vec<(Address, u128)> = Vec::new();
        let mut blocked_sibling = false;
        let mut seen: rustc_hash::FxHashSet<Address> = rustc_hash::FxHashSet::default();
        for edge in &cycle.as_ref().edges {
            if edge.protocol != crate::core::types::ProtocolType::UniswapV3 {
                // V4 tickless hops also block a "full unlock" this tick.
                if edge.protocol == crate::core::types::ProtocolType::UniswapV4 {
                    let tickless = matches!(
                        arena.pool_state(edge.pool_index),
                        Some(PoolState::V4(st)) if st.ticks.is_empty()
                    );
                    if tickless {
                        blocked_sibling = true;
                    }
                }
                continue;
            }
            let Some(addr) = arena.pool_address(edge.pool_index) else {
                continue;
            };
            let (tickless, liq) = match arena.pool_state(edge.pool_index) {
                Some(PoolState::V3(st)) => (st.ticks.is_empty(), st.liquidity),
                _ => (false, 0),
            };
            if !tickless || !seen.insert(addr) {
                continue;
            }
            if is_empty_tick_on_cooldown(addr) || is_probe_narrow_miss_on_cooldown(addr) {
                blocked_sibling = true;
                continue;
            }
            fetchable.push((addr, liq));
        }
        let sole = !blocked_sibling && fetchable.len() == 1;
        for (addr, liq) in fetchable {
            let e = freq.entry(addr).or_insert((0, 0, liq));
            if sole {
                e.0 = e.0.saturating_add(1);
            }
            e.1 = e.1.saturating_add(1);
            e.2 = e.2.max(liq);
        }
    }
    let all = freq.len();
    let mut ranked: Vec<(Address, u32, u32, u128)> = freq
        .into_iter()
        .map(|(addr, (sole, hits, liq))| (addr, sole, hits, liq))
        .collect();
    ranked.sort_unstable_by(|a, b| {
        b.1.cmp(&a.1)
            .then_with(|| b.2.cmp(&a.2))
            .then_with(|| b.3.cmp(&a.3))
    });
    let out: Vec<Address> = ranked.into_iter().take(cap).map(|(a, _, _, _)| a).collect();
    (all, out)
}

fn tickless_v4_targets_prioritized<C: AsRef<FoundCycle>>(
    arena: &StateArena,
    cycles: &[C],
    pool_metas: &[crate::pipeline::types::PoolMeta],
    cap: usize,
) -> (usize, Vec<(PoolIndex, alloy::primitives::FixedBytes<32>)>) {
    use rustc_hash::FxHashMap;
    // pool_index -> (pool_id, sole_unlocks, membership_hits, liquidity)
    let mut freq: FxHashMap<PoolIndex, (alloy::primitives::FixedBytes<32>, u32, u32, u128)> =
        FxHashMap::default();
    for cycle in cycles {
        let mut fetchable: Vec<(PoolIndex, alloy::primitives::FixedBytes<32>, u128)> = Vec::new();
        let mut blocked_sibling = false;
        let mut seen: rustc_hash::FxHashSet<PoolIndex> = rustc_hash::FxHashSet::default();
        for edge in &cycle.as_ref().edges {
            match edge.protocol {
                crate::core::types::ProtocolType::UniswapV3 => {
                    // Any tickless V3 sibling means this V4 hop alone cannot unlock.
                    if matches!(
                        arena.pool_state(edge.pool_index),
                        Some(PoolState::V3(st)) if st.ticks.is_empty()
                    ) {
                        blocked_sibling = true;
                    }
                }
                crate::core::types::ProtocolType::UniswapV4 => {
                    let idx = edge.pool_index;
                    let (tickless, pool_id, liq) = match arena.pool_state(idx) {
                        Some(PoolState::V4(st)) => {
                            let pid = crate::pipeline::types::pool_meta_at(pool_metas, idx)
                                .and_then(|meta| meta.pool_id);
                            (st.ticks.is_empty(), pid, st.liquidity)
                        }
                        _ => (false, None, 0),
                    };
                    if !tickless || !seen.insert(idx) {
                        continue;
                    }
                    let Some(pool_id) = pool_id else {
                        blocked_sibling = true;
                        continue;
                    };
                    let addr = arena.pool_address(idx);
                    let is_on_cooldown = is_empty_v4_tick_on_cooldown(pool_id)
                        || is_probe_narrow_miss_v4_on_cooldown(pool_id)
                        || addr.is_some_and(is_empty_tick_on_cooldown);
                    if is_on_cooldown {
                        blocked_sibling = true;
                        continue;
                    }
                    fetchable.push((idx, pool_id, liq));
                }
                _ => {}
            }
        }
        let sole = !blocked_sibling && fetchable.len() == 1;
        for (idx, pool_id, liq) in fetchable {
            let e = freq.entry(idx).or_insert((pool_id, 0, 0, liq));
            if sole {
                e.1 = e.1.saturating_add(1);
            }
            e.2 = e.2.saturating_add(1);
            e.3 = e.3.max(liq);
        }
    }
    let all = freq.len();
    let mut ranked: Vec<(PoolIndex, alloy::primitives::FixedBytes<32>, u32, u32, u128)> = freq
        .into_iter()
        .map(|(idx, (pool_id, sole, hits, liq))| (idx, pool_id, sole, hits, liq))
        .collect();
    ranked.sort_unstable_by(|a, b| {
        b.2.cmp(&a.2)
            .then_with(|| b.3.cmp(&a.3))
            .then_with(|| b.4.cmp(&a.4))
    });
    let out: Vec<(PoolIndex, alloy::primitives::FixedBytes<32>)> = ranked
        .into_iter()
        .take(cap)
        .map(|(idx, pid, _, _, _)| (idx, pid))
        .collect();
    (all, out)
}

/// On probe-tick hydrate timeout the enrich future is dropped before it can
/// mark misses, so the next tick would re-burn the whole budget on the same
/// pools. Briefly cooldown still-tickless hops; live pools recover on the next
/// completed attempt. Returns how many were cooled.
///
/// `cap` must match the set `hydrate_tickless_cl_for_cycles` attempted (budget-
/// scaled). Using the hard 64-cap here used to cool pools never in-flight.
///
/// Does **not** re-rank via [`tickless_cl_targets_shared_cap`]: that skips
/// `probe_narrow_miss` pools armed mid-pass before the outer timeout fires
/// (live iter21: 21/25 timeouts → cooled 0 → same tickless re-selected).
pub(crate) fn mark_probe_hydrate_timeout_cooldown<C: AsRef<FoundCycle>>(
    arena: &StateArena,
    cycles: &[C],
    pool_metas: &[crate::pipeline::types::PoolMeta],
    cap: usize,
) -> usize {
    if cap == 0 {
        return 0;
    }
    use rustc_hash::FxHashSet;
    let mut v3: Vec<Address> = Vec::new();
    let mut v4: Vec<alloy::primitives::FixedBytes<32>> = Vec::new();
    let mut seen_v3 = FxHashSet::default();
    let mut seen_v4 = FxHashSet::default();
    for cycle in cycles {
        for edge in &cycle.as_ref().edges {
            match edge.protocol {
                crate::core::types::ProtocolType::UniswapV3 => {
                    let tickless = matches!(
                        arena.pool_state(edge.pool_index),
                        Some(PoolState::V3(s)) if s.ticks.is_empty()
                    );
                    if !tickless {
                        continue;
                    }
                    let Some(addr) = arena.pool_address(edge.pool_index) else {
                        continue;
                    };
                    // EMPTY already has a longer cool — timeout cool is redundant.
                    if is_empty_tick_on_cooldown(addr) || !seen_v3.insert(addr) {
                        continue;
                    }
                    v3.push(addr);
                }
                crate::core::types::ProtocolType::UniswapV4 => {
                    let tickless = matches!(
                        arena.pool_state(edge.pool_index),
                        Some(PoolState::V4(s)) if s.ticks.is_empty()
                    );
                    if !tickless {
                        continue;
                    }
                    let Some(pool_id) =
                        crate::pipeline::types::pool_meta_at(pool_metas, edge.pool_index)
                            .and_then(|meta| meta.pool_id)
                    else {
                        continue;
                    };
                    if is_empty_v4_tick_on_cooldown(pool_id)
                        || arena
                            .pool_address(edge.pool_index)
                            .is_some_and(is_empty_tick_on_cooldown)
                        || !seen_v4.insert(pool_id)
                    {
                        continue;
                    }
                    v4.push(pool_id);
                }
                _ => {}
            }
            if v3.len() + v4.len() >= cap {
                break;
            }
        }
        if v3.len() + v4.len() >= cap {
            break;
        }
    }
    v3.truncate(cap);
    let v4_cap = cap.saturating_sub(v3.len());
    v4.truncate(v4_cap);
    mark_tick_hydrate_timeout_cooldown(v3.iter().copied());
    mark_v4_tick_hydrate_timeout_cooldown(v4.iter().copied());
    v3.len() + v4.len()
}

/// Split `pool_cap` across V3+V4. Prefer V3 but reserve ≥1 V4 slot when both
/// families need work (live iter2: v4_total>0 while v4_fetch=0 under full V3 cap).
fn tickless_cl_targets_shared_cap<C: AsRef<FoundCycle>>(
    arena: &StateArena,
    cycles: &[C],
    pool_metas: &[crate::pipeline::types::PoolMeta],
    pool_cap: usize,
) -> (
    usize,
    Vec<Address>,
    usize,
    Vec<(PoolIndex, alloy::primitives::FixedBytes<32>)>,
) {
    let (v3_total, mut v3) = tickless_v3_addresses_prioritized(arena, cycles, pool_cap);
    let (v4_total, mut v4) = tickless_v4_targets_prioritized(arena, cycles, pool_metas, pool_cap);
    let v4_reserve = usize::from(v4_total > 0 && v3_total > 0 && pool_cap >= 2);
    v3.truncate(pool_cap.saturating_sub(v4_reserve));
    let v4_cap = pool_cap.saturating_sub(v3.len());
    v4.truncate(v4_cap);
    (v3_total, v3, v4_total, v4)
}

/// Still-tickless hops on cycles that already have ≥1 hydrated CL hop.
///
/// Primary ranking skips `probe_narrow_miss` pools, so a hub can load while the
/// sibling stays cooled → live `v3_loaded=N` with `cycles_tickless=1→1`. Bypass
/// narrow-miss here; still respect empty-tick cooldowns (wide already failed).
fn sibling_completion_cl_targets<C: AsRef<FoundCycle>>(
    arena: &StateArena,
    cycles: &[C],
    pool_metas: &[crate::pipeline::types::PoolMeta],
    cap: usize,
) -> (
    Vec<Address>,
    Vec<(PoolIndex, alloy::primitives::FixedBytes<32>)>,
) {
    use rustc_hash::FxHashMap;
    // addr → (sole_remaining, hits, liq)
    let mut v3_freq: FxHashMap<Address, (u32, u32, u128)> = FxHashMap::default();
    // pool_index → (pool_id, sole_remaining, hits, liq)
    let mut v4_freq: FxHashMap<PoolIndex, (alloy::primitives::FixedBytes<32>, u32, u32, u128)> =
        FxHashMap::default();

    for cycle in cycles {
        let edges = &cycle.as_ref().edges;
        let mut has_hydrated_cl = false;
        let mut v3_rem: Vec<(Address, u128)> = Vec::new();
        let mut v4_rem: Vec<(PoolIndex, alloy::primitives::FixedBytes<32>, u128)> = Vec::new();
        let mut seen_v3: rustc_hash::FxHashSet<Address> = rustc_hash::FxHashSet::default();
        let mut seen_v4: rustc_hash::FxHashSet<PoolIndex> = rustc_hash::FxHashSet::default();

        for edge in edges {
            match edge.protocol {
                crate::core::types::ProtocolType::UniswapV3 => {
                    let Some(addr) = arena.pool_address(edge.pool_index) else {
                        continue;
                    };
                    match arena.pool_state(edge.pool_index) {
                        Some(PoolState::V3(st)) if !st.ticks.is_empty() => {
                            has_hydrated_cl = true;
                        }
                        Some(PoolState::V3(st)) if st.ticks.is_empty() => {
                            // Wide-empty already failed recently — don't re-hammer.
                            if is_empty_tick_on_cooldown(addr) {
                                continue;
                            }
                            if seen_v3.insert(addr) {
                                v3_rem.push((addr, st.liquidity));
                            }
                        }
                        _ => {}
                    }
                }
                crate::core::types::ProtocolType::UniswapV4 => {
                    let idx = edge.pool_index;
                    match arena.pool_state(idx) {
                        Some(PoolState::V4(st)) if !st.ticks.is_empty() => {
                            has_hydrated_cl = true;
                        }
                        Some(PoolState::V4(st)) if st.ticks.is_empty() => {
                            let Some(pool_id) =
                                crate::pipeline::types::pool_meta_at(pool_metas, idx)
                                    .and_then(|meta| meta.pool_id)
                            else {
                                continue;
                            };
                            if is_empty_v4_tick_on_cooldown(pool_id)
                                || arena
                                    .pool_address(idx)
                                    .is_some_and(is_empty_tick_on_cooldown)
                            {
                                continue;
                            }
                            if seen_v4.insert(idx) {
                                v4_rem.push((idx, pool_id, st.liquidity));
                            }
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }

        if !has_hydrated_cl || (v3_rem.is_empty() && v4_rem.is_empty()) {
            continue;
        }
        let sole = v3_rem.len() + v4_rem.len() == 1;
        for (addr, liq) in v3_rem {
            let e = v3_freq.entry(addr).or_insert((0, 0, liq));
            if sole {
                e.0 = e.0.saturating_add(1);
            }
            e.1 = e.1.saturating_add(1);
            e.2 = e.2.max(liq);
        }
        for (idx, pool_id, liq) in v4_rem {
            let e = v4_freq.entry(idx).or_insert((pool_id, 0, 0, liq));
            if sole {
                e.1 = e.1.saturating_add(1);
            }
            e.2 = e.2.saturating_add(1);
            e.3 = e.3.max(liq);
        }
    }

    let mut v3_ranked: Vec<(Address, u32, u32, u128)> = v3_freq
        .into_iter()
        .map(|(addr, (sole, hits, liq))| (addr, sole, hits, liq))
        .collect();
    v3_ranked.sort_unstable_by(|a, b| {
        b.1.cmp(&a.1)
            .then_with(|| b.2.cmp(&a.2))
            .then_with(|| b.3.cmp(&a.3))
    });
    let mut v4_ranked: Vec<(PoolIndex, alloy::primitives::FixedBytes<32>, u32, u32, u128)> =
        v4_freq
            .into_iter()
            .map(|(idx, (pid, sole, hits, liq))| (idx, pid, sole, hits, liq))
            .collect();
    v4_ranked.sort_unstable_by(|a, b| {
        b.2.cmp(&a.2)
            .then_with(|| b.3.cmp(&a.3))
            .then_with(|| b.4.cmp(&a.4))
    });

    // Prefer sole-remaining V3, reserve ≥1 V4 slot when both need work.
    let v4_reserve = usize::from(!v4_ranked.is_empty() && !v3_ranked.is_empty() && cap >= 2);
    let v3_cap = cap.saturating_sub(v4_reserve);
    let v3: Vec<Address> = v3_ranked
        .into_iter()
        .take(v3_cap)
        .map(|(a, _, _, _)| a)
        .collect();
    let v4_cap = cap.saturating_sub(v3.len());
    let v4: Vec<(PoolIndex, alloy::primitives::FixedBytes<32>)> = v4_ranked
        .into_iter()
        .take(v4_cap)
        .map(|(idx, pid, _, _, _)| (idx, pid))
        .collect();
    (v3, v4)
}

/// Hydrate empty V3/V4 tick arrays on selected HF cycles before probe ranking.
/// Hot-cache refresh drops ticks when price/liquidity moves; without this, those
/// routes classify as `cl_tickless` and never reach Brent/economics.
///
/// `pool_cap` limits how many tickless pools to fetch this tick (budget-scaled).
/// `budget` bounds sequential wide/sibling passes so they finish under the outer
/// `timeout` instead of burning 1500ms then cooling 0 (live iter21).
pub(crate) struct TicklessHydrateBudget {
    pub word_range: i16,
    pub block_number: Option<u64>,
    pub pool_cap: usize,
    pub budget: std::time::Duration,
}

pub(crate) async fn hydrate_tickless_cl_for_cycles<C: AsRef<FoundCycle>>(
    rpc: &RpcPool,
    arena: &mut StateArena,
    cycles: &[C],
    pool_metas: &[crate::pipeline::types::PoolMeta],
    params: TicklessHydrateBudget,
) -> ProbeTickHydrateStats {
    let TicklessHydrateBudget {
        word_range,
        block_number,
        pool_cap,
        budget,
    } = params;
    let mut stats = ProbeTickHydrateStats {
        cycles_tickless_before: count_tickless_cl_cycles(arena, cycles),
        ..ProbeTickHydrateStats::default()
    };
    if cycles.is_empty() || pool_cap == 0 {
        stats.cycles_tickless_after = stats.cycles_tickless_before;
        return stats;
    }
    let hydrate_started = std::time::Instant::now();
    let one_pool = std::time::Duration::from_millis(HF_PROBE_TICK_MS_PER_POOL);
    let budget_allows_pass = || hydrate_started.elapsed() + one_pool <= budget;
    let (v3_total, v3, v4_total, v4) =
        tickless_cl_targets_shared_cap(arena, cycles, pool_metas, pool_cap);
    stats.v3_total = v3_total;
    stats.v3_needed = v3.len();
    stats.v4_total = v4_total;
    stats.v4_needed = v4.len();
    if stats.v3_needed == 0 && stats.v4_needed == 0 {
        stats.cycles_tickless_after = stats.cycles_tickless_before;
        return stats;
    }
    let (algebra_pools, algebra_integral_pools) =
        crate::pipeline::tick_fetch::collect_algebra_pools(arena, pool_metas, &v3);
    // Targets are already tickless — skip clear (was a no-op for empty; keep
    // any partial ticks from incomplete prior loads if we ever expand selection).
    // Probe: fewer bitmap words finish inside residual (default word_range=10 → 21
    // multicall items/pool; 4 → 9). Partial depth still unlocks probe rank.
    // V4 previously used full word_range + unconditional widen — free-tier 429s
    // on 27-pool LF batches poisoned the shared RPC budget for HF probe too.
    let probe_word_range = word_range.min(4);
    let pass = hydrate_cl_ticks_with_rpc_fallback(
        rpc,
        arena,
        &v3,
        &v4,
        &algebra_pools,
        &algebra_integral_pools,
        probe_word_range,
        probe_word_range,
        false, // probe residual budget — narrow only (V3 + V4)
        block_number,
        "hf probe-tick hydrate",
    )
    .await;
    stats.v3_loaded = pass.v3_loaded;
    stats.v3_empty = pass.v3_empty;
    stats.v3_incomplete = pass.v3_incomplete;
    stats.v3_algebra_loaded = pass.v3_algebra_loaded;
    stats.v4_loaded = pass.v4_loaded;
    stats.cycles_tickless_after = count_tickless_cl_cycles(arena, cycles);

    // Narrow word_range≤4 often misses sparse liquidity (live: v3_loaded=0 empty=N
    // with cycles_tickless=N→N). Wide TickLens — same widen formula as LF
    // (`word*3` clamped 24..48) — without re-widening the whole probe batch.
    // Use the already-ranked `v3` list: probe_narrow_miss just armed and would
    // exclude these from a fresh `tickless_v3_addresses_prioritized` pass.
    // Also when the batch partially loaded (live: v3_loaded=1 empty=1) — widen the
    // remaining empty sole-unlocks, not only the all-empty case.
    // Cap 8: iter15 still had 31/61 hydrates 1→1 with cap=4; ranked batch is already
    // pool_cap-bounded so wider retry stays inside the probe RPC budget.
    const PROBE_WIDE_EMPTY_RETRY_CAP: usize = 8;
    if budget_allows_pass() && stats.v3_empty > 0 && stats.cycles_tickless_after > 0 {
        let wide_range = probe_wide_tick_word_range(probe_word_range);
        let retry: Vec<Address> = v3
            .iter()
            .copied()
            .filter(|&addr| {
                arena
                    .address_to_pool()
                    .get(&addr)
                    .and_then(|&idx| arena.pool_state(idx))
                    .is_some_and(|st| matches!(st, PoolState::V3(s) if s.ticks.is_empty()))
            })
            .take(PROBE_WIDE_EMPTY_RETRY_CAP)
            .collect();
        if !retry.is_empty() {
            let (alg, alg_int) =
                crate::pipeline::tick_fetch::collect_algebra_pools(arena, pool_metas, &retry);
            let wide = hydrate_cl_ticks_with_rpc_fallback(
                rpc,
                arena,
                &retry,
                &[],
                &alg,
                &alg_int,
                wide_range,
                wide_range,
                true, // allow Algebra/TickLens widen after the wide seed
                block_number,
                "hf probe-tick hydrate wide",
            )
            .await;
            stats.v3_loaded = stats.v3_loaded.saturating_add(wide.v3_loaded);
            stats.v3_algebra_loaded = stats
                .v3_algebra_loaded
                .saturating_add(wide.v3_algebra_loaded);
            let unlocked = wide.v3_loaded.saturating_add(wide.v3_algebra_loaded);
            if unlocked > 0 {
                stats.v3_empty = stats.v3_empty.saturating_sub(unlocked);
            }
            stats.v3_incomplete = stats.v3_incomplete.saturating_add(wide.v3_incomplete);
            stats.cycles_tickless_after = count_tickless_cl_cycles(arena, cycles);
        }
    }

    // Mirror V3: mixed V3+V4 cycles stay tickless when V3 loads but V4 narrow
    // (word≤4) misses sparse liquidity (live: v3_loaded=2 v4_fetch=1 v4_loaded=0,
    // cycles_tickless=1→1 → cl_tickless). Also when partial V4 load leaves a sibling
    // empty (live iter12: gate was v4_loaded==0 only — skipped still-empty widen).
    // Reuse pre-ranked `v4` — probe_narrow_miss_v4 just armed and would exclude
    // them from a fresh prioritize pass.
    if budget_allows_pass() && stats.v4_needed > 0 && stats.cycles_tickless_after > 0 {
        let wide_range = probe_wide_tick_word_range(probe_word_range);
        let retry: Vec<(PoolIndex, alloy::primitives::FixedBytes<32>)> = v4
            .iter()
            .copied()
            .filter(|&(idx, _)| {
                matches!(arena.pool_state(idx), Some(PoolState::V4(s)) if s.ticks.is_empty())
            })
            .take(PROBE_WIDE_EMPTY_RETRY_CAP)
            .collect();
        if !retry.is_empty() {
            let empty_alg = rustc_hash::FxHashSet::default();
            let wide = hydrate_cl_ticks_with_rpc_fallback(
                rpc,
                arena,
                &[],
                &retry,
                &empty_alg,
                &empty_alg,
                wide_range,
                wide_range,
                true,
                block_number,
                "hf probe-tick hydrate v4 wide",
            )
            .await;
            stats.v4_loaded = stats.v4_loaded.saturating_add(wide.v4_loaded);
            stats.cycles_tickless_after = count_tickless_cl_cycles(arena, cycles);
        }
    }

    // Partial unlock residue (live iter17: 36× cycles_tickless=1→1 with v3_loaded≥1
    // while a sibling stayed on probe_narrow_miss). Finish those hops with wide depth;
    // narrow-miss bypass lives in `sibling_completion_cl_targets`.
    const PROBE_SIBLING_COMPLETION_CAP: usize = 4;
    if budget_allows_pass() && stats.cycles_tickless_after > 0 {
        let (sib_v3, sib_v4) =
            sibling_completion_cl_targets(arena, cycles, pool_metas, PROBE_SIBLING_COMPLETION_CAP);
        if !sib_v3.is_empty() || !sib_v4.is_empty() {
            let wide_range = probe_wide_tick_word_range(probe_word_range);
            let (alg, alg_int) =
                crate::pipeline::tick_fetch::collect_algebra_pools(arena, pool_metas, &sib_v3);
            let wide = hydrate_cl_ticks_with_rpc_fallback(
                rpc,
                arena,
                &sib_v3,
                &sib_v4,
                &alg,
                &alg_int,
                wide_range,
                wide_range,
                true,
                block_number,
                "hf probe-tick hydrate sibling",
            )
            .await;
            stats.v3_loaded = stats.v3_loaded.saturating_add(wide.v3_loaded);
            stats.v3_algebra_loaded = stats
                .v3_algebra_loaded
                .saturating_add(wide.v3_algebra_loaded);
            stats.v3_empty = stats.v3_empty.saturating_add(wide.v3_empty);
            stats.v3_incomplete = stats.v3_incomplete.saturating_add(wide.v3_incomplete);
            stats.v4_loaded = stats.v4_loaded.saturating_add(wide.v4_loaded);
            stats.v3_needed = stats.v3_needed.saturating_add(sib_v3.len());
            stats.v4_needed = stats.v4_needed.saturating_add(sib_v4.len());
            stats.cycles_tickless_after = count_tickless_cl_cycles(arena, cycles);
        }
    }

    // Still-tickless after a real fetch attempt used to wait for probe NoSimulation
    // before probe-miss armed (live iter20: stuck11=21 while nosim already cooled).
    // Arm now so `drain_cooldown_stuck_tickless_cycles` clears phantoms this tick.
    if stats.cycles_tickless_after > 0 && (stats.v3_needed > 0 || stats.v4_needed > 0) {
        let mut marked = 0usize;
        for cycle in cycles {
            if cycle_has_tickless_cl(arena, cycle.as_ref()) {
                marked = marked.saturating_add(mark_cycle_tickless_cl_probe_miss(
                    arena,
                    cycle.as_ref(),
                    pool_metas,
                ));
            }
        }
        if marked > 0 {
            crate::debug!(
                "hf probe-tick hydrate: armed probe-miss on {marked} still-tickless hops (cycles_tickless={})",
                stats.cycles_tickless_after
            );
        }
    }
    stats
}

/// LF-matched widen depth for a post-narrow sole-unlock TickLens retry.
#[inline]
#[must_use]
pub(crate) fn probe_wide_tick_word_range(narrow: i16) -> i16 {
    narrow.saturating_mul(3).clamp(24, 48)
}

async fn enrich_dispatch_cl_ticks(
    rpc: &RpcPool,
    arena: &mut StateArena,
    cycles: &[&FoundCycle],
    pool_metas: &[crate::pipeline::types::PoolMeta],
    word_range: i16,
    block_number: Option<u64>,
) {
    if cycles.is_empty() {
        return;
    }
    // Mirror LF: only hydrate pools that are still tickless after route refresh.
    let tick_pools = still_tickless_v3(arena, &collect_v3_pool_addresses(arena, cycles));
    let v4_targets = still_tickless_v4(arena, &collect_v4_tick_targets(cycles, pool_metas));
    if tick_pools.is_empty() && v4_targets.is_empty() {
        return;
    }
    let snapshots = dispatch_cl_tick_snapshot(arena, &tick_pools, &v4_targets);
    let (algebra_pools, algebra_integral_pools) =
        crate::pipeline::tick_fetch::collect_algebra_pools(arena, pool_metas, &tick_pools);
    crate::pipeline::tick_fetch::clear_v3_pool_ticks(arena, &tick_pools);
    crate::pipeline::tick_fetch::clear_v4_pool_ticks(arena, &v4_targets);
    let pass = hydrate_cl_ticks_with_rpc_fallback(
        rpc,
        arena,
        &tick_pools,
        &v4_targets,
        &algebra_pools,
        &algebra_integral_pools,
        word_range,
        word_range,
        true, // dispatch path can afford widen
        block_number,
        "dispatch tick hydration",
    )
    .await;
    if pass.v3_pending || pass.v4_pending {
        restore_dispatch_cl_ticks(arena, snapshots);
        crate::warn!(
            "dispatch tick hydration failed on all state RPCs — retained prior ticks for still-empty pools"
        );
    }
}

/// Refresh route pools and re-sim before vault verification (stale BAL state → phantom profit).
pub(crate) async fn refresh_and_resim_profitable(
    refresh: &crate::services::state_refresh::StateRefreshService,
    cache: &crate::services::state_cache::StateCache,
    arena: &mut StateArena,
    profitable: Vec<HfEvalResult>,
    reassess: &HfEvalInputOwned,
) -> (Vec<HfEvalResult>, bool, u64) {
    if profitable.is_empty() {
        return (profitable, false, cache.generation());
    }
    let batch_started = crate::util::now_ms();
    let in_count = profitable.len();
    let pools = collect_route_pool_addresses(arena, &profitable);
    let pool_count = pools.len();
    let mut pools_refreshed = false;
    let mut state_generation = cache.generation();
    let refresh_started = crate::util::now_ms();
    if !pools.is_empty() {
        match refresh_route_pools_into_arena(refresh, cache, arena, &pools).await {
            Ok((fetched, generation)) => {
                state_generation = generation;
                pools_refreshed = fetched;
            }
            Err(RoutePoolRefreshAbort::NotIndexed { .. }) => {
                crate::warn!(
                    "resim aborted: route pools not in discovery index ({pool_count} pools) — dropping {in_count} candidates"
                );
                return (Vec::new(), false, state_generation);
            }
            Err(RoutePoolRefreshAbort::NoUpdates { pool_count }) => {
                crate::warn!(
                    "resim aborted: route pool refresh 0/{pool_count} — dropping {in_count} candidates"
                );
                return (Vec::new(), false, state_generation);
            }
            Err(RoutePoolRefreshAbort::Rpc(e)) => {
                crate::warn!(
                    "resim aborted: route pool refresh failed ({e:#}) — dropping {in_count} candidates"
                );
                return (Vec::new(), false, state_generation);
            }
        }
    }
    let refresh_ms = crate::util::now_ms().saturating_sub(refresh_started);
    let route_gas = RouteGasLookup::for_fingerprints(
        reassess.gas_oracle.as_ref(),
        profitable.iter().map(|r| r.route_fingerprint),
    );
    let eval = reassess.as_eval_input(&route_gas);
    let flash = reassess.flash_liquidity.load();
    let flash_ttl = reassess.flash_liquidity.ttl();
    let mut resim_unprofitable = 0usize;
    let mut resim_failed = 0usize;
    let mut resim_profit_drift = 0usize;
    let mut resim_hop_fidelity = 0usize;
    let mut reassess_reject = 0usize;
    let filtered: Vec<HfEvalResult> = profitable
        .into_iter()
        .filter_map(|mut result| {
            let baseline = result.sim;
            let amount = result.opt.optimal_input;
            let hop_caps = local_sim::precompute_route_shallow_caps(arena, &result.cycle.edges);
            let Some(refreshed) = simulate_route_detailed_with_caps(
                arena,
                &result.cycle.edges,
                amount,
                hop_caps.as_ref(),
            ) else {
                resim_failed += 1;
                crate::debug!("resim gate sim: fp={} failed", result.route_fingerprint);
                return None;
            };
            if refreshed.profit.is_zero() {
                resim_unprofitable += 1;
                return None;
            }
            let mut resim_profile = local_sim::ResimFidelityProfile::default();
            if let Some(reason) = local_sim::route_resim_fidelity_reject_profiled(
                &baseline,
                &refreshed,
                &mut resim_profile,
            ) {
                resim_profit_drift += 1;
                crate::debug!(
                    "resim gate profit: fp={} reason={reason} profit_drift_bps={}",
                    result.route_fingerprint,
                    resim_profile.profit_drift_bps,
                );
                return None;
            }
            let mut hop_profile = local_sim::HopFidelityProfile::default();
            if let Some(reject) = local_sim::route_hop_fidelity_reject_profiled(
                arena,
                &result.cycle.edges,
                &refreshed.hop_amounts,
                Some(&mut hop_profile),
                true,
                hop_caps.as_ref(),
            ) {
                resim_hop_fidelity += 1;
                crate::debug!(
                    "resim gate hop: fp={} reject={reject:?} cl_depth_sims={}",
                    result.route_fingerprint,
                    hop_profile.cl_depth_sims,
                );
                return None;
            }
            result.sim = refreshed;
            let flash_source = resolve_flash_source_for_cycle(
                &result.cycle,
                arena,
                &flash,
                flash_ttl,
                reassess.flash_policy,
                result.sim.amount_in,
            )?;
            let assessment = reassess_hf_eval_result(&result, &eval, flash_source)?;
            if !assessment.should_execute {
                reassess_reject += 1;
                return None;
            }
            result.assessment = assessment;
            result.flash_source = flash_source;
            Some(result)
        })
        .collect();
    let out_count = filtered.len();
    let total_ms = crate::util::now_ms().saturating_sub(batch_started);
    if in_count > 0 && out_count < in_count {
        crate::info!(
            "resim gate: in={in_count} out={out_count} pools={pool_count} refreshed={pools_refreshed} refresh_ms={refresh_ms} \
             sim_failed={resim_failed} unprofitable={resim_unprofitable} profit_drift={resim_profit_drift} hop_fidelity={resim_hop_fidelity} reassess={reassess_reject} total_ms={total_ms}"
        );
    } else if in_count > 0 {
        crate::debug!(
            "resim gate: in={in_count} out={out_count} pools={pool_count} refreshed={pools_refreshed} refresh_ms={refresh_ms} total_ms={total_ms}"
        );
    }
    (filtered, pools_refreshed, state_generation)
}

const BATCH_VERIFY_CONCURRENCY: usize = 3;

struct BalancerVerifyJob {
    result: HfEvalResult,
    hops: Vec<crate::services::execution::calldata::CalldataHop>,
    start_token: alloy::primitives::Address,
    slippage: u64,
}

/// Drop Balancer-only candidates whose vault `queryBatchSwap` disagrees with local sim.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn filter_balancer_onchain_verified<
    P: Provider<Ethereum> + Clone + Send + 'static,
>(
    execution: Arc<crate::services::execution::ExecutionService>,
    arena: &StateArena,
    candidates: Vec<HfEvalResult>,
    sim_provider: &P,
    executor: alloy::primitives::Address,
    operator: alloy::primitives::Address,
    pool_metas: &[crate::pipeline::types::PoolMeta],
    slippage_bps: u64,
    reassess: Arc<HfEvalInputOwned>,
    state_block: u64,
) -> Vec<HfEvalResult> {
    let pool_metas_by_pool: FxHashMap<
        crate::core::types::PoolIndex,
        &crate::pipeline::types::PoolMeta,
    > = pool_metas.iter().map(|m| (m.pool_index, m)).collect();

    let (balancer_only, passthrough): (Vec<_>, Vec<_>) = candidates
        .into_iter()
        .partition(|result| route_is_balancer_only(&result.cycle));

    let passthrough_n = u32::try_from(passthrough.len()).unwrap_or(u32::MAX);
    let mut already_verified = Vec::new();
    let mut need_onchain_verify = Vec::with_capacity(balancer_only.len());
    for result in balancer_only {
        if result.balancer_batch_verified {
            already_verified.push(result);
        } else {
            need_onchain_verify.push(result);
        }
    }
    let skip_verified_n = u32::try_from(already_verified.len()).unwrap_or(u32::MAX);

    let mut jobs = Vec::with_capacity(need_onchain_verify.len());
    for result in need_onchain_verify {
        let fp = result.route_fingerprint;
        let Some(start_token) = arena.token_address(result.cycle.start_token) else {
            execution.quarantine_batch_query_failure(fp);
            record_balancer_batch_reject(BalancerBatchReject::MissingStartToken);
            crate::debug!("balancer: batch_filter fp={fp} reject=missing_start_token");
            continue;
        };
        if execution.cycle_has_quarantined_token(arena, &result.cycle.edges) {
            execution.quarantine_batch_query_failure(fp);
            record_balancer_batch_reject(BalancerBatchReject::ZeroRealized);
            crate::debug!(
                "balancer: batch_filter fp={fp} reject=token_quarantined start={start_token}"
            );
            continue;
        }
        let hops = match build_calldata_hops(
            arena,
            &result.cycle.edges,
            &result.sim.hop_amounts,
            &pool_metas_by_pool,
        ) {
            Ok(h) => h,
            Err(reason) => {
                execution.quarantine_batch_query_failure(fp);
                record_balancer_batch_reject(BalancerBatchReject::CalldataBuildFailed);
                crate::debug!(
                    "balancer: batch_filter fp={fp} reject=calldata_build reason={reason} modeled={} net_matic={}",
                    result.sim.profit,
                    result.assessment.net_profit_after_gas_matic_wei,
                );
                continue;
            }
        };
        if !crate::services::execution::balancer_verify::balancer_batch_within_max_in_ratio(
            arena, &hops,
        ) {
            execution.quarantine_batch_query_failure(fp);
            record_balancer_batch_reject(BalancerBatchReject::MaxInRatio);
            crate::debug!(
                "balancer: batch_filter fp={fp} reject=max_in_ratio modeled={} net_matic={}",
                result.sim.profit,
                result.assessment.net_profit_after_gas_matic_wei,
            );
            continue;
        }
        jobs.push(BalancerVerifyJob {
            slippage: result.effective_slippage_bps.max(slippage_bps),
            result,
            hops,
            start_token,
        });
    }

    let jobs_n = u32::try_from(jobs.len()).unwrap_or(u32::MAX);
    let route_gas = RouteGasLookup::for_fingerprints(
        reassess.gas_oracle.as_ref(),
        jobs.iter().map(|job| job.result.route_fingerprint),
    );
    // JoinSet matches pool_fetch/rpc concurrency — true runtime tasks, not local Stream poll.
    let mut tasks = JoinSet::new();
    let mut job_iter = jobs.into_iter();
    let mut verified_balancer = Vec::new();
    loop {
        while tasks.len() < BATCH_VERIFY_CONCURRENCY {
            let Some(job) = job_iter.next() else {
                break;
            };
            let sim_provider = sim_provider.clone();
            let execution = Arc::clone(&execution);
            let reassess = Arc::clone(&reassess);
            let route_gas = route_gas.clone();
            tasks.spawn(async move {
                let eval = reassess.as_eval_input(&route_gas);
                verify_balancer_batch_job(
                    execution.as_ref(),
                    job,
                    &sim_provider,
                    executor,
                    operator,
                    &eval,
                    &route_gas,
                    state_block,
                )
                .await
            });
        }
        let Some(joined) = tasks.join_next().await else {
            break;
        };
        match joined {
            Ok(Some(result)) => verified_balancer.push(result),
            Ok(None) => {}
            Err(e) => crate::warn!("balancer batch verify task failed: {e}"),
        }
    }

    record_balancer_filter_window(jobs_n, skip_verified_n, passthrough_n);
    log_balancer_batch_filter_summary();

    let mut verified = passthrough;
    verified.extend(verified_balancer);
    verified.extend(already_verified);
    verified
}

#[allow(clippy::too_many_arguments)]
async fn verify_balancer_batch_job<P: Provider<Ethereum>>(
    execution: &crate::services::execution::ExecutionService,
    job: BalancerVerifyJob,
    sim_provider: &P,
    executor: alloy::primitives::Address,
    operator: alloy::primitives::Address,
    eval: &HfEvalInput<'_>,
    _route_gas: &RouteGasLookup,
    state_block: u64,
) -> Option<HfEvalResult> {
    let BalancerVerifyJob {
        result,
        hops,
        start_token,
        slippage,
    } = job;
    let fp = result.route_fingerprint;
    let query_block = (state_block > 0).then_some(state_block);
    let outcome =
        query_balancer_batch_profit(sim_provider, executor, &hops, start_token, query_block).await;
    match evaluate_batch_query(outcome, result.sim.amount_in, slippage) {
        BatchQueryVerdict::Accepted(on_chain_profit) => {
            let mut accepted = result;
            accepted.sim.profit = on_chain_profit;
            accepted.sim.amount_out = accepted.sim.amount_in.saturating_add(on_chain_profit);
            accepted.sim.profitable = true;
            let assessment = reassess_hf_eval_result(&accepted, eval, FlashLoanSource::Direct)?;
            if !assessment.should_execute {
                execution.quarantine_batch_query_failure(fp);
                record_balancer_batch_reject(BalancerBatchReject::ReassessAfterOnChain);
                crate::debug!(
                    "balancer: batch_filter fp={fp} reject=reassess_after_on_chain on_chain={on_chain_profit} net_matic={}",
                    assessment.net_profit_after_gas_matic_wei,
                );
                return None;
            }
            let Some(min_profit) = on_chain_min_profit_from_assessment(&assessment) else {
                execution.quarantine_batch_query_failure(fp);
                record_balancer_batch_reject(BalancerBatchReject::BuildDecodeFailed);
                return None;
            };
            let realized = match confirm_direct_batch_realized_profit(
                sim_provider,
                executor,
                operator,
                &hops,
                start_token,
                accepted.sim.amount_in,
                min_profit,
                query_block,
            )
            .await
            {
                Ok(v) => v,
                Err(reason) => {
                    execution.quarantine_batch_query_failure(fp);
                    // Only FoT-cool the start token on semantic zero/transfer failure.
                    // Live poison: RPC "header not found" / timeout was recorded as
                    // zero_realized and blackholed a proven Direct profit token for 30m
                    // right after multiple confirms on the same token.
                    if confirm_reason_is_semantic_fot(&reason) {
                        execution.quarantine_direct_token_zero_realized(start_token);
                        record_balancer_batch_reject(BalancerBatchReject::ZeroRealized);
                        crate::info!(
                            "balancer: batch_filter fp={fp} reject=zero_realized token={start_token} query_profit={on_chain_profit} min_profit={min_profit} reason={reason}"
                        );
                    } else if reason.contains("confirm_timeout") || reason.contains("timeout") {
                        record_balancer_batch_reject(BalancerBatchReject::Timeout);
                        crate::info!(
                            "balancer: batch_filter fp={fp} reject=confirm_timeout token={start_token} query_profit={on_chain_profit} reason={reason}"
                        );
                    } else {
                        record_balancer_batch_reject(BalancerBatchReject::RpcError);
                        crate::info!(
                            "balancer: batch_filter fp={fp} reject=confirm_rpc token={start_token} query_profit={on_chain_profit} reason={reason}"
                        );
                    }
                    return None;
                }
            };
            // Prefer executor-realized profit over vault-query delta when they diverge.
            if realized != on_chain_profit {
                accepted.sim.profit = realized;
                accepted.sim.amount_out = accepted.sim.amount_in.saturating_add(realized);
                let reassessment =
                    reassess_hf_eval_result(&accepted, eval, FlashLoanSource::Direct)?;
                if !reassessment.should_execute {
                    execution.quarantine_batch_query_failure(fp);
                    record_balancer_batch_reject(BalancerBatchReject::ReassessAfterOnChain);
                    crate::debug!(
                        "balancer: batch_filter fp={fp} reject=reassess_after_realized query={on_chain_profit} realized={realized}"
                    );
                    return None;
                }
                accepted.assessment = reassessment;
            } else {
                accepted.assessment = assessment;
            }
            accepted.flash_source = FlashLoanSource::Direct;
            accepted.balancer_batch_verified = true;
            record_balancer_filter_accept();
            Some(accepted)
        }
        BatchQueryVerdict::Rejected(reason) => {
            execution.quarantine_batch_query_failure(fp);
            record_balancer_batch_reject(reason);
            let modeled = result.sim.profit;
            let net_matic = result.assessment.net_profit_after_gas_matic_wei;
            crate::debug!(
                "balancer: batch_filter fp={fp} reject={reason:?} modeled={modeled} net_matic={net_matic} slippage_bps={slippage}",
            );
            None
        }
    }
}

/// True when a Direct confirm error is a real FoT / zero-balance signal, not RPC noise.
///
/// Live: "header not found" was classified as zero_realized and cooled the start token
/// for 30m after multiple confirmed Direct arbs on that same token. Only explicit
/// semantic markers cool the token; route fingerprint still cools via batch_query.
#[must_use]
fn confirm_reason_is_semantic_fot(reason: &str) -> bool {
    let r = reason.to_ascii_lowercase();
    r.contains("confirm_zero_return")
        || r.contains("transferfailed")
        || r.contains("transfer failed")
        || r.contains("transfer amount exceeds")
        || r.contains("insufficient balance")
        || r.contains("fee-on-transfer")
}

/// Local sim retained less than this fraction of vault `queryBatchSwap` profit → cool.
/// Live: modeled 26.8e15 / on_chain 5.9e15 ≈ 22% retain (4.5× overstate) kept winning
/// best-eval while only logging; 50% is well above that phantom band and below
/// normal re-sim noise (RESIM allows 10% drift).
const BALANCER_SIM_MIN_RETAIN_BPS: u64 = 5_000;

/// On-chain retain bps of local-sim profit (`on_chain * 10_000 / modeled`).
#[must_use]
pub(crate) fn balancer_sim_retain_bps(modeled: U256, on_chain: U256) -> u64 {
    if modeled.is_zero() {
        return 10_000;
    }
    let bps = on_chain.saturating_mul(U256::from(10_000u64)) / modeled;
    u64::try_from(bps).unwrap_or(u64::MAX).min(10_000)
}

/// True when vault profit is so far below local sim that the edge is a phantom.
#[must_use]
pub(crate) fn balancer_sim_overstated(modeled: U256, on_chain: U256) -> bool {
    if modeled.is_zero() {
        return false;
    }
    balancer_sim_retain_bps(modeled, on_chain) < BALANCER_SIM_MIN_RETAIN_BPS
}

/// On-chain `queryBatchSwap` probe for balancer near-misses (sim vs vault delta).
pub(crate) async fn probe_near_miss_balancer<P: Provider<Ethereum>>(
    execution: &crate::services::execution::ExecutionService,
    arena: &StateArena,
    result: &HfEvalResult,
    pool_metas: &[crate::pipeline::types::PoolMeta],
    sim_provider: &P,
    executor: alloy::primitives::Address,
    state_block: u64,
) {
    if !route_is_balancer_only(&result.cycle) {
        return;
    }
    let fp = result.route_fingerprint;
    let Some(start_token) = arena.token_address(result.cycle.start_token) else {
        return;
    };
    let pool_metas_by_pool: FxHashMap<
        crate::core::types::PoolIndex,
        &crate::pipeline::types::PoolMeta,
    > = pool_metas.iter().map(|m| (m.pool_index, m)).collect();
    let hops = match build_calldata_hops(
        arena,
        &result.cycle.edges,
        &result.sim.hop_amounts,
        &pool_metas_by_pool,
    ) {
        Ok(h) => h,
        Err(reason) => {
            crate::info!(
                "hf near-miss-verify: fp={fp} reject=calldata_build_failed reason={reason} modeled={}",
                result.sim.profit
            );
            return;
        }
    };
    let route = hops
        .iter()
        .map(|hop| {
            format!(
                "{}@{}",
                hop.protocol_label.as_deref().unwrap_or("unknown"),
                hop.pool_address
            )
        })
        .collect::<Vec<_>>()
        .join("->");
    let modeled = result.sim.profit;
    let query_block = (state_block > 0).then_some(state_block);
    match query_balancer_batch_profit(sim_provider, executor, &hops, start_token, query_block).await
    {
        BatchQueryOutcome::Profit(on_chain) => {
            let retain_bps = balancer_sim_retain_bps(modeled, on_chain);
            if balancer_sim_overstated(modeled, on_chain) {
                // Structural cool: stale balances / wrong pool kind / math drift.
                // Cool every start-rotation so the same pools do not re-enter as a
                // different fingerprint (same pattern as underwater cool).
                execution.quarantine_batch_query_failure(fp);
                let n = result.cycle.edges.len();
                if n > 1 {
                    let mut rotated =
                        crate::core::types::CycleEdges::from_slice(&result.cycle.edges);
                    for _ in 0..n {
                        let rfp = crate::services::execution::candidate::hash_cycle_edges(&rotated);
                        if rfp != fp {
                            execution.quarantine_batch_query_failure(rfp);
                        }
                        rotated.rotate_left(1);
                    }
                }
                crate::info!(
                    "hf near-miss-verify: fp={fp} route={route} modeled={modeled} on_chain={on_chain} retain_bps={retain_bps} net_matic={} sim_overstate quarantined (+rotations)",
                    result.assessment.net_profit_after_gas_matic_wei,
                );
            } else {
                crate::info!(
                    "hf near-miss-verify: fp={fp} route={route} modeled={modeled} on_chain={on_chain} retain_bps={retain_bps} net_matic={}",
                    result.assessment.net_profit_after_gas_matic_wei,
                );
            }
        }
        BatchQueryOutcome::NonPositiveDelta(delta) => {
            execution.quarantine_batch_query_failure(fp);
            crate::info!(
                "hf near-miss-verify: fp={fp} route={route} phantom sim modeled={modeled} vault_delta={delta} quarantined",
            );
        }
        BatchQueryOutcome::RpcError(reason) => {
            crate::info!(
                "hf near-miss-verify: fp={fp} route={route} rpc_error={reason} modeled={modeled}"
            );
        }
        BatchQueryOutcome::Timeout => {
            crate::info!("hf near-miss-verify: fp={fp} route={route} timeout modeled={modeled}");
        }
        BatchQueryOutcome::BuildFailed | BatchQueryOutcome::DecodeFailed => {
            crate::info!(
                "hf near-miss-verify: fp={fp} route={route} build_decode_failed modeled={modeled}"
            );
        }
    }
}

fn collect_route_pool_addresses(
    arena: &StateArena,
    routes: &[HfEvalResult],
) -> Vec<alloy::primitives::Address> {
    let mut seen = rustc_hash::FxHashSet::default();
    let mut out = Vec::new();
    for route in routes {
        for edge in &route.cycle.edges {
            if let Some(addr) = arena.pool_address(edge.pool_index)
                && seen.insert(addr)
            {
                out.push(addr);
            }
        }
    }
    out
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use super::*;
    use crate::core::types::{PoolState, V3PoolState, V3Tick};
    use crate::services::state_refresh::PoolRefreshResult;

    #[test]
    fn probe_wide_tick_word_range_matches_lf_widen() {
        assert_eq!(probe_wide_tick_word_range(4), 24);
        assert_eq!(probe_wide_tick_word_range(10), 30);
        assert_eq!(probe_wide_tick_word_range(20), 48);
    }

    #[test]
    fn sibling_completion_targets_partial_unlock_only() {
        use crate::core::types::{CycleEdges, Edge, FoundCycle, ProtocolType, TokenIndex};
        use crate::pipeline::tick_fetch::{
            clear_tick_hydrate_cooldown, mark_tick_hydrate_timeout_cooldown,
        };

        let hub = Address::from([1u8; 20]);
        let sibling = Address::from([2u8; 20]);
        let tick = V3Tick {
            tick: 0,
            liquidity_gross: 1,
            liquidity_net: 0,
        };
        let mut arena = StateArena::default();
        let hub_idx = arena.register_pool(
            hub,
            Arc::new(PoolState::V3(V3PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                liquidity: 1_000_000,
                tick: 0,
                fee: U256::from(3_000u32),
                tick_spacing: 60,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
                ticks: Arc::from(vec![tick]),
            })),
        );
        let sib_idx = arena.register_pool(
            sibling,
            Arc::new(PoolState::V3(V3PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                liquidity: 500_000,
                tick: 0,
                fee: U256::from(3_000u32),
                tick_spacing: 60,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
                ticks: Arc::from(vec![]),
            })),
        );
        let cycle = FoundCycle {
            start_token: TokenIndex(0),
            edges: CycleEdges::from_iter([
                Edge {
                    pool_index: hub_idx,
                    protocol: ProtocolType::UniswapV3,
                    token_in: TokenIndex(0),
                    token_out: TokenIndex(1),
                    zero_for_one: true,
                    fee_bps: 30,
                    token_in_idx: 0,
                    token_out_idx: 1,
                },
                Edge {
                    pool_index: sib_idx,
                    protocol: ProtocolType::UniswapV3,
                    token_in: TokenIndex(1),
                    token_out: TokenIndex(0),
                    zero_for_one: false,
                    fee_bps: 30,
                    token_in_idx: 1,
                    token_out_idx: 0,
                },
            ]),
            hop_count: 2,
            log_weight: 0.0,
            cumulative_fee_bps: 0,
            score: 1.0,
            cycle_ratio: U256::from(1u64),
        };
        // Hub already has ticks → sibling is the completion target (narrow-miss
        // is intentionally not consulted by this helper).
        let (v3, v4) = sibling_completion_cl_targets(&arena, std::slice::from_ref(&cycle), &[], 4);
        assert!(v4.is_empty());
        assert_eq!(v3, vec![sibling]);

        // Empty-tick (wide already failed) must not re-fetch.
        mark_tick_hydrate_timeout_cooldown([sibling]);
        let (v3_cool, _) = sibling_completion_cl_targets(&arena, &[cycle], &[], 4);
        assert!(v3_cool.is_empty());
        clear_tick_hydrate_cooldown(sibling);
        clear_tick_hydrate_cooldown(hub);
    }

    #[test]
    fn v3_tickless_hf_hydrate_exhausted_when_empty_cooldown() {
        use crate::core::types::{CycleEdges, Edge, FoundCycle, ProtocolType, TokenIndex};
        use crate::pipeline::tick_fetch::{
            clear_tick_hydrate_cooldown, mark_tick_hydrate_timeout_cooldown,
        };

        let addr = Address::from([42u8; 20]);
        let mut arena = StateArena::default();
        let idx = arena.register_pool(
            addr,
            Arc::new(PoolState::V3(V3PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                liquidity: 1_000_000,
                tick: 0,
                fee: U256::from(3_000u32),
                tick_spacing: 60,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
                ticks: Arc::from(vec![]),
            })),
        );
        let cycle = FoundCycle {
            start_token: TokenIndex(0),
            edges: CycleEdges::from_iter([Edge {
                pool_index: idx,
                protocol: ProtocolType::UniswapV3,
                token_in: TokenIndex(0),
                token_out: TokenIndex(1),
                zero_for_one: true,
                fee_bps: 30,
                token_in_idx: 0,
                token_out_idx: 1,
            }]),
            hop_count: 1,
            log_weight: 0.0,
            cumulative_fee_bps: 0,
            score: 1.0,
            cycle_ratio: U256::from(1u64),
        };
        assert!(
            !cycle_v3_tickless_hf_hydrate_exhausted(&arena, &cycle),
            "fresh tickless pool should still be fetchable"
        );
        mark_tick_hydrate_timeout_cooldown([addr]);
        assert!(cycle_v3_tickless_hf_hydrate_exhausted(&arena, &cycle));
        clear_tick_hydrate_cooldown(addr);
        assert!(!cycle_v3_tickless_hf_hydrate_exhausted(&arena, &cycle));
    }

    #[test]
    fn resim_depth_replaces_pre_refresh_slippage() {
        let initial = effective_slippage_after_resim_depth(25, 2, 100, FlashLoanSource::AaveV3);
        let refreshed = effective_slippage_after_resim_depth(25, 2, 900, FlashLoanSource::AaveV3);

        assert!(refreshed > initial);
        // Direct does not hop-compound the encode floor.
        assert_eq!(
            effective_slippage_after_resim_depth(0, 3, 0, FlashLoanSource::Direct),
            50
        );
    }

    #[test]
    fn route_pool_refresh_failed_only_when_attempted_zero_updates() {
        assert!(!route_pool_refresh_failed(&PoolRefreshResult {
            updated: 5,
            attempted: true,
            matched: 10,
        }));
        assert!(route_pool_refresh_failed(&PoolRefreshResult {
            updated: 0,
            attempted: true,
            matched: 10,
        }));
        assert!(!route_pool_refresh_failed(&PoolRefreshResult {
            updated: 0,
            attempted: false,
            matched: 10,
        }));
    }

    #[test]
    fn probe_tick_cap_reserves_v4_when_both_families_need_work() {
        // Mirror tickless_cl_targets_shared_cap reservation math.
        let split = |pool_cap: usize, v3_total: usize, v4_total: usize| {
            let v4_reserve = usize::from(v4_total > 0 && v3_total > 0 && pool_cap >= 2);
            let v3_cap = pool_cap.saturating_sub(v4_reserve);
            let v3_take = v3_total.min(v3_cap);
            let v4_cap = pool_cap.saturating_sub(v3_take);
            (v3_take, v4_cap.min(v4_total))
        };
        assert_eq!(split(6, 8, 3), (5, 1));
        assert_eq!(split(1, 8, 3), (1, 0)); // cap=1: no reserve
        assert_eq!(split(6, 8, 0), (6, 0));
        assert_eq!(split(6, 0, 3), (0, 3));
    }

    #[test]
    fn probe_tick_pool_cap_scales_with_residual_budget() {
        use std::time::Duration;
        assert_eq!(probe_tick_pool_cap_for_budget(Duration::from_millis(40)), 0);
        // Sub-pool budget must not force cap=1 (that always timed out + cooled).
        assert_eq!(
            probe_tick_pool_cap_for_budget(Duration::from_millis(
                HF_PROBE_TICK_MS_PER_POOL.saturating_sub(1)
            )),
            0
        );
        assert_eq!(
            probe_tick_pool_cap_for_budget(Duration::from_millis(HF_PROBE_TICK_MS_PER_POOL)),
            1
        );
        assert_eq!(
            probe_tick_pool_cap_for_budget(Duration::from_millis(
                HF_PROBE_TICK_MS_PER_POOL.saturating_mul(2)
            )),
            2.min(HF_PROBE_TICK_POOL_CAP)
        );
        // Full residual prep must never exceed hard cap (live cooled 45 under 64).
        assert_eq!(
            probe_tick_pool_cap_for_budget(Duration::from_millis(5_000)),
            HF_PROBE_TICK_POOL_CAP
        );
    }

    #[test]
    fn clear_dispatch_cl_ticks_removes_stale_v3_ticks() {
        let address = alloy::primitives::Address::from([1u8; 20]);
        let mut arena = StateArena::default();
        let pool = arena.register_pool(
            address,
            Arc::new(PoolState::V3(V3PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                liquidity: 1_000_000,
                tick: 0,
                fee: U256::from(3_000u32),
                tick_spacing: 60,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
                ticks: Arc::from(vec![V3Tick {
                    tick: -60,
                    liquidity_gross: 1_000_000,
                    liquidity_net: 1_000_000,
                }]),
            })),
        );

        crate::pipeline::tick_fetch::clear_v3_pool_ticks(&mut arena, &[address]);

        let Some(PoolState::V3(state)) = arena.pool_state(pool) else {
            panic!("registered pool must retain V3 state");
        };
        assert!(state.ticks.is_empty());
    }

    #[test]
    fn restore_dispatch_cl_ticks_recovers_v3_ticks_after_rpc_failure() {
        let address = alloy::primitives::Address::from([2u8; 20]);
        let mut arena = StateArena::default();
        let pool = arena.register_pool(
            address,
            Arc::new(PoolState::V3(V3PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                liquidity: 1_000_000,
                tick: 0,
                fee: U256::from(3_000u32),
                tick_spacing: 60,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
                ticks: Arc::from(vec![V3Tick {
                    tick: -60,
                    liquidity_gross: 1_000_000,
                    liquidity_net: 1_000_000,
                }]),
            })),
        );

        let snapshots = dispatch_cl_tick_snapshot(&arena, &[address], &[]);
        crate::pipeline::tick_fetch::clear_v3_pool_ticks(&mut arena, &[address]);
        restore_dispatch_cl_ticks(&mut arena, snapshots);

        let Some(PoolState::V3(state)) = arena.pool_state(pool) else {
            panic!("registered pool must retain V3 state");
        };
        assert_eq!(state.ticks.len(), 1);
        assert_eq!(state.ticks[0].tick, -60);
    }

    #[test]
    fn restore_dispatch_cl_ticks_keeps_hydrated_ticks() {
        let address = alloy::primitives::Address::from([3u8; 20]);
        let mut arena = StateArena::default();
        let pool = arena.register_pool(
            address,
            Arc::new(PoolState::V3(V3PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                liquidity: 1_000_000,
                tick: 0,
                fee: U256::from(3_000u32),
                tick_spacing: 60,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
                ticks: Arc::from(vec![V3Tick {
                    tick: -60,
                    liquidity_gross: 1_000_000,
                    liquidity_net: 1_000_000,
                }]),
            })),
        );
        let snapshots = dispatch_cl_tick_snapshot(&arena, &[address], &[]);
        // Simulate successful hydrate with a different tick set.
        if let Some(PoolState::V3(state)) = arena.pool_state_mut(pool) {
            state.ticks = Arc::from(vec![V3Tick {
                tick: 60,
                liquidity_gross: 2_000_000,
                liquidity_net: -2_000_000,
            }]);
        }
        restore_dispatch_cl_ticks(&mut arena, snapshots);
        let Some(PoolState::V3(state)) = arena.pool_state(pool) else {
            panic!("registered pool must retain V3 state");
        };
        assert_eq!(state.ticks.len(), 1);
        assert_eq!(state.ticks[0].tick, 60);
    }

    #[test]
    fn tickless_v3_prioritizes_sole_unlock_over_membership_hub() {
        use crate::core::types::{CycleEdges, Edge, FoundCycle, ProtocolType, TokenIndex};
        let hub = Address::from([10u8; 20]);
        let sole = Address::from([11u8; 20]);
        let sibling = Address::from([12u8; 20]);
        let mut arena = StateArena::default();
        let mk_v3 = |arena: &mut StateArena, addr, liq| {
            arena.register_pool(
                addr,
                Arc::new(PoolState::V3(V3PoolState {
                    sqrt_price_x96: U256::from(1u128 << 96),
                    liquidity: liq,
                    tick: 0,
                    fee: U256::from(3_000u32),
                    tick_spacing: 60,
                    unlocked: true,
                    fee_protocol: 0,
                    observation_cardinality: 1,
                    ticks: Arc::from(vec![]),
                })),
            )
        };
        let hub_idx = mk_v3(&mut arena, hub, 9_000_000);
        let sole_idx = mk_v3(&mut arena, sole, 1_000);
        let sib_idx = mk_v3(&mut arena, sibling, 9_000_000);
        let edge = |pool| Edge {
            pool_index: pool,
            protocol: ProtocolType::UniswapV3,
            token_in: TokenIndex(0),
            token_out: TokenIndex(1),
            zero_for_one: true,
            fee_bps: 30,
            token_in_idx: 0,
            token_out_idx: 1,
        };
        // Hub appears in two multi-hop tickless cycles (high membership + liq).
        let multi = |a, b, id| {
            Arc::new(FoundCycle {
                start_token: TokenIndex(id),
                edges: CycleEdges::from_iter([edge(a), edge(b)]),
                hop_count: 2,
                log_weight: 0.0,
                cumulative_fee_bps: 0,
                score: 0.0,
                cycle_ratio: U256::ZERO,
            })
        };
        let one = Arc::new(FoundCycle {
            start_token: TokenIndex(9),
            edges: CycleEdges::from_iter([edge(sole_idx)]),
            hop_count: 1,
            log_weight: 0.0,
            cumulative_fee_bps: 0,
            score: 0.0,
            cycle_ratio: U256::ZERO,
        });
        let cycles = vec![multi(hub_idx, sib_idx, 1), multi(hub_idx, sib_idx, 2), one];
        let (total, ranked) = tickless_v3_addresses_prioritized(&arena, &cycles, 1);
        assert_eq!(total, 3);
        // Cap=1 must fetch the sole-unlock pool, not the high-liq membership hub.
        assert_eq!(ranked, vec![sole]);
    }

    #[test]
    fn drain_removes_only_cooldown_stuck_tickless_cycles() {
        use crate::core::types::{CycleEdges, Edge, FoundCycle, ProtocolType, TokenIndex};
        let addr_dead = Address::from([3u8; 20]);
        let addr_live = Address::from([4u8; 20]);
        let mut arena = StateArena::default();
        let dead = arena.register_pool(
            addr_dead,
            Arc::new(PoolState::V3(V3PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                liquidity: 1_000_000,
                tick: 0,
                fee: U256::from(3_000u32),
                tick_spacing: 60,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
                ticks: Arc::from(vec![]),
            })),
        );
        let live = arena.register_pool(
            addr_live,
            Arc::new(PoolState::V3(V3PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                liquidity: 1_000_000,
                tick: 0,
                fee: U256::from(3_000u32),
                tick_spacing: 60,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
                ticks: Arc::from(vec![V3Tick {
                    tick: -60,
                    liquidity_gross: 1,
                    liquidity_net: 1,
                }]),
            })),
        );
        mark_tick_hydrate_timeout_cooldown([addr_dead]);
        let pool_metas: &[crate::pipeline::types::PoolMeta] = &[];
        let edge = |pool| Edge {
            pool_index: pool,
            protocol: ProtocolType::UniswapV3,
            token_in: TokenIndex(0),
            token_out: TokenIndex(1),
            zero_for_one: true,
            fee_bps: 30,
            token_in_idx: 0,
            token_out_idx: 1,
        };
        let mk = |pool, id| {
            Arc::new(FoundCycle {
                start_token: TokenIndex(id),
                edges: CycleEdges::from_iter([edge(pool)]),
                hop_count: 1,
                log_weight: 0.0,
                cumulative_fee_bps: 0,
                score: 0.0,
                cycle_ratio: U256::ZERO,
            })
        };
        let mut cycles = vec![mk(dead, 1), mk(live, 2)];
        let removed = drain_cooldown_stuck_tickless_cycles(&arena, &mut cycles, pool_metas);
        assert_eq!(removed, 1);
        assert_eq!(cycles.len(), 1);
        assert_eq!(cycles[0].start_token, TokenIndex(2));

        // All-stuck set must clear entirely (previously early-returned 0 and kept phantoms).
        let mut all_stuck = vec![mk(dead, 3), mk(dead, 4)];
        let removed_all = drain_cooldown_stuck_tickless_cycles(&arena, &mut all_stuck, pool_metas);
        assert_eq!(removed_all, 2);
        assert!(all_stuck.is_empty());

        // Selection skip keys off cooldown membership even when LF still has ticks.
        assert!(cycle_has_cl_pool_on_miss_cooldown(
            &arena,
            &mk(dead, 5),
            pool_metas
        ));
        assert!(!cycle_has_cl_pool_on_miss_cooldown(
            &arena,
            &mk(live, 6),
            pool_metas
        ));
        crate::pipeline::tick_fetch::clear_tick_hydrate_cooldown(addr_dead);
        assert!(!cycle_has_cl_pool_on_miss_cooldown(
            &arena,
            &mk(dead, 7),
            pool_metas
        ));
    }

    #[test]
    fn balancer_sim_retain_detects_live_4x_overstate() {
        // Live near-miss: modeled 26_779… / on_chain 5_896… ≈ 22% retain.
        let modeled = U256::from(26_779_070_152_868_894u128);
        let on_chain = U256::from(5_896_560_291_339_009u128);
        let retain = balancer_sim_retain_bps(modeled, on_chain);
        assert!(retain < 2_500, "live overstate retain_bps={retain}");
        assert!(balancer_sim_overstated(modeled, on_chain));
        // Near-exact vault match must not cool.
        let tight = U256::from(26_779_070_152_868_614u128);
        assert!(!balancer_sim_overstated(modeled, tight));
        assert_eq!(balancer_sim_retain_bps(modeled, modeled), 10_000);
        assert!(!balancer_sim_overstated(U256::ZERO, on_chain));
    }

    #[test]
    fn mixed_flash_balancer_gas_seed_near_live_band() {
        // Aave + BAL×2 + V3 (sticky half-cover route shape). Prior 340k hop → ~996k;
        // reverse-calc ~294k/BAL → total ~916k with 300k hop.
        use crate::core::constants::{GAS_BALANCER_HOP, GAS_V3_BASE};
        use crate::core::types::{Edge, ProtocolType, TokenIndex};
        use crate::pipeline::local_sim::estimate_route_gas;

        let edges = [
            Edge {
                pool_index: crate::core::types::PoolIndex(0),
                token_in: TokenIndex(0),
                token_out: TokenIndex(1),
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::BalancerV2,
                fee_bps: 30,
                zero_for_one: true,
            },
            Edge {
                pool_index: crate::core::types::PoolIndex(1),
                token_in: TokenIndex(1),
                token_out: TokenIndex(2),
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::BalancerV2,
                fee_bps: 30,
                zero_for_one: true,
            },
            Edge {
                pool_index: crate::core::types::PoolIndex(2),
                token_in: TokenIndex(2),
                token_out: TokenIndex(0),
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV3,
                fee_bps: 30,
                zero_for_one: true,
            },
        ];
        // Not pure-Balancer Direct (V3 hop) → per-hop flash path.
        let gas = estimate_route_gas(&edges);
        let hop = GAS_BALANCER_HOP * 2 + GAS_V3_BASE;
        assert_eq!(GAS_BALANCER_HOP, 300_000);
        assert!(
            (850_000..=950_000).contains(&gas),
            "mixed BAL+BAL+V3 assess gas {gas} outside 850k–950k (hop_sum={hop})"
        );
    }

    #[test]
    fn tickless_v4_targets_prioritized_ranks_by_hits_and_liquidity() {
        use crate::core::types::{
            CycleEdges, Edge, FoundCycle, ProtocolType, TokenIndex, V4PoolState,
        };
        use crate::pipeline::types::PoolMeta;
        use alloy::primitives::{FixedBytes, U256};

        let mut arena = StateArena::default();
        let pool_id1 = FixedBytes::from([1u8; 32]);
        let pool_id2 = FixedBytes::from([2u8; 32]);

        let p1 = arena.register_pool(
            Address::from([1u8; 20]),
            Arc::new(PoolState::V4(V4PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                liquidity: 100,
                tick: 0,
                fee: U256::from(3000u32),
                tick_spacing: 60,
                ticks: Arc::from([]),
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
            })),
        );
        let p2 = arena.register_pool(
            Address::from([2u8; 20]),
            Arc::new(PoolState::V4(V4PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                liquidity: 1000,
                tick: 0,
                fee: U256::from(3000u32),
                tick_spacing: 60,
                ticks: Arc::from([]),
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
            })),
        );

        let metas = vec![
            PoolMeta {
                pool_index: p1,
                protocol: ProtocolType::UniswapV4,
                tokens: vec![],
                fee_bps: 30,
                bpt_index: None,
                pool_id: Some(pool_id1),
                protocol_label: None,
                pool_type: None,
                hooks: None,
                tick_spacing: Some(60),
            },
            PoolMeta {
                pool_index: p2,
                protocol: ProtocolType::UniswapV4,
                tokens: vec![],
                fee_bps: 30,
                bpt_index: None,
                pool_id: Some(pool_id2),
                protocol_label: None,
                pool_type: None,
                hooks: None,
                tick_spacing: Some(60),
            },
        ];

        let edge = |pool| Edge {
            pool_index: pool,
            protocol: ProtocolType::UniswapV4,
            token_in: TokenIndex(0),
            token_out: TokenIndex(1),
            zero_for_one: true,
            fee_bps: 30,
            token_in_idx: 0,
            token_out_idx: 1,
        };

        let cycle1 = Arc::new(FoundCycle {
            start_token: TokenIndex(0),
            edges: CycleEdges::from_iter([edge(p1)]),
            hop_count: 1,
            log_weight: 0.0,
            cumulative_fee_bps: 0,
            score: 0.0,
            cycle_ratio: U256::ZERO,
        });

        let cycle2 = Arc::new(FoundCycle {
            start_token: TokenIndex(0),
            edges: CycleEdges::from_iter([edge(p2)]),
            hop_count: 1,
            log_weight: 0.0,
            cumulative_fee_bps: 0,
            score: 0.0,
            cycle_ratio: U256::ZERO,
        });

        // p2 is in two cycles, p1 is in one cycle.
        let cycles = vec![cycle1, cycle2.clone(), cycle2];
        let (total, targets) = tickless_v4_targets_prioritized(&arena, &cycles, &metas, 1);
        assert_eq!(total, 2);
        assert_eq!(targets.len(), 1);
        // p2 should be picked first because it has 2 hits vs p1's 1 hit.
        assert_eq!(targets[0], (p2, pool_id2));
    }
}
