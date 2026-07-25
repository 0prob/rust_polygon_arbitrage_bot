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

use crate::pipeline::tick_fetch::{
    collect_v3_pool_addresses, collect_v4_tick_targets, hydrate_cl_ticks_with_rpc_fallback,
    is_cl_tick_on_hydrate_cooldown, is_empty_tick_on_cooldown, is_probe_narrow_miss_on_cooldown,
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
            let fp = r.route_fingerprint;
            !ctx.execution.is_route_quarantined(fp)
                && !ctx.execution.is_route_on_cooldown(fp, &ctx.config)
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
            ExecutionOutcome::SkippedCircuitBreaker | ExecutionOutcome::SkippedShutdown
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
/// ms per tickless pool — observed hydrate often 100–250ms; keep headroom.
const HF_PROBE_TICK_MS_PER_POOL: u64 = 250;

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

fn cl_pool_on_hydrate_cooldown(
    arena: &StateArena,
    pool_metas: &[crate::pipeline::types::PoolMeta],
    pool_index: PoolIndex,
    protocol: crate::core::types::ProtocolType,
) -> bool {
    let Some(addr) = arena.pool_address(pool_index) else {
        return false;
    };
    let v4_pool_id = (protocol == crate::core::types::ProtocolType::UniswapV4)
        .then(|| {
            crate::pipeline::types::pool_meta_at(pool_metas, pool_index)
                .and_then(|meta| meta.pool_id)
        })
        .flatten();
    is_cl_tick_on_hydrate_cooldown(addr, v4_pool_id)
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

/// True when the cycle has tickless CL hops and every such pool is on tick-miss cooldown
/// (wide fetch already failed recently — further HF eval is dust-only noise).
pub(crate) fn cycle_tickless_cl_all_on_miss_cooldown(
    arena: &StateArena,
    cycle: &FoundCycle,
    pool_metas: &[crate::pipeline::types::PoolMeta],
) -> bool {
    let mut saw_tickless = false;
    for edge in &cycle.edges {
        let tickless = matches!(
            (arena.pool_state(edge.pool_index), edge.protocol),
            (Some(PoolState::V3(s)), crate::core::types::ProtocolType::UniswapV3)
            | (Some(PoolState::V4(s)), crate::core::types::ProtocolType::UniswapV4)
                if s.ticks.is_empty()
        );
        if !tickless {
            continue;
        }
        saw_tickless = true;
        if !cl_pool_on_hydrate_cooldown(arena, pool_metas, edge.pool_index, edge.protocol) {
            return false;
        }
    }
    saw_tickless
}

/// True when any V3/V4 hop's pool is on tick-miss cooldown.
///
/// Previously used at HF selection (over-pruned to `selected=0`); select now uses
/// [`cycle_tickless_cl_all_on_miss_cooldown`].
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
/// Returns how many cycles were removed. No-op when that would empty the set.
pub(crate) fn drain_cooldown_stuck_tickless_cycles(
    arena: &StateArena,
    cycles: &mut Vec<std::sync::Arc<FoundCycle>>,
    pool_metas: &[crate::pipeline::types::PoolMeta],
) -> usize {
    if cycles.is_empty() {
        return 0;
    }
    let before = cycles.len();
    cycles.retain(|c| !cycle_tickless_cl_all_on_miss_cooldown(arena, c.as_ref(), pool_metas));
    // Previously refused to drain when *every* cycle was stuck (kept.is_empty →
    // return 0), which left HF evaluating only dust-only phantoms for the tick.
    // Empty remaining is fine — the tick already handles cycles_considered=0.
    before.saturating_sub(cycles.len())
}

/// Collect tickless V3 addresses ranked by how many selected cycles they unlock
/// (then liquidity). Cap=1 under residual prep — cycle order alone left the same
/// low-value pool hydrating while multi-cycle hubs stayed tickless (live).
fn tickless_v3_addresses_prioritized<C: AsRef<FoundCycle>>(
    arena: &StateArena,
    cycles: &[C],
    cap: usize,
) -> (usize, Vec<Address>) {
    use rustc_hash::FxHashMap;
    // addr → (cycle_hits, liquidity, pool_index)
    let mut freq: FxHashMap<Address, (u32, u128)> = FxHashMap::default();
    for cycle in cycles {
        let mut in_cycle: rustc_hash::FxHashSet<Address> = rustc_hash::FxHashSet::default();
        for edge in &cycle.as_ref().edges {
            if edge.protocol != crate::core::types::ProtocolType::UniswapV3 {
                continue;
            }
            let Some(addr) = arena.pool_address(edge.pool_index) else {
                continue;
            };
            let (tickless, liq) = match arena.pool_state(edge.pool_index) {
                Some(PoolState::V3(st)) => (st.ticks.is_empty(), st.liquidity),
                _ => (false, 0),
            };
            if !tickless
                || is_empty_tick_on_cooldown(addr)
                || is_probe_narrow_miss_on_cooldown(addr)
            {
                continue;
            }
            if in_cycle.insert(addr) {
                let e = freq.entry(addr).or_insert((0, liq));
                e.0 = e.0.saturating_add(1);
                e.1 = e.1.max(liq);
            }
        }
    }
    let all = freq.len();
    let mut ranked: Vec<(Address, u32, u128)> = freq
        .into_iter()
        .map(|(addr, (hits, liq))| (addr, hits, liq))
        .collect();
    ranked.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| b.2.cmp(&a.2)));
    let out: Vec<Address> = ranked.into_iter().take(cap).map(|(a, _, _)| a).collect();
    (all, out)
}

fn tickless_v4_targets_prioritized<C: AsRef<FoundCycle>>(
    arena: &StateArena,
    cycles: &[C],
    pool_metas: &[crate::pipeline::types::PoolMeta],
    cap: usize,
) -> (usize, Vec<(PoolIndex, alloy::primitives::FixedBytes<32>)>) {
    let mut all = collect_v4_tick_targets(cycles, pool_metas);
    all.retain(|(idx, _)| match arena.pool_state(*idx) {
        Some(PoolState::V4(st)) => st.ticks.is_empty(),
        _ => false,
    });
    let total = all.len();
    all.retain(|(idx, pool_id)| {
        let Some(addr) = arena.pool_address(*idx) else {
            return true;
        };
        !is_cl_tick_on_hydrate_cooldown(addr, Some(*pool_id))
    });
    if all.len() > cap {
        all.truncate(cap);
    }
    (total, all)
}

/// On probe-tick hydrate timeout the enrich future is dropped before it can
/// mark misses, so the next tick would re-burn the whole budget on the same
/// pools. Briefly cooldown the targets it was about to fetch; live pools
/// recover on the next completed attempt. Returns how many were cooled.
///
/// `cap` must match the set `hydrate_tickless_cl_for_cycles` attempted (budget-
/// scaled). Using the hard 64-cap here used to cool pools never in-flight.
pub(crate) fn mark_probe_hydrate_timeout_cooldown<C: AsRef<FoundCycle>>(
    arena: &StateArena,
    cycles: &[C],
    pool_metas: &[crate::pipeline::types::PoolMeta],
    cap: usize,
) -> usize {
    if cap == 0 {
        return 0;
    }
    let (_, v3, _, v4) = tickless_cl_targets_shared_cap(arena, cycles, pool_metas, cap);
    mark_tick_hydrate_timeout_cooldown(v3.iter().copied());
    mark_v4_tick_hydrate_timeout_cooldown(v4.iter().map(|&(_, pool_id)| pool_id));
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

/// Hydrate empty V3/V4 tick arrays on selected HF cycles before probe ranking.
/// Hot-cache refresh drops ticks when price/liquidity moves; without this, those
/// routes classify as `cl_tickless` and never reach Brent/economics.
///
/// `pool_cap` limits how many tickless pools to fetch this tick (budget-scaled).
pub(crate) async fn hydrate_tickless_cl_for_cycles<C: AsRef<FoundCycle>>(
    rpc: &RpcPool,
    arena: &mut StateArena,
    cycles: &[C],
    pool_metas: &[crate::pipeline::types::PoolMeta],
    word_range: i16,
    block_number: Option<u64>,
    pool_cap: usize,
) -> ProbeTickHydrateStats {
    let mut stats = ProbeTickHydrateStats {
        cycles_tickless_before: count_tickless_cl_cycles(arena, cycles),
        ..ProbeTickHydrateStats::default()
    };
    if cycles.is_empty() || pool_cap == 0 {
        stats.cycles_tickless_after = stats.cycles_tickless_before;
        return stats;
    }
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
    let probe_word_range = word_range.min(4);
    let pass = hydrate_cl_ticks_with_rpc_fallback(
        rpc,
        arena,
        &v3,
        &v4,
        &algebra_pools,
        &algebra_integral_pools,
        probe_word_range,
        word_range,
        false, // probe residual budget — narrow only
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
    stats
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
    match evaluate_batch_query(
        outcome,
        result.sim.amount_in,
        slippage,
    ) {
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
            crate::info!(
                "hf near-miss-verify: fp={fp} route={route} modeled={modeled} on_chain={on_chain} net_matic={}",
                result.assessment.net_profit_after_gas_matic_wei,
            );
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
    fn resim_depth_replaces_pre_refresh_slippage() {
        let initial =
            effective_slippage_after_resim_depth(25, 2, 100, FlashLoanSource::AaveV3);
        let refreshed =
            effective_slippage_after_resim_depth(25, 2, 900, FlashLoanSource::AaveV3);

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
}
