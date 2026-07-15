use alloy::network::Ethereum;
use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use futures_util::{StreamExt, stream};
use rustc_hash::FxHashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::core::constants::AAVE_V3_POOL;
use crate::core::types::FoundCycle;
use crate::core::types::FlashLoanSource;
use crate::orchestrator::hf::HfContext;
use crate::orchestrator::hf_eval::{
    HfEvalInput, HfEvalInputOwned, HfEvalResult, reassess_hf_eval_result,
};
use crate::pipeline::arena::StateArena;
use crate::pipeline::local_sim::{self, simulate_route_detailed};

use crate::pipeline::tick_fetch::{
    collect_v3_pool_addresses, collect_v4_tick_targets, enrich_v3_ticks, enrich_v4_ticks,
};
use crate::services::execution::flash_liquidity::resolve_flash_source_for_cycle;
use crate::services::execution::flash_liquidity::{
    aave_flash_reserve_viable, collect_flash_tokens_for_cycle, cycle_has_dodo_pool,
};
use crate::services::execution::gas_oracle::RouteGasLookup;

use crate::services::execution::balancer_verify::{
    BatchQueryOutcome, balancer_batch_within_max_in_ratio, batch_profit_covers_min,
    query_balancer_batch_profit,
};
use crate::services::execution::calldata::build_calldata_hops;
use crate::services::execution::flash_liquidity::route_is_balancer_only;
use crate::services::execution::{
    CandidateBuildConfig, ExecutionOutcome, PrepareDispatchInput, build_execution_candidate,
    prepare_evaluated_route,
};
use crate::services::oracle::ensure_matic_usd_for_flash_cap;
use crate::services::oracle::resolve_token_to_matic_rate_or_bootstrap;
use crate::services::state_refresh::PoolRefreshResult;

enum RoutePoolRefreshAbort {
    NotIndexed { pool_count: usize },
    Rpc(anyhow::Error),
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
    let result = refresh
        .refresh_pool_states_for(pools, pool_count)
        .await
        .map_err(RoutePoolRefreshAbort::Rpc)?;
    if !result.can_use_cached_state() {
        return Err(RoutePoolRefreshAbort::NotIndexed { pool_count });
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
        crate::debug!(
            "route pool refresh: {pool_count} pools already fresh — using cached state"
        );
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
    prepare: AtomicU32,
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
            "prepare" => self.prepare.fetch_add(1, Ordering::Relaxed),
            "build" => self.build.fetch_add(1, Ordering::Relaxed),
            _ => 0,
        };
    }

    fn log_if_any(&self) {
        let quarantine = self.quarantine.load(Ordering::Relaxed);
        let cooldown = self.cooldown.load(Ordering::Relaxed);
        let resim_fail = self.resim_fail.load(Ordering::Relaxed);
        let resim_drift = self.resim_drift.load(Ordering::Relaxed);
        let hop_fidelity = self.hop_fidelity.load(Ordering::Relaxed);
        let prepare = self.prepare.load(Ordering::Relaxed);
        let build = self.build.load(Ordering::Relaxed);
        let fidelity = resim_fail + resim_drift + hop_fidelity;
        if build > 0 || quarantine > 0 || cooldown > 0 || fidelity > 0 || prepare > 0 {
            crate::info!(
                "dispatch summary: quarantine={quarantine}, cooldown={cooldown}, fidelity={fidelity} (resim_fail={resim_fail}, resim_drift={resim_drift}, hop={hop_fidelity}), prepare={prepare}, build={build}",
            );
        }
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
        crate::warn!("dispatch skipped: EXECUTOR_ADDRESS not configured");
        return;
    };

    // ponytail: skip executor bytecode check in dry-run mode — no on-chain txs to sign
    let sim_provider = if ctx.config.is_dry_run() {
        match ctx.rpc.connect_simulation() {
            Ok(p) => p,
            Err(e) => {
                crate::warn!("dispatch skipped: simulation RPC unavailable: {e:#}");
                return;
            }
        }
    } else {
        match ctx.rpc.connect_simulation_checked(executor).await {
            Ok(p) => p,
            Err(e) => {
                let msg = format!("{e:#}");
                if msg.contains("no executor bytecode") {
                    crate::debug!("dispatch skipped: {msg}");
                } else {
                    crate::warn!("dispatch skipped: simulation RPC/executor check failed: {msg}");
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
    let min_profit_matic = ctx.config.min_profit_matic;

    dispatch_with_provider(
        ctx,
        arena,
        profitable,
        &sim_provider,
        operator,
        executor,
        min_profit_matic,
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

    ctx.execution.shutdown_resync(&sim_provider, operator).await;
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_with_provider<P: Provider<Ethereum> + Clone + Send + 'static>(
    ctx: &HfContext,
    arena: &mut StateArena,
    profitable: Vec<HfEvalResult>,
    sim_provider: &P,
    operator: alloy::primitives::Address,
    executor: alloy::primitives::Address,
    min_profit_matic: U256,
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
        crate::warn!("dispatch skipped: execution circuit breaker active");
        return;
    }
    let flash_policy = ctx.config.flash_policy;
    let Some(gas_price) = ctx.gas_oracle.conservative_gas_price() else {
        crate::warn!("dispatch skipped: gas oracle has no fee snapshot yet");
        return;
    };
    let brent_iters = ctx.config.routing.ternary_search_iterations;
    let base_slippage_bps = ctx.config.execution.slippage_bps;
    let min_profit_roi_bps = ctx.config.execution.min_profit_roi_bps;
    let max_flash_loan_usd = ctx.config.execution.max_flash_loan_usd;
    let matic_usd = match matic_usd_hint
        .and_then(crate::pipeline::sim_sanity::matic_usd_for_flash_cap)
    {
        Some(usd) => usd,
        None => match ensure_matic_usd_for_flash_cap(&ctx.price_oracle, Some(sim_provider)).await
        {
            Some(usd) => usd,
            None => {
                crate::warn!("dispatch skipped: MATIC/USD oracle unavailable for flash loan cap");
                return;
            }
        },
    };
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
        match refresh_route_pools_into_arena(
            &ctx.refresh,
            &ctx.cache,
            arena,
            &dispatch_pools,
        )
        .await
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
                        sim_provider,
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
            Err(RoutePoolRefreshAbort::Rpc(e)) => {
                crate::warn!("dispatch aborted: route pool refresh failed ({e:#})");
                return;
            }
        }
    };

    // HF tick already prefetches stale flash tokens (750ms budget); avoid a second blocking refresh here.
    if !flash_tokens.is_empty() {
        ctx.execution.flash_liquidity.spawn_refresh_if_stale(
            Arc::clone(&ctx.rpc),
            &flash_tokens,
        );
    }

    let skipped = Arc::new(SkipCounts::default());
    let shutdown = ctx.shutdown.clone();
    let arena_ref: &StateArena = &*arena;
    let chain_head_hint = sim_provider.get_block_number().await.ok();

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
    }

    skipped.log_if_any();
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
    if ctx.execution.is_route_quarantined(fp) {
        skipped.record("quarantine");
        return None;
    }
    if ctx.execution.is_route_on_cooldown(fp, &ctx.config) {
        skipped.record("cooldown");
        return None;
    }

    let Some(start_token_addr) = arena.token_address(evaluated.cycle.start_token) else {
        skipped.record("prepare");
        return None;
    };
    let Some(resolved_token_decimals) = token_decimals.get(&start_token_addr).copied() else {
        skipped.record("prepare");
        return None;
    };
    let Some(token_to_matic_rate) =
        resolve_token_to_matic_rate_or_bootstrap(evaluated.cycle.start_token, token_to_matic_rates)
    else {
        skipped.record("prepare");
        return None;
    };

    let sim = if pools_refreshed {
        let amount_in = evaluated.sim.amount_in;
        let Some(refreshed) = simulate_route_detailed(arena, &evaluated.cycle.edges, amount_in)
        else {
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
            crate::debug!(
                "dispatch skip: fp={fp} resim gate failed: {reason} baseline_profit={} refreshed_profit={} profit_drift_bps={} max_hop_drift_bps={}",
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
        refreshed
    } else {
        evaluated.sim
    };
    evaluated.sim = sim;

    let liquidity = ctx.execution.flash_liquidity.snapshot(start_token_addr);
    let slippage_bps = evaluated.effective_slippage_bps.max(base_slippage_bps);
    let search_low = evaluated.opt.search_low;
    let evaluated = crate::services::execution::candidate::evaluated_from_sim(
        evaluated.cycle,
        evaluated.sim,
        evaluated.assessment,
        slippage_bps,
    );
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
        slippage_bps,
        max_flash_loan_usd,
        matic_usd,
        matic_usd_chainlink: ctx.price_oracle.fresh_matic_usd_chainlink_raw(),
        safety_multiplier_bps: ctx.config.execution.profit_safety_multiplier_bps,
        profit_priority_alpha_bps: ctx.config.execution.profit_priority_fee_alpha_bps,
        route_fingerprint: fp,
        gas_oracle: &ctx.gas_oracle,
        search_low,
        risk_multiplier_bps: ctx.execution.route_risk_multiplier_bps(fp),
        existing_assessment: None,
        log_skips: log_prepare_skip,
    }) else {
        skipped.record("prepare");
        if log_prepare_skip {
            ctx.execution.record_prepare_skip(fp);
        }
        return None;
    };

    if prepared.flash_source == FlashLoanSource::AaveV3 {
        let cached = ctx.execution.flash_liquidity.snapshot(start_token_addr);
        let cache_fresh = ctx.execution.flash_liquidity.has_fresh_entry(start_token_addr);
        let cache_viable =
            cache_fresh && cached.aave_listed && !cached.aave.is_zero();
        if !cache_viable
            && !aave_flash_reserve_viable(sim_provider, AAVE_V3_POOL, start_token_addr).await
        {
            skipped.record("prepare");
            ctx.execution
                .flash_liquidity
                .mark_aave_inactive(start_token_addr);
            if log_prepare_skip {
                crate::info!(
                    "prepare skip: fp={fp} Aave reserve inactive for flash borrow token {start_token_addr}"
                );
            } else {
                crate::debug!(
                    "dispatch skip: fp={fp} Aave reserve inactive for flash borrow token {start_token_addr}"
                );
            }
            return None;
        }
    }

    if prepared.flash_source == FlashLoanSource::Direct
        && route_is_balancer_only(&prepared.evaluated.cycle)
        && !balancer_batch_verified
    {
        let Some(hops) = build_calldata_hops(
            arena,
            &prepared.evaluated.cycle.edges,
            &prepared.evaluated.result.hop_amounts,
            pool_metas_by_pool,
        ) else {
            skipped.record("prepare");
            return None;
        };
        if !balancer_batch_within_max_in_ratio(arena, &hops) {
            skipped.record("prepare");
            ctx.execution.quarantine_batch_query_failure(fp);
            crate::debug!("prepare skip: fp={fp} queryBatchSwap would exceed MAX_IN_RATIO");
            return None;
        }
        // ponytail: dry-run must queryBatchSwap too — local Balancer sim overstates profit
        // and executeArbDirect reverts with opaque ExternalCallFailed without this gate.
        let query_block = (state_block > 0).then_some(state_block);
        match query_balancer_batch_profit(
            sim_provider,
            executor,
            &hops,
            start_token_addr,
            query_block,
        )
        .await
        {
            BatchQueryOutcome::Profit(on_chain_profit)
                if batch_profit_covers_min(
                    on_chain_profit,
                    prepared.evaluated.result.amount_in,
                    slippage_bps,
                    prepared.evaluated.cycle.hop_count,
                ) => {}
            BatchQueryOutcome::Profit(on_chain_profit) => {
                skipped.record("prepare");
                ctx.execution.quarantine_batch_query_failure(fp);
                if log_prepare_skip {
                    crate::info!(
                        "prepare skip: fp={fp} queryBatchSwap profit {on_chain_profit} below min floor (modeled={})",
                        prepared.evaluated.result.profit,
                    );
                } else {
                    crate::debug!(
                        "dispatch skip: fp={fp} queryBatchSwap profit {on_chain_profit} below min floor (modeled={})",
                        prepared.evaluated.result.profit,
                    );
                }
                return None;
            }
            BatchQueryOutcome::NonPositiveDelta(delta) => {
                skipped.record("prepare");
                ctx.execution.quarantine_batch_query_failure(fp);
                if log_prepare_skip {
                    crate::info!(
                        "prepare skip: fp={fp} queryBatchSwap non-positive delta {delta} (modeled={})",
                        prepared.evaluated.result.profit,
                    );
                } else {
                    crate::debug!(
                        "dispatch skip: fp={fp} queryBatchSwap non-positive delta {delta} (modeled={})",
                        prepared.evaluated.result.profit,
                    );
                }
                return None;
            }
            BatchQueryOutcome::RpcError(reason) => {
                skipped.record("prepare");
                ctx.execution.quarantine_batch_query_failure(fp);
                if log_prepare_skip {
                    crate::info!("prepare skip: fp={fp} queryBatchSwap RPC error: {reason}");
                } else {
                    crate::debug!("dispatch skip: fp={fp} queryBatchSwap RPC error: {reason}");
                }
                return None;
            }
            other => {
                skipped.record("prepare");
                ctx.execution.quarantine_batch_query_failure(fp);
                let reason = match other {
                    BatchQueryOutcome::Timeout => "timeout",
                    BatchQueryOutcome::BuildFailed => "build failed",
                    BatchQueryOutcome::DecodeFailed => "decode failed",
                    BatchQueryOutcome::Profit(_)
                    | BatchQueryOutcome::NonPositiveDelta(_)
                    | BatchQueryOutcome::RpcError(_) => unreachable!(),
                };
                if log_prepare_skip {
                    crate::info!("prepare skip: fp={fp} queryBatchSwap {reason}");
                } else {
                    crate::debug!("dispatch skip: fp={fp} queryBatchSwap {reason}");
                }
                return None;
            }
        }
    }

    let build_cfg = CandidateBuildConfig {
        executor_address: executor,
        slippage_bps,
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
        has_dodo_pool: cycle_has_dodo_pool(arena, &prepared.evaluated.cycle),
        trust_prepared_flash: true,
    };

    let candidate =
        match build_execution_candidate(arena, &prepared.evaluated, &build_cfg, pool_metas_by_pool)
        {
            Ok(c) => c,
            Err(e) => {
                crate::warn!("dispatch build failed: fp={fp}: {e:#}");
                skipped.record("build");
                return None;
            }
        };

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
            ctx.hypersync.as_deref(),
            Some(&ctx.ui_hook),
            Some(&ctx.shutdown),
            None,
            chain_head_hint,
        )
        .await;
    Some(outcome)
}

async fn enrich_dispatch_cl_ticks<P: Provider<Ethereum> + Clone + Send + 'static>(
    provider: &P,
    arena: &mut StateArena,
    cycles: &[&FoundCycle],
    pool_metas: &[crate::pipeline::types::PoolMeta],
    word_range: i16,
    block_number: Option<u64>,
) {
    if cycles.is_empty() {
        return;
    }
    let tick_pools = collect_v3_pool_addresses(arena, cycles);
    let v4_targets = collect_v4_tick_targets(cycles, pool_metas);
    if tick_pools.is_empty() && v4_targets.is_empty() {
        return;
    }
    crate::pipeline::tick_fetch::clear_v3_pool_ticks(arena, &tick_pools);
    crate::pipeline::tick_fetch::clear_v4_pool_ticks(arena, &v4_targets);
    let (algebra_pools, algebra_integral_pools) =
        crate::pipeline::tick_fetch::collect_algebra_pools(arena, pool_metas);
    let v3_loaded = enrich_v3_ticks(
        provider,
        arena,
        &tick_pools,
        word_range,
        &algebra_pools,
        &algebra_integral_pools,
        block_number,
    )
    .await;
    let v4_loaded = enrich_v4_ticks(provider, arena, &v4_targets, word_range, block_number).await;
    crate::debug!(
        "dispatch tick enrich: v3_pools={} v3_loaded={v3_loaded} v4_targets={} v4_loaded={v4_loaded}",
        tick_pools.len(),
        v4_targets.len(),
    );
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
    let mut reassess_reject = 0usize;
    let filtered: Vec<HfEvalResult> = profitable
        .into_iter()
        .filter_map(|mut result| {
            let baseline = result.sim;
            let amount = result.opt.optimal_input;
            let refreshed = simulate_route_detailed(arena, &result.cycle.edges, amount)?;
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
                reassess_reject += 1;
                crate::debug!(
                    "resim fidelity drop: fp={} reason={reason} profit_drift_bps={}",
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
            ) {
                reassess_reject += 1;
                crate::debug!(
                    "resim hop fidelity drop: fp={} reject={reject:?}",
                    result.route_fingerprint,
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
            Some(result)
        })
        .collect();
    let out_count = filtered.len();
    let total_ms = crate::util::now_ms().saturating_sub(batch_started);
    if in_count > 0 && out_count < in_count {
        crate::info!(
            "resim batch: in={in_count} out={out_count} pools={pool_count} refreshed={pools_refreshed} refresh_ms={refresh_ms} unprofitable={resim_unprofitable} reassess_reject={reassess_reject} total_ms={total_ms}"
        );
    } else {
        crate::debug!(
            "resim batch: in={in_count} out={out_count} pools={pool_count} refreshed={pools_refreshed} refresh_ms={refresh_ms} unprofitable={resim_unprofitable} reassess_reject={reassess_reject} total_ms={total_ms}"
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

    let mut already_verified = Vec::new();
    let mut need_onchain_verify = Vec::with_capacity(balancer_only.len());
    for result in balancer_only {
        if result.balancer_batch_verified {
            already_verified.push(result);
        } else {
            need_onchain_verify.push(result);
        }
    }

    let mut jobs = Vec::with_capacity(need_onchain_verify.len());
    for result in need_onchain_verify {
        let fp = result.route_fingerprint;
        let Some(start_token) = arena.token_address(result.cycle.start_token) else {
            execution.quarantine_batch_query_failure(fp);
            crate::debug!("hf batch-filter: fp={fp} reject=missing_start_token");
            continue;
        };
        let Some(hops) = build_calldata_hops(
            arena,
            &result.cycle.edges,
            &result.sim.hop_amounts,
            &pool_metas_by_pool,
        ) else {
            execution.quarantine_batch_query_failure(fp);
            crate::debug!(
                "hf batch-filter: fp={fp} reject=calldata_build_failed modeled={} net_matic={}",
                result.sim.profit,
                result.assessment.net_profit_after_gas_matic_wei,
            );
            continue;
        };
        if !balancer_batch_within_max_in_ratio(arena, &hops) {
            execution.quarantine_batch_query_failure(fp);
            crate::debug!(
                "hf batch-filter: fp={fp} reject=max_in_ratio modeled={} net_matic={}",
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

    let route_gas = RouteGasLookup::for_fingerprints(
        reassess.gas_oracle.as_ref(),
        jobs.iter().map(|job| job.result.route_fingerprint),
    );
    let verified_balancer = stream::iter(jobs)
        .map(|job| {
            let sim_provider = sim_provider.clone();
            let execution = Arc::clone(&execution);
            let reassess = Arc::clone(&reassess);
            let route_gas = route_gas.clone();
            async move {
                let eval = reassess.as_eval_input(&route_gas);
                verify_balancer_batch_job(
                    execution.as_ref(),
                    job,
                    &sim_provider,
                    executor,
                    &eval,
                    &route_gas,
                    state_block,
                )
                .await
            }
        })
        .buffer_unordered(BATCH_VERIFY_CONCURRENCY)
        .filter_map(|result| async move { result })
        .collect::<Vec<_>>()
        .await;

    let mut verified = passthrough;
    verified.extend(verified_balancer);
    verified.extend(already_verified);
    verified
}

async fn verify_balancer_batch_job<P: Provider<Ethereum>>(
    execution: &crate::services::execution::ExecutionService,
    job: BalancerVerifyJob,
    sim_provider: &P,
    executor: alloy::primitives::Address,
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
    let accept = match &outcome {
        BatchQueryOutcome::Profit(on_chain_profit) => batch_profit_covers_min(
            *on_chain_profit,
            result.sim.amount_in,
            slippage,
            result.cycle.hop_count,
        ),
        _ => false,
    };
    if accept {
        let mut accepted = result;
        if let BatchQueryOutcome::Profit(on_chain_profit) = outcome {
            accepted.sim.profit = on_chain_profit;
            accepted.sim.amount_out = accepted.sim.amount_in.saturating_add(on_chain_profit);
            accepted.sim.profitable = true;
            let assessment = reassess_hf_eval_result(&accepted, eval, FlashLoanSource::Direct)?;
            if !assessment.should_execute {
                execution.quarantine_batch_query_failure(fp);
                crate::debug!(
                    "hf batch-filter: fp={fp} reject=reassess_after_on_chain on_chain={on_chain_profit} net_matic={}",
                    assessment.net_profit_after_gas_matic_wei,
                );
                return None;
            }
            accepted.assessment = assessment;
            accepted.balancer_batch_verified = true;
        }
        return Some(accepted);
    }
    execution.quarantine_batch_query_failure(fp);
    let modeled = result.sim.profit;
    let net_matic = result.assessment.net_profit_after_gas_matic_wei;
    match outcome {
        BatchQueryOutcome::Profit(on_chain) => {
            crate::debug!(
                "hf batch-filter: fp={fp} reject=on_chain_profit_below_min on_chain={on_chain} modeled={modeled} net_matic={net_matic} slippage_bps={slippage}"
            );
        }
        BatchQueryOutcome::NonPositiveDelta(delta) => {
            crate::debug!(
                "hf batch-filter: fp={fp} reject=non_positive_delta delta={delta} modeled={modeled} net_matic={net_matic}"
            );
        }
        BatchQueryOutcome::RpcError(reason) => {
            crate::debug!(
                "hf batch-filter: fp={fp} reject=rpc_error reason={reason} modeled={modeled} net_matic={net_matic}"
            );
        }
        BatchQueryOutcome::Timeout => {
            crate::debug!(
                "hf batch-filter: fp={fp} reject=timeout modeled={modeled} net_matic={net_matic}"
            );
        }
        BatchQueryOutcome::BuildFailed | BatchQueryOutcome::DecodeFailed => {
            crate::debug!(
                "hf batch-filter: fp={fp} reject=batch_query_build_decode modeled={modeled} net_matic={net_matic}"
            );
        }
    }
    None
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
    let Some(hops) = build_calldata_hops(
        arena,
        &result.cycle.edges,
        &result.sim.hop_amounts,
        &pool_metas_by_pool,
    ) else {
        crate::info!(
            "hf near-miss-verify: fp={fp} reject=calldata_build_failed modeled={}",
            result.sim.profit
        );
        return;
    };
    let modeled = result.sim.profit;
    let query_block = (state_block > 0).then_some(state_block);
    match query_balancer_batch_profit(sim_provider, executor, &hops, start_token, query_block).await
    {
        BatchQueryOutcome::Profit(on_chain) => {
            crate::info!(
                "hf near-miss-verify: fp={fp} modeled={modeled} on_chain={on_chain} net_matic={}",
                result.assessment.net_profit_after_gas_matic_wei,
            );
        }
        BatchQueryOutcome::NonPositiveDelta(delta) => {
            execution.quarantine_batch_query_failure(fp);
            log_phantom_balancer_diag(arena, result, &hops);
            crate::info!(
                "hf near-miss-verify: fp={fp} phantom sim modeled={modeled} vault_delta={delta} quarantined",
            );
        }
        BatchQueryOutcome::RpcError(reason) => {
            crate::info!("hf near-miss-verify: fp={fp} rpc_error={reason} modeled={modeled}");
        }
        BatchQueryOutcome::Timeout => {
            crate::info!("hf near-miss-verify: fp={fp} timeout modeled={modeled}");
        }
        BatchQueryOutcome::BuildFailed | BatchQueryOutcome::DecodeFailed => {
            crate::info!("hf near-miss-verify: fp={fp} build_decode_failed modeled={modeled}");
        }
    }
}

fn log_phantom_balancer_diag(
    arena: &StateArena,
    result: &HfEvalResult,
    hops: &[crate::services::execution::calldata::CalldataHop],
) {
    use crate::core::types::{BalancerPoolKind, PoolState};
    let mut hop = U256::ZERO;
    for (i, h) in hops.iter().enumerate() {
        let edge = &h.edge;
        let Some(addr) = arena.pool_address(edge.pool_index) else {
            continue;
        };
        let detail = match arena.pool_state(edge.pool_index) {
            Some(PoolState::Balancer(s)) => {
                let tin = edge.token_in_idx as usize;
                let tout = edge.token_out_idx as usize;
                let kind = match s.pool_type {
                    BalancerPoolKind::Weighted => "weighted",
                    BalancerPoolKind::Stable => "stable",
                    BalancerPoolKind::Linear => "linear",
                };
                let sim_out =
                    crate::core::math::balancer::simulate_balancer_swap(s, h.amount_in, tin, tout);
                format!(
                    "pool={addr} kind={kind} tin={tin} tout={tout} bal_in={} bal_out={} amt_in={} sim_out={sim_out} bpt={:?}",
                    s.balances.get(tin).copied().unwrap_or_default(),
                    s.balances.get(tout).copied().unwrap_or_default(),
                    h.amount_in,
                    s.bpt_index,
                )
            }
            _ => "non_balancer_state".into(),
        };
        crate::info!(
            "hf phantom-hop[{i}]: fp={} route_amt={} {detail}",
            result.route_fingerprint,
            if i == 0 { result.sim.amount_in } else { hop },
        );
        hop = h.amount_out;
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
mod tests {
    use super::*;
    use crate::core::types::{PoolState, V3PoolState, V3Tick};

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
}
