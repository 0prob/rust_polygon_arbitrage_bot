use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use alloy::primitives::{Address, U256};
use tokio::sync::watch;
use tokio::time::timeout;

use crate::config::AppConfig;
use crate::config::WalletSecrets;
use crate::core::constants::BPS_SCALE;
use crate::core::types::{FoundCycle, ProtocolType};
use crate::infra::hypersync::HyperSyncService;
use crate::infra::rpc::RpcPool;
use crate::orchestrator::hf_eval::HfEvalResult;
use crate::orchestrator::hf_eval::{HfEvalInputOwned, rescore_rank_and_evaluate_async};
use crate::orchestrator::hf_execute::{
    dispatch_profitable_candidates, filter_balancer_onchain_verified, probe_near_miss_balancer,
    refresh_and_resim_profitable,
};
use crate::orchestrator::ui_hook::SharedUiHook;
use crate::pipeline::arena::StateArena;
use crate::pipeline::sim_sanity::matic_usd_for_flash_cap;
use crate::pipeline::types::{PoolMeta, compare_cycle_score};
use crate::services::execution::flash_liquidity::{
    collect_flash_tokens_for_cycle, route_is_balancer_only,
};
use crate::services::execution::{
    ExecutionService, GasOracle, hash_cycle_edges, rotate_cycle_to_start,
};
use crate::services::hf_snapshot::SnapshotStore;
use crate::services::oracle::has_reliable_matic_rate;
use crate::services::oracle::price_oracle::PriceOracle;
use crate::services::partial_cache::PartialPoolCache;
use crate::services::state_cache::StateCache;
use crate::services::state_refresh::StateRefreshService;
use crate::util::now_ms;
use rustc_hash::FxHashSet;

pub struct HfContext {
    pub config: Arc<AppConfig>,
    pub refresh: Arc<StateRefreshService>,
    pub cache: Arc<StateCache>,
    pub partial_cache: Arc<PartialPoolCache>,
    pub snapshots: Arc<SnapshotStore>,
    pub execution: Arc<ExecutionService>,
    pub gas_oracle: Arc<GasOracle>,
    pub price_oracle: Arc<PriceOracle>,
    pub wallet: Arc<WalletSecrets>,
    pub rpc: Arc<RpcPool>,
    pub hypersync: Option<Arc<HyperSyncService>>,
    pub shutdown: watch::Receiver<bool>,
    pub ui_hook: SharedUiHook,
}

pub struct HfTickResult {
    pub cycles_considered: usize,
    pub profitable_count: usize,
    pub best_profit: U256,
    pub elapsed_ms: u64,
}

const HF_ACTIVITY_WINDOW_MS: u64 = 300_000;
const HF_SUMMARY_INTERVAL_MS: u64 = 15_000;
const HF_BEST_EVAL_INTERVAL_MS: u64 = 60_000;
const HF_EVAL_BUDGET: Duration = Duration::from_secs(30);
static HF_SUMMARY_LOG_AT: AtomicU64 = AtomicU64::new(0);
static HF_BEST_EVAL_LOG_AT: AtomicU64 = AtomicU64::new(0);

/// Prefer `start_token` when priced; otherwise rotate to the first hop token with a rate.
fn cycle_with_reliable_start(
    cycle: &Arc<FoundCycle>,
    token_to_matic_rates: &rustc_hash::FxHashMap<crate::core::types::TokenIndex, U256>,
) -> Option<Arc<FoundCycle>> {
    if has_reliable_matic_rate(cycle.start_token, token_to_matic_rates) {
        return Some(Arc::clone(cycle));
    }
    for edge in &cycle.edges {
        if has_reliable_matic_rate(edge.token_in, token_to_matic_rates) {
            return rotate_cycle_to_start(cycle, edge.token_in).map(Arc::new);
        }
    }
    None
}

fn should_log_hf_summary() -> bool {
    let now = now_ms();
    let last = HF_SUMMARY_LOG_AT.load(Ordering::Relaxed);
    if now.saturating_sub(last) < HF_SUMMARY_INTERVAL_MS {
        return false;
    }
    HF_SUMMARY_LOG_AT.store(now, Ordering::Relaxed);
    true
}

fn near_miss_verify_provider(
    rpc: &RpcPool,
    execution_mode: &str,
) -> anyhow::Result<alloy::providers::DynProvider> {
    if execution_mode.eq_ignore_ascii_case("dry-run") {
        rpc.connect_state().or_else(|_| rpc.connect_simulation())
    } else {
        rpc.connect_simulation()
    }
}

fn should_log_best_eval() -> bool {
    let now = now_ms();
    let last = HF_BEST_EVAL_LOG_AT.load(Ordering::Relaxed);
    if now.saturating_sub(last) < HF_BEST_EVAL_INTERVAL_MS {
        return false;
    }
    HF_BEST_EVAL_LOG_AT.store(now, Ordering::Relaxed);
    true
}

struct BestEvalDiag {
    fp: u64,
    hops: u32,
    input: U256,
    sim_gas: u32,
    gross: U256,
    net_matic: U256,
    gas_matic: U256,
    slippage: U256,
    flash_fee: U256,
    reject: Option<String>,
}

fn log_best_eval_diagnostic(diag: &BestEvalDiag) {
    let reason = diag.reject.as_deref().unwrap_or("unknown");
    crate::info!(
        "hf best-eval: fp={} hops={} input={} sim_gas={} gross={} net_matic={} gas_matic={} slippage={} flash_fee={} reject={}",
        diag.fp,
        diag.hops,
        diag.input,
        diag.sim_gas,
        diag.gross,
        diag.net_matic,
        diag.gas_matic,
        diag.slippage,
        diag.flash_fee,
        reason,
    );
}

fn cycle_activity_score(
    cycle: &FoundCycle,
    arena: &crate::pipeline::arena::StateArena,
    partial_cache: &PartialPoolCache,
    activity_now: u64,
) -> u64 {
    cycle
        .edges
        .iter()
        .filter_map(|edge| arena.pool_address(edge.pool_index))
        .filter_map(|address| partial_cache.get(&address))
        .map(|state| {
            if activity_now.saturating_sub(state.patched_at_ms) <= HF_ACTIVITY_WINDOW_MS {
                state.activity_count
            } else {
                0
            }
        })
        .sum()
}

/// Rank LF cycles for HF rescore: quarantine filter → score pre-prune → activity rank.
fn select_cycles_for_rescore(
    snap_cycles: &[Arc<FoundCycle>],
    arena: &crate::pipeline::arena::StateArena,
    partial_cache: &PartialPoolCache,
    execution: &ExecutionService,
    token_to_matic_rates: &rustc_hash::FxHashMap<crate::core::types::TokenIndex, U256>,
    rescore_cap: usize,
) -> (Vec<Arc<FoundCycle>>, FxHashSet<Address>, usize, usize) {
    let activity_now = now_ms();
    let mut candidates: Vec<(Arc<FoundCycle>, u64)> = Vec::with_capacity(snap_cycles.len());
    let mut quarantine_skipped = 0usize;
    let mut rate_skipped = 0usize;
    for cycle in snap_cycles {
        let fp = hash_cycle_edges(&cycle.edges);
        if execution.is_route_quarantined(fp) {
            quarantine_skipped += 1;
            continue;
        }
        let Some(ready) = cycle_with_reliable_start(cycle, token_to_matic_rates) else {
            rate_skipped += 1;
            continue;
        };
        candidates.push((ready, 0));
    }
    if candidates.is_empty() {
        return (
            Vec::new(),
            FxHashSet::default(),
            quarantine_skipped,
            rate_skipped,
        );
    }

    let prefilter_cap = rescore_cap.saturating_mul(3).max(rescore_cap + 1);
    if candidates.len() > prefilter_cap {
        let pivot = prefilter_cap - 1;
        candidates.select_nth_unstable_by(pivot, |a, b| {
            compare_cycle_score(a.0.as_ref(), b.0.as_ref())
        });
        candidates.truncate(prefilter_cap);
    }

    for (cycle, score) in &mut candidates {
        *score = cycle_activity_score(cycle.as_ref(), arena, partial_cache, activity_now);
    }
    // ponytail: all candidates already passed has_reliable_matic_rate filter
    // above, so sort by activity score then cycle score directly.
    candidates.sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then_with(|| compare_cycle_score(a.0.as_ref(), b.0.as_ref()))
    });
    candidates.truncate(rescore_cap);

    let mut cycles = Vec::with_capacity(candidates.len());
    let mut hot_pools = FxHashSet::default();
    for (cycle, _) in candidates {
        for edge in &cycle.edges {
            if let Some(addr) = arena.pool_address(edge.pool_index) {
                hot_pools.insert(addr);
            }
        }
        cycles.push(cycle);
    }
    (cycles, hot_pools, quarantine_skipped, rate_skipped)
}

pub async fn run_hf_tick(
    ctx: Arc<HfContext>,
    stream_triggered: bool,
) -> anyhow::Result<HfTickResult> {
    if ctx.refresh.is_indexer_stale() && ctx.config.pipeline.indexer_pause_on_lag {
        ctx.refresh.maybe_refresh_indexer_health().await;
        if ctx.refresh.is_indexer_stale() {
            crate::warn!("hf tick skipped: indexer lag exceeds threshold");
            return Ok(HfTickResult {
                cycles_considered: 0,
                profitable_count: 0,
                best_profit: U256::ZERO,
                elapsed_ms: 0,
            });
        }
    }

    let start = now_ms();
    let pipeline = &ctx.config.pipeline;
    let rescore_cap = pipeline.hf_score_cap;
    let sim_cap = pipeline.hf_sim_cap;

    let mut snap = ctx.snapshots.read();
    if snap.cycles.is_empty() {
        return Ok(HfTickResult {
            cycles_considered: 0,
            profitable_count: 0,
            best_profit: U256::ZERO,
            elapsed_ms: now_ms().saturating_sub(start),
        });
    }

    let snap_generation = snap.generation;
    let mut token_to_matic_rates = Arc::clone(&snap.token_to_matic_rates);
    let mut token_decimals = Arc::clone(&snap.token_decimals);
    let mut pool_metas_for_dispatch = Arc::clone(&snap.pool_metas);
    let mut arena_base = snap.arena.clone();
    let mut snap_cycle_count = snap.cycles.len();
    let mut cycles;
    let mut hot_pools_set;
    let mut quarantine_skipped;
    let mut rate_skipped;
    (cycles, hot_pools_set, quarantine_skipped, rate_skipped) = select_cycles_for_rescore(
        &snap.cycles,
        &arena_base,
        &ctx.partial_cache,
        &ctx.execution,
        &token_to_matic_rates,
        rescore_cap,
    );
    if cycles.is_empty() {
        if should_log_hf_summary() {
            crate::info!(
                "hf tick: 0 cycles after filter (snap={snap_cycle_count}, quarantine={quarantine_skipped}, no_rate={rate_skipped})"
            );
        }
        return Ok(HfTickResult {
            cycles_considered: 0,
            profitable_count: 0,
            best_profit: U256::ZERO,
            elapsed_ms: now_ms().saturating_sub(start),
        });
    }
    if stream_triggered && pipeline.stream_enabled {
        for addr in ctx.partial_cache.dirty_addresses() {
            hot_pools_set.insert(addr);
        }
    }
    let mut hot_pools: Arc<Vec<_>> = Arc::new(hot_pools_set.into_iter().collect());

    let Some(gas_price) = ctx.gas_oracle.conservative_gas_price() else {
        crate::warn!("hf tick skipped: gas oracle has no fee snapshot yet");
        return Ok(HfTickResult {
            cycles_considered: 0,
            profitable_count: 0,
            best_profit: U256::ZERO,
            elapsed_ms: now_ms().saturating_sub(start),
        });
    };

    let refresh = Arc::clone(&ctx.refresh);
    let prefetch_count = pipeline.hf_prefetch_count.min(hot_pools.len().max(1));
    let skip_prefetch = stream_triggered && pipeline.stream_enabled;
    let mut prefetch_ok = skip_prefetch;
    let prefetch = if skip_prefetch || hot_pools.is_empty() {
        None
    } else {
        let prefetch_hot = Arc::clone(&hot_pools);
        Some(tokio::spawn(async move {
            refresh
                .refresh_pool_states_for(prefetch_hot.as_ref(), prefetch_count)
                .await
        }))
    };

    if let Some(handle) = prefetch {
        let prefetch_budget =
            std::time::Duration::from_millis(pipeline.hf_prefetch_budget_ms.max(1));
        match tokio::time::timeout(prefetch_budget, handle).await {
            Ok(Ok(Ok(_))) => prefetch_ok = true,
            Ok(Ok(Err(e))) => crate::debug!("hf prefetch failed: {e:#}"),
            Ok(Err(e)) => crate::debug!("hf prefetch task failed: {e}"),
            Err(_) => crate::debug!(
                "hf prefetch timed out after {}ms",
                prefetch_budget.as_millis()
            ),
        }
    }

    if stream_triggered && pipeline.stream_enabled {
        let _ = ctx
            .partial_cache
            .flush_to_state_cache(&ctx.cache, hot_pools.as_ref());
    }

    let latest_generation = ctx.snapshots.generation();
    if latest_generation != snap_generation {
        snap = ctx.snapshots.read();
        token_to_matic_rates = Arc::clone(&snap.token_to_matic_rates);
        token_decimals = Arc::clone(&snap.token_decimals);
        pool_metas_for_dispatch = Arc::clone(&snap.pool_metas);
        arena_base = snap.arena.clone();
        snap_cycle_count = snap.cycles.len();
        let selected = select_cycles_for_rescore(
            &snap.cycles,
            &arena_base,
            &ctx.partial_cache,
            &ctx.execution,
            &token_to_matic_rates,
            rescore_cap,
        );
        cycles = selected.0;
        hot_pools = Arc::new(selected.1.into_iter().collect());
        quarantine_skipped = selected.2;
        rate_skipped = selected.3;
        if cycles.is_empty() {
            if should_log_hf_summary() {
                crate::info!(
                    "hf tick: 0 cycles after refresh (snap={snap_cycle_count}, quarantine={quarantine_skipped}, no_rate={rate_skipped})"
                );
            }
            return Ok(HfTickResult {
                cycles_considered: 0,
                profitable_count: 0,
                best_profit: U256::ZERO,
                elapsed_ms: now_ms().saturating_sub(start),
            });
        }
    }

    let mut arena = arena_base;
    let evaluation_state_generation = arena.apply_hot_cache(&ctx.cache, hot_pools.as_ref());
    ctx.execution
        .route_sim_cache
        .clear_stale(evaluation_state_generation);

    let mut flash_tokens = FxHashSet::default();
    let mut flash_token_list = Vec::new();
    for c in &cycles {
        collect_flash_tokens_for_cycle(
            &arena,
            c.as_ref(),
            &mut flash_tokens,
            &mut flash_token_list,
        );
    }
    if !flash_token_list.is_empty() {
        let flash_cache = Arc::clone(&ctx.execution.flash_liquidity);
        flash_cache.track_hot_tokens(&flash_token_list);
        let stale: Vec<Address> = flash_token_list
            .iter()
            .copied()
            .filter(|addr| !flash_cache.has_fresh_entry(*addr))
            .collect();
        if !stale.is_empty() {
            let flash_budget = std::time::Duration::from_millis(750);
            match tokio::time::timeout(
                flash_budget,
                flash_cache.refresh_with_fallback(&ctx.rpc, &stale),
            )
            .await
            {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => crate::debug!("hf flash prefetch failed: {e:#}"),
                Err(_) => crate::debug!(
                    "hf flash prefetch timed out after {}ms (stale={})",
                    flash_budget.as_millis(),
                    stale.len()
                ),
            }
        }
        flash_cache.spawn_refresh_if_stale(Arc::clone(&ctx.rpc), &flash_token_list);
    }

    let flash_policy = ctx.config.flash_policy;
    let matic_usd = matic_usd_for_flash_cap(ctx.price_oracle.cached_matic_usd().unwrap_or(0.0));

    let dispatch_token_to_matic_rates = Arc::clone(&token_to_matic_rates);
    let dispatch_token_decimals = Arc::clone(&token_decimals);
    let reassess_ctx = Arc::new(HfEvalInputOwned {
        arena: Arc::new(arena),
        token_to_matic_rates,
        token_decimals,
        gas_oracle: Arc::clone(&ctx.gas_oracle),
        state_generation: evaluation_state_generation,
        brent_iters: ctx.config.routing.ternary_search_iterations,
        min_profit_matic: ctx.config.min_profit_matic,
        min_profit_roi_bps: ctx.config.execution.min_profit_roi_bps,
        gas_price,
        slippage_bps: ctx.config.execution.slippage_bps,
        flash_policy,
        max_flash_loan_usd: ctx.config.execution.max_flash_loan_usd,
        matic_usd,
        safety_multiplier_bps: ctx.config.execution.profit_safety_multiplier_bps,
        profit_priority_alpha_bps: ctx.config.execution.profit_priority_fee_alpha_bps,
        flash_liquidity: Arc::clone(&ctx.execution.flash_liquidity),
        execution: Arc::clone(&ctx.execution),
    });
    let cycles_considered = cycles.len();
    let (eval_results, mut eval_arena, probe_kept) = match timeout(
        HF_EVAL_BUDGET,
        rescore_rank_and_evaluate_async(cycles, Arc::clone(&reassess_ctx), sim_cap),
    )
    .await
    {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            crate::warn!(
                "hf eval timed out after {}ms (cycles={cycles_considered}, sim_cap={sim_cap})",
                HF_EVAL_BUDGET.as_millis()
            );
            return Ok(HfTickResult {
                cycles_considered,
                profitable_count: 0,
                best_profit: U256::ZERO,
                elapsed_ms: now_ms().saturating_sub(start),
            });
        }
    };
    let eval_count = eval_results.len();

    let mut profitable: Vec<HfEvalResult> = Vec::new();
    let mut best_profit_matic = U256::ZERO;
    let mut best_near_miss: Option<HfEvalResult> = None;
    let mut best_gross_diag: Option<BestEvalDiag> = None;
    let mut best_gross_probe: Option<HfEvalResult> = None;

    for result in eval_results {
        let matic = result.assessment.net_profit_after_gas_matic_wei;
        if matic > best_profit_matic {
            best_profit_matic = matic;
        }
        let assessment = &result.assessment;
        let gross_dominated = best_gross_diag
            .as_ref()
            .is_none_or(|best| assessment.gross_profit > best.gross);
        if gross_dominated && !assessment.gross_profit.is_zero() {
            best_gross_diag = Some(BestEvalDiag {
                fp: result.route_fingerprint,
                hops: result.cycle.hop_count,
                input: result.sim.amount_in,
                sim_gas: result.sim.total_gas,
                gross: assessment.gross_profit,
                net_matic: assessment.net_profit_after_gas_matic_wei,
                gas_matic: assessment.revert_penalty,
                slippage: assessment.slippage_deduction,
                flash_fee: assessment.flash_loan_fee,
                reject: assessment.reject_reason.clone(),
            });
            if route_is_balancer_only(&result.cycle) {
                best_gross_probe = Some(result.clone());
            }
        }
        if assessment.should_execute {
            profitable.push(result);
        } else if matic > U256::ZERO {
            let dominated = best_near_miss
                .as_ref()
                .is_none_or(|best| matic > best.assessment.net_profit_after_gas_matic_wei);
            if dominated {
                best_near_miss = Some(result);
            }
        }
    }

    let mut skip_dispatch_refresh = prefetch_ok;
    let mut dispatch_state_generation = evaluation_state_generation;
    if !profitable.is_empty()
        && let Some(executor) = ctx.config.execution.executor_address
        && let Ok(sim_provider) = ctx.rpc.connect_simulation()
    {
        let (resimmed, resim_refreshed, resim_generation) = refresh_and_resim_profitable(
            &ctx.refresh,
            &ctx.cache,
            Arc::make_mut(&mut eval_arena),
            profitable,
            reassess_ctx.as_ref(),
        )
        .await;
        profitable = resimmed;
        skip_dispatch_refresh = prefetch_ok || resim_refreshed;
        if resim_refreshed {
            dispatch_state_generation = resim_generation;
        }
        profitable = filter_balancer_onchain_verified(
            Arc::clone(&ctx.execution),
            eval_arena.as_ref(),
            profitable,
            &sim_provider,
            executor,
            pool_metas_for_dispatch.as_ref(),
            ctx.config.execution.slippage_bps,
            Arc::clone(&reassess_ctx),
        )
        .await;
        best_profit_matic = profitable
            .iter()
            .map(|r| r.assessment.net_profit_after_gas_matic_wei)
            .max()
            .unwrap_or(U256::ZERO);
    }

    profitable.sort_unstable_by(|a, b| {
        let a_direct = route_is_balancer_only(&a.cycle);
        let b_direct = route_is_balancer_only(&b.cycle);
        b_direct.cmp(&a_direct).then_with(|| {
            b.assessment
                .net_profit_after_gas_matic_wei
                .cmp(&a.assessment.net_profit_after_gas_matic_wei)
        })
    });
    profitable.truncate(pipeline.hf_max_dispatch);
    let profitable_count = profitable.len();
    let elapsed_ms = now_ms().saturating_sub(start);

    if cycles_considered > 0 {
        if profitable_count > 0 || should_log_hf_summary() {
            crate::info!(
                "hf tick: {profitable_count} profitable of {cycles_considered} cycles ({elapsed_ms}ms, best_profit_matic={best_profit_matic}, probe_kept={probe_kept}, evaluated={eval_count})"
            );
        }
        if eval_count == 0 {
            crate::debug!(
                "hf assess: 0/{cycles_considered} routes produced assessments (sim_cap={sim_cap})"
            );
        } else if profitable_count == 0
            && let Some(ref near_miss) = best_near_miss
        {
            log_near_miss_diagnostic(
                &ctx.execution,
                near_miss,
                eval_arena.as_ref(),
                pool_metas_for_dispatch.as_ref(),
                ctx.config.execution.profit_safety_multiplier_bps,
                ctx.config.min_profit_matic,
            );
            if route_is_balancer_only(&near_miss.cycle)
                && let Some(executor) = ctx.config.execution.executor_address
                && let Ok(sim_provider) =
                    near_miss_verify_provider(&ctx.rpc, &ctx.config.execution.mode)
            {
                probe_near_miss_balancer(
                    &ctx.execution,
                    eval_arena.as_ref(),
                    near_miss,
                    pool_metas_for_dispatch.as_ref(),
                    &sim_provider,
                    executor,
                )
                .await;
            }
        } else if profitable_count == 0
            && best_profit_matic.is_zero()
            && should_log_best_eval()
            && let Some(ref diag) = best_gross_diag
        {
            log_best_eval_diagnostic(diag);
            if let Some(ref probe) = best_gross_probe
                && let Some(executor) = ctx.config.execution.executor_address
                && let Ok(sim_provider) =
                    near_miss_verify_provider(&ctx.rpc, &ctx.config.execution.mode)
            {
                probe_near_miss_balancer(
                    &ctx.execution,
                    eval_arena.as_ref(),
                    probe,
                    pool_metas_for_dispatch.as_ref(),
                    &sim_provider,
                    executor,
                )
                .await;
            }
        }
    }

    if profitable_count > 0 {
        dispatch_profitable_candidates(
            &ctx,
            Arc::make_mut(&mut eval_arena),
            profitable,
            crate::orchestrator::hf_execute::DispatchInputs {
                pool_metas: pool_metas_for_dispatch.as_ref(),
                token_to_matic_rates: dispatch_token_to_matic_rates.as_ref(),
                token_decimals: dispatch_token_decimals.as_ref(),
                state_generation: dispatch_state_generation,
                state_block: snap.state_block,
                state_hash: snap.state_hash,
                skip_dispatch_refresh,
            },
        )
        .await;
    }

    let tick_result = HfTickResult {
        cycles_considered,
        profitable_count,
        best_profit: best_profit_matic,
        elapsed_ms,
    };

    ctx.ui_hook.on_hf_tick(&tick_result, cycles_considered);
    if let Some(fee) = ctx.gas_oracle.snapshot() {
        let gwei = crate::util::u256_to_f64(fee.base_fee + fee.priority_fee) / 1e9;
        ctx.ui_hook.on_gas_update(gwei);
    }

    Ok(tick_result)
}

fn protocol_tag(protocol: ProtocolType) -> &'static str {
    match protocol {
        ProtocolType::UniswapV2 => "V2",
        ProtocolType::UniswapV3 => "V3",
        ProtocolType::UniswapV4 => "V4",
        ProtocolType::BalancerV2 => "BAL",
        ProtocolType::CurveStable => "CRV-S",
        ProtocolType::CurveCrypto => "CRV-C",
        ProtocolType::Dodo => "DODO",
        ProtocolType::Woofi => "WOOFI",
    }
}

fn near_miss_route_summary(
    arena: &StateArena,
    cycle: &FoundCycle,
    pool_metas: &[PoolMeta],
) -> String {
    // ponytail: single String alloc with write! avoids per-format! allocs
    let cap = cycle.edges.len().saturating_mul(64).max(64);
    let mut buf = String::with_capacity(cap);
    use std::fmt::Write;
    if let Some(addr) = arena.token_address(cycle.start_token) {
        // Write first 6 hex chars directly without intermediate hex::encode.
        let bytes = addr.as_slice();
        let _ = write!(
            buf,
            "0x{:02x}{:02x}..{:02x}{:02x}",
            bytes[0], bytes[1], bytes[2], bytes[3]
        );
    } else {
        let _ = write!(buf, "t{}", cycle.start_token.0);
    }
    for edge in &cycle.edges {
        let tag = crate::pipeline::types::pool_meta_at(pool_metas, edge.pool_index)
            .map(|m| protocol_tag(m.protocol))
            .unwrap_or_else(|| protocol_tag(edge.protocol));
        let _ = write!(buf, "->{tag}:");
        if let Some(addr) = arena.token_address(edge.token_out) {
            let bytes = addr.as_slice();
            let _ = write!(
                buf,
                "0x{:02x}{:02x}..{:02x}{:02x}",
                bytes[0], bytes[1], bytes[2], bytes[3]
            );
        } else {
            let _ = write!(buf, "t{}", edge.token_out.0);
        }
    }
    buf
}

fn log_near_miss_diagnostic(
    execution: &ExecutionService,
    result: &HfEvalResult,
    arena: &StateArena,
    pool_metas: &[PoolMeta],
    safety_bps: u64,
    min_profit_matic: U256,
) {
    let assessment = &result.assessment;
    let net_matic = assessment.net_profit_after_gas_matic_wei;
    if !execution.should_log_near_miss(result.route_fingerprint, net_matic) {
        return;
    }
    let safety_floor = crate::services::execution::profit::safety_floor_matic_wei(
        assessment.revert_penalty,
        safety_bps,
    );
    let gap = safety_floor.saturating_sub(net_matic);
    let roi_bps = (assessment.roi * f64::from(BPS_SCALE)).round() as u64;
    let reason = assessment.reject_reason.as_deref().unwrap_or("unknown");
    crate::info!(
        "hf near-miss: fp={} hops={} score={:.4} route={} input={} gross={} net_matic={} safety_floor={} gap={} min_profit={} roi_bps={} gas_matic={} slippage={} flash_fee={} reject={}",
        result.route_fingerprint,
        result.cycle.hop_count,
        result.cycle.score,
        near_miss_route_summary(arena, &result.cycle, pool_metas),
        result.opt.optimal_input,
        assessment.gross_profit,
        net_matic,
        safety_floor,
        gap,
        min_profit_matic,
        roi_bps,
        assessment.revert_penalty,
        assessment.slippage_deduction,
        assessment.flash_loan_fee,
        reason,
    );
}
