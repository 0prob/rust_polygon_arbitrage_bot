use std::sync::Arc;

use parking_lot::Mutex as ParkingMutex;
use ruint::aliases::U256;
use tokio::sync::{Mutex, watch};
use tracing::{debug, info, instrument, warn};

use crate::config::AppConfig;
use crate::config::WalletSecrets;
use crate::infra::hypersync::HyperSyncService;
use crate::infra::metrics::PipelineMetrics;
use crate::infra::rpc::RpcPool;
use crate::infra::tracing_util::{pool_addrs_csv, start_token_addr};
use crate::orchestrator::dispatch_queue::{
    PendingDispatch, queue_pending_dispatch, take_pending_dispatch,
};
use crate::orchestrator::hf_eval::{HfEvalInputOwned, rescore_rank_and_evaluate_async};
use crate::orchestrator::hf_execute::dispatch_profitable_candidates;
use crate::services::execution::flash_liquidity::collect_flash_tokens_for_cycle;
use crate::orchestrator::ui_hook::SharedUiHook;
use crate::pipeline::types::{route_fingerprint as fp};
use rustc_hash::FxHashSet;
use crate::services::execution::{
    ExecutionService, GasOracle, OpportunityRecord, evaluated_from_sim,
    flash_policy::{hf_eval_flash_source, parse_flash_policy},
    log_opportunity_evaluated,
};
use crate::services::hf_snapshot::SnapshotStore;
use crate::services::partial_cache::PartialPoolCache;
use crate::services::state_cache::StateCache;
use crate::services::state_refresh::StateRefreshService;
use crate::util::now_ms;

pub struct HfContext {
    pub config: Arc<AppConfig>,
    pub refresh: Arc<StateRefreshService>,
    pub cache: Arc<StateCache>,
    pub partial_cache: Arc<PartialPoolCache>,
    pub snapshots: Arc<SnapshotStore>,
    pub execution: Arc<ExecutionService>,
    pub gas_oracle: Arc<GasOracle>,
    pub wallet: Arc<WalletSecrets>,
    pub rpc: Arc<RpcPool>,
    pub metrics: Arc<PipelineMetrics>,
    pub hypersync: Option<Arc<HyperSyncService>>,
    pub shutdown: watch::Receiver<bool>,
    /// Prevents overlapping execution dispatches from stacking RPC/submit work.
    pub dispatch_lock: Arc<Mutex<()>>,
    pub pending_dispatch: Arc<ParkingMutex<Option<PendingDispatch>>>,
    pub ui_hook: SharedUiHook,
}

pub struct HfTickResult {
    pub cycles_considered: usize,
    pub profitable_count: usize,
    pub best_profit: U256,
    pub elapsed_ms: u64,
}

#[instrument(
    skip(ctx),
    fields(
        cycles_considered = tracing::field::Empty,
        profitable_count = tracing::field::Empty,
        best_profit = tracing::field::Empty,
        elapsed_ms = tracing::field::Empty,
    )
)]
pub async fn run_hf_tick(
    ctx: Arc<HfContext>,
    stream_triggered: bool,
) -> anyhow::Result<HfTickResult> {
    sync_indexer_execution_gate(&ctx).await;
    try_drain_pending_dispatch(&ctx).await;

    let start = now_ms();
    let snap = ctx.snapshots.read();

    if snap.cycles.is_empty() {
        return Ok(HfTickResult {
            cycles_considered: 0,
            profitable_count: 0,
            best_profit: U256::ZERO,
            elapsed_ms: now_ms().saturating_sub(start),
        });
    }

    let pipeline = &ctx.config.pipeline;
    let rescore_cap = pipeline.hf_score_cap;
    let sim_cap = pipeline.hf_sim_cap;

    let mut hot_pools = rustc_hash::FxHashSet::default();
    for cycle in snap.cycles.iter().take(rescore_cap) {
        for edge in &cycle.edges {
            if let Some(addr) = snap.arena.pool_address(edge.pool_index) {
                hot_pools.insert(addr);
            }
        }
    }
    if pipeline.stream_enabled {
        for addr in ctx.partial_cache.tracked_addresses() {
            hot_pools.insert(addr);
        }
    }
    let hot_pools: Vec<_> = hot_pools.into_iter().collect();

    let refresh = Arc::clone(&ctx.refresh);
    let prefetch_count = pipeline.hf_prefetch_count.min(hot_pools.len().max(1));
    let skip_prefetch = stream_triggered && pipeline.stream_enabled;
    let prefetch_hot = hot_pools.clone();
    let prefetch = if skip_prefetch || prefetch_hot.is_empty() {
        None
    } else {
        Some(tokio::spawn(async move {
            refresh
                .refresh_pool_states_for(&prefetch_hot, prefetch_count)
                .await
        }))
    };

    let cycles: Vec<_> = snap.cycles.iter().take(rescore_cap).cloned().collect();
    let mut arena = snap.arena.clone();
    let gas_price = ctx.gas_oracle.conservative_gas_price();

    if let Some(handle) = prefetch {
        let _ = handle.await??;
    }

    if stream_triggered && pipeline.stream_enabled {
        let flushed = ctx
            .partial_cache
            .flush_to_state_cache(&ctx.cache, &hot_pools);
        if flushed > 0 {
            debug!(flushed, "partial cache flushed to state cache");
        }
    }
    arena.apply_hot_cache(&ctx.cache, &hot_pools);

    let mut flash_tokens = FxHashSet::default();
    let mut flash_token_list = Vec::new();
    for c in &cycles {
        collect_flash_tokens_for_cycle(&arena, c, &mut flash_tokens, &mut flash_token_list);
    }
    if !flash_token_list.is_empty() {
        if let Ok(provider) = ctx.rpc.connect_state() {
            let _ = ctx
                .execution
                .flash_liquidity
                .refresh(&provider, &flash_token_list)
                .await;
        }
    }

    let flash_source = hf_eval_flash_source(parse_flash_policy(
        &ctx.config.execution.flash_loan_source,
    ));

    let owned = HfEvalInputOwned::with_shared_maps(
        arena,
        Arc::clone(&snap.token_to_matic_rates),
        Arc::clone(&snap.token_decimals),
        Arc::clone(&ctx.gas_oracle),
        ctx.config.routing.ternary_search_iterations,
        parse_min_profit(&ctx.config)?,
        ctx.config.execution.min_profit_roi_bps,
        gas_price,
        ctx.config.execution.slippage_bps,
        flash_source,
        ctx.config.execution.max_flash_loan_usd,
        ctx.config.execution.profit_safety_multiplier_bps,
        Arc::clone(&ctx.execution.flash_liquidity),
    );
    let eval_arena = owned.arena.clone();
    let eval_gas_price = owned.gas_price;
    let cycles_considered = cycles.len();
    let eval_results = rescore_rank_and_evaluate_async(cycles, owned, sim_cap).await?;

    let mut profitable = Vec::new();
    let mut best_profit = U256::ZERO;

    for result in eval_results {
        let route_fp = fp(&result.cycle.edges);
        if result.assessment.net_profit_after_gas > best_profit {
            best_profit = result.assessment.net_profit_after_gas;
        }

        if result.assessment.should_execute {
            debug!(
                route_fingerprint = route_fp,
                hop_count = result.cycle.hop_count,
                amount_in = %result.opt.optimal_input,
                gross_profit = %result.sim.profit,
                net_profit_matic = %result.assessment.net_profit_after_gas_matic_wei,
                gas_units = result.sim.total_gas,
                gas_price_wei = %eval_gas_price,
                roi = result.assessment.roi,
                pool_addrs = pool_addrs_csv(&eval_arena, &result.cycle.edges),
                start_token = start_token_addr(&eval_arena, &result.cycle)
                    .map(|t| format!("{t}"))
                    .unwrap_or_default(),
                "opportunity profitable"
            );
            let record = OpportunityRecord::from_assessment(
                route_fp,
                result.cycle.hop_count,
                result.sim.amount_in,
                result.sim.total_gas,
                eval_gas_price,
                result.effective_slippage_bps,
                &result.assessment,
                true,
                now_ms().saturating_sub(start),
            );
            profitable.push(evaluated_from_sim(
                result.cycle,
                result.sim,
                result.assessment,
                result.effective_slippage_bps,
            ));
            log_opportunity_evaluated(&record);
        } else {
            debug!(
                route_fingerprint = route_fp,
                net_profit_matic = %result.assessment.net_profit_after_gas_matic_wei,
                reject_reason = result
                    .assessment
                    .reject_reason
                    .as_deref()
                    .unwrap_or("unknown"),
                "opportunity rejected"
            );
            log_opportunity_evaluated(&OpportunityRecord::from_assessment(
                route_fp,
                result.cycle.hop_count,
                result.sim.amount_in,
                result.sim.total_gas,
                eval_gas_price,
                result.effective_slippage_bps,
                &result.assessment,
                false,
                now_ms().saturating_sub(start),
            ));
        }
    }

    profitable.sort_by(|a, b| {
        b.assessment
            .as_ref()
            .map(|x| x.net_profit_after_gas_matic_wei)
            .unwrap_or(U256::ZERO)
            .cmp(
                &a.assessment
                    .as_ref()
                    .map(|x| x.net_profit_after_gas_matic_wei)
                    .unwrap_or(U256::ZERO),
            )
    });
    profitable.truncate(pipeline.hf_max_dispatch);
    let profitable_count = profitable.len();
    let elapsed_ms = now_ms().saturating_sub(start);

    let span = tracing::Span::current();
    span.record("cycles_considered", cycles_considered);
    span.record("profitable_count", profitable_count);
    span.record("best_profit", tracing::field::display(&best_profit));
    span.record("elapsed_ms", elapsed_ms);

    if profitable_count > 0 {
        let indexer_stale = ctx.refresh.is_indexer_stale();
        if indexer_stale && ctx.config.pipeline.indexer_pause_on_lag {
            warn!(
                lag = ctx.refresh.indexer_lag_blocks(),
                max_lag = ctx.config.pipeline.indexer_max_lag_blocks,
                profitable = profitable_count,
                "indexer stale — skipping dispatch"
            );
            return Ok(HfTickResult {
                cycles_considered,
                profitable_count,
                best_profit,
                elapsed_ms,
            });
        }

        let route_fps: Vec<u64> = profitable.iter().map(|r| fp(&r.cycle.edges)).collect();
        info!(
            profitable = profitable_count,
            best_profit = %best_profit,
            dry_run = ctx.config.is_dry_run(),
            route_fingerprints = ?route_fps,
            elapsed_ms,
            "hf profitable cycles"
        );
        // #region agent log
        crate::debug_agent::log(
            "H-B",
            "hf.rs:run_hf_tick",
            "profitable_dispatch_spawn",
            serde_json::json!({
                "profitable_count": profitable_count,
                "dry_run": ctx.config.is_dry_run(),
                "indexer_stale": indexer_stale,
                "route_fingerprints": route_fps,
                "best_profit": best_profit.to_string(),
            }),
        );
        // #endregion

        let pool_metas = snap.pool_metas.clone();
        let dispatch_arena = eval_arena.clone();
        drop(snap);

        let dispatch_ctx = Arc::clone(&ctx);
        let dispatch_lock = Arc::clone(&ctx.dispatch_lock);
        ctx.metrics.record_dispatch_started();
        tokio::spawn(async move {
            let Ok(guard) = dispatch_lock.try_lock() else {
                dispatch_ctx.metrics.record_dispatch_deferred();
                warn!(
                    profitable = profitable_count,
                    "dispatch deferred — queued for next slot"
                );
                queue_pending_dispatch(
                    &dispatch_ctx.pending_dispatch,
                    PendingDispatch {
                        arena: dispatch_arena,
                        profitable,
                        pool_metas,
                    },
                );
                return;
            };
            let _guard = guard;
            run_dispatch_loop(&dispatch_ctx, dispatch_arena, profitable, pool_metas).await;
        });
    } else {
        debug!(cycles = cycles_considered, "hf tick — no profitable cycles");
    }

    let tick_result = HfTickResult {
        cycles_considered,
        profitable_count,
        best_profit,
        elapsed_ms,
    };

    ctx.ui_hook.on_hf_tick(&tick_result, cycles_considered);
    if let Some(fee) = ctx.gas_oracle.snapshot() {
        let gwei = crate::util::u256_to_f64(fee.base_fee + fee.priority_fee) / 1e9;
        ctx.ui_hook.on_gas_update(gwei);
    }

    Ok(tick_result)
}

async fn try_drain_pending_dispatch(ctx: &HfContext) {
    if ctx.pending_dispatch.lock().is_none() {
        return;
    }
    let Ok(guard) = ctx.dispatch_lock.try_lock() else {
        return;
    };
    let _guard = guard;
    let Some(pending) = take_pending_dispatch(&ctx.pending_dispatch) else {
        return;
    };
    info!(
        routes = pending.profitable.len(),
        "processing queued dispatch before hf eval"
    );
    run_dispatch_loop(
        ctx,
        pending.arena,
        pending.profitable,
        pending.pool_metas,
    )
    .await;
}

async fn sync_indexer_execution_gate(ctx: &HfContext) {
    ctx.refresh.maybe_refresh_indexer_health().await;
}

fn parse_min_profit(config: &AppConfig) -> anyhow::Result<U256> {
    config
        .execution
        .min_profit_matic_wei
        .parse::<U256>()
        .map_err(|e| anyhow::anyhow!("invalid min_profit_matic_wei: {e}"))
}

async fn run_dispatch_loop(
    ctx: &HfContext,
    arena: crate::pipeline::arena::StateArena,
    profitable: Vec<crate::core::types::EvaluatedRoute>,
    pool_metas: Vec<crate::pipeline::types::PoolMeta>,
) {
    let mut current_arena = arena;
    let mut current_routes = profitable;
    let mut current_metas = pool_metas;

    loop {
        dispatch_profitable_candidates(ctx, &current_arena, current_routes, &current_metas).await;
        let Some(pending) = take_pending_dispatch(&ctx.pending_dispatch) else {
            break;
        };
        current_arena = pending.arena;
        current_routes = pending.profitable;
        current_metas = pending.pool_metas;
        info!(
            routes = current_routes.len(),
            "processing queued dispatch batch"
        );
    }
}
