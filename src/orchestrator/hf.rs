use std::sync::Arc;

use alloy::primitives::{Address, U256};
use tokio::sync::watch;

use crate::config::AppConfig;
use crate::config::WalletSecrets;
use crate::core::constants::BPS_SCALE;
use crate::core::types::{FoundCycle, ProtocolType};
use crate::infra::hypersync::HyperSyncService;
use crate::infra::rpc::RpcPool;
use crate::orchestrator::hf_eval::HfEvalResult;
use crate::orchestrator::hf_eval::{HfEvalInputOwned, rescore_rank_and_evaluate_async};
use crate::orchestrator::hf_execute::dispatch_profitable_candidates;
use crate::orchestrator::ui_hook::SharedUiHook;
use crate::pipeline::arena::StateArena;
use crate::pipeline::types::{PoolMeta, compare_cycle_score};
use crate::services::execution::flash_liquidity::collect_flash_tokens_for_cycle;
use crate::services::execution::{
    ExecutionService, GasOracle, flash_policy::parse_flash_policy, hash_cycle_edges,
};
use crate::services::hf_snapshot::SnapshotStore;
use crate::services::oracle::has_reliable_matic_rate;
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
) -> (Vec<Arc<FoundCycle>>, FxHashSet<Address>) {
    let activity_now = now_ms();
    let mut candidates: Vec<(usize, u64)> = Vec::with_capacity(snap_cycles.len());
    for (i, cycle) in snap_cycles.iter().enumerate() {
        let fp = hash_cycle_edges(&cycle.edges);
        if execution.is_route_quarantined(fp) {
            continue;
        }
        if !has_reliable_matic_rate(cycle.start_token, token_to_matic_rates) {
            continue;
        }
        candidates.push((i, 0));
    }
    if candidates.is_empty() {
        return (Vec::new(), FxHashSet::default());
    }

    let prefilter_cap = rescore_cap.saturating_mul(3).max(rescore_cap + 1);
    if candidates.len() > prefilter_cap {
        let pivot = prefilter_cap - 1;
        candidates.select_nth_unstable_by(pivot, |a, b| {
            compare_cycle_score(&snap_cycles[a.0], &snap_cycles[b.0])
        });
        candidates.truncate(prefilter_cap);
    }

    for (idx, score) in &mut candidates {
        *score = cycle_activity_score(&snap_cycles[*idx], arena, partial_cache, activity_now);
    }
    // ponytail: all candidates already passed has_reliable_matic_rate filter
    // above, so sort by activity score then cycle score directly.
    candidates.sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then_with(|| compare_cycle_score(&snap_cycles[a.0], &snap_cycles[b.0]))
    });
    candidates.truncate(rescore_cap);

    let mut cycles = Vec::with_capacity(candidates.len());
    let mut hot_pools = FxHashSet::default();
    for (idx, _) in candidates {
        let cycle = &snap_cycles[idx];
        for edge in &cycle.edges {
            if let Some(addr) = arena.pool_address(edge.pool_index) {
                hot_pools.insert(addr);
            }
        }
        cycles.push(Arc::clone(cycle));
    }
    (cycles, hot_pools)
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
    let token_to_matic_rates = Arc::clone(&snap.token_to_matic_rates);
    let token_decimals = Arc::clone(&snap.token_decimals);
    let pool_metas_for_dispatch = Arc::clone(&snap.pool_metas);
    let arena_base = snap.arena.clone();

    let (cycles, mut hot_pools) = select_cycles_for_rescore(
        &snap.cycles,
        &arena_base,
        &ctx.partial_cache,
        &ctx.execution,
        &token_to_matic_rates,
        rescore_cap,
    );
    drop(snap);
    if cycles.is_empty() {
        return Ok(HfTickResult {
            cycles_considered: 0,
            profitable_count: 0,
            best_profit: U256::ZERO,
            elapsed_ms: now_ms().saturating_sub(start),
        });
    }
    if pipeline.stream_enabled {
        for addr in ctx.partial_cache.tracked_addresses() {
            hot_pools.insert(addr);
        }
    }
    let mut hot_vec = Vec::with_capacity(hot_pools.len());
    hot_vec.extend(hot_pools);
    let hot_pools: Arc<Vec<_>> = Arc::new(hot_vec);

    let refresh = Arc::clone(&ctx.refresh);
    let prefetch_count = pipeline.hf_prefetch_count.min(hot_pools.len().max(1));
    let skip_prefetch = stream_triggered && pipeline.stream_enabled;
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
    let mut arena = arena_base;
    let Some(gas_price) = ctx.gas_oracle.conservative_gas_price() else {
        if let Some(handle) = prefetch {
            handle.abort();
        }
        crate::warn!("hf tick skipped: gas oracle has no fee snapshot yet");
        return Ok(HfTickResult {
            cycles_considered: 0,
            profitable_count: 0,
            best_profit: U256::ZERO,
            elapsed_ms: now_ms().saturating_sub(start),
        });
    };

    if let Some(handle) = prefetch {
        const PREFETCH_BUDGET: std::time::Duration = std::time::Duration::from_millis(1500);
        match tokio::time::timeout(PREFETCH_BUDGET, handle).await {
            Ok(Ok(Ok(_))) => {}
            Ok(Ok(Err(e))) => crate::debug!("hf prefetch failed: {e:#}"),
            Ok(Err(e)) => crate::debug!("hf prefetch task failed: {e}"),
            Err(_) => crate::debug!(
                "hf prefetch timed out after {}ms",
                PREFETCH_BUDGET.as_millis()
            ),
        }
    }

    if stream_triggered && pipeline.stream_enabled {
        let _ = ctx
            .partial_cache
            .flush_to_state_cache(&ctx.cache, hot_pools.as_ref());
    }

    let evaluation_state_generation = arena.apply_hot_cache(&ctx.cache, hot_pools.as_ref());

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
        match ctx.rpc.connect_state() {
            Ok(provider) => {
                if let Err(e) = ctx
                    .execution
                    .flash_liquidity
                    .refresh(&provider, &flash_token_list)
                    .await
                {
                    crate::warn!("hf flash liquidity refresh failed: {e:#}");
                }
            }
            Err(e) => {
                crate::warn!("hf flash liquidity refresh skipped: state RPC unavailable: {e:#}")
            }
        }
    }

    let flash_policy = parse_flash_policy(&ctx.config.execution.flash_loan_source);

    let dispatch_token_to_matic_rates = Arc::clone(&token_to_matic_rates);
    let dispatch_token_decimals = Arc::clone(&token_decimals);
    let owned = HfEvalInputOwned {
        arena,
        token_to_matic_rates,
        token_decimals,
        gas_oracle: Arc::clone(&ctx.gas_oracle),
        brent_iters: ctx.config.routing.ternary_search_iterations,
        min_profit_matic: ctx.config.min_profit_matic,
        min_profit_roi_bps: ctx.config.execution.min_profit_roi_bps,
        gas_price,
        slippage_bps: ctx.config.execution.slippage_bps,
        flash_policy,
        max_flash_loan_usd: ctx.config.execution.max_flash_loan_usd,
        safety_multiplier_bps: ctx.config.execution.profit_safety_multiplier_bps,
        flash_liquidity: Arc::clone(&ctx.execution.flash_liquidity),
        execution: Arc::clone(&ctx.execution),
    };
    let eval_arena = owned.arena.clone();
    let cycles_considered = cycles.len();
    let eval_results = rescore_rank_and_evaluate_async(cycles, owned, sim_cap).await?;
    let eval_count = eval_results.len();

    let mut profitable: Vec<HfEvalResult> = Vec::new();
    let mut best_profit_matic = U256::ZERO;
    let mut best_near_miss: Option<HfEvalResult> = None;

    for result in eval_results {
        let matic = result.assessment.net_profit_after_gas_matic_wei;
        if matic > best_profit_matic {
            best_profit_matic = matic;
        }

        if result.assessment.should_execute {
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

    profitable.sort_unstable_by(|a, b| {
        b.assessment
            .net_profit_after_gas_matic_wei
            .cmp(&a.assessment.net_profit_after_gas_matic_wei)
    });
    profitable.truncate(pipeline.hf_max_dispatch);
    let profitable_count = profitable.len();
    let elapsed_ms = now_ms().saturating_sub(start);

    if cycles_considered > 0 {
        if profitable_count > 0 {
            crate::info!(
                "hf tick: {profitable_count} profitable of {cycles_considered} cycles ({elapsed_ms}ms, best_profit_matic={best_profit_matic}, evaluated={eval_count})"
            );
        } else {
            crate::debug!(
                "hf tick: {profitable_count} profitable of {cycles_considered} cycles ({elapsed_ms}ms, best_profit_matic={best_profit_matic}, evaluated={eval_count})"
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
                &eval_arena,
                pool_metas_for_dispatch.as_ref(),
                ctx.config.execution.profit_safety_multiplier_bps,
                ctx.config.min_profit_matic,
            );
        }
    }

    if profitable_count > 0 {
        dispatch_profitable_candidates(
            &ctx,
            &eval_arena,
            profitable,
            pool_metas_for_dispatch.as_ref(),
            dispatch_token_to_matic_rates.as_ref(),
            dispatch_token_decimals.as_ref(),
            evaluation_state_generation,
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
        let hex = alloy::primitives::hex::encode(&addr.as_slice()[..6]);
        let _ = write!(buf, "0x{}..{}", &hex[..4], &hex[4..6]);
    } else {
        let _ = write!(buf, "t{}", cycle.start_token.0);
    }
    for edge in &cycle.edges {
        let tag = crate::pipeline::types::pool_meta_at(pool_metas, edge.pool_index)
            .map(|m| protocol_tag(m.protocol))
            .unwrap_or_else(|| protocol_tag(edge.protocol));
        let _ = write!(buf, "->{tag}:");
        if let Some(addr) = arena.token_address(edge.token_out) {
            let hex = alloy::primitives::hex::encode(&addr.as_slice()[..6]);
            let _ = write!(buf, "0x{}..{}", &hex[..4], &hex[4..6]);
        } else {
            let _ = write!(buf, "t{}", edge.token_out.0);
        }
    }
    buf
}

fn near_miss_safety_floor(
    assessment: &crate::core::types::ProfitAssessment,
    safety_bps: u64,
) -> U256 {
    assessment
        .revert_penalty
        .saturating_mul(U256::from(safety_bps))
        / U256::from(BPS_SCALE)
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
    let safety_floor = near_miss_safety_floor(assessment, safety_bps);
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
