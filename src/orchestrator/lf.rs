use std::sync::Arc;

use alloy::primitives::{Address, U256};
use anyhow::Context;
use parking_lot::Mutex;
use rustc_hash::FxHashMap;
use smallvec::SmallVec;
use tokio::sync::{Mutex as AsyncMutex, watch};
use tokio::time::{Duration, MissedTickBehavior, interval};

use crate::config::AppConfig;
use crate::core::constants::{HOP_CAP, POLYGON_HUB_TOKENS};
use crate::core::math::fixed_point::ONE;
use crate::core::types::{PoolIndex, TokenIndex};
use crate::infra::rpc::RpcPool;
use crate::orchestrator::ui_hook::SharedUiHook;
use crate::pipeline::arena::StateArena;
use crate::pipeline::cycle_filter::{ProbeContext, retain_cycles_with_priced_start};
use crate::pipeline::cycle_finder::CYCLE_ENUM_PATCH_BUDGET;
use crate::pipeline::cycle_search::{find_cycles_for_mode, find_cycles_for_mode_with_budget};
use crate::pipeline::graph::{
    GraphBuildGate, attach_missing_eligible_pools_with_gate, build_graph_with_gate,
    count_graph_eligible_pools_with_gate, has_missing_eligible_pools_with_gate,
};
use crate::pipeline::graph_cache::GraphCache;
use crate::pipeline::spot_price::{
    SpotTable, finalize_enumerated_cycles, rescore_cycles_with_table,
};
use crate::pipeline::tick_fetch::{
    TickEnrichment, collect_v3_pool_addresses, collect_v4_tick_targets, enrich_v3_ticks,
    enrich_v4_ticks, still_tickless_v3, still_tickless_v4,
};
use crate::pipeline::types::CycleSearchPass;
use crate::services::execution::GasOracle;
use crate::services::execution::flash_liquidity::FlashLiquidityCache;
use crate::services::hf_snapshot::SnapshotStore;
use crate::services::oracle::price_oracle::PriceOracle;
use crate::services::oracle::{
    HubPathRateParams, RateEnrichContext, arena_tokens_without_decimal_hints,
    enrich_token_to_matic_rates, enrich_token_to_matic_rates_offline, expand_hub_spoke_resolvable,
    merge_token_rates, resolvable_token_set,
};
use crate::services::partial_cache::{
    PartialPoolCache, StreamAddressSet, select_stream_targets_with_epoch,
};
use crate::services::pipeline_survival::PipelineSurvival;
use crate::services::state_cache::StateCache;
use crate::services::state_refresh::StateRefreshService;

struct LfCpuWork {
    graph_cache: Arc<Mutex<GraphCache>>,
    cache: Arc<StateCache>,
    arena: StateArena,
    pool_metas: Arc<Vec<crate::pipeline::types::PoolMeta>>,
    dirty_pools: Vec<PoolIndex>,
    state_generation: u64,
    lf_pass: u64,
    max_paths: usize,
    max_hops: u32,
    cycle_finder: crate::config::CycleFinderMode,
    /// Prior-tick MATIC rates — enough for economic probe sizing at enumeration.
    prior_rates: Arc<FxHashMap<TokenIndex, U256>>,
    token_decimals: Arc<FxHashMap<Address, u8>>,
    gas_price_wei: Option<U256>,
    flash_liquidity: Arc<FlashLiquidityCache>,
    /// Topic-observed pools just admitted — growth rebuilds keep the cycle cache,
    /// so force an incremental refind or those venues never appear on HF cycles.
    force_cycle_refind: bool,
    /// Arena indices for `force_cycle_refind` — pinned through diversity selection.
    observed_pool_indices: Vec<PoolIndex>,
}

struct LfCpuResult {
    graph: Arc<crate::pipeline::types::RoutingGraph>,
    cycles: Arc<Vec<crate::core::types::FoundCycle>>,
    /// Cycle enumeration only (0 when graph cache reused).
    enumeration_ms: u64,
    enumerated_cycles: usize,
}

/// Keep cycles that touch freshly observed pools, then fill remaining slots with
/// the normal protocol-diverse selection (so live WSS venues are not dropped).
fn pin_cycles_touching_pools(
    cycles: Vec<crate::core::types::FoundCycle>,
    pin_pools: &[PoolIndex],
    max_cycles: usize,
) -> Vec<crate::core::types::FoundCycle> {
    if max_cycles == 0 || cycles.is_empty() || pin_pools.is_empty() {
        return finalize_enumerated_cycles(cycles, max_cycles);
    }
    let pin: rustc_hash::FxHashSet<PoolIndex> = pin_pools.iter().copied().collect();
    let mut pinned = Vec::new();
    let mut rest = Vec::with_capacity(cycles.len());
    for cycle in cycles {
        if cycle
            .edges
            .iter()
            .any(|edge| pin.contains(&edge.pool_index))
        {
            pinned.push(cycle);
        } else {
            rest.push(cycle);
        }
    }
    if pinned.is_empty() {
        return finalize_enumerated_cycles(rest, max_cycles);
    }
    pinned.sort_by(crate::pipeline::types::compare_cycle_score);
    pinned.truncate(max_cycles);
    let pin_kept = pinned.len();
    let mut seen: rustc_hash::FxHashSet<u64> = pinned
        .iter()
        .map(|c| crate::pipeline::cycle_filter::cycle_key(&c.edges))
        .collect();
    let fill = finalize_enumerated_cycles(rest, max_cycles.saturating_sub(pinned.len()));
    for cycle in fill {
        let key = crate::pipeline::cycle_filter::cycle_key(&cycle.edges);
        if seen.insert(key) {
            pinned.push(cycle);
            if pinned.len() >= max_cycles {
                break;
            }
        }
    }
    crate::info!(
        "stream observed-live: pinned_cycles={pin_kept} total={} (cap={max_cycles})",
        pinned.len()
    );
    pinned
}

/// Token endpoints of observed pools — seed incremental DFS (hub starts miss peripherals).
fn observed_pool_start_tokens(
    pool_metas: &[crate::pipeline::types::PoolMeta],
    pools: &[PoolIndex],
) -> Vec<TokenIndex> {
    if pools.is_empty() {
        return Vec::new();
    }
    let want: rustc_hash::FxHashSet<PoolIndex> = pools.iter().copied().collect();
    let mut out = Vec::new();
    let mut seen = rustc_hash::FxHashSet::default();
    for meta in pool_metas {
        if !want.contains(&meta.pool_index) {
            continue;
        }
        for &t in &meta.tokens {
            if seen.insert(t) {
                out.push(t);
            }
        }
    }
    out
}

fn cycle_search_passes(max_hops: u32, max_paths: usize) -> SmallVec<[CycleSearchPass; 2]> {
    let max_hops = max_hops.clamp(2, HOP_CAP);
    let mut passes: SmallVec<[CycleSearchPass; 2]> = SmallVec::new();
    if max_hops <= 4 {
        // One pass through the configured hop cap — avoids spending half the shared
        // DFS deadline on a 3-only tranche when max_hops is 4 (common on Polygon).
        passes.push(CycleSearchPass {
            max_hops,
            max_cycles: max_paths,
        });
        return passes;
    }

    passes.push(CycleSearchPass {
        max_hops: 3,
        max_cycles: max_paths / 2,
    });
    passes.push(CycleSearchPass {
        max_hops,
        max_cycles: max_paths,
    });
    passes
}

fn lf_graph_build_gate(work: &LfCpuWork) -> GraphBuildGate {
    GraphBuildGate {
        token_to_matic_rates: Arc::clone(&work.prior_rates),
        flash: work.flash_liquidity.load(),
        flash_ttl: work.flash_liquidity.ttl(),
    }
}

/// Merge LF-tick dirty pools with any cache writes that landed while the CPU job was queued.
fn merge_dirty_pool_indices(mut base: Vec<PoolIndex>, extra: Vec<PoolIndex>) -> Vec<PoolIndex> {
    if extra.is_empty() {
        return base;
    }
    if base.is_empty() {
        return extra;
    }
    // O(n+m) vs Vec::contains O(n*m) when both sides are large.
    let mut seen: rustc_hash::FxHashSet<u32> = rustc_hash::FxHashSet::with_capacity_and_hasher(
        base.len() + extra.len(),
        Default::default(),
    );
    for idx in &base {
        seen.insert(idx.0);
    }
    for idx in extra {
        if seen.insert(idx.0) {
            base.push(idx);
        }
    }
    base
}

/// Overlay cache-hot pool states onto the LF arena snapshot before graph rescoring.
fn overlay_dirty_cache_states(work: &mut LfCpuWork) {
    let fresh_dirty = work
        .cache
        .take_dirty_pool_indices(work.arena.address_to_pool());
    work.dirty_pools = merge_dirty_pool_indices(std::mem::take(&mut work.dirty_pools), fresh_dirty);
    if work.dirty_pools.is_empty() {
        work.state_generation = work.cache.generation();
        return;
    }
    let addrs: Vec<Address> = work
        .dirty_pools
        .iter()
        .filter_map(|idx| work.arena.pool_address(*idx))
        .collect();
    if !addrs.is_empty() {
        work.arena.apply_hot_cache(&work.cache, &addrs);
    }
    work.state_generation = work.cache.generation();
}

fn run_lf_cpu_work(mut work: LfCpuWork) -> LfCpuResult {
    overlay_dirty_cache_states(&mut work);
    let routable_count = work.pool_metas.len();
    let graph_gate = lf_graph_build_gate(&work);
    let gate_ref = graph_gate.active().then_some(&graph_gate);
    let layout_fp = work.arena.routing_layout_fingerprint();
    let eligible_count =
        count_graph_eligible_pools_with_gate(&work.arena, work.pool_metas.as_ref(), gate_ref);

    // Snapshot decisions without holding lock for duration of heavy work.
    // Pure eligible-pool growth (`connectivity_stale`) is handled by attach_missing
    // below — OR-ing it into needs_rebuild forced a full rebuild every LF warmup tick.
    let (needs_rebuild, connectivity_stale) = {
        let gc = work.graph_cache.lock();
        (
            gc.needs_connectivity_rebuild(routable_count, layout_fp),
            gc.connectivity_stale(eligible_count),
        )
    };

    let graph_action;
    // Capture cycle-cache validity *before* rebuild store can replace graph meta.
    let (cycle_cache_valid, prior_cycles) = {
        let gc = work.graph_cache.lock();
        (
            gc.cycle_cache_still_valid(routable_count, layout_fp),
            gc.cycles(),
        )
    };
    let mut graph = if needs_rebuild {
        graph_action = if connectivity_stale {
            "rebuild_eligible_growth"
        } else {
            "rebuild"
        };
        if connectivity_stale {
            let gc = work.graph_cache.lock();
            crate::debug!(
                "lf graph rebuild: eligible_pools={eligible_count} cached_eligible={}",
                gc.cached_eligible_pool_count()
            );
        }
        let build_started = crate::util::now_ms();
        let unpriced_pools = gate_ref.map_or(0, |gate| {
            crate::pipeline::graph::count_graph_eligible_unpriced_pools(
                &work.arena,
                work.pool_metas.as_ref(),
                gate,
            )
        });
        // Build outside lock to keep critical section short.
        let g = Arc::new(build_graph_with_gate(
            &work.arena,
            work.pool_metas.as_ref(),
            gate_ref,
        ));
        let build_ms = crate::util::now_ms().saturating_sub(build_started);
        let stats = crate::pipeline::graph::topology_stats(g.as_ref());
        stats.log_summary(graph_action);
        crate::info!(
            "graph build: ms={build_ms} eligible={eligible_count} unpriced_pools={unpriced_pools} routable_metas={routable_count}",
        );
        let mut gc = work.graph_cache.lock();
        // Keep prior cycles when PoolIndex layout is still valid (growth). Clearing
        // them here forced a full 1s DFS every warmup rebuild.
        let keep_cycles = if cycle_cache_valid {
            prior_cycles.clone()
        } else {
            None
        };
        gc.store(
            Arc::clone(&g),
            keep_cycles,
            routable_count,
            layout_fp,
            work.state_generation,
            eligible_count,
        );
        g
    } else {
        graph_action = "cache";
        let mut gc = work.graph_cache.lock();
        if gc.graph().is_none() {
            let gg = Arc::new(build_graph_with_gate(
                &work.arena,
                work.pool_metas.as_ref(),
                gate_ref,
            ));
            gc.store(
                Arc::clone(&gg),
                None,
                routable_count,
                layout_fp,
                work.state_generation,
                eligible_count,
            );
        }
        if work.state_generation != gc.cached_state_generation() {
            // Use helper: mutates under &mut self when rc==1 (no prior .graph() clone in scope)
            // avoiding the full RoutingGraph data clone on every state-gen change.
            gc.rescore_dirty_and_update(
                &work.arena,
                &work.dirty_pools,
                work.pool_metas.len(),
                work.state_generation,
                layout_fp,
                routable_count,
                eligible_count,
            )
            .or_else(|| gc.graph())
            .unwrap_or_else(|| {
                let gg = Arc::new(crate::pipeline::graph::build_graph(
                    &work.arena,
                    work.pool_metas.as_ref(),
                ));
                gc.store(
                    Arc::clone(&gg),
                    None,
                    routable_count,
                    layout_fp,
                    work.state_generation,
                    eligible_count,
                );
                gg
            })
        } else {
            gc.graph().unwrap_or_else(|| {
                let gg = Arc::new(crate::pipeline::graph::build_graph(
                    &work.arena,
                    work.pool_metas.as_ref(),
                ));
                gc.store(
                    Arc::clone(&gg),
                    None,
                    routable_count,
                    layout_fp,
                    work.state_generation,
                    eligible_count,
                );
                gg
            })
        }
    };

    let missing_graph_pools = if has_missing_eligible_pools_with_gate(
        &work.arena,
        work.pool_metas.as_ref(),
        graph.as_ref(),
        gate_ref,
    ) {
        let g = Arc::make_mut(&mut graph);
        attach_missing_eligible_pools_with_gate(&work.arena, g, work.pool_metas.as_ref(), gate_ref)
            .attached_pools
    } else {
        0
    };
    if missing_graph_pools > 0 {
        let stats = crate::pipeline::graph::topology_stats(graph.as_ref());
        stats.log_summary("patch_attach");
        crate::info!(
            "lf graph patch: attached {missing_graph_pools} eligible pools missing from cached adjacency"
        );
        // Preserve cycle cache through attach — short incremental DFS merges new routes.
        let keep_cycles = if cycle_cache_valid {
            prior_cycles.clone()
        } else {
            None
        };
        work.graph_cache.lock().store(
            Arc::clone(&graph),
            keep_cycles,
            routable_count,
            layout_fp,
            work.state_generation,
            eligible_count,
        );
    }

    let cached_cycles = {
        let gc = work.graph_cache.lock();
        gc.cycles()
    }
    .or(prior_cycles);
    let need_cycle_refind = {
        let gc = work.graph_cache.lock();
        // Newly attached pools are invisible to cached cycles until we re-enumerate.
        // Growth rebuilds no longer clear the cycle cache when indices stay valid.
        (needs_rebuild && !cycle_cache_valid)
            || missing_graph_pools > 0
            || work.force_cycle_refind
            || gc.cycles().is_none()
            || cached_cycles.is_none()
            || gc.needs_cycle_refind(
                routable_count,
                layout_fp,
                work.state_generation,
                work.dirty_pools.len(),
                work.arena.pool_count(),
            )
    };
    // Incremental: attach/observed-admit — full rebuild (interval/shrink/reorder) keeps 1s budget.
    let incremental_refind = cycle_cache_valid
        && cached_cycles.as_ref().is_some_and(|c| !c.is_empty())
        && (work.force_cycle_refind
            || (!needs_rebuild && (missing_graph_pools > 0 || connectivity_stale)));
    let mut enumeration_ms = 0u64;
    let (cycles, enumerated_cycles) = if need_cycle_refind {
        crate::debug!(
            "lf cycle refind: pass={} dirty_pools={} state_gen={} incremental={} attached={} rebuild={}",
            work.lf_pass,
            work.dirty_pools.len(),
            work.state_generation,
            incremental_refind,
            missing_graph_pools,
            needs_rebuild
        );
        let passes = cycle_search_passes(work.max_hops, work.max_paths);
        let probe_ctx = ProbeContext {
            token_to_matic_rates: Some(work.prior_rates.as_ref()),
            token_decimals: Some(work.token_decimals.as_ref()),
            gas_price_wei: work.gas_price_wei,
        };
        // Arena tokens are append-only; a cached graph may lag. Grow token slots
        // before DFS/BF so used_tokens / dist arrays cover every TokenIndex.
        if work.arena.token_count() > graph.token_count {
            Arc::make_mut(&mut graph).ensure_token_capacity(work.arena.token_count());
        }
        let enum_started = crate::util::now_ms();
        let obs_starts =
            observed_pool_start_tokens(work.pool_metas.as_ref(), &work.observed_pool_indices);
        if !obs_starts.is_empty() {
            crate::info!(
                "stream observed-live: dfs_seed tokens={} pools={} incremental={incremental_refind} lf_pass={}",
                obs_starts.len(),
                work.observed_pool_indices.len(),
                work.lf_pass
            );
        }
        let outcome = if incremental_refind {
            find_cycles_for_mode_with_budget(
                work.cycle_finder,
                &work.arena,
                &graph,
                work.pool_metas.as_ref(),
                passes.as_slice(),
                true,
                Some(&probe_ctx),
                CYCLE_ENUM_PATCH_BUDGET,
                &obs_starts,
            )
        } else if obs_starts.is_empty() {
            find_cycles_for_mode(
                work.cycle_finder,
                &work.arena,
                &graph,
                work.pool_metas.as_ref(),
                passes.as_slice(),
                true,
                Some(&probe_ctx),
            )
        } else {
            // Full refind still hub-seeds by default — inject observed endpoints.
            find_cycles_for_mode_with_budget(
                work.cycle_finder,
                &work.arena,
                &graph,
                work.pool_metas.as_ref(),
                passes.as_slice(),
                true,
                Some(&probe_ctx),
                crate::pipeline::cycle_finder::CYCLE_ENUM_TIME_BUDGET,
                &obs_starts,
            )
        };
        outcome.diag.log_summary();
        let mut result = outcome.cycles;
        if incremental_refind && let Some(cached) = cached_cycles.as_ref() {
            result.reserve(cached.len());
            result.extend(cached.iter().cloned());
        }
        if work.lf_pass <= 2 || work.lf_pass.is_multiple_of(30) {
            let mut hop_hist = [0u32; HOP_CAP as usize + 1];
            for c in &result {
                let h = c.hop_count.min(HOP_CAP) as usize;
                hop_hist[h] = hop_hist[h].saturating_add(1);
            }
            crate::debug!(
                "cycle search hops: max_hops={} passes={} pre_diversity={} by_hop={hop_hist:?}",
                work.max_hops,
                passes.len(),
                result.len()
            );
        }
        enumeration_ms = crate::util::now_ms().saturating_sub(enum_started);
        let enumerated_cycles = result.len();
        let diversified = if work.observed_pool_indices.is_empty() {
            finalize_enumerated_cycles(result, work.max_paths)
        } else {
            pin_cycles_touching_pools(result, &work.observed_pool_indices, work.max_paths)
        };
        if work.lf_pass <= 2 || work.lf_pass.is_multiple_of(30) {
            crate::debug!(
                "cycle search diversity: cap={} post_diversity={}",
                work.max_paths,
                diversified.len()
            );
        }
        let cycles = Arc::new(diversified);
        work.graph_cache.lock().store(
            Arc::clone(&graph),
            Some(Arc::clone(&cycles)),
            routable_count,
            layout_fp,
            work.state_generation,
            eligible_count,
        );
        (cycles, enumerated_cycles)
    } else {
        let cached = cached_cycles.unwrap_or_default();
        if work.lf_pass <= 2 || work.lf_pass.is_multiple_of(30) {
            crate::debug!(
                "lf cycle cache: pass={} cycles={}",
                work.lf_pass,
                cached.len()
            );
        }
        // Keep cache metadata current even when cycles are reused.
        work.graph_cache.lock().store(
            Arc::clone(&graph),
            Some(Arc::clone(&cached)),
            routable_count,
            layout_fp,
            work.state_generation,
            eligible_count,
        );
        let enumerated_cycles = cached.len();
        (cached, enumerated_cycles)
    };

    if graph_action == "cache"
        && missing_graph_pools == 0
        && (work.lf_pass <= 2 || work.lf_pass.is_multiple_of(30))
    {
        crate::pipeline::graph::topology_stats(graph.as_ref()).log_summary("cache_hit");
    }

    LfCpuResult {
        graph,
        cycles,
        enumeration_ms,
        enumerated_cycles,
    }
}

async fn run_lf_cpu_async(work: LfCpuWork) -> anyhow::Result<LfCpuResult> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    crate::util::lf_cpu_pool().spawn(move || {
        let result = crate::util::run_lf_cpu(|| run_lf_cpu_work(work));
        let _ = tx.send(result);
    });
    rx.await.context("lf cpu worker dropped")
}

pub struct LfContext {
    pub config: Arc<AppConfig>,
    pub refresh: Arc<StateRefreshService>,
    pub cache: Arc<StateCache>,
    pub snapshots: Arc<SnapshotStore>,
    pub stream_addresses: StreamAddressSet,
    pub partial_cache: Arc<PartialPoolCache>,
    pub price_oracle: Arc<PriceOracle>,
    pub gas_oracle: Arc<GasOracle>,
    pub rpc: Arc<RpcPool>,
    pub graph_cache: Arc<Mutex<GraphCache>>,
    pub arena: Arc<parking_lot::Mutex<StateArena>>,
    pub tick_lock: Arc<AsyncMutex<()>>,
    pub ui_hook: SharedUiHook,
    pub flash_liquidity: Arc<FlashLiquidityCache>,
}

pub async fn run_lf_tick(ctx: &LfContext, shutdown: &watch::Receiver<bool>) -> anyhow::Result<()> {
    if *shutdown.borrow() {
        return Ok(());
    }
    let lf_started = crate::util::now_ms();
    let lf_pass = ctx.graph_cache.lock().advance_pass();
    let refresh_batch = ctx.refresh.lf_refresh_batch(lf_pass);
    if lf_pass <= 2 || lf_pass.is_multiple_of(30) {
        crate::info!("lf refresh: pass={lf_pass} batch={refresh_batch}");
    }

    let discovery_started = crate::util::now_ms();
    let _ = ctx.refresh.maybe_discover().await?;
    let discovery_ms = crate::util::now_ms().saturating_sub(discovery_started);

    // Promote topic-observed venues into this tick's refresh before arena sync.
    // Filter to discovery-routable streamable pools — live wssobs was dominated by
    // disabled QS/Uni V2 (`0x882df4…`) that can never match the index / arena.
    let observed_raw = ctx.partial_cache.take_observed_live();
    let observed = ctx.refresh.filter_observed_live_routable(&observed_raw);
    if !observed_raw.is_empty() {
        crate::info!(
            "stream observed-live: topic_n={} routable={} skipped={}",
            observed_raw.len(),
            observed.len(),
            observed_raw.len().saturating_sub(observed.len())
        );
    }
    if !observed.is_empty() {
        let mut hot = ctx.refresh.hot_addresses().as_ref().to_vec();
        hot.extend(observed.iter().copied());
        hot.sort_unstable();
        hot.dedup();
        ctx.refresh.set_hot_addresses(hot);
        match ctx
            .refresh
            .refresh_pool_states_for(&observed, observed.len().min(64))
            .await
        {
            Ok(result) => crate::info!(
                "stream observed-live: refresh matched={} targeted_updated={}",
                result.matched,
                result.updated
            ),
            Err(e) => crate::warn!("stream observed-live targeted refresh failed: {e:#}"),
        }
    }

    let refresh_started = crate::util::now_ms();
    let refresh_result = ctx.refresh.refresh_pool_states(refresh_batch).await?;
    let refreshed_pools = refresh_result.updated;
    let refresh_ms = crate::util::now_ms().saturating_sub(refresh_started);
    ctx.refresh.prune_dead_pools_if_due(lf_pass);

    let pools = ctx.refresh.discovered_pools();
    if pools.is_empty() {
        if ctx.refresh.is_discovery_bootstrapping() {
            crate::info!("lf tick waiting for postgres bootstrap to finish");
        } else {
            crate::warn!("lf tick skipped: no discovered pools after bootstrap");
        }
        ctx.ui_hook.on_lf_complete(0, 0, pools.len());
        return Ok(());
    }
    if lf_pass <= 2 {
        crate::services::discovery::log_protocol_distribution(&pools);
    }

    let mut arena = ctx.arena.lock().clone();
    crate::debug!(
        "lf sync: discovered={}, cache_size={}",
        pools.len(),
        ctx.cache.len()
    );
    let decimals = ctx.refresh.token_decimals_map();
    let arena_sync_started = crate::util::now_ms();
    let pool_metas = Arc::new(
        ctx.refresh
            .sync_routable_arena(&mut arena, Some(decimals.as_ref())),
    );
    let arena_sync_ms = crate::util::now_ms().saturating_sub(arena_sync_started);
    if lf_pass <= 2 || lf_pass.is_multiple_of(30) {
        let hints_missing = arena_tokens_without_decimal_hints(&arena, decimals.as_ref());
        crate::debug!(
            "token arena: tokens={} decimals_map={} missing_hints={} pool_metas={}",
            arena.token_count(),
            decimals.len(),
            hints_missing,
            pool_metas.len()
        );
    }
    let max_paths = ctx.config.routing.enumeration_max_paths as usize;
    let max_hops = ctx.config.routing.max_hops;

    let prior_rates = ctx.snapshots.token_to_matic_rates();

    let state_provider = ctx.rpc.connect_state().ok();
    let state_generation = ctx.cache.generation();
    let dirty_pools = ctx.cache.take_dirty_pool_indices(arena.address_to_pool());
    let gas_price_wei = ctx.gas_oracle.conservative_gas_price();
    if gas_price_wei.is_none() && lf_pass <= 2 {
        crate::debug!("lf cycle prefilter: gas oracle not warm yet (no gas floor at enumeration)");
    }
    let observed_pool_indices: Vec<PoolIndex> = {
        let map = arena.address_to_pool();
        observed
            .iter()
            .filter_map(|addr| map.get(addr).copied())
            .collect()
    };
    let force_cycle_refind = !observed_pool_indices.is_empty();
    // Keep a copy — post-CPU `finalize_enumerated_cycles` was dropping pinned
    // live cycles (forceenum: pinned_cycles≤14 in CPU, cycles_touching=0 after).
    let observed_pin = observed_pool_indices.clone();
    let cpu_work = LfCpuWork {
        graph_cache: Arc::clone(&ctx.graph_cache),
        cache: Arc::clone(&ctx.cache),
        arena: arena.clone(),
        pool_metas: Arc::clone(&pool_metas),
        dirty_pools,
        state_generation,
        lf_pass,
        max_paths,
        max_hops,
        cycle_finder: ctx.config.routing.cycle_finder,
        prior_rates: Arc::clone(&prior_rates),
        token_decimals: Arc::clone(&decimals),
        gas_price_wei,
        flash_liquidity: Arc::clone(&ctx.flash_liquidity),
        force_cycle_refind,
        observed_pool_indices,
    };
    // ponytail: skip enrich_all_token_to_matic_rates — prior_rates from last tick
    // is sufficient for resolvable set computation. Cycle-token enrichment below
    // provides fresh rates for profit evaluation, saving one oracle RPC pass.
    let cpu_started = crate::util::now_ms();
    let cpu = run_lf_cpu_async(cpu_work).await;
    let cpu = cpu?;
    let cpu_ms = crate::util::now_ms().saturating_sub(cpu_started);
    if *shutdown.borrow() {
        return Ok(());
    }

    let mut resolvable = resolvable_token_set(&prior_rates, &arena);
    expand_hub_spoke_resolvable(&mut resolvable, pool_metas.as_ref(), &arena);
    let resolvable_count = resolvable.len();

    let cycles_arc = cpu.cycles;
    let mut routing_graph = cpu.graph;
    let cycle_search_ms = cpu.enumeration_ms;
    let enumerated_cycles = cpu.enumerated_cycles;

    // ponytail: collect unique cycle tokens without intermediate HashSet→Vec copy
    let mut cycle_tokens: Vec<TokenIndex> =
        Vec::with_capacity(cycles_arc.len().saturating_mul(4).max(16));
    let mut cycle_tokens_set = rustc_hash::FxHashSet::default();
    for c in cycles_arc.iter() {
        if cycle_tokens_set.insert(c.start_token) {
            cycle_tokens.push(c.start_token);
        }
        for e in &c.edges {
            if cycle_tokens_set.insert(e.token_in) {
                cycle_tokens.push(e.token_in);
            }
            if cycle_tokens_set.insert(e.token_out) {
                cycle_tokens.push(e.token_out);
            }
        }
    }
    for hub in POLYGON_HUB_TOKENS {
        if let Some(&idx) = arena.address_to_token().get(&hub)
            && cycle_tokens_set.insert(idx)
        {
            cycle_tokens.push(idx);
        }
    }

    let ticks_started = crate::util::now_ms();
    let mut v3_tick_targets = 0usize;
    let mut v3_ticks_loaded = 0usize;
    let mut v3_ticks_ms = 0u64;
    let mut v4_tick_targets = 0usize;
    let mut v4_ticks_loaded = 0usize;
    let mut v4_ticks_ms = 0u64;
    if state_provider.is_some() {
        let tick_pools = collect_v3_pool_addresses(&arena, cycles_arc.as_ref());
        let v4_tick_pools = collect_v4_tick_targets(cycles_arc.as_ref(), pool_metas.as_ref());
        let state_block = ctx.refresh.last_state_block();
        let pinned_block = (state_block > 0).then_some(state_block);
        let (algebra_pools, algebra_integral_pools) =
            crate::pipeline::tick_fetch::collect_algebra_pools(&arena, pool_metas.as_ref());
        // Only hydrate pools missing ticks — arena sync now preserves CL ticks
        // when sqrt_price/liquidity/tick are unchanged, so most LF passes skip RPC.
        let tick_pools_needed: Vec<Address> = tick_pools
            .iter()
            .copied()
            .filter(|addr| {
                // Skip pools that stayed empty after a recent full hydrate.
                if crate::pipeline::tick_fetch::is_empty_tick_on_cooldown(*addr) {
                    return false;
                }
                let Some(&idx) = arena.address_to_pool().get(addr) else {
                    return false;
                };
                match arena.pool_state(idx) {
                    Some(crate::core::types::PoolState::V3(s)) => s.ticks.is_empty(),
                    _ => true,
                }
            })
            .collect();
        let v4_tick_pools_needed: Vec<_> = v4_tick_pools
            .iter()
            .copied()
            .filter(|(idx, pool_id)| {
                // Skip pools that stayed empty after a recent full hydrate.
                if crate::pipeline::tick_fetch::is_empty_v4_tick_on_cooldown(*pool_id) {
                    return false;
                }
                match arena.pool_state(*idx) {
                    Some(crate::core::types::PoolState::V4(s)) => s.ticks.is_empty(),
                    _ => true,
                }
            })
            .collect();
        // Log how many cycle CL pools still need hydration (not the full set).
        v3_tick_targets = tick_pools_needed.len();
        v4_tick_targets = v4_tick_pools_needed.len();
        if !tick_pools_needed.is_empty() || !v4_tick_pools_needed.is_empty() {
            // Clear once before URL fallback — retry must not wipe a family that
            // already hydrated when the other family's RPC failed.
            crate::pipeline::tick_fetch::clear_v3_pool_ticks(&mut arena, &tick_pools_needed);
            crate::pipeline::tick_fetch::clear_v4_pool_ticks(&mut arena, &v4_tick_pools_needed);
            let mut v3_pending = !tick_pools_needed.is_empty();
            let mut v4_pending = !v4_tick_pools_needed.is_empty();
            let mut v3 = TickEnrichment::default();
            let mut v4 = TickEnrichment::default();
            for (url_index, url) in ctx.rpc.state_url_candidates().iter().enumerate() {
                let Ok(provider) = ctx.rpc.connect_state_at(url) else {
                    ctx.rpc.deprioritize_state_url(url);
                    continue;
                };
                if v3_pending {
                    let needed = still_tickless_v3(&arena, &tick_pools_needed);
                    if needed.is_empty() {
                        v3_pending = false;
                    } else {
                        let v3_ticks_started = crate::util::now_ms();
                        v3 = enrich_v3_ticks(
                            &provider,
                            &mut arena,
                            &needed,
                            ctx.config.oracle.tick_word_range,
                            &algebra_pools,
                            &algebra_integral_pools,
                            pinned_block,
                        )
                        .await;
                        v3_ticks_ms = v3_ticks_ms
                            .saturating_add(crate::util::now_ms().saturating_sub(v3_ticks_started));
                        v3_ticks_loaded = v3_ticks_loaded.saturating_add(v3.loaded);
                        v3_pending = v3.rpc_failed;
                    }
                }
                if v4_pending {
                    let needed = still_tickless_v4(&arena, &v4_tick_pools_needed);
                    if needed.is_empty() {
                        v4_pending = false;
                    } else {
                        let v4_ticks_started = crate::util::now_ms();
                        v4 = enrich_v4_ticks(
                            &provider,
                            &mut arena,
                            &needed,
                            ctx.config.oracle.tick_word_range,
                            pinned_block,
                        )
                        .await;
                        v4_ticks_ms = v4_ticks_ms
                            .saturating_add(crate::util::now_ms().saturating_sub(v4_ticks_started));
                        v4_ticks_loaded = v4_ticks_loaded.saturating_add(v4.loaded);
                        v4_pending = v4.rpc_failed;
                    }
                }
                if !v3_pending && !v4_pending {
                    if url_index > 0 {
                        crate::info!(
                            "LF tick hydration fallback succeeded (url_index={url_index}, v3_loaded={}, v4_loaded={})",
                            v3.loaded,
                            v4.loaded
                        );
                    }
                    break;
                }
                ctx.rpc.deprioritize_state_url(url);
                crate::warn!(
                    "LF tick hydration RPC failed — trying fallback (url_index={url_index}, v3_pending={v3_pending}, v4_pending={v4_pending})"
                );
            }
        } // tick_pools_needed non-empty
    }
    let ticks_ms = crate::util::now_ms().saturating_sub(ticks_started);

    let finalize_started = crate::util::now_ms();
    let mut capped = (*cycles_arc).clone();
    // Hold topic-live cycles aside before post-hydrate dead prune — rescore was
    // zeroing them out (repin: CPU pinned_cycles≤23, post-filter touching=0).
    let observed_pin_set: rustc_hash::FxHashSet<PoolIndex> = observed_pin.iter().copied().collect();
    let pre_hold_touching = if observed_pin_set.is_empty() {
        0
    } else {
        capped
            .iter()
            .filter(|cycle| {
                cycle
                    .edges
                    .iter()
                    .any(|edge| observed_pin_set.contains(&edge.pool_index))
            })
            .count()
    };
    if !observed_pin.is_empty() {
        crate::info!(
            "stream observed-live: pre_hold_touching={pre_hold_touching}/{} lf_pass={lf_pass}",
            capped.len()
        );
    }
    let mut live_held = Vec::new();
    if !observed_pin_set.is_empty() {
        capped.retain(|cycle| {
            let touch = cycle
                .edges
                .iter()
                .any(|edge| observed_pin_set.contains(&edge.pool_index));
            if touch {
                live_held.push(cycle.clone());
                false
            } else {
                true
            }
        });
    }
    let mut table = SpotTable::new(arena.pool_count());
    table.populate_from_graph(&routing_graph);
    rescore_cycles_with_table(&arena, &mut table, &mut capped);
    // Prune any that became unroutable due to dirty state updates (prevents
    // polluting HF candidate pool with now-dead cycles kept from graph cache).
    capped.retain(|c| {
        c.score < crate::pipeline::cycle_finder::DEAD_EDGE_LOG_WEIGHT
            && (c.cycle_ratio.is_zero() || c.cycle_ratio > ONE)
    });
    // Priced-cycle filter runs after enrich+merge below so newly configured feeds
    // (batch mints) can rotate/retain cycles on the same LF tick.
    // ponytail: rescore reorders by score and would undo enumeration-time protocol
    // diversity; re-apply so Balancer multi-token hubs cannot refill the cap.
    capped = finalize_enumerated_cycles(capped, max_paths.saturating_sub(live_held.len()));
    if !live_held.is_empty() {
        live_held.sort_by(crate::pipeline::types::compare_cycle_score);
        live_held.truncate(32);
        let mut seen: rustc_hash::FxHashSet<u64> = live_held
            .iter()
            .map(|c| crate::pipeline::cycle_filter::cycle_key(&c.edges))
            .collect();
        let mut merged = live_held;
        for cycle in capped {
            let key = crate::pipeline::cycle_filter::cycle_key(&cycle.edges);
            if seen.insert(key) {
                merged.push(cycle);
                if merged.len() >= max_paths {
                    break;
                }
            }
        }
        crate::info!(
            "stream observed-live: live_held={} snap_total={}",
            merged
                .iter()
                .filter(|c| {
                    c.edges
                        .iter()
                        .any(|e| observed_pin_set.contains(&e.pool_index))
                })
                .count(),
            merged.len()
        );
        capped = merged;
    }
    let finalize_ms = crate::util::now_ms().saturating_sub(finalize_started);

    let rates_started = crate::util::now_ms();
    let rate_ctx = RateEnrichContext {
        graph: Some(&routing_graph),
        hub_path: HubPathRateParams {
            enabled: ctx.config.oracle.hub_path_rates,
            max_hops: ctx.config.oracle.hub_path_max_hops.max(1),
        },
    };
    let (fresh_rates, rate_stats) = if let Some(ref provider) = state_provider {
        enrich_token_to_matic_rates(
            &ctx.price_oracle,
            &arena,
            cycle_tokens.iter().copied(),
            Some(provider),
            rate_ctx,
        )
        .await
    } else {
        enrich_token_to_matic_rates_offline(
            &ctx.price_oracle,
            &arena,
            cycle_tokens.iter().copied(),
            rate_ctx,
        )
        .await
    };
    let rates = merge_token_rates(&prior_rates, &cycle_tokens_set, fresh_rates);
    let rates_built_at = std::time::Instant::now();
    // Priced-start filter can drop live-held cycles whose start token is still
    // warming — pull them aside, filter the rest, then restore priced live ones.
    let mut live_for_rates = Vec::new();
    if !observed_pin_set.is_empty() {
        capped.retain(|cycle| {
            let touch = cycle
                .edges
                .iter()
                .any(|edge| observed_pin_set.contains(&edge.pool_index));
            if touch {
                live_for_rates.push(cycle.clone());
                false
            } else {
                true
            }
        });
    }
    retain_cycles_with_priced_start(&mut capped, rates.as_ref());
    retain_cycles_with_priced_start(&mut live_for_rates, rates.as_ref());
    if !live_for_rates.is_empty() {
        let mut seen: rustc_hash::FxHashSet<u64> = live_for_rates
            .iter()
            .map(|c| crate::pipeline::cycle_filter::cycle_key(&c.edges))
            .collect();
        let mut merged = live_for_rates;
        for cycle in capped {
            let key = crate::pipeline::cycle_filter::cycle_key(&cycle.edges);
            if seen.insert(key) {
                merged.push(cycle);
                if merged.len() >= max_paths {
                    break;
                }
            }
        }
        capped = merged;
    }
    if !observed_pin.is_empty() {
        let touching = capped
            .iter()
            .filter(|cycle| {
                cycle
                    .edges
                    .iter()
                    .any(|edge| observed_pin_set.contains(&edge.pool_index))
            })
            .count();
        crate::info!(
            "stream observed-live: cycles_touching={touching}/{} observed_pools={} lf_pass={lf_pass}",
            capped.len(),
            observed_pin.len()
        );
    }
    if rates.len() > prior_rates.len() {
        let post_gate = GraphBuildGate {
            token_to_matic_rates: Arc::clone(&rates),
            flash: ctx.flash_liquidity.load(),
            flash_ttl: ctx.flash_liquidity.ttl(),
        };
        if post_gate.active()
            && has_missing_eligible_pools_with_gate(
                &arena,
                pool_metas.as_ref(),
                routing_graph.as_ref(),
                Some(&post_gate),
            )
        {
            let layout_fp = arena.routing_layout_fingerprint();
            let routable_count = pool_metas.len();
            let eligible =
                count_graph_eligible_pools_with_gate(&arena, pool_metas.as_ref(), Some(&post_gate));
            let state_generation = ctx.cache.generation();
            let g = Arc::make_mut(&mut routing_graph);
            let report = attach_missing_eligible_pools_with_gate(
                &arena,
                g,
                pool_metas.as_ref(),
                Some(&post_gate),
            );
            if report.attached_pools > 0 {
                ctx.graph_cache.lock().store(
                    Arc::clone(&routing_graph),
                    None,
                    routable_count,
                    layout_fp,
                    state_generation,
                    eligible,
                );
                crate::info!(
                    "lf graph post-rate patch: attached={} merged_rates={} prior_rates={}",
                    report.attached_pools,
                    rates.len(),
                    prior_rates.len()
                );
            }
        }
    }
    crate::services::oracle::record_unmapped_token_demand(
        &ctx.price_oracle,
        &arena,
        pool_metas.as_ref(),
        cycles_arc.as_ref(),
    );
    crate::services::oracle::log_ranked_unmapped_demand(&ctx.price_oracle, lf_pass, &rate_stats);
    if lf_pass <= 2 || lf_pass.is_multiple_of(30) {
        crate::debug!(
            "token rates: cycle_tokens={} prior_rates={} merged_rates={} {:?}",
            cycle_tokens.len(),
            prior_rates.len(),
            rates.len(),
            rate_stats
        );
    }
    let cycle_and_rates_ms = crate::util::now_ms().saturating_sub(cpu_started);
    let post_cpu_ms = cycle_and_rates_ms.saturating_sub(cpu_ms);
    let rates_ms = crate::util::now_ms().saturating_sub(rates_started);

    let hot: Vec<Address> = {
        let set: rustc_hash::FxHashSet<Address> = capped
            .iter()
            .flat_map(|c| c.edges.iter())
            .filter_map(|e| arena.pool_address(e.pool_index))
            .collect();
        let mut hot = Vec::with_capacity(set.len());
        hot.extend(set);
        hot
    };

    let _cycle_count = capped.len();
    let _pool_count = pool_metas.len();
    let graph_pool_count = routing_graph.active_pool_count();
    crate::info!(
        "lf tick: cycles={}, enumerated_cycles={}, cycle_search_ms={}, arena_pools={}, graph_pools={}, discovered={}, resolvable_tokens={}",
        _cycle_count,
        enumerated_cycles,
        cycle_search_ms,
        _pool_count,
        graph_pool_count,
        pools.len(),
        resolvable_count
    );
    crate::info!(
        "lf latency: total_ms={} discovery_ms={} refresh_ms={} arena_sync_ms={} cpu_ms={} post_cpu_ms={} ticks_ms={} v3_tick_targets={} v3_ticks_loaded={} v3_ticks_ms={} v4_tick_targets={} v4_ticks_loaded={} v4_ticks_ms={} finalize_ms={} cycle_and_rates_ms={} rates_ms={} refreshed_pools={} cycle_search_ms={}",
        crate::util::now_ms().saturating_sub(lf_started),
        discovery_ms,
        refresh_ms,
        arena_sync_ms,
        cpu_ms,
        post_cpu_ms,
        ticks_ms,
        v3_tick_targets,
        v3_ticks_loaded,
        v3_ticks_ms,
        v4_tick_targets,
        v4_ticks_loaded,
        v4_ticks_ms,
        finalize_ms,
        cycle_and_rates_ms,
        rates_ms,
        refreshed_pools,
        cycle_search_ms,
    );

    if lf_pass <= 3 || lf_pass.is_multiple_of(10) {
        let mut survival = PipelineSurvival::from_lf_tick(
            &pools,
            &ctx.cache,
            pool_metas.as_ref(),
            routing_graph.as_ref(),
        );
        if let Some(stats) = ctx.refresh.bootstrap_parse_stats() {
            survival = survival.with_index_stats(&stats);
        }
        survival.log_summary(lf_pass);
        crate::services::index_diag::log_index_summary();
    }

    let stream_targets = ctx.config.pipeline.stream_enabled.then(|| {
        let force = ctx.stream_addresses.force_replace_pending();
        let epoch = if force {
            ctx.stream_addresses.reselect_epoch()
        } else {
            0
        };
        let demote: Vec<_> = if force {
            ctx.stream_addresses.read().clone()
        } else {
            Vec::new()
        };
        let cap = ctx.config.pipeline.stream_max_pools;
        let mut targets = select_stream_targets_with_epoch(
            &pools,
            &hot,
            Some(routing_graph.as_ref()),
            pool_metas.as_ref(),
            &arena,
            &ctx.partial_cache,
            cap,
            crate::util::now_ms(),
            epoch,
            &demote,
        );
        // Pin topic-observed routable pools that made it into the arena this tick
        // so the next Sync/Swap sets wake_hf=true (interest path).
        if !observed.is_empty() {
            let addr_to_pool = arena.address_to_pool();
            for addr in &observed {
                if targets.len() >= cap {
                    break;
                }
                if addr_to_pool.contains_key(addr) && !targets.contains(addr) {
                    targets.push(*addr);
                }
            }
        }
        targets
    });

    if *shutdown.borrow() {
        return Ok(());
    }

    // Universe = streamable pools present in the arena (superset of interest).
    let stream_universe: Option<Vec<_>> = ctx.config.pipeline.stream_enabled.then(|| {
        pool_metas
            .iter()
            .filter(|m| crate::services::partial_cache::is_streamable_protocol(m.protocol))
            .filter_map(|m| arena.pool_address(m.pool_index))
            .collect()
    });

    // Topic-observed V3 pools admitted this tick — nudge HF after snapshot publish
    // so we do not wait for a second Swap (live: wake_hf stayed 0 on one-shot venues).
    let observed_in_arena: Vec<Address> = if observed.is_empty() {
        Vec::new()
    } else {
        let map = arena.address_to_pool();
        observed
            .iter()
            .filter(|addr| map.contains_key(*addr))
            .copied()
            .collect()
    };

    *ctx.arena.lock() = arena.clone();
    let graph_active_by_protocol = Arc::new(
        crate::services::pipeline_survival::graph_active_protocol_counts(
            pool_metas.as_ref(),
            routing_graph.as_ref(),
        ),
    );
    ctx.snapshots
        .publish(crate::services::hf_snapshot::HfSnapshot {
            state_block: ctx.refresh.last_state_block(),
            state_hash: ctx.refresh.last_state_hash(),
            cycles: capped.into_iter().map(Arc::new).collect(),
            token_to_matic_rates: rates,
            token_decimals: decimals,
            pool_metas,
            arena,
            discovered_pools: Arc::clone(&pools),
            graph_active_by_protocol,
            rates_built_at: Some(rates_built_at),
            ..Default::default()
        });
    // Publish first so the TUI poller cannot pair fresh LF metrics with the
    // previous snapshot generation.
    ctx.ui_hook
        .on_lf_complete(_cycle_count, cycle_search_ms, pools.len());

    // Keep topic-observed routable pools in the refresh hot set (cycle-only `hot`
    // used to wipe them every LF tick).
    let mut publish_hot = hot;
    publish_hot.extend(observed.iter().copied());
    publish_hot.sort_unstable();
    publish_hot.dedup();
    ctx.refresh.set_hot_addresses(publish_hot);

    if let Some(targets) = stream_targets {
        // Apply hysteresis first so retain/seed match the addresses WSS actually
        // watches (replace may keep the prior set on small top-N churn).
        let force = ctx.stream_addresses.force_replace_pending();
        // Log score-order sample before replace() sorts by address.
        let sample_n = targets.len().min(5);
        let score_sample: Vec<String> = targets
            .iter()
            .take(sample_n)
            .map(|a| format!("{a}"))
            .collect();
        let replaced = ctx.stream_addresses.replace(targets);
        if replaced {
            crate::info!(
                "stream targets updated: pools={} force={} epoch={} top=[{}]",
                ctx.stream_addresses.read().len(),
                force,
                ctx.stream_addresses.reselect_epoch(),
                score_sample.join(",")
            );
        }
        let tracked: Vec<_> = ctx.stream_addresses.read().clone();
        if let Some(universe) = stream_universe {
            ctx.partial_cache.set_stream_universe(&universe);
        }
        ctx.partial_cache.retain_tracked(&tracked);
        ctx.partial_cache
            .seed_from_state_cache(&ctx.cache, &tracked, crate::util::now_ms());
    }

    // Same-tick HF nudge: do not wait for a second V3 Swap after arena admission.
    let nudged = if !observed_in_arena.is_empty() {
        ctx.partial_cache.wake_dirty_pools(&observed_in_arena);
        true
    } else if !observed.is_empty() {
        ctx.partial_cache.wake_for_admitted_observed(&observed);
        false
    } else {
        false
    };
    if !observed.is_empty() {
        crate::info!(
            "stream observed-live: arena_hit={}/{} nudged_hf={}",
            observed_in_arena.len(),
            observed.len(),
            u8::from(nudged)
        );
    }

    Ok(())
}

#[must_use]
pub fn spawn_lf_background(
    lf_ctx: Arc<LfContext>,
    lf_interval_ms: u64,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut timer = interval(Duration::from_millis(lf_interval_ms.max(1)));
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        break;
                    }
                }
                _ = timer.tick() => {
                    let Ok(guard) = lf_ctx.tick_lock.try_lock() else {
                        crate::debug!("lf tick skipped: previous pass still running");
                        continue;
                    };
                    if let Err(e) = run_lf_tick(&lf_ctx, &shutdown).await {
                        crate::warn!("lf tick failed: {e}");
                    }
                    drop(guard);
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::cycle_search_passes;
    use crate::core::constants::HOP_CAP;

    #[test]
    fn short_hop_search_uses_single_pass_through_configured_cap() {
        for max_hops in [2, 3, 4] {
            let passes = cycle_search_passes(max_hops, 5_000);
            assert_eq!(passes.len(), 1);
            assert_eq!(passes[0].max_hops, max_hops);
            assert_eq!(passes[0].max_cycles, 5_000);
        }
    }

    #[test]
    fn long_hop_search_prioritizes_short_routes_then_expands() {
        let passes = cycle_search_passes(5, 5_000);
        assert_eq!(passes.len(), 2);
        assert_eq!(passes[0].max_hops, 3);
        assert_eq!(passes[0].max_cycles, 2_500);
        assert_eq!(passes[1].max_hops, 5);
        assert_eq!(passes[1].max_cycles, 5_000);
    }

    #[test]
    fn hop_search_is_defensively_bounded_by_storage_capacity() {
        let passes = cycle_search_passes(u32::MAX, 5_000);
        assert_eq!(passes.last().map(|pass| pass.max_hops), Some(HOP_CAP));
    }
}
