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
use crate::core::types::{PoolIndex, TokenIndex};
use crate::infra::rpc::RpcPool;
use crate::orchestrator::ui_hook::SharedUiHook;
use crate::pipeline::arena::StateArena;
use crate::pipeline::cycle_filter::ProbeContext;
use crate::pipeline::cycle_search::find_cycles_for_mode;
use crate::pipeline::graph_cache::GraphCache;
use crate::pipeline::spot_price::{
    SpotTable, finalize_enumerated_cycles, rescore_cycles_with_table,
};
use crate::pipeline::tick_fetch::{
    collect_v3_pool_addresses, collect_v4_tick_targets, enrich_v3_ticks, enrich_v4_ticks,
};
use crate::pipeline::types::CycleSearchPass;
use crate::services::execution::GasOracle;
use crate::services::hf_snapshot::SnapshotStore;
use crate::services::oracle::price_oracle::PriceOracle;
use crate::services::oracle::{
    arena_tokens_without_decimal_hints, enrich_token_to_matic_rates,
    enrich_token_to_matic_rates_offline, expand_hub_spoke_resolvable, merge_token_rates,
    resolvable_token_set,
};
use crate::services::partial_cache::{PartialPoolCache, StreamAddressSet, select_stream_targets};
use crate::services::pipeline_survival::PipelineSurvival;
use crate::services::state_cache::StateCache;
use crate::services::state_refresh::StateRefreshService;

struct LfCpuWork {
    graph_cache: Arc<Mutex<GraphCache>>,
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
}

struct LfCpuResult {
    graph: Arc<crate::pipeline::types::RoutingGraph>,
    cycles: Arc<Vec<crate::core::types::FoundCycle>>,
    /// Cycle enumeration only (0 when graph cache reused).
    enumeration_ms: u64,
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

fn run_lf_cpu_work(work: &LfCpuWork) -> LfCpuResult {
    let routable_count = work.pool_metas.len();
    // Graph admission is topology-only; token valuation is enforced later in
    // HF evaluation. Pricing changes must not force connectivity rebuilds.
    let layout_fp = work.arena.routing_layout_fingerprint();
    let eligible_count =
        crate::pipeline::graph::count_graph_eligible_pools(&work.arena, work.pool_metas.as_ref());

    // Snapshot decisions without holding lock for duration of heavy work.
    let (needs_rebuild, connectivity_stale) = {
        let gc = work.graph_cache.lock();
        let stale = gc.connectivity_stale(eligible_count);
        (
            gc.needs_connectivity_rebuild(routable_count, layout_fp) || stale,
            stale,
        )
    };

    let graph = if needs_rebuild {
        if connectivity_stale {
            let gc = work.graph_cache.lock();
            crate::debug!(
                "lf graph rebuild: eligible_pools={eligible_count} cached_eligible={}",
                gc.cached_eligible_pool_count()
            );
        }
        // Build outside lock to keep critical section short.
        let g = Arc::new(crate::pipeline::graph::build_graph(
            &work.arena,
            work.pool_metas.as_ref(),
        ));
        let mut gc = work.graph_cache.lock();
        gc.store(
            Arc::clone(&g),
            None,
            routable_count,
            layout_fp,
            work.state_generation,
            eligible_count,
        );
        g
    } else {
        let mut gc = work.graph_cache.lock();
        if gc.graph().is_none() {
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

    let mut graph = graph;
    let missing_graph_pools = {
        let g = Arc::make_mut(&mut graph);
        crate::pipeline::graph::attach_missing_eligible_pools(
            &work.arena,
            g,
            work.pool_metas.as_ref(),
        )
    };
    if missing_graph_pools > 0 {
        crate::info!(
            "lf graph patch: attached {missing_graph_pools} eligible pools missing from cached adjacency"
        );
        work.graph_cache.lock().store(
            Arc::clone(&graph),
            None,
            routable_count,
            layout_fp,
            work.state_generation,
            eligible_count,
        );
    }

    let need_cycle_refind = {
        let gc = work.graph_cache.lock();
        needs_rebuild
            || gc.needs_cycle_refind(
                routable_count,
                layout_fp,
                work.state_generation,
                work.dirty_pools.len(),
                work.arena.pool_count(),
            )
    };
    let mut enumeration_ms = 0u64;
    let cycles = if need_cycle_refind {
        crate::debug!(
            "lf cycle refind: pass={} dirty_pools={} state_gen={}",
            work.lf_pass,
            work.dirty_pools.len(),
            work.state_generation
        );
        let passes = cycle_search_passes(work.max_hops, work.max_paths);
        let probe_ctx = ProbeContext {
            token_to_matic_rates: Some(work.prior_rates.as_ref()),
            token_decimals: Some(work.token_decimals.as_ref()),
            gas_price_wei: work.gas_price_wei,
        };
        let enum_started = crate::util::now_ms();
        let result = find_cycles_for_mode(
            work.cycle_finder,
            &work.arena,
            &graph,
            work.pool_metas.as_ref(),
            passes.as_slice(),
            true,
            Some(&probe_ctx),
        );
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
        let diversified = finalize_enumerated_cycles(result, work.max_paths);
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
        cycles
    } else {
        let gc = work.graph_cache.lock();
        let cached = gc.cycles().unwrap_or_default();
        if work.lf_pass <= 2 || work.lf_pass.is_multiple_of(30) {
            crate::debug!(
                "lf cycle cache: pass={} cycles={}",
                work.lf_pass,
                cached.len()
            );
        }
        cached
    };

    LfCpuResult {
        graph,
        cycles,
        enumeration_ms,
    }
}

async fn run_lf_cpu_async(work: LfCpuWork) -> anyhow::Result<LfCpuResult> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    crate::util::lf_cpu_pool().spawn(move || {
        let result = crate::util::run_lf_cpu(|| run_lf_cpu_work(&work));
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
    let refresh_started = crate::util::now_ms();
    let refreshed_pools = ctx.refresh.refresh_pool_states(refresh_batch).await?;
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

    let mut arena = StateArena::default();
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

    let prior_rates = Arc::clone(&ctx.snapshots.read().token_to_matic_rates);

    let state_provider = ctx.rpc.connect_state().ok();
    let state_generation = ctx.cache.generation();
    let dirty_pools = ctx.cache.take_dirty_pool_indices(arena.address_to_pool());
    let gas_price_wei = ctx.gas_oracle.conservative_gas_price();
    if gas_price_wei.is_none() && lf_pass <= 2 {
        crate::debug!("lf cycle prefilter: gas oracle not warm yet (no gas floor at enumeration)");
    }
    let cpu_work = LfCpuWork {
        graph_cache: Arc::clone(&ctx.graph_cache),
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
    let routing_graph = cpu.graph;
    let cycle_search_ms = cpu.enumeration_ms;

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
    if let Some(ref provider) = state_provider {
        let tick_pools = collect_v3_pool_addresses(&arena, cycles_arc.as_ref());
        let v4_tick_targets = collect_v4_tick_targets(cycles_arc.as_ref(), pool_metas.as_ref());
        let state_block = ctx.refresh.last_state_block();
        let pinned_block = (state_block > 0).then_some(state_block);
        let (algebra_pools, algebra_integral_pools) =
            crate::pipeline::tick_fetch::collect_algebra_pools(&arena, pool_metas.as_ref());
        let _ticks_loaded = enrich_v3_ticks(
            provider,
            &mut arena,
            &tick_pools,
            ctx.config.oracle.tick_word_range,
            &algebra_pools,
            &algebra_integral_pools,
            pinned_block,
        )
        .await;
        let _v4_ticks_loaded = enrich_v4_ticks(
            provider,
            &mut arena,
            &v4_tick_targets,
            ctx.config.oracle.tick_word_range,
            pinned_block,
        )
        .await;
    }
    let ticks_ms = crate::util::now_ms().saturating_sub(ticks_started);

    let finalize_started = crate::util::now_ms();
    let mut capped = Arc::try_unwrap(cycles_arc).unwrap_or_else(|arc| (*arc).clone());
    let mut table = SpotTable::new(arena.pool_count());
    table.populate_from_graph(&routing_graph);
    rescore_cycles_with_table(&arena, &mut table, &mut capped);
    // Prune any that became unroutable due to dirty state updates (prevents
    // polluting HF candidate pool with now-dead cycles kept from graph cache).
    capped.retain(|c| c.score < crate::pipeline::cycle_finder::DEAD_EDGE_LOG_WEIGHT);
    // ponytail: rescore reorders by score and would undo enumeration-time protocol
    // diversity; re-apply so Balancer multi-token hubs cannot refill the cap.
    capped = finalize_enumerated_cycles(capped, max_paths);
    let finalize_ms = crate::util::now_ms().saturating_sub(finalize_started);

    let rates_started = crate::util::now_ms();
    let (fresh_rates, rate_stats) = if let Some(ref provider) = state_provider {
        enrich_token_to_matic_rates(
            &ctx.price_oracle,
            &arena,
            cycle_tokens.iter().copied(),
            Some(provider),
        )
        .await
    } else {
        enrich_token_to_matic_rates_offline(&ctx.price_oracle, &arena, cycle_tokens.iter().copied())
            .await
    };
    let rates = merge_token_rates(&prior_rates, fresh_rates);
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
    let pools_snapshot = pools.clone();

    let graph_pool_count = routing_graph.active_pool_count();
    crate::info!(
        "lf tick: cycles={}, cycle_search_ms={}, arena_pools={}, graph_pools={}, discovered={}, resolvable_tokens={}",
        _cycle_count,
        cycle_search_ms,
        _pool_count,
        graph_pool_count,
        pools.len(),
        resolvable_count
    );
    crate::info!(
        "lf latency: total_ms={} discovery_ms={} refresh_ms={} arena_sync_ms={} cpu_ms={} ticks_ms={} finalize_ms={} cycle_and_rates_ms={} rates_ms={} refreshed_pools={} cycle_search_ms={}",
        crate::util::now_ms().saturating_sub(lf_started),
        discovery_ms,
        refresh_ms,
        arena_sync_ms,
        cpu_ms,
        ticks_ms,
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
    }

    let stream_targets = ctx.config.pipeline.stream_enabled.then(|| {
        select_stream_targets(
            &pools,
            &hot,
            Some(routing_graph.as_ref()),
            pool_metas.as_ref(),
            &arena,
            &ctx.partial_cache,
            ctx.config.pipeline.stream_max_pools,
            crate::util::now_ms(),
        )
    });

    if *shutdown.borrow() {
        return Ok(());
    }

    *ctx.arena.lock() = arena.clone();
    let snapshot_now = std::time::Instant::now();
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
            discovered_pools: pools_snapshot,
            graph_active_by_protocol,
            rates_built_at: Some(snapshot_now),
            ..Default::default()
        });
    // Publish first so the TUI poller cannot pair fresh LF metrics with the
    // previous snapshot generation.
    ctx.ui_hook
        .on_lf_complete(_cycle_count, cycle_search_ms, pools.len());

    ctx.refresh.set_hot_addresses(hot);

    if let Some(targets) = stream_targets {
        ctx.partial_cache.retain_tracked(&targets);
        ctx.partial_cache
            .seed_from_state_cache(&ctx.cache, &targets, crate::util::now_ms());
        let _ = ctx.stream_addresses.replace(targets);
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
