use std::sync::Arc;

use alloy::primitives::Address;
use rustc_hash::FxHashMap;
use parking_lot::Mutex;
use tokio::sync::{Mutex as AsyncMutex, watch};
use tokio::time::{Duration, MissedTickBehavior, interval};
use tracing::{debug, error, info, instrument, warn};

use crate::config::AppConfig;
use crate::config::CycleFinderKind;
use crate::core::types::TokenIndex;
use crate::infra::metrics::PipelineMetrics;
use crate::infra::rpc::RpcPool;
use crate::pipeline::arena::StateArena;
use crate::pipeline::bellman_ford::find_cycles_bellman_ford_multi_pass;
use crate::pipeline::cycle_finder::find_cycles_multi_pass;
use crate::pipeline::cycle_search::find_cycles_hybrid_multi_pass;
use crate::pipeline::graph_cache::GraphCache;
use crate::pipeline::johnson::find_cycles_johnson_multi_pass;
use crate::pipeline::spot_price::{finalize_enumerated_cycles, rescore_cycles_by_spot_price};
use crate::pipeline::tick_fetch::{collect_v3_pool_addresses, enrich_v3_ticks};
use crate::pipeline::types::{CycleSearchPass, compare_cycle_score};
use crate::services::hf_snapshot::SnapshotStore;
use crate::services::oracle::enrich_token_to_matic_rates;
use crate::services::oracle::price_oracle::PriceOracle;
use crate::orchestrator::ui_hook::SharedUiHook;
use crate::services::partial_cache::{PartialPoolCache, StreamAddressSet, select_stream_targets};
use crate::services::state_cache::StateCache;
use crate::services::state_refresh::StateRefreshService;

pub struct LfContext {
    pub config: Arc<AppConfig>,
    pub refresh: Arc<StateRefreshService>,
    pub cache: Arc<StateCache>,
    pub snapshots: Arc<SnapshotStore>,
    pub stream_addresses: StreamAddressSet,
    pub partial_cache: Arc<PartialPoolCache>,
    pub price_oracle: Arc<PriceOracle>,
    pub rpc: Arc<RpcPool>,
    pub metrics: Arc<PipelineMetrics>,
    pub graph_cache: Arc<Mutex<GraphCache>>,
    /// Dropped when a tick is already running — prevents overlapping LF work.
    pub tick_lock: Arc<AsyncMutex<()>>,
    pub ui_hook: SharedUiHook,
}

#[instrument(skip(ctx), fields(cycles = tracing::field::Empty, pools = tracing::field::Empty))]
pub async fn run_lf_tick(ctx: &LfContext) -> anyhow::Result<()> {
    ctx.metrics.record_lf_tick();
    let lf_pass = ctx.refresh.lf_tick();
    let refresh_batch = ctx.refresh.lf_refresh_batch(lf_pass);

    let _ = ctx.refresh.maybe_discover().await?;
    let _ = ctx.refresh.refresh_pool_states(refresh_batch).await?;
    ctx.refresh.prune_dead_pools();

    let pools = ctx.refresh.discovered_pools();
    if pools.is_empty() {
        return Ok(());
    }

    let mut arena = StateArena::new();
    let pool_metas = arena.sync_from_discovery(&ctx.cache, &pools);
    let max_paths = ctx.config.routing.enumeration_max_paths as usize;
    let max_hops = ctx.config.routing.max_hops;
    let finder = ctx.config.routing.cycle_finder;

    let (_graph, mut capped, cycles_from_cache) = tokio::task::block_in_place(|| {
        let graph = {
            let gc = ctx.graph_cache.lock();
            if gc.needs_rebuild(&ctx.cache, pool_metas.len()) {
                let pools_copy = pool_metas.clone();
                let arena_copy = arena.clone();
                drop(gc);
                let new_g = Arc::new(crate::pipeline::graph::build_graph(&arena_copy, &pools_copy));
                ctx.graph_cache.lock().store(Arc::clone(&new_g), None, &ctx.cache, pool_metas.len());
                new_g
            } else if let Some(g) = gc.graph() {
                g
            } else {
                drop(gc);
                let new_g = Arc::new(crate::pipeline::graph::build_graph(&arena, &pool_metas));
                ctx.graph_cache.lock().store(Arc::clone(&new_g), None, &ctx.cache, pool_metas.len());
                new_g
            }
        };

        let gc = ctx.graph_cache.lock();
        if let Some(cached) = gc.get_cached_cycles(&ctx.cache, pool_metas.len()) {
            debug!(count = cached.len(), "cycle cache hit");
            (graph, (*cached).clone(), true)
        } else {
            drop(gc);
            let passes = vec![
                CycleSearchPass {
                    max_hops: max_hops.min(3),
                    max_cycles: max_paths / 2,
                },
                CycleSearchPass {
                    max_hops,
                    max_cycles: max_paths,
                },
            ];
            let result = match finder {
                CycleFinderKind::Hybrid => {
                    find_cycles_hybrid_multi_pass(&arena, &graph, &passes, true)
                }
                CycleFinderKind::Johnson => {
                    find_cycles_johnson_multi_pass(&arena, &graph, &passes)
                }
                CycleFinderKind::BellmanFord => {
                    find_cycles_bellman_ford_multi_pass(&arena, &graph, &passes)
                }
                CycleFinderKind::Dfs => find_cycles_multi_pass(&arena, &graph, &passes),
            };
            let finalized = finalize_enumerated_cycles(&arena, result, max_paths);
            ctx.graph_cache.lock().store(graph.clone(), Some(Arc::new(finalized.clone())), &ctx.cache, pool_metas.len());
            (graph, finalized, false)
        }
    });

    let decimals = ctx.refresh.token_decimals_map();
    let mut cycle_tokens_set = rustc_hash::FxHashSet::default();
    for c in &capped {
        cycle_tokens_set.insert(c.start_token);
        for e in &c.edges {
            cycle_tokens_set.insert(e.token_in);
            cycle_tokens_set.insert(e.token_out);
        }
    }
    let cycle_tokens: Vec<TokenIndex> = cycle_tokens_set.into_iter().collect();

    let rates = if cycles_from_cache {
        if ctx.config.oracle.enabled {
            snap_oracle_rates(ctx, &arena, &cycle_tokens, &decimals).await
        } else {
            ctx.snapshots.read().token_to_matic_rates.clone()
        }
    } else if let Ok(provider) = ctx.rpc.connect_state() {
        let tick_pools = collect_v3_pool_addresses(&arena, &capped);
        let ticks_loaded = enrich_v3_ticks(
            &provider,
            &mut arena,
            &tick_pools,
            ctx.config.oracle.tick_word_range,
        )
        .await;
        rescore_cycles_by_spot_price(&arena, &mut capped);
        capped.sort_by(compare_cycle_score);
        if capped.len() > max_paths {
            capped.truncate(max_paths);
        }
        if ticks_loaded > 0 {
            info!(ticks_loaded, "v3 tick enrichment");
        }

        if ctx.config.oracle.enabled {
            enrich_token_to_matic_rates(
                &ctx.price_oracle,
                &arena,
                &cycle_tokens,
                &decimals,
                Some(&provider),
            )
            .await
        } else {
            FxHashMap::default()
        }
    } else {
        FxHashMap::default()
    };

    let hot: Vec<_> = capped
        .iter()
        .flat_map(|c| c.edges.iter())
        .filter_map(|e| arena.pool_address(e.pool_index))
        .collect();

    let cycle_count = capped.len();
    let pool_count = pool_metas.len();
    let pools_snapshot = pools.clone();

    ctx.ui_hook.on_lf_complete(&arena, &capped, &pool_metas, 0, pools.len());

    ctx.snapshots
        .publish(crate::services::hf_snapshot::HfSnapshot {
            generation: 0,
            cycles: capped,
            token_to_matic_rates: rates,
            token_decimals: decimals,
            pool_metas,
            arena,
            discovered_pools: pools_snapshot,
        });

    info!(
        cycles = cycle_count,
        cache_size = ctx.cache.len(),
        pools = pool_count,
        lf_pass,
        refresh_batch,
        "lf tick complete"
    );

    tracing::Span::current().record("cycles", cycle_count);
    tracing::Span::current().record("pools", pool_count);

    ctx.refresh.set_hot_addresses(hot.clone());

    if ctx.config.pipeline.stream_enabled {
        let targets = select_stream_targets(&pools, &hot, ctx.config.pipeline.stream_max_pools);
        if ctx.stream_addresses.replace(targets.clone()) {
            info!(pools = targets.len(), "WSS stream target pools updated");
        }
        ctx.partial_cache
            .seed_from_state_cache(&ctx.cache, &targets, crate::util::now_ms());
    }

    Ok(())
}

async fn snap_oracle_rates(
    ctx: &LfContext,
    arena: &StateArena,
    cycle_tokens: &[TokenIndex],
    decimals: &std::collections::HashMap<Address, u8>,
) -> FxHashMap<TokenIndex, ruint::aliases::U256> {
    if !ctx.config.oracle.enabled {
        return FxHashMap::default();
    }
    match ctx.rpc.connect_state() {
        Ok(provider) => {
            enrich_token_to_matic_rates(
                &ctx.price_oracle,
                arena,
                cycle_tokens,
                decimals,
                Some(&provider),
            )
            .await
        }
        Err(e) => {
            debug!(error = %e, "oracle enrichment skipped — no state RPC");
            FxHashMap::default()
        }
    }
}

pub fn spawn_lf_background(
    lf_ctx: Arc<LfContext>,
    lf_interval_ms: u64,
    mut shutdown: watch::Receiver<bool>,
    _stream_addresses: StreamAddressSet,
    _partial_cache: Arc<PartialPoolCache>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        info!("LF background task started (interval={}ms)", lf_interval_ms);
        let mut timer = interval(Duration::from_millis(lf_interval_ms));
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        if let Err(e) = run_lf_tick(&lf_ctx).await {
            warn!(error = %e, "initial lf tick failed");
        }

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("LF background task shutting down");
                        break;
                    }
                }
                _ = timer.tick() => {
                    let Ok(guard) = lf_ctx.tick_lock.try_lock() else {
                        lf_ctx.metrics.record_lf_skipped();
                        debug!("lf tick skipped — previous tick still running");
                        continue;
                    };
                    if let Err(e) = run_lf_tick(&lf_ctx).await {
                        error!(error = %e, "lf tick failed");
                    }
                    drop(guard);
                }
            }
        }
    })
}
