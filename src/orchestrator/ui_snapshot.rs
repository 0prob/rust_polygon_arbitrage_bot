#[cfg(feature = "tui")]
use std::sync::Arc;
#[cfg(feature = "tui")]
use std::time::{Duration, Instant};

#[cfg(feature = "tui")]
use alloy::primitives::Address;
#[cfg(feature = "tui")]
use anyhow::Context;
#[cfg(feature = "tui")]
use rustc_hash::FxHashSet;
#[cfg(feature = "tui")]
use tokio::sync::watch;

#[cfg(feature = "tui")]
use crate::orchestrator::RuntimeContext;
use crate::pipeline::sim_sanity::matic_usd_for_flash_cap;
#[cfg(feature = "tui")]
use crate::tui::bridge::publish_snapshot;
#[cfg(feature = "tui")]
use crate::tui::update::{
    RouteBuildCache, RuntimeSnapshotInput, build_config_rows, build_diagnostics,
    build_portfolio_rows, build_route_cache, build_snapshot, graph_snapshot_for_poll,
};
#[cfg(feature = "tui")]
use crate::util::u256_to_f64;

#[cfg(feature = "tui")]
pub fn spawn_snapshot_publisher(
    ctx: Arc<RuntimeContext>,
    snapshot_tx: watch::Sender<Option<Arc<crate::tui::app::DashboardSnapshot>>>,
    started_at: Instant,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        const SNAPSHOT_POLL_INTERVAL: Duration = Duration::from_millis(1_500);
        const ORACLE_REFRESH_INTERVAL: Duration = Duration::from_secs(10);
        const PORTFOLIO_REFRESH_INTERVAL: Duration = Duration::from_secs(15);

        let mut ticker = tokio::time::interval(SNAPSHOT_POLL_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Defer RPC oracle/portfolio until after the first paint — forcing both on tick 0
        // blocked dashboard updates for up to ~12s at startup.
        let mut last_oracle_refresh = Instant::now();
        let mut last_portfolio_refresh = Instant::now();
        let mut portfolio_rows = Vec::new();
        let mut route_cache = RouteBuildCache::default();

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        break;
                    }
                }
                _ = ticker.tick() => {}
            }

            let refresh_oracle = last_oracle_refresh.elapsed() >= ORACLE_REFRESH_INTERVAL;
            let refresh_portfolio = last_portfolio_refresh.elapsed() >= PORTFOLIO_REFRESH_INTERVAL;

            match tokio::time::timeout(
                Duration::from_secs(12),
                build_ui_snapshot(
                    Arc::clone(&ctx),
                    started_at,
                    refresh_oracle,
                    refresh_portfolio,
                    &mut route_cache,
                    std::mem::take(&mut portfolio_rows),
                ),
            )
            .await
            {
                Ok(Ok((snapshot, rows))) => {
                    portfolio_rows = rows;
                    if refresh_oracle {
                        last_oracle_refresh = Instant::now();
                    }
                    if refresh_portfolio {
                        last_portfolio_refresh = Instant::now();
                    }
                    publish_snapshot(&snapshot_tx, snapshot);
                }
                Ok(Err(e)) => {
                    crate::warn!("snapshot poll failed: {e}");
                }
                Err(_) => {
                    crate::warn!("snapshot poll timed out");
                }
            }
        }
    })
}

#[cfg(feature = "tui")]
async fn build_ui_snapshot(
    ctx: Arc<RuntimeContext>,
    started_at: Instant,
    refresh_oracle: bool,
    refresh_portfolio: bool,
    route_cache: &mut RouteBuildCache,
    mut portfolio_rows: Vec<crate::tui::app::PortfolioRow>,
) -> anyhow::Result<(
    crate::tui::app::DashboardSnapshot,
    Vec<crate::tui::app::PortfolioRow>,
)> {
    let snap = ctx.snapshots.read();

    let mut matic_usd = ctx
        .price_oracle
        .resolve_matic_usd_cached()
        .and_then(matic_usd_for_flash_cap)
        .unwrap_or(0.0);
    let route_tokens = if refresh_oracle || matic_usd <= 0.0 || refresh_portfolio {
        unique_route_tokens(&snap)
    } else {
        Vec::new()
    };

    let oracle_task = if refresh_oracle || matic_usd <= 0.0 {
        let ctx = Arc::clone(&ctx);
        let route_tokens = route_tokens.clone();
        Some(tokio::spawn(async move {
            let provider = ctx.rpc.connect_state().ok();
            let provider_ref = provider.as_ref();
            let mut prefetch_addrs = route_tokens;
            prefetch_addrs.extend(crate::core::constants::POLYGON_HUB_TOKENS);
            prefetch_addrs.sort_unstable();
            prefetch_addrs.dedup();
            if let Some(provider) = provider_ref {
                ctx.price_oracle
                    .prefetch_token_usd(&prefetch_addrs, Some(provider))
                    .await;
                ctx.price_oracle.get_matic_usd(Some(provider)).await
            } else {
                ctx.price_oracle
                    .prefetch_token_usd_offline(&prefetch_addrs)
                    .await;
                ctx.price_oracle.get_matic_usd_offline().await
            }
        }))
    } else {
        None
    };

    let portfolio_task = if refresh_portfolio {
        let ctx = Arc::clone(&ctx);
        let snap = Arc::clone(&snap);
        let route_tokens = route_tokens.clone();
        Some(tokio::spawn(async move {
            let provider = ctx.rpc.connect_state().ok();
            let provider_ref = provider.as_ref();
            let balance_account = portfolio_balance_account(&ctx);
            build_portfolio_rows(
                provider_ref,
                &ctx.price_oracle,
                &snap,
                &route_tokens,
                balance_account,
            )
            .await
        }))
    } else {
        None
    };

    if let Some(mut task) = oracle_task {
        match tokio::time::timeout(Duration::from_secs(6), &mut task).await {
            Ok(Ok(refreshed)) => {
                if let Some(usd) = matic_usd_for_flash_cap(refreshed) {
                    matic_usd = usd;
                }
            }
            Ok(Err(e)) => crate::debug!("snapshot oracle refresh failed: {e:#}"),
            Err(_) => {
                // Dropping a JoinHandle detaches the task (tokio docs); abort+await.
                task.abort();
                let _ = task.await;
                crate::debug!("snapshot oracle refresh timed out");
            }
        }
    }

    if let Some(mut task) = portfolio_task {
        match tokio::time::timeout(Duration::from_secs(6), &mut task).await {
            Ok(Ok(rows)) => portfolio_rows = rows,
            Ok(Err(e)) => crate::debug!("snapshot portfolio refresh failed: {e:#}"),
            Err(_) => {
                task.abort();
                let _ = task.await;
                crate::debug!("snapshot portfolio refresh timed out");
            }
        }
    }

    let gas_gwei = ctx
        .gas_oracle
        .snapshot()
        .map(|fee| u256_to_f64(fee.base_fee + fee.priority_fee) / 1e9);

    // Hot-cache + route sims only when generation/gas change; otherwise reuse cache.
    let display_arena =
        if snap.generation != route_cache.generation || route_cache.gas_gwei != gas_gwei {
            let hot_pools = hot_pool_addresses(&snap);
            let mut display_arena = snap.arena.clone();
            display_arena.apply_hot_cache(&ctx.cache, &hot_pools);
            let arena = display_arena.clone();
            let snap_arc = Arc::clone(&snap);
            let slippage_bps = ctx.config.execution.slippage_bps;
            let safety_multiplier_bps = ctx.config.execution.profit_safety_multiplier_bps;
            let matic = matic_usd;
            let prev_graph_generation = route_cache.graph_generation;
            let prev_graph = route_cache.graph.take();
            // CPU-bound route sims — rayon pool, not tokio spawn_blocking (tokio docs).
            let (tx, rx) = tokio::sync::oneshot::channel();
            crate::util::cpu_pool().spawn(move || {
                let _ = tx.send(build_route_cache(
                    &snap_arc,
                    &arena,
                    matic,
                    gas_gwei,
                    slippage_bps,
                    safety_multiplier_bps,
                ));
            });
            *route_cache = rx.await.context("route cache build task failed")?;
            // Route rebuild must not drop the graph COW cache.
            route_cache.graph_generation = prev_graph_generation;
            route_cache.graph = prev_graph;
            display_arena
        } else {
            snap.arena.clone()
        };

    let hypersync_height = ctx.hypersync.as_ref().and_then(|hs| hs.latest_height());
    let graph = graph_snapshot_for_poll(
        route_cache,
        &snap,
        hypersync_height,
        ctx.refresh.indexer_lag_blocks(),
        ctx.refresh.is_indexer_stale(),
    );

    let diagnostics = build_diagnostics(
        &ctx.config,
        &ctx.refresh,
        gas_gwei,
        matic_usd,
        hypersync_height,
    );
    let config_rows = build_config_rows(&ctx.config);

    let execution_trades = ctx
        .execution
        .total_trades
        .load(std::sync::atomic::Ordering::Relaxed);
    let execution_losses = ctx
        .execution
        .total_losses
        .load(std::sync::atomic::Ordering::Relaxed);
    let (_, daily_pnl_wei) = ctx.execution.pnl_snapshot();
    let runtime_input = RuntimeSnapshotInput {
        started_at,
        snapshot: Arc::clone(&snap),
        arena: display_arena,
        config: Arc::clone(&ctx.config),
        refresh: Arc::clone(&ctx.refresh),
        execution_trades,
        execution_losses,
        daily_pnl_wei,
        gas_gwei,
        hypersync_height,
        matic_usd,
        portfolio_rows: portfolio_rows.clone(),
        diagnostics,
        config_rows,
        route_cache: Some(route_cache.clone()),
        graph,
    };

    Ok((build_snapshot(runtime_input).await, portfolio_rows))
}

#[cfg(feature = "tui")]
fn portfolio_balance_account(ctx: &RuntimeContext) -> Option<Address> {
    ctx.config
        .execution
        .executor_address
        .map(|executor| ctx.wallet.operator_address(executor))
}

#[cfg(feature = "tui")]
fn hot_pool_addresses(snap: &crate::services::hf_snapshot::HfSnapshot) -> Vec<Address> {
    let mut set = FxHashSet::default();
    for cycle in snap.cycles.iter().take(48) {
        for edge in &cycle.edges {
            if let Some(addr) = snap.arena.pool_address(edge.pool_index) {
                set.insert(addr);
            }
        }
    }
    set.into_iter().collect()
}

#[cfg(feature = "tui")]
fn unique_route_tokens(snap: &crate::services::hf_snapshot::HfSnapshot) -> Vec<Address> {
    use crate::core::constants::POLYGON_HUB_TOKENS;

    const TOKEN_LIMIT: usize = 24;
    let mut seen = FxHashSet::default();
    let mut ordered = Vec::with_capacity(TOKEN_LIMIT);

    for cycle in snap.cycles.iter().take(24) {
        if ordered.len() >= TOKEN_LIMIT {
            break;
        }
        if let Some(addr) = snap.arena.token_address(cycle.start_token)
            && seen.insert(addr)
            && ordered.len() < TOKEN_LIMIT
        {
            ordered.push(addr);
        }
        for edge in &cycle.edges {
            if ordered.len() >= TOKEN_LIMIT {
                break;
            }
            if let Some(addr) = snap.arena.token_address(edge.token_in)
                && seen.insert(addr)
                && ordered.len() < TOKEN_LIMIT
            {
                ordered.push(addr);
            }
            if let Some(addr) = snap.arena.token_address(edge.token_out)
                && seen.insert(addr)
                && ordered.len() < TOKEN_LIMIT
            {
                ordered.push(addr);
            }
        }
    }
    for hub in POLYGON_HUB_TOKENS {
        if ordered.len() >= TOKEN_LIMIT {
            break;
        }
        if seen.insert(hub) && ordered.len() < TOKEN_LIMIT {
            ordered.push(hub);
        }
    }
    ordered
}
