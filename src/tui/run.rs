use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy::primitives::Address;
use rustc_hash::FxHashSet;

use anyhow::Context;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::watch;

use crate::orchestrator::{RuntimeContext, run_pass_loop};
use crate::util::u256_to_f64;

use super::app::{App, Severity};
use super::bridge::{TuiBridge, publish_snapshot};
use super::events::{UiEvent, spawn_input_thread};
use super::terminal::TerminalGuard;
use super::update::{
    RouteBuildCache, RuntimeSnapshotInput, apply_event, build_config_rows, build_diagnostics,
    build_portfolio_rows, build_route_cache, build_snapshot,
};

const PASS_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);
const SNAPSHOT_POLL_INTERVAL: Duration = Duration::from_millis(1_500);
const ORACLE_REFRESH_INTERVAL: Duration = Duration::from_secs(10);
const PORTFOLIO_REFRESH_INTERVAL: Duration = Duration::from_secs(15);
/// Caps terminal writes while keeping metric charts visibly responsive.
const REDRAW_INTERVAL: Duration = Duration::from_millis(250);

pub async fn run_tui(
    ctx: Arc<RuntimeContext>,
    bridge: TuiBridge,
    rx: UnboundedReceiver<UiEvent>,
) -> anyhow::Result<()> {
    let started_at = Instant::now();
    let mut terminal = TerminalGuard::enter().context("failed to initialize terminal")?;
    let mut app = App::new();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let mut pass_handle = tokio::spawn(run_pass_loop(Arc::clone(&ctx), shutdown_rx));

    let tx = bridge.sender();
    let input_thread = spawn_input_thread(tx.clone()).context("spawn input thread")?;
    let poller = spawn_runtime_poller(Arc::clone(&ctx), tx.clone(), started_at);

    draw_frame(&mut terminal, &app)?;

    let mut rx = rx;
    let mut redraw = tokio::time::interval(REDRAW_INTERVAL);
    redraw.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut manual_refresh: Option<tokio::task::JoinHandle<()>> = None;
    let mut needs_redraw = true;

    loop {
        tokio::select! {
            maybe_event = rx.recv() => {
                let Some(event) = maybe_event else { break; };
                let input_event = matches!(event, UiEvent::Input(_));
                apply_event(&mut app, event);
                if app.should_quit {
                    break;
                }
                needs_redraw = true;
                if input_event {
                    draw_frame(&mut terminal, &app)?;
                    needs_redraw = false;
                }
            }
            _ = redraw.tick(), if !app.should_quit => {
                if manual_refresh.as_ref().is_some_and(tokio::task::JoinHandle::is_finished) {
                    // The task publishes its result/error through the UI channel.
                    if let Some(task) = manual_refresh.take() {
                        let _ = task.await;
                    }
                }
                if app.snapshot_refresh_pending {
                    app.snapshot_refresh_pending = false;
                    if manual_refresh.is_none() {
                        manual_refresh = Some(spawn_manual_refresh(
                            Arc::clone(&ctx),
                            tx.clone(),
                            started_at,
                        ));
                    } else {
                        app.push_activity(Severity::Info, "manual refresh already running");
                    }
                }
                // Periodic rendering keeps uptime/snapshot age moving. All
                // non-input events arriving in this window are coalesced into
                // this single terminal write. Only draw if needed.
                if needs_redraw {
                    draw_frame(&mut terminal, &app)?;
                    needs_redraw = false;
                }
            }
        }
    }

    let _ = shutdown_tx.send(true);
    // Let the pass loop observe shutdown before we tear down UI I/O.
    tokio::task::yield_now().await;

    drop(tx);
    poller.abort();
    if let Some(refresh) = manual_refresh {
        refresh.abort();
    }
    // Never block the tokio runtime on join — that prevents pass_loop from exiting.
    drop(input_thread);
    terminal.restore().ok();

    match tokio::time::timeout(PASS_SHUTDOWN_TIMEOUT, &mut pass_handle).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(e))) => {
            crate::warn!("pass loop failed during shutdown: {e}");
        }
        Ok(Err(e)) => {
            crate::warn!("pass loop panicked during shutdown: {e}");
        }
        Err(_) => {
            pass_handle.abort();
            crate::warn!("pass loop shutdown timed out after {PASS_SHUTDOWN_TIMEOUT:?}");
        }
    }

    Ok(())
}

fn spawn_manual_refresh(
    ctx: Arc<RuntimeContext>,
    tx: tokio::sync::mpsc::UnboundedSender<UiEvent>,
    started_at: Instant,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut route_cache = RouteBuildCache {
            generation: 0,
            opportunities: Arc::new(Vec::new()),
            simulations: Arc::new(Vec::new()),
        };
        let result = tokio::time::timeout(
            Duration::from_secs(8),
            collect_snapshot(ctx, started_at, true, true, &mut route_cache, Vec::new()),
        )
        .await;
        match result {
            Ok(Ok((snapshot, _))) => publish_snapshot(&tx, snapshot),
            Ok(Err(e)) => {
                let _ = tx.send(UiEvent::Message {
                    severity: Severity::Warn,
                    message: format!("manual refresh failed: {e}"),
                });
            }
            Err(_) => {
                let _ = tx.send(UiEvent::Message {
                    severity: Severity::Warn,
                    message: "manual refresh timed out".to_string(),
                });
            }
        }
    })
}

fn draw_frame(terminal: &mut TerminalGuard, app: &App) -> anyhow::Result<()> {
    terminal
        .terminal()
        .draw(|frame| super::widgets::render(frame, app))?;
    Ok(())
}

fn spawn_runtime_poller(
    ctx: Arc<RuntimeContext>,
    tx: tokio::sync::mpsc::UnboundedSender<UiEvent>,
    started_at: Instant,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(SNAPSHOT_POLL_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_oracle_refresh = Instant::now()
            .checked_sub(ORACLE_REFRESH_INTERVAL)
            .unwrap_or_else(Instant::now);
        let mut last_portfolio_refresh = Instant::now()
            .checked_sub(PORTFOLIO_REFRESH_INTERVAL)
            .unwrap_or_else(Instant::now);
        let mut portfolio_rows = Vec::new();
        let mut route_cache = RouteBuildCache {
            generation: 0,
            opportunities: Arc::new(Vec::new()),
            simulations: Arc::new(Vec::new()),
        };

        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                () = tx.closed() => break,
            }

            let refresh_oracle = last_oracle_refresh.elapsed() >= ORACLE_REFRESH_INTERVAL;
            let refresh_portfolio = last_portfolio_refresh.elapsed() >= PORTFOLIO_REFRESH_INTERVAL;

            match tokio::time::timeout(
                Duration::from_secs(8),
                collect_snapshot(
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
                    publish_snapshot(&tx, snapshot);
                }
                Ok(Err(e)) => {
                    let _ = tx.send(UiEvent::Message {
                        severity: Severity::Warn,
                        message: format!("snapshot poll failed: {e}"),
                    });
                }
                Err(_) => {
                    let _ = tx.send(UiEvent::Message {
                        severity: Severity::Warn,
                        message: "snapshot poll timed out".to_string(),
                    });
                }
            }
        }
    })
}

async fn collect_snapshot(
    ctx: Arc<RuntimeContext>,
    started_at: Instant,
    refresh_oracle: bool,
    refresh_portfolio: bool,
    route_cache: &mut RouteBuildCache,
    mut portfolio_rows: Vec<super::app::PortfolioRow>,
) -> anyhow::Result<(super::app::DashboardSnapshot, Vec<super::app::PortfolioRow>)> {
    let snap = ctx.snapshots.read();

    let mut matic_usd = ctx.price_oracle.cached_matic_usd().unwrap_or(0.0);
    let route_tokens = if refresh_oracle || matic_usd <= 0.0 || refresh_portfolio {
        unique_route_tokens(&snap)
    } else {
        Vec::new()
    };

    if refresh_oracle || matic_usd <= 0.0 {
        let provider = ctx.rpc.connect_state().ok();
        let provider_ref = provider.as_ref();
        let refreshed = if let Some(provider) = provider_ref {
            ctx.price_oracle.get_matic_usd(Some(provider)).await
        } else {
            ctx.price_oracle.get_matic_usd_offline().await
        };
        if refreshed > 0.0 {
            matic_usd = refreshed;
        }
        if let Some(provider) = provider_ref {
            ctx.price_oracle
                .prefetch_token_usd(&route_tokens, Some(provider))
                .await;
        } else {
            ctx.price_oracle
                .prefetch_token_usd_offline(&route_tokens)
                .await;
        }
    }

    if refresh_portfolio {
        let provider = ctx.rpc.connect_state().ok();
        let provider_ref = provider.as_ref();
        let balance_account = portfolio_balance_account(&ctx);
        portfolio_rows = build_portfolio_rows(
            provider_ref,
            &ctx.price_oracle,
            &snap,
            &route_tokens,
            balance_account,
        )
        .await;
    }

    let input_arena = if snap.generation != route_cache.generation {
        let hot_pools = hot_pool_addresses(&snap);
        let mut arena = snap.arena.clone();
        arena.apply_hot_cache(&ctx.cache, &hot_pools);
        *route_cache = build_route_cache(&snap, &arena, matic_usd);
        arena
    } else {
        snap.arena.clone()
    };

    let gas_gwei = ctx
        .gas_oracle
        .snapshot()
        .map(|fee| u256_to_f64(fee.base_fee + fee.priority_fee) / 1e9);

    let hypersync_height = ctx.hypersync.as_ref().and_then(|hs| hs.latest_height());

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
    let (total_profit_wei, daily_pnl_wei) = ctx.execution.pnl_snapshot();
    let total_trade_count = execution_trades + execution_losses;
    let runtime_input = RuntimeSnapshotInput {
        started_at,
        snapshot: Arc::clone(&snap),
        arena: input_arena,
        config: Arc::clone(&ctx.config),
        refresh: Arc::clone(&ctx.refresh),
        execution_trades,
        execution_losses,
        daily_pnl_wei,
        total_profit_wei,
        total_trade_count,
        gas_gwei,
        hypersync_height,
        matic_usd,
        portfolio_rows: portfolio_rows.clone(),
        diagnostics,
        config_rows,
        history: Vec::new(),
        last_search_ms: 0,
        last_hf_ms: 0,
        last_profitable: 0,
        last_cycles_considered: snap.cycles.len(),
        last_best_profit_wei: None,
        route_cache: Some(route_cache.clone()),
    };

    Ok((build_snapshot(runtime_input).await, portfolio_rows))
}

fn portfolio_balance_account(ctx: &RuntimeContext) -> Option<Address> {
    ctx.config
        .execution
        .executor_address
        .map(|executor| ctx.wallet.operator_address(executor))
}

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

fn unique_route_tokens(snap: &crate::services::hf_snapshot::HfSnapshot) -> Vec<Address> {
    const TOKEN_LIMIT: usize = 24;
    let mut seen = FxHashSet::default();
    let mut ordered = Vec::with_capacity(TOKEN_LIMIT);
    let mut push = |addr: Address| -> bool {
        if seen.insert(addr) && ordered.len() < TOKEN_LIMIT {
            ordered.push(addr);
        }
        ordered.len() >= TOKEN_LIMIT
    };

    'cycles: for cycle in snap.cycles.iter().take(24) {
        if let Some(addr) = snap.arena.token_address(cycle.start_token)
            && push(addr)
        {
            break 'cycles;
        }
        for edge in &cycle.edges {
            if let Some(addr) = snap.arena.token_address(edge.token_in)
                && push(addr)
            {
                break 'cycles;
            }
            if let Some(addr) = snap.arena.token_address(edge.token_out)
                && push(addr)
            {
                break 'cycles;
            }
        }
    }
    ordered
}
