use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use alloy::primitives::U256;
use parking_lot::Mutex as ParkingMutex;
use tokio::sync::{Mutex, Semaphore, watch};
use tokio::task::JoinHandle;
use tokio::time::{Duration, MissedTickBehavior, interval};

use alloy::providers::Provider;

use crate::config::{AppConfig, OracleConfig, WalletSecrets};
use crate::info;
use crate::infra::hypersync::HyperSyncService;
use crate::infra::pg::PgClient;
use crate::infra::rpc::RpcPool;
use crate::infra::wss_feed::spawn_pool_log_feed;
use crate::orchestrator::hf::{HfContext, run_hf_tick};
use crate::orchestrator::lf::{LfContext, spawn_lf_background};
use crate::orchestrator::ui_hook::SharedUiHook;
#[cfg(feature = "tui")]
use crate::orchestrator::ui_snapshot::spawn_snapshot_publisher;
use crate::pipeline::arena::StateArena;
use crate::pipeline::graph_cache::GraphCache;
use crate::services::execution::ExecutionService;
use crate::services::execution::GasOracle;
use crate::services::hf_snapshot::SnapshotStore;
use crate::services::oracle::price_oracle::PriceOracle;
use crate::services::partial_cache::{PartialPoolCache, StreamAddressSet};
use crate::services::state_cache::StateCache;
use crate::services::state_refresh::StateRefreshService;

pub struct RuntimeContext {
    pub config: Arc<AppConfig>,
    pub wallet: Arc<WalletSecrets>,
    pub rpc: Arc<RpcPool>,
    pub cache: Arc<StateCache>,
    pub partial_cache: Arc<PartialPoolCache>,
    pub stream_addresses: StreamAddressSet,
    pub snapshots: Arc<SnapshotStore>,
    pub refresh: Arc<StateRefreshService>,
    pub execution: Arc<ExecutionService>,
    pub gas_oracle: Arc<GasOracle>,
    pub price_oracle: Arc<PriceOracle>,
    pub hypersync: Option<Arc<HyperSyncService>>,
    pub graph_cache: Arc<parking_lot::Mutex<GraphCache>>,
    pub arena: Arc<parking_lot::Mutex<StateArena>>,
    pub lf_tick_lock: Arc<Mutex<()>>,
    pub ui_hook: SharedUiHook,
    #[cfg(feature = "tui")]
    pub ui_snapshot_tx: Option<watch::Sender<Option<Arc<crate::tui::app::DashboardSnapshot>>>>,
}

impl RuntimeContext {
    pub fn new(
        config: AppConfig,
        wallet: WalletSecrets,
        hypersync: Option<HyperSyncService>,
    ) -> anyhow::Result<Self> {
        let rebuild_interval = config.pipeline.graph_rebuild_interval;
        let cycle_refind_interval = config.pipeline.cycle_refind_interval;
        let config = Arc::new(config);
        let wallet = Arc::new(wallet);
        let rpc = Arc::new(RpcPool::from_config(&config));
        let cache = Arc::new(StateCache::default());
        let partial_cache = Arc::new(PartialPoolCache::with_capacity(
            config.pipeline.stream_max_pools,
        ));
        let stream_addresses = StreamAddressSet::new();
        let snapshots = Arc::new(SnapshotStore::new());
        let refresh = Arc::new(StateRefreshService::new(
            config.clone(),
            cache.clone(),
            rpc.clone(),
        )?);
        let execution = Arc::new(ExecutionService::from_config(&config));
        let gas_oracle = Arc::new(GasOracle::default());
        let price_oracle = Arc::new(PriceOracle::new(
            rpc.http().clone(),
            config.oracle.pyth_hermes_url.clone(),
            config.oracle.cache_ttl_ms,
        ));
        register_configured_oracle_feeds(&price_oracle, &config.oracle);
        Ok(Self {
            config,
            wallet,
            rpc,
            cache,
            partial_cache,
            stream_addresses,
            snapshots,
            refresh,
            execution,
            gas_oracle,
            price_oracle,
            hypersync: hypersync.map(Arc::new),
            graph_cache: Arc::new(parking_lot::Mutex::new(GraphCache::with_intervals(
                rebuild_interval,
                cycle_refind_interval,
            ))),
            arena: Arc::new(parking_lot::Mutex::new(StateArena::default())),
            lf_tick_lock: Arc::new(Mutex::new(())),
            ui_hook: Arc::new(()),
            #[cfg(feature = "tui")]
            ui_snapshot_tx: None,
        })
    }

    #[must_use]
    pub fn with_ui_hook(mut self, hook: SharedUiHook) -> Self {
        self.ui_hook = hook;
        self
    }

    #[cfg(feature = "tui")]
    #[must_use]
    pub fn with_ui_snapshot_tx(
        mut self,
        tx: watch::Sender<Option<Arc<crate::tui::app::DashboardSnapshot>>>,
    ) -> Self {
        self.ui_snapshot_tx = Some(tx);
        self
    }

    #[must_use]
    pub fn lf_context(&self) -> LfContext {
        LfContext {
            config: Arc::clone(&self.config),
            refresh: Arc::clone(&self.refresh),
            cache: Arc::clone(&self.cache),
            snapshots: Arc::clone(&self.snapshots),
            stream_addresses: self.stream_addresses.clone(),
            partial_cache: Arc::clone(&self.partial_cache),
            price_oracle: Arc::clone(&self.price_oracle),
            rpc: Arc::clone(&self.rpc),
            graph_cache: Arc::clone(&self.graph_cache),
            arena: Arc::clone(&self.arena),
            tick_lock: Arc::clone(&self.lf_tick_lock),
            ui_hook: Arc::clone(&self.ui_hook),
        }
    }

    #[must_use]
    pub fn hf_context(&self, shutdown: watch::Receiver<bool>) -> HfContext {
        HfContext {
            config: Arc::clone(&self.config),
            refresh: Arc::clone(&self.refresh),
            cache: Arc::clone(&self.cache),
            partial_cache: Arc::clone(&self.partial_cache),
            snapshots: Arc::clone(&self.snapshots),
            execution: Arc::clone(&self.execution),
            gas_oracle: Arc::clone(&self.gas_oracle),
            price_oracle: Arc::clone(&self.price_oracle),
            wallet: Arc::clone(&self.wallet),
            rpc: Arc::clone(&self.rpc),
            hypersync: self.hypersync.clone(),
            shutdown,
            ui_hook: Arc::clone(&self.ui_hook),
        }
    }
}

pub async fn run_pass_loop(
    ctx: Arc<RuntimeContext>,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    info!("pass loop started");

    ctx.rpc
        .spawn_periodic_probe(shutdown.clone(), std::time::Duration::from_secs(600));
    // Rank RPC endpoints in the background; LF owns initial PG bootstrap via
    // maybe_discover on its first tick so HF/WSS can start without blocking.
    {
        let rpc = Arc::clone(&ctx.rpc);
        tokio::spawn(async move {
            rpc.probe_and_rank_state_urls().await;
        });
    }

    let daily_loss_guard = spawn_daily_loss_guard(&ctx, &shutdown);

    // Spawn PostgreSQL LISTEN/NOTIFY consumer — triggers early discovery on pool changes.
    {
        let notify_flag = ctx.refresh.notify_flag();
        let pg_url = ctx.config.pg_url.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            if let Err(e) = PgClient::spawn_notify_listener(&pg_url, notify_flag, shutdown).await {
                crate::warn!("pg LISTEN/NOTIFY not available — polling only: {e:#}");
            }
        });
    }

    let rpc = Arc::clone(&ctx.rpc);
    tokio::spawn(async move {
        if let Ok(p) = rpc.connect_state()
            && let Err(e) =
                crate::services::execution::flash_liquidity::fetch_and_cache_aave_flash_loan_fee_bps(
                    &p,
                )
                .await
        {
            crate::warn!("aave flash loan fee fetch failed: {e}");
        }
    });

    let lf_ctx = Arc::new(ctx.lf_context());
    let hf_ctx = Arc::new(ctx.hf_context(shutdown.clone()));

    let mut hf_timer = interval(Duration::from_millis(ctx.config.hf_interval_ms.max(1)));
    hf_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let hf_inflight = Arc::new(Semaphore::new(1));
    let hf_pending = Arc::new(AtomicBool::new(false));
    let hf_stream_pending = Arc::new(AtomicBool::new(false));
    let hf_task: Arc<ParkingMutex<Option<JoinHandle<()>>>> = Arc::new(ParkingMutex::new(None)); // ponytail: simple shared handle tracking

    if let Some(url) = ctx.rpc.private_url().or_else(|| ctx.rpc.execution_url()) {
        let probe_url = url.to_string();
        tokio::spawn(async move {
            let _probe =
                crate::services::execution::private_submit::probe_submit_endpoint(&probe_url).await;
            if let Some(auth) = std::env::var("BLOXROUTE_AUTH_HEADER")
                .ok()
                .filter(|s| !s.is_empty())
            {
                let _ =
                    crate::services::execution::private_submit::probe_bloxroute_auth(&auth).await;
            }
        });
    }

    Arc::clone(&ctx.execution.flash_liquidity)
        .start_background(Arc::clone(&ctx.rpc), shutdown.clone());
    spawn_gas_oracle_background(
        Arc::clone(&ctx.gas_oracle),
        Arc::clone(&ctx.rpc),
        shutdown.clone(),
    );

    #[cfg(feature = "tui")]
    let snapshot_handle = ctx.ui_snapshot_tx.clone().map(|tx| {
        spawn_snapshot_publisher(
            Arc::clone(&ctx),
            tx,
            std::time::Instant::now(),
            shutdown.clone(),
        )
    });

    let mut height_rx = ctx.hypersync.as_ref().map(|hs| hs.stream_height());
    let mut hs_reconnect_log_at = 0u64;
    let mut hs_height_fallback_at = 0u64;
    let mut hs_restart_backoff = Duration::from_secs(2);

    let lf_shutdown = shutdown.clone();
    let mut lf_handle = spawn_lf_background(lf_ctx, ctx.config.lf_interval_ms, lf_shutdown);

    let stream_feed = spawn_pool_log_feed(
        &ctx.config,
        Arc::clone(&ctx.partial_cache),
        ctx.stream_addresses.clone(),
        shutdown.clone(),
    );

    let mut stream_rx = if ctx.config.pipeline.stream_enabled {
        Some(ctx.partial_cache.trigger().subscribe())
    } else {
        None
    };
    let stream_hf_min_interval = Duration::from_millis(ctx.config.hf_interval_ms.max(1));
    let mut last_stream_hf_at = std::time::Instant::now()
        .checked_sub(stream_hf_min_interval)
        .unwrap_or_else(std::time::Instant::now);

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
            }
            _ = hf_timer.tick() => {
                schedule_hf_tick(
                    Arc::clone(&hf_ctx),
                    Arc::clone(&hf_inflight),
                    Arc::clone(&hf_task),
                    Arc::clone(&hf_pending),
                    Arc::clone(&hf_stream_pending),
                    false,
                );
            }
            event = async {
                match height_rx.as_mut() {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            }, if height_rx.is_some() => {
                use hypersync_client::HeightStreamEvent;
                match event {
                    Some(HeightStreamEvent::Height(height)) => {
                        hs_restart_backoff = Duration::from_secs(2);
                        if let Some(hs) = ctx.hypersync.as_ref() {
                            hs.record_height(height);
                        }
                        schedule_hf_tick(
                            Arc::clone(&hf_ctx),
                            Arc::clone(&hf_inflight),
                            Arc::clone(&hf_task),
                            Arc::clone(&hf_pending),
                            Arc::clone(&hf_stream_pending),
                            false,
                        );
                    }
                    Some(HeightStreamEvent::Reconnecting { delay, error_msg }) => {
                        let now = crate::util::now_ms();
                        if delay >= Duration::from_secs(5)
                            && now.saturating_sub(hs_height_fallback_at) >= 15_000
                            && let Ok(provider) = ctx.rpc.connect_state()
                            && let Ok(height) = provider.get_block_number().await
                        {
                            if let Some(hs) = ctx.hypersync.as_ref() {
                                hs.record_height(height);
                            }
                            hs_height_fallback_at = now;
                            crate::debug!("hypersync height fallback from RPC: {height}");
                        }
                        let reason = error_msg
                            .lines()
                            .map(str::trim)
                            .find(|line| !line.is_empty() && !line.starts_with("Caused by:"))
                            .unwrap_or(error_msg.as_str());
                        if now.saturating_sub(hs_reconnect_log_at) >= 300_000
                            || (delay.as_millis() == 0 && now.saturating_sub(hs_reconnect_log_at) >= 30_000)
                        {
                            crate::warn!(
                                "hypersync height stream reconnecting in {}ms: {reason}",
                                delay.as_millis()
                            );
                            hs_reconnect_log_at = now;
                        }
                    }
                    Some(HeightStreamEvent::Connected) => {
                        hs_restart_backoff = Duration::from_secs(2);
                        crate::debug!("hypersync height stream connected");
                    }
                    None => {
                        crate::warn!(
                            "hypersync height stream closed, restarting in {}ms",
                            hs_restart_backoff.as_millis()
                        );
                        tokio::time::sleep(hs_restart_backoff).await;
                        hs_restart_backoff =
                            (hs_restart_backoff * 2).min(Duration::from_secs(30));
                        height_rx = ctx.hypersync.as_ref().map(|hs| hs.stream_height());
                    }
                }
            }
            result = async {
                match stream_rx.as_mut() {
                    Some(rx) => rx.changed().await,
                    None => std::future::pending::<Result<(), tokio::sync::watch::error::RecvError>>().await,
                }
            }, if stream_rx.is_some() => {
                match result {
                    Ok(()) => {
                        let now = std::time::Instant::now();
                        if now.duration_since(last_stream_hf_at) >= stream_hf_min_interval {
                            last_stream_hf_at = now;
                            schedule_hf_tick(
                                Arc::clone(&hf_ctx),
                                Arc::clone(&hf_inflight),
                                Arc::clone(&hf_task),
                                Arc::clone(&hf_pending),
                                Arc::clone(&hf_stream_pending),
                                true,
                            );
                        }
                    }
                    Err(_) => {
                        // Attempt to re-subscribe on error. If the trigger is
                        // gone (all senders dropped), this is terminal.
                        let new_rx = ctx.partial_cache.trigger().subscribe();
                        if new_rx.has_changed().is_err() {
                            crate::warn!("stream trigger permanently closed — WSS-triggered HF ticks disabled");
                            stream_rx = None;
                        } else {
                            stream_rx = Some(new_rx);
                        }
                    }
                }
            }
        }
    }

    info!("pass loop shutting down");
    drop(height_rx);
    if let Some(handle) = stream_feed {
        handle.abort();
    }
    if let Some(handle) = daily_loss_guard {
        handle.abort();
    }
    let hf_join = hf_task.lock().take();
    if let Some(mut handle) = hf_join
        && tokio::time::timeout(Duration::from_secs(5), &mut handle)
            .await
            .is_err()
    {
        handle.abort();
        let _ = handle.await;
        crate::warn!("hf task aborted after 5s shutdown timeout");
    }
    if let (Some(executor), Ok(provider)) = (
        ctx.config.execution.executor_address,
        ctx.rpc.connect_state(),
    ) {
        let operator = ctx.wallet.operator_address(executor);
        ctx.execution.shutdown_resync(&provider, operator).await;
    }
    if tokio::time::timeout(Duration::from_secs(5), &mut lf_handle)
        .await
        .is_err()
    {
        lf_handle.abort();
        let _ = lf_handle.await;
        crate::warn!("lf background task aborted after 5s shutdown timeout");
    }
    #[cfg(feature = "tui")]
    if let Some(handle) = snapshot_handle {
        handle.abort();
        if tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .is_err()
        {
            crate::warn!("ui snapshot task did not exit within 2s of shutdown");
        }
    }
    Ok(())
}

/// ponytail: global lock on daily loss check. Per-circuit refinement if needed.
fn spawn_daily_loss_guard(
    ctx: &Arc<RuntimeContext>,
    shutdown_rx: &watch::Receiver<bool>,
) -> Option<JoinHandle<()>> {
    let max_loss = match ctx
        .config
        .execution
        .max_daily_loss_matic_wei
        .parse::<U256>()
    {
        Ok(v) if !v.is_zero() => v,
        Ok(_) => return None,
        Err(_) => {
            crate::warn!(
                "failed to parse max_daily_loss_matic_wei='{}' — daily loss guard disabled",
                ctx.config.execution.max_daily_loss_matic_wei
            );
            return None;
        }
    };

    let execution = Arc::clone(&ctx.execution);
    let mut rx = shutdown_rx.clone();
    Some(tokio::spawn(async move {
        use std::time::Instant;

        let mut ticker = interval(Duration::from_secs(60));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = rx.changed() => {
                    if *rx.borrow() {
                        break;
                    }
                }
                _ = ticker.tick() => {
                    let daily_loss = execution.pnl_snapshot().1;
                    if daily_loss < 0 {
                        let abs_loss = U256::from(daily_loss.unsigned_abs());
                        if abs_loss >= max_loss {
                            execution.quarantine_global(Duration::from_secs(3600), Instant::now());
                            crate::error!(
                                "DAILY LOSS LIMIT BREACHED: {abs_loss} >= {max_loss} wei — execution quarantined 1h"
                            );
                            break;
                        }
                    }
                }
            }
        }
    }))
}

/// Start fee polling once state RPC is reachable (retries if startup connect fails).
fn spawn_gas_oracle_background(
    gas_oracle: Arc<GasOracle>,
    rpc: Arc<RpcPool>,
    mut shutdown: watch::Receiver<bool>,
) {
    tokio::spawn(async move {
        let retry = Duration::from_secs(5);
        loop {
            if *shutdown.borrow() {
                return;
            }
            match rpc.connect_state() {
                Ok(provider) => {
                    gas_oracle.start_background(provider, shutdown);
                    return;
                }
                Err(e) => {
                    crate::warn!("gas oracle waiting for state RPC: {e:#}");
                    tokio::select! {
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() {
                                return;
                            }
                        }
                        _ = tokio::time::sleep(retry) => {}
                    }
                }
            }
        }
    });
}

fn schedule_hf_tick(
    hf_ctx: Arc<HfContext>,
    hf_inflight: Arc<Semaphore>,
    hf_task: Arc<ParkingMutex<Option<JoinHandle<()>>>>,
    hf_pending: Arc<AtomicBool>,
    hf_stream_pending: Arc<AtomicBool>,
    stream_triggered: bool,
) {
    let hf_inflight_acquire = Arc::clone(&hf_inflight);
    let hf_inflight_coalesce = Arc::clone(&hf_inflight);
    let Ok(permit) = hf_inflight_acquire.try_acquire_owned() else {
        hf_pending.store(true, Ordering::Release);
        if stream_triggered {
            hf_stream_pending.store(true, Ordering::Release);
        }
        return;
    };
    let hf_ctx_run = Arc::clone(&hf_ctx);
    let hf_task_store = Arc::clone(&hf_task);
    let hf_task_coalesce = Arc::clone(&hf_task);
    let hf_pending_coalesce = Arc::clone(&hf_pending);
    let hf_stream_pending_coalesce = Arc::clone(&hf_stream_pending);
    *hf_task_store.lock() = Some(tokio::spawn(async move {
        let _permit = permit;
        if stream_triggered {
            let _ = hf_ctx_run.partial_cache.trigger().take_stream_triggered();
        }
        if let Err(e) = run_hf_tick(Arc::clone(&hf_ctx_run), stream_triggered).await {
            crate::warn!("hf tick failed: {e:#}");
        }
        drop(_permit);
        if take_pending_hf_tick(&hf_pending_coalesce) {
            let coalesced_stream = take_pending_hf_stream(&hf_stream_pending_coalesce);
            schedule_hf_tick(
                hf_ctx_run,
                hf_inflight_coalesce,
                hf_task_coalesce,
                hf_pending_coalesce,
                hf_stream_pending_coalesce,
                coalesced_stream,
            );
        }
    }));
}

fn take_pending_hf_tick(hf_pending: &AtomicBool) -> bool {
    hf_pending.swap(false, Ordering::AcqRel)
}

fn take_pending_hf_stream(hf_stream_pending: &AtomicBool) -> bool {
    hf_stream_pending.swap(false, Ordering::AcqRel)
}

fn register_configured_oracle_feeds(oracle: &PriceOracle, config: &OracleConfig) {
    use alloy::primitives::Address;

    for pair in config.pyth_feeds.split(',').filter(|s| !s.is_empty()) {
        let Some((token_str, feed_id)) = pair.split_once('=') else {
            continue;
        };
        let Ok(token) = token_str.trim().parse::<Address>() else {
            continue;
        };
        oracle.register_pyth_feed(token, feed_id.trim().to_string());
    }

    for pair in config.chainlink_feeds.split(',').filter(|s| !s.is_empty()) {
        let Some((token_str, feed_str)) = pair.split_once('=') else {
            continue;
        };
        let Ok(token) = token_str.trim().parse::<Address>() else {
            continue;
        };
        let Ok(feed) = feed_str.trim().parse::<Address>() else {
            continue;
        };
        oracle.register_chainlink_feed(token, feed);
    }
}

#[cfg(test)]
mod tests {
    use super::{take_pending_hf_stream, take_pending_hf_tick};
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn pending_hf_tick_is_consumed_once() {
        let pending = AtomicBool::new(true);
        assert!(take_pending_hf_tick(&pending));
        assert!(!pending.load(Ordering::Acquire));
        assert!(!take_pending_hf_tick(&pending));
    }

    #[test]
    fn pending_hf_stream_is_consumed_once() {
        let pending = AtomicBool::new(true);
        assert!(take_pending_hf_stream(&pending));
        assert!(!pending.load(Ordering::Acquire));
        assert!(!take_pending_hf_stream(&pending));
    }
}
