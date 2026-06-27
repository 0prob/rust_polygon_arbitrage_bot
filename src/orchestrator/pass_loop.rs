use std::sync::Arc;

use tokio::sync::{Mutex, Semaphore, watch};
use tokio::time::{Duration, MissedTickBehavior, interval};
use tracing::{Instrument, debug, error, info, warn};

use crate::config::{AppConfig, WalletSecrets};
use crate::error::ArbError;
use crate::infra::hypersync::HyperSyncService;
use crate::infra::metrics::PipelineMetrics;
use crate::infra::rpc::RpcPool;
use crate::infra::wss_feed::spawn_pool_log_feed;
use crate::orchestrator::hf::{HfContext, run_hf_tick};
use crate::orchestrator::lf::{LfContext, spawn_lf_background};
use crate::orchestrator::ui_hook::{SharedUiHook, noop_ui_hook};
use crate::pipeline::graph_cache::{GraphCache, set_graph_rebuild_interval};
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
    pub metrics: Arc<PipelineMetrics>,
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
    pub lf_tick_lock: Arc<Mutex<()>>,
    pub ui_hook: SharedUiHook,
}

impl RuntimeContext {
    pub fn new(
        config: AppConfig,
        wallet: WalletSecrets,
        hypersync: Option<HyperSyncService>,
    ) -> Result<Self, ArbError> {
        set_graph_rebuild_interval(config.pipeline.graph_rebuild_interval);
        let config = Arc::new(config);
        let wallet = Arc::new(wallet);
        let rpc = Arc::new(RpcPool::from_config(&config));
        let metrics = Arc::new(PipelineMetrics::default());
        let cache = Arc::new(StateCache::default());
        let partial_cache = Arc::new(PartialPoolCache::new());
        let stream_addresses = StreamAddressSet::new();
        let snapshots = Arc::new(SnapshotStore::new());
        let refresh = Arc::new(StateRefreshService::new(
            config.clone(),
            cache.clone(),
            rpc.clone(),
        )?);
        let execution = Arc::new(ExecutionService::with_circuit_breaker_cooldown(
            std::time::Duration::from_secs(config.execution.circuit_breaker_cooldown_secs),
        ));
        let gas_oracle = Arc::new(GasOracle::default());
        let price_oracle = Arc::new(PriceOracle::new(config.oracle.pyth_hermes_url.clone())?);
        Ok(Self {
            config,
            wallet,
            rpc,
            metrics,
            cache,
            partial_cache,
            stream_addresses,
            snapshots,
            refresh,
            execution,
            gas_oracle,
            price_oracle,
            hypersync: hypersync.map(Arc::new),
            graph_cache: Arc::new(parking_lot::Mutex::new(GraphCache::new())),
            lf_tick_lock: Arc::new(Mutex::new(())),
            ui_hook: noop_ui_hook(),
        })
    }

    pub fn with_ui_hook(mut self, hook: SharedUiHook) -> Self {
        self.ui_hook = hook;
        self
    }

    /// Attach a TUI update channel (requires `tui` feature).
    #[cfg(feature = "tui")]
    pub fn with_ui_bridge(self, bridge: crate::tui::UiBridge) -> Self {
        self.with_ui_hook(Arc::new(crate::tui::TuiUiHook::new(bridge)))
    }

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
            metrics: Arc::clone(&self.metrics),
            graph_cache: Arc::clone(&self.graph_cache),
            tick_lock: Arc::clone(&self.lf_tick_lock),
            ui_hook: Arc::clone(&self.ui_hook),
        }
    }

    pub fn hf_context(&self, shutdown: watch::Receiver<bool>) -> HfContext {
        HfContext {
            config: Arc::clone(&self.config),
            refresh: Arc::clone(&self.refresh),
            cache: Arc::clone(&self.cache),
            partial_cache: Arc::clone(&self.partial_cache),
            snapshots: Arc::clone(&self.snapshots),
            execution: Arc::clone(&self.execution),
            gas_oracle: Arc::clone(&self.gas_oracle),
            wallet: Arc::clone(&self.wallet),
            rpc: Arc::clone(&self.rpc),
            metrics: Arc::clone(&self.metrics),
            hypersync: self.hypersync.clone(),
            shutdown,
            dispatch_lock: Arc::new(Mutex::new(())),
            pending_dispatch: Arc::new(parking_lot::Mutex::new(None)),
            ui_hook: Arc::clone(&self.ui_hook),
        }
    }
}

async fn run_hf_tick_logged(ctx: Arc<HfContext>, stream_triggered: bool) {
    match run_hf_tick(Arc::clone(&ctx), stream_triggered).await {
        Ok(result) => {
            ctx.metrics.record_hf_tick(result.elapsed_ms, result.profitable_count);
        }
        Err(e) => error!(error = %e, "hf tick failed"),
    }
}

pub async fn run_pass_loop(
    ctx: Arc<RuntimeContext>,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let span = tracing::info_span!("pass_loop");
    async move {
        info!(
            lf_interval_ms = ctx.config.lf_interval_ms,
            hf_interval_ms = ctx.config.hf_interval_ms,
            dry_run = ctx.config.is_dry_run(),
            stream_enabled = ctx.config.pipeline.stream_enabled,
            private_submit = ctx.rpc.private_url().is_some(),
            require_private_submit = ctx.config.execution.require_private_submit,
            "pass loop starting"
        );

        let lf_ctx = Arc::new(ctx.lf_context());
        let hf_ctx = Arc::new(ctx.hf_context(shutdown.clone()));

        let mut hf_timer = interval(Duration::from_millis(ctx.config.hf_interval_ms));
        hf_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let hf_inflight = Arc::new(Semaphore::new(1));

        if let Some(url) = ctx.rpc.private_url().or_else(|| ctx.rpc.execution_url()) {
            let probe_url = url.to_string();
            tokio::spawn(async move {
                let probe =
                    crate::services::execution::private_submit::probe_submit_endpoint(&probe_url)
                        .await;
                crate::services::execution::private_submit::log_probe_report(&probe);
                if let Some(auth) = std::env::var("BLOXROUTE_AUTH_HEADER")
                    .ok()
                    .filter(|s| !s.is_empty())
                {
                    let ok =
                        crate::services::execution::private_submit::probe_bloxroute_auth(&auth)
                            .await;
                    if ok {
                        info!("bloXroute polygon_private_tx auth probe succeeded");
                    } else {
                        warn!("bloXroute auth configured but probe failed");
                    }
                }
            });
        }

        if let Ok(provider) = ctx.rpc.connect_state() {
            let gas_shutdown = shutdown.clone();
            if let Err(e) = ctx
                .gas_oracle
                .clone()
                .start_background(provider.clone(), gas_shutdown)
                .await
            {
                warn!(error = %e, "gas oracle background task failed to start");
            } else {
                info!("gas oracle polling started");
            }

            spawn_operator_balance_monitor(&ctx, provider, shutdown.clone());
        }

        let mut height_rx = ctx.hypersync.as_ref().map(|hs| {
            info!("HF block trigger enabled via HyperSync height stream");
            hs.stream_height()
        });

        let lf_shutdown = shutdown.clone();
        let _lf_handle = spawn_lf_background(
            lf_ctx,
            ctx.config.lf_interval_ms,
            lf_shutdown,
        );

        let _stream_feed = spawn_pool_log_feed(
            &ctx.config,
            Arc::clone(&ctx.partial_cache),
            ctx.stream_addresses.clone(),
            Arc::clone(&ctx.metrics),
            shutdown.clone(),
        );

        let stream_hf = ctx.config.pipeline.stream_enabled;
        let mut stream_rx = if stream_hf {
            Some(ctx.partial_cache.trigger().subscribe())
        } else {
            None
        };

        let spawn_hf_tick =
            |hf_ctx: Arc<HfContext>, hf_inflight: Arc<Semaphore>, stream_triggered: bool| {
                let permit = match hf_inflight.try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        hf_ctx.metrics.record_hf_skipped();
                        debug!("hf tick skipped — previous tick still running");
                        return;
                    }
                };
                tokio::spawn(async move {
                    if stream_triggered {
                        hf_ctx.partial_cache.trigger().take_stream_triggered();
                    }
                    run_hf_tick_logged(hf_ctx, stream_triggered).await;
                    drop(permit);
                });
            };

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("pass loop shutdown");
                        break;
                    }
                }
                _ = hf_timer.tick() => {
                    spawn_hf_tick(Arc::clone(&hf_ctx), Arc::clone(&hf_inflight), false);
                }
                event = async {
                    match height_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                }, if height_rx.is_some() => {
                    use hypersync_client::HeightStreamEvent;
                    match event {
                        Some(HeightStreamEvent::Height(_)) => {
                            hf_ctx.metrics.record_block_triggered_hf();
                            spawn_hf_tick(
                                Arc::clone(&hf_ctx),
                                Arc::clone(&hf_inflight),
                                false,
                            );
                        }
                        Some(HeightStreamEvent::Reconnecting { delay, error_msg }) => {
                            debug!(
                                delay_ms = delay.as_millis(),
                                error_msg,
                                "HyperSync height stream reconnecting"
                            );
                        }
                        Some(HeightStreamEvent::Connected) => {
                            debug!("HyperSync height stream connected");
                        }
                        None => {
                            info!("HyperSync height stream closed — disabling block trigger");
                            height_rx = None;
                        }
                    }
                }
                _ = async {
                    match stream_rx.as_mut() {
                        Some(rx) => rx.changed().await,
                        None => std::future::pending().await,
                    }
                }, if stream_hf => {
                    hf_ctx.metrics.record_stream_triggered_hf();
                    spawn_hf_tick(Arc::clone(&hf_ctx), Arc::clone(&hf_inflight), true);
                }
            }
        }

        Ok(())
    }
    .instrument(span)
    .await
}

fn spawn_operator_balance_monitor(
    ctx: &Arc<RuntimeContext>,
    provider: alloy::providers::DynProvider,
    mut shutdown: watch::Receiver<bool>,
) {
    use alloy::providers::Provider;
    use ruint::aliases::U256;

    let min_wei = ctx
        .config
        .execution
        .min_operator_matic_wei
        .parse::<U256>()
        .unwrap_or(U256::ZERO);
    if min_wei.is_zero() {
        return;
    }

    let Some(executor) = ctx.config.execution.executor_address else {
        return;
    };
    let operator = ctx.wallet.operator_address(executor);
    let execution = Arc::clone(&ctx.execution);

    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(30));
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        break;
                    }
                }
                _ = ticker.tick() => {
                    match provider.get_balance(operator).await {
                        Ok(balance) => {
                            let balance_u256 = U256::from(balance);
                            if !execution.circuit_breaker.check_operator_balance(balance_u256, min_wei) {
                                warn!(
                                    operator = %operator,
                                    balance = %balance_u256,
                                    floor = %min_wei,
                                    "operator balance below floor"
                                );
                            }
                        }
                        Err(e) => debug!(error = %e, "operator balance poll failed"),
                    }
                }
            }
        }
    });
}
