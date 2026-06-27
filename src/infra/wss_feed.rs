use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::{Address, B256};
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use alloy::pubsub::Subscription;
use alloy::rpc::types::Filter;
use futures::StreamExt;
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use crate::config::AppConfig;
use crate::infra::metrics::PipelineMetrics;
use crate::services::partial_cache::{
    PartialPoolCache, StreamAddressSet, V2_SYNC_TOPIC, V3_SWAP_TOPIC,
};
use crate::util::now_ms;

/// Max pool addresses per `eth_subscribe` filter (provider limits vary; 50 is conservative).
const SUBSCRIBE_CHUNK: usize = 50;

/// Base reconnect delay (doubles on each failure, capped at MAX_RECONNECT_DELAY_MS).
const BASE_RECONNECT_DELAY_MS: u64 = 500;
const MAX_RECONNECT_DELAY_MS: u64 = 30_000;

/// Silence timeout: if no log arrives within this window, the connection is considered stale.
const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(120);

pub struct PoolLogFeed {
    wss_url: String,
    partial: Arc<PartialPoolCache>,
    addresses: StreamAddressSet,
    metrics: Arc<PipelineMetrics>,
    shutdown: watch::Receiver<bool>,
}

impl PoolLogFeed {
    pub fn new(
        wss_url: String,
        partial: Arc<PartialPoolCache>,
        addresses: StreamAddressSet,
        metrics: Arc<PipelineMetrics>,
        shutdown: watch::Receiver<bool>,
    ) -> Self {
        Self {
            wss_url,
            partial,
            addresses,
            metrics,
            shutdown,
        }
    }

    pub async fn run(mut self) {
        let mut addr_rx = self.addresses.watch();
        let mut current_addrs = addr_rx.borrow().clone();
        let mut backoff_ms = BASE_RECONNECT_DELAY_MS;

        loop {
            if *self.shutdown.borrow() {
                info!("pool log feed shutting down");
                break;
            }

            if current_addrs.is_empty() {
                backoff_ms = BASE_RECONNECT_DELAY_MS;
                tokio::select! {
                    _ = self.shutdown.changed() => {
                        if *self.shutdown.borrow() { break; }
                    }
                    _ = addr_rx.changed() => {
                        current_addrs.clone_from(&addr_rx.borrow());
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {}
                }
                continue;
            }

            info!(
                pools = current_addrs.len(),
                chunks = current_addrs.len().div_ceil(SUBSCRIBE_CHUNK),
                "starting filtered log subscriptions"
            );

            let outcome = self
                .run_subscriptions(&current_addrs, self.shutdown.clone(), &mut addr_rx)
                .await;

            // Pick up any address changes that arrived during the subscription.
            current_addrs.clone_from(&addr_rx.borrow());

            match &outcome {
                Ok(()) => warn!("pool log feed subscriptions ended — reconnecting"),
                Err(e) => error!(error = %e, "pool log feed error — reconnecting"),
            }

            // ponytail: linear backoff capped at MAX_RECONNECT_DELAY. Exponential backoff
            // adds complexity; linear to 30s is sufficient given the WSS reconnect triggers
            // within one idle cycle.
            tokio::select! {
                _ = self.shutdown.changed() => {
                    if *self.shutdown.borrow() { break; }
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)) => {
                    if backoff_ms < MAX_RECONNECT_DELAY_MS {
                        backoff_ms = (backoff_ms * 2).min(MAX_RECONNECT_DELAY_MS);
                    }
                }
                _ = addr_rx.changed() => {
                    current_addrs.clone_from(&addr_rx.borrow());
                }
            }
        }
    }

    async fn run_subscriptions(
        &self,
        addresses: &[Address],
        mut shutdown: watch::Receiver<bool>,
        addr_rx: &mut watch::Receiver<Vec<Address>>,
    ) -> anyhow::Result<()> {
        let ws = WsConnect::new(self.wss_url.clone());
        let provider = ProviderBuilder::new().connect_ws(ws).await?;

        let mut subs: Vec<Subscription<alloy::rpc::types::Log>> = Vec::new();
        for chunk in addresses.chunks(SUBSCRIBE_CHUNK) {
            let filter = Filter::new()
                .address(chunk.to_vec())
                .event_signature(vec![V2_SYNC_TOPIC, V3_SWAP_TOPIC]);
            let sub = provider.subscribe_logs(&filter).await?;
            subs.push(sub);
        }

        let mut merged = futures::stream::select_all(
            subs.into_iter()
                .map(alloy::pubsub::Subscription::into_stream),
        );

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        return Ok(());
                    }
                }
                _ = addr_rx.changed() => {
                    return Ok(());
                }
                maybe_log = merged.next() => {
                    let Some(log) = maybe_log else {
                        return Ok(());
                    };
                    self.handle_log(&log);
                }
                _ = tokio::time::sleep(STREAM_IDLE_TIMEOUT) => {
                    warn!("WSS stream idle for {STREAM_IDLE_TIMEOUT:?} — reconnecting");
                    return Ok(());
                }
            }
        }
    }

    fn handle_log(&self, log: &alloy::rpc::types::Log) {
        let pool = log.address();
        let topic0 = log.topics().first().copied().unwrap_or(B256::ZERO);
        let data = log.data().data.as_ref();
        let ts = now_ms();
        if self.partial.apply_log(pool, topic0, data, ts) {
            self.metrics.record_stream_log();
            debug!(pool = %pool, "partial cache patched from WSS log");
        }
    }
}

/// Spawn the WSS log feed when streaming is enabled in config.
pub fn spawn_pool_log_feed(
    config: &AppConfig,
    partial: Arc<PartialPoolCache>,
    addresses: StreamAddressSet,
    metrics: Arc<PipelineMetrics>,
    shutdown: watch::Receiver<bool>,
) -> Option<tokio::task::JoinHandle<()>> {
    if !config.pipeline.stream_enabled {
        return None;
    }
    let wss_url = config.rpc.wss_url.clone().or_else(|| {
        config
            .rpc
            .state_rpc_url
            .as_ref()
            .or(config.rpc.polygon_rpc_urls.first())
            .and_then(|url| http_to_wss(url.as_str()))
    });
    let Some(wss_url) = wss_url else {
        warn!("stream enabled but no WSS URL configured or derivable from RPC URLs");
        return None;
    };
    info!(url = %mask_wss_url(&wss_url), "pool log WSS feed enabled");
    let feed = PoolLogFeed::new(wss_url, partial, addresses, metrics, shutdown);
    Some(tokio::spawn(async move {
        feed.run().await;
    }))
}

fn http_to_wss(url: &str) -> Option<String> {
    if url.starts_with("wss://") || url.starts_with("ws://") {
        return Some(url.to_string());
    }
    if let Some(rest) = url.strip_prefix("https://") {
        return Some(format!("wss://{rest}"));
    }
    if let Some(rest) = url.strip_prefix("http://") {
        return Some(format!("ws://{rest}"));
    }
    None
}

fn mask_wss_url(url: &str) -> String {
    if let Some((base, _)) = url.split_once("/v2/").or_else(|| url.split_once("/v3/")) {
        return format!("{base}/…");
    }
    if url.len() > 48 {
        format!("{}…", &url[..48])
    } else {
        url.to_string()
    }
}
