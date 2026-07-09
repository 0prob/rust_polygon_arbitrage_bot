use std::sync::Arc;

use parking_lot::Mutex;
use std::time::{Duration, Instant};

use crate::config::AppConfig;
use crate::services::partial_cache::{
    PartialPoolCache, StreamAddressSet, V2_SYNC_TOPIC, V3_SWAP_TOPIC,
};
use crate::util::now_ms;
use crate::{info, warn};
use alloy::primitives::{Address, B256};
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use alloy::pubsub::Subscription;
use alloy::rpc::types::Filter;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

/// Max pool addresses per `eth_subscribe` filter (provider limits vary; 50 is conservative).
const SUBSCRIBE_CHUNK: usize = 50;

/// Base reconnect delay (doubles on each failure, capped at MAX_RECONNECT_DELAY_MS).
/// Reduced for HFT sensitivity — Polygon WSS endpoints typically reconnect in <200ms.
const BASE_RECONNECT_DELAY_MS: u64 = 100;
const MAX_RECONNECT_DELAY_MS: u64 = 5_000;

/// Silence timeout: if no log arrives within this window, the connection is considered stale.
const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Per-endpoint connect + `eth_blockNumber` probe budget.
const WSS_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
/// Coalesce rapid LF stream-target updates before tearing down subscriptions.
const WSS_ADDR_DEBOUNCE: Duration = Duration::from_millis(400);

pub struct PoolLogFeed {
    wss_urls: Vec<String>,
    sticky_url: Arc<Mutex<Option<String>>>,
    partial: Arc<PartialPoolCache>,
    addresses: StreamAddressSet,
    shutdown: watch::Receiver<bool>,
}

impl PoolLogFeed {
    pub fn new(
        wss_urls: Vec<String>,
        partial: Arc<PartialPoolCache>,
        addresses: StreamAddressSet,
        shutdown: watch::Receiver<bool>,
    ) -> Self {
        Self {
            wss_urls,
            sticky_url: Arc::new(Mutex::new(None)),
            partial,
            addresses,
            shutdown,
        }
    }

    pub async fn run(mut self) {
        let mut addr_rx = self.addresses.watch();
        let mut current_addrs = addr_rx.borrow().clone();
        let mut backoff_ms = BASE_RECONNECT_DELAY_MS;

        loop {
            if *self.shutdown.borrow() {
                break;
            }

            if current_addrs.is_empty() {
                backoff_ms = BASE_RECONNECT_DELAY_MS;
            } else {
                let sticky = self.sticky_url.lock().clone();
                match select_wss_url(&self.wss_urls, sticky.as_deref()).await {
                    Some(url) => {
                        backoff_ms = BASE_RECONNECT_DELAY_MS;
                        match self
                            .run_subscriptions(
                                &url,
                                &current_addrs,
                                self.shutdown.clone(),
                                &mut addr_rx,
                            )
                            .await
                        {
                            Err(e) => {
                                warn!("WSS subscription error ({url}): {e}");
                                *self.sticky_url.lock() = None;
                            }
                            Ok(()) => {
                                *self.sticky_url.lock() = Some(url);
                            }
                        }
                    }
                    None => warn!("no WSS endpoint available — retrying"),
                }
                current_addrs.clone_from(&addr_rx.borrow());
            }

            tokio::select! {
                _ = self.shutdown.changed() => {
                    if *self.shutdown.borrow() { break; }
                }
                _ = addr_rx.changed() => {
                    current_addrs.clone_from(&addr_rx.borrow());
                }
                () = tokio::time::sleep(Duration::from_millis(backoff_ms)) => {
                    backoff_ms = (backoff_ms * 2).min(MAX_RECONNECT_DELAY_MS);
                }
            }
        }
    }

    async fn run_subscriptions(
        &self,
        wss_url: &str,
        addresses: &[Address],
        mut shutdown: watch::Receiver<bool>,
        addr_rx: &mut watch::Receiver<Vec<Address>>,
    ) -> anyhow::Result<()> {
        let ws = WsConnect::new(wss_url.to_string());
        let provider = ProviderBuilder::new().connect_ws(ws).await?;

        let mut subs: Vec<Subscription<alloy::rpc::types::Log>> =
            Vec::with_capacity(addresses.len().div_ceil(SUBSCRIBE_CHUNK));
        let mut join_set = tokio::task::JoinSet::new();
        for chunk in addresses.chunks(SUBSCRIBE_CHUNK) {
            let filter = Filter::new()
                .address(chunk.to_vec())
                .event_signature(vec![V2_SYNC_TOPIC, V3_SWAP_TOPIC]);
            let provider = provider.clone();
            join_set.spawn(async move { provider.subscribe_logs(&filter).await });
        }
        while let Some(result) = join_set.join_next().await {
            subs.push(result??);
        }

        let (log_tx, mut log_rx) = mpsc::channel(1024);
        let mut readers = JoinSet::new();
        for mut sub in subs {
            let log_tx = log_tx.clone();
            readers.spawn(async move {
                while let Ok(log) = sub.recv().await {
                    if log_tx.send(log).await.is_err() {
                        break;
                    }
                }
            });
        }
        drop(log_tx);

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        return Ok(());
                    }
                }
                _ = addr_rx.changed() => {
                    tokio::time::sleep(WSS_ADDR_DEBOUNCE).await;
                    while addr_rx.has_changed().unwrap_or(false) {
                        tokio::time::sleep(WSS_ADDR_DEBOUNCE).await;
                    }
                    return Ok(());
                }
                maybe_log = log_rx.recv() => {
                    let Some(log) = maybe_log else {
                        warn!("WSS feed disconnected ({wss_url}), reconnecting...");
                        return Ok(());
                    };
                    self.handle_log(&log);
                }
                () = tokio::time::sleep(STREAM_IDLE_TIMEOUT) => {
                    warn!("WSS feed idle timeout ({wss_url}), reconnecting...");
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
        self.partial.apply_log(pool, topic0, data, ts);
    }
}

/// Spawn the WSS log feed when streaming is enabled in config.
pub fn spawn_pool_log_feed(
    config: &AppConfig,
    partial: Arc<PartialPoolCache>,
    addresses: StreamAddressSet,
    shutdown: watch::Receiver<bool>,
) -> Option<tokio::task::JoinHandle<()>> {
    if !config.pipeline.stream_enabled {
        return None;
    }
    let wss_urls = wss_url_candidates(config);
    if wss_urls.is_empty() {
        return None;
    }
    let feed = PoolLogFeed::new(wss_urls, partial, addresses, shutdown);
    Some(tokio::spawn(async move {
        feed.run().await;
    }))
}

fn wss_url_candidates(config: &AppConfig) -> Vec<String> {
    if let Some(url) = config.rpc.wss_url.as_deref().filter(|u| !u.is_empty()) {
        return vec![url.to_string()];
    }

    let mut urls = Vec::new();
    for url in &config.rpc.polygon_wss_urls {
        if !url.is_empty() && !urls.iter().any(|u| u == url) {
            urls.push(url.clone());
        }
    }
    if urls.is_empty()
        && let Some(url) = config
            .rpc
            .state_rpc_url
            .as_ref()
            .or(config.rpc.polygon_rpc_urls.first())
            .and_then(|url| http_to_wss(url.as_str()))
    {
        urls.push(url);
    }
    urls
}

fn ordered_wss_urls(urls: &[String], sticky: Option<&str>) -> Vec<String> {
    let mut ordered = Vec::with_capacity(urls.len());
    if let Some(url) = sticky.filter(|s| urls.iter().any(|u| u == *s)) {
        ordered.push(url.to_string());
    }
    for url in urls {
        if !ordered.iter().any(|u| u == url) {
            ordered.push(url.clone());
        }
    }
    ordered
}

async fn probe_wss_latency(url: &str) -> Option<Duration> {
    let started = Instant::now();
    let ws = WsConnect::new(url.to_string());
    let provider = tokio::time::timeout(WSS_PROBE_TIMEOUT, ProviderBuilder::new().connect_ws(ws))
        .await
        .ok()?
        .ok()?;
    tokio::time::timeout(WSS_PROBE_TIMEOUT, provider.get_block_number())
        .await
        .ok()?
        .ok()?;
    Some(started.elapsed())
}

async fn probe_wss_urls(urls: &[String]) -> Option<(String, Duration)> {
    let mut probes = tokio::task::JoinSet::new();
    for url in urls {
        let url = url.clone();
        probes.spawn(async move {
            let latency = probe_wss_latency(&url).await?;
            Some((url, latency))
        });
    }
    let mut best: Option<(String, Duration)> = None;
    while let Some(result) = probes.join_next().await {
        let Ok(Some((url, latency))) = result else {
            continue;
        };
        let replace = best
            .as_ref()
            .is_none_or(|(_, best_latency)| latency < *best_latency);
        if replace {
            best = Some((url, latency));
        }
    }
    best
}

async fn select_wss_url(urls: &[String], sticky: Option<&str>) -> Option<String> {
    if urls.is_empty() {
        return None;
    }
    if sticky.is_some()
        && let Some(url) = sticky.filter(|s| urls.iter().any(|u| u == *s))
    {
        crate::debug!("WSS sticky reconnect ({url}, probe skipped)");
        return Some(url.to_string());
    }

    let candidates = ordered_wss_urls(urls, sticky);
    let (url, latency) = probe_wss_urls(&candidates).await?;
    info!(
        "WSS endpoint selected ({url}, probe_ms={})",
        latency.as_millis()
    );
    Some(url)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_config() -> AppConfig {
        AppConfig::default()
    }

    #[test]
    fn wss_url_override_skips_list() {
        let mut config = sample_config();
        config.rpc.wss_url = Some("wss://only".into());
        config.rpc.polygon_wss_urls = vec!["wss://a".into(), "wss://b".into()];
        assert_eq!(wss_url_candidates(&config), vec!["wss://only".to_string()]);
    }

    #[test]
    fn wss_url_candidates_dedupes_polygon_list() {
        let mut config = sample_config();
        config.rpc.polygon_wss_urls = vec!["wss://a".into(), "wss://b".into(), "wss://a".into()];
        assert_eq!(
            wss_url_candidates(&config),
            vec!["wss://a".to_string(), "wss://b".to_string()]
        );
    }

    #[test]
    fn ordered_wss_urls_puts_sticky_first() {
        let urls = vec![
            "wss://a".to_string(),
            "wss://b".to_string(),
            "wss://c".to_string(),
        ];
        assert_eq!(
            ordered_wss_urls(&urls, Some("wss://c")),
            vec![
                "wss://c".to_string(),
                "wss://a".to_string(),
                "wss://b".to_string()
            ]
        );
    }

    #[test]
    fn http_to_wss_converts_https() {
        assert_eq!(
            http_to_wss("https://polygon.example/v1/key"),
            Some("wss://polygon.example/v1/key".to_string())
        );
    }

    #[tokio::test]
    async fn subscription_reader_join_set_aborts_pending_tasks_on_drop() {
        struct DropNotify(Option<tokio::sync::oneshot::Sender<()>>);

        impl Drop for DropNotify {
            fn drop(&mut self) {
                if let Some(tx) = self.0.take() {
                    let _ = tx.send(());
                }
            }
        }

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        let mut readers = JoinSet::new();
        readers.spawn(async move {
            let _notify = DropNotify(Some(dropped_tx));
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        });

        started_rx.await.expect("reader task should start");
        drop(readers);

        tokio::time::timeout(Duration::from_secs(1), dropped_rx)
            .await
            .expect("reader task should be aborted")
            .expect("reader task should drop notify guard");
    }
}
