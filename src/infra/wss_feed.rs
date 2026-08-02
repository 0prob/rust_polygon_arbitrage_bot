use std::sync::Arc;

use parking_lot::Mutex;
use std::time::{Duration, Instant};

use crate::config::AppConfig;
use crate::infra::rpc::rpc_host_label;
use crate::services::partial_cache::{
    PartialPoolCache, StreamAddressSet, V2_SYNC_TOPIC, V3_SWAP_TOPIC,
};
use crate::util::now_ms;
use crate::{info, warn};
use alloy::primitives::{Address, B256};
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use alloy::rpc::types::Filter;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant as TokioInstant, MissedTickBehavior, interval_at, timeout};

/// Base reconnect delay (doubles on each failure, capped at MAX_RECONNECT_DELAY_MS).
/// Reduced for HFT sensitivity — Polygon WSS endpoints typically reconnect in <200ms.
const BASE_RECONNECT_DELAY_MS: u64 = 100;
const MAX_RECONNECT_DELAY_MS: u64 = 5_000;

/// Silence timeout: if no Sync/Swap arrives within this window, reconnect.
const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(90);
/// RPC ping interval — keeps the WS alive when the chain is quiet.
const WSS_PING_INTERVAL: Duration = Duration::from_secs(15);

/// Per-endpoint connect + `eth_blockNumber` probe budget (fail-fast multi-URL select).
const WSS_PROBE_TIMEOUT: Duration = Duration::from_secs(3);
const WSS_SUBSCRIPTION_FAILURE_COOLDOWN_MS: u64 = 60_000;
/// Free-plan / 429 subscription failures need longer cool-off (live: dRPC code 15
/// every ~60s when rotated back too soon).
const WSS_RATE_LIMIT_COOLDOWN_MS: u64 = 120_000;
/// No Sync/Swap after arm → force LF to rotate interest set / re-seed ranking.
const WSS_LOG_SILENCE_FORCE: Duration = Duration::from_secs(20);
const WSS_DEDUP_TTL_MS: u64 = 120_000;
const WSS_DEDUP_CAP: usize = 16_384;

/// Why a live subscription session ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubscriptionExit {
    /// LF stream targets changed; keep the current endpoint.
    AddressChange,
    /// Transport or health-check failure; clear sticky and re-probe endpoints.
    Unhealthy,
}

pub struct PoolLogFeed {
    wss_urls: Vec<String>,
    sticky_url: Arc<Mutex<Option<String>>>,
    subscription_cooldowns: Arc<Mutex<rustc_hash::FxHashMap<String, u64>>>,
    seen_logs: Arc<Mutex<rustc_hash::FxHashMap<(B256, u64), u64>>>,
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
        Self::with_preferred_url(
            wss_urls,
            None,
            partial,
            addresses,
            shutdown,
            Arc::new(Mutex::new(rustc_hash::FxHashMap::default())),
        )
    }

    fn with_preferred_url(
        wss_urls: Vec<String>,
        preferred_url: Option<String>,
        partial: Arc<PartialPoolCache>,
        addresses: StreamAddressSet,
        shutdown: watch::Receiver<bool>,
        seen_logs: Arc<Mutex<rustc_hash::FxHashMap<(B256, u64), u64>>>,
    ) -> Self {
        Self {
            wss_urls,
            sticky_url: Arc::new(Mutex::new(preferred_url)),
            subscription_cooldowns: Arc::new(Mutex::new(rustc_hash::FxHashMap::default())),
            seen_logs,
            partial,
            addresses,
            shutdown,
        }
    }

    pub async fn run(mut self) {
        let mut addr_rx = self.addresses.watch();
        let mut current_addrs = addr_rx.borrow().clone();
        let mut backoff_ms = BASE_RECONNECT_DELAY_MS;
        let mut last_no_endpoint_warn_at = 0u64;

        loop {
            if *self.shutdown.borrow() {
                break;
            }

            if current_addrs.is_empty() {
                backoff_ms = BASE_RECONNECT_DELAY_MS;
            } else {
                let sticky = self.sticky_url.lock().clone();
                // Order under the lock; only clone candidate URLs (not the cooldown map).
                let selection = {
                    let cooldowns = self.subscription_cooldowns.lock();
                    let now = now_ms();
                    let sticky_eligible = sticky
                        .as_deref()
                        .filter(|url| cooldowns.get(*url).is_none_or(|until| *until <= now));
                    match sticky_eligible {
                        Some(url) if self.wss_urls.iter().any(|u| u == url) => {
                            WssUrlPick::Sticky(url.to_string())
                        }
                        sticky_for_order => WssUrlPick::Probe(ordered_wss_urls(
                            &self.wss_urls,
                            sticky_for_order,
                            &cooldowns,
                            now,
                        )),
                    }
                };
                match selection.resolve().await {
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
                                warn!("wss subscribe error: host={} err={e}", rpc_host_label(&url));
                                self.cool_down_subscription_from_error(&url, &e);
                                *self.sticky_url.lock() = None;
                            }
                            Ok(SubscriptionExit::AddressChange) => {
                                *self.sticky_url.lock() = Some(url);
                            }
                            Ok(SubscriptionExit::Unhealthy) => {
                                self.cool_down_subscription_endpoint(&url);
                                *self.sticky_url.lock() = None;
                            }
                        }
                    }
                    None => {
                        let now = now_ms();
                        if now.saturating_sub(last_no_endpoint_warn_at) >= 30_000 {
                            warn!("wss: no endpoint available — check WSS_URL / POLYGON_WSS_URLS");
                            last_no_endpoint_warn_at = now;
                        }
                    }
                }
                current_addrs.clone_from(&addr_rx.borrow());
            }

            tokio::select! {
                changed = self.shutdown.changed() => {
                    if changed.is_err() || *self.shutdown.borrow() { break; }
                }
                changed = addr_rx.changed() => {
                    if changed.is_err() { break; }
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
    ) -> anyhow::Result<SubscriptionExit> {
        let ws = WsConnect::new(wss_url.to_string());
        // Read-only subscriptions: no RecommendedFillers.
        // Bound connect — hung TCP handshakes blocked reconnect forever (no timeout).
        const WSS_CONNECT_TIMEOUT: Duration = Duration::from_secs(8);
        let provider = match timeout(
            WSS_CONNECT_TIMEOUT,
            ProviderBuilder::new()
                .disable_recommended_fillers()
                .connect_ws(ws),
        )
        .await
        {
            Ok(Ok(p)) => p,
            Ok(Err(e)) => return Err(anyhow::anyhow!("WSS connect failed: {e}")),
            Err(_) => {
                return Err(anyhow::anyhow!(
                    "WSS connect timed out after {}ms",
                    WSS_CONNECT_TIMEOUT.as_millis()
                ));
            }
        };

        // Topic-only Sync|Swap: address-filtered subs stayed armed but delivered
        // ~0 logs because predicted top-N pools were deep-but-quiet. Topic-wide
        // feeds ~10 logs/s on Polygon; client decode filters unknowns.
        let filter = Filter::new().event_signature(vec![V2_SYNC_TOPIC, V3_SWAP_TOPIC]);
        let sub = match timeout(Duration::from_secs(8), provider.subscribe_logs(&filter)).await {
            Ok(Ok(sub)) => sub,
            Ok(Err(e)) => return Err(anyhow::anyhow!("subscribe_logs failed: {e}")),
            Err(_elapsed) => return Err(anyhow::anyhow!("subscribe_logs timed out")),
        };

        let (log_tx, mut log_rx) = mpsc::channel(1024);
        let mut readers = JoinSet::new();
        let mut sub = sub;
        readers.spawn(async move {
            while let Ok(log) = sub.recv().await {
                if log_tx.send(log).await.is_err() {
                    break;
                }
            }
        });
        info!(
            "wss armed: host={} mode=topic_sync_swap interest_pools={}",
            rpc_host_label(wss_url),
            addresses.len()
        );

        // Topic filter does not depend on address membership — ignore LF target
        // churn for resubscribe (only consume watch updates so has_changed stays
        // fresh). Prior address-chunk resubscribes starved log delivery.
        let mut last_log_at = Instant::now();
        let mut silence_forced = false;
        let armed_at = Instant::now();
        let mut ping_timer = interval_at(
            TokioInstant::now() + WSS_PING_INTERVAL,
            WSS_PING_INTERVAL,
        );
        ping_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(SubscriptionExit::AddressChange);
                    }
                }
                changed = addr_rx.changed() => {
                    if changed.is_err() {
                        return Ok(SubscriptionExit::AddressChange);
                    }
                    let _ = addr_rx.borrow_and_update();
                }
                maybe_log = log_rx.recv() => {
                    let Some(log) = maybe_log else {
                        warn!("wss disconnected: host={} — reconnecting", rpc_host_label(wss_url));
                        return Ok(SubscriptionExit::Unhealthy);
                    };
                    last_log_at = Instant::now();
                    silence_forced = false;
                    self.handle_log(&log);
                }
                _ = ping_timer.tick() => {
                    if timeout(WSS_PROBE_TIMEOUT, provider.get_block_number())
                        .await
                        .ok()
                        .and_then(|r| r.ok())
                        .is_none()
                    {
                        warn!("wss ping failed: host={} — reconnecting", rpc_host_label(wss_url));
                        return Ok(SubscriptionExit::Unhealthy);
                    }
                    if !silence_forced
                        && armed_at.elapsed() >= WSS_LOG_SILENCE_FORCE
                        && last_log_at.elapsed() >= WSS_LOG_SILENCE_FORCE
                    {
                        silence_forced = true;
                        self.addresses.request_force_replace();
                        warn!(
                            "wss silence: host={} idle_s={} — forcing stream target reselect",
                            rpc_host_label(wss_url),
                            WSS_LOG_SILENCE_FORCE.as_secs()
                        );
                    }
                    if last_log_at.elapsed() >= STREAM_IDLE_TIMEOUT {
                        warn!(
                            "wss idle timeout: host={} — reconnecting",
                            rpc_host_label(wss_url)
                        );
                        return Ok(SubscriptionExit::Unhealthy);
                    }
                }
            }
        }
    }

    fn handle_log(&self, log: &alloy::rpc::types::Log) {
        let pool = log.address();
        let topic0 = log.topics().first().copied().unwrap_or(B256::ZERO);
        let data = log.data().data.as_ref();
        let ts = now_ms();
        if let (Some(tx_hash), Some(log_index)) = (log.transaction_hash, log.log_index) {
            let mut seen = self.seen_logs.lock();
            seen.retain(|_, seen_at| ts.saturating_sub(*seen_at) <= WSS_DEDUP_TTL_MS);
            if seen.len() >= WSS_DEDUP_CAP {
                seen.clear();
            }
            if seen.insert((tx_hash, log_index), ts).is_some() {
                return;
            }
        }
        static RAW: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        static MISS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        if crate::log::every_n(&RAW, 500) {
            let raw = RAW.load(std::sync::atomic::Ordering::Relaxed);
            crate::debug!(
                "wss raw: n={raw} pool={pool} topic0={topic0} data_len={}",
                data.len()
            );
        }
        // Wake on cycle-covered (universe) or interest. Interest-only still
        // early-returns when active_candidates=0; universe-only missed all
        // trading venues (live: wake_true=0 while interest saw Sync/Swap).
        let in_interest = {
            let addrs = self.addresses.read();
            addrs.binary_search(&pool).is_ok()
        };
        let in_universe = self.partial.in_stream_universe(&pool);
        let wake_hf = in_interest || in_universe;
        // Rate-limited wake-gate funnel (live: ~95% wake_hf=false → active_candidates=0).
        {
            static WAKE_TRUE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            static WAKE_FALSE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            static INTEREST: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            static UNIVERSE_ONLY: std::sync::atomic::AtomicU64 =
                std::sync::atomic::AtomicU64::new(0);
            if wake_hf {
                WAKE_TRUE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if in_interest {
                    INTEREST.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                } else {
                    UNIVERSE_ONLY.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            } else {
                WAKE_FALSE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            let total = WAKE_TRUE.load(std::sync::atomic::Ordering::Relaxed)
                + WAKE_FALSE.load(std::sync::atomic::Ordering::Relaxed);
            if total == 1 || total.is_multiple_of(500) {
                crate::debug!(
                    "wss wake gate: n={total} wake_true={} wake_false={} in_interest={} universe_only={} sample_pool={pool} in_interest={in_interest} in_universe={in_universe}",
                    WAKE_TRUE.load(std::sync::atomic::Ordering::Relaxed),
                    WAKE_FALSE.load(std::sync::atomic::Ordering::Relaxed),
                    INTEREST.load(std::sync::atomic::Ordering::Relaxed),
                    UNIVERSE_ONLY.load(std::sync::atomic::Ordering::Relaxed),
                );
            }
        }
        if !self
            .partial
            .apply_log_notify(pool, topic0, data, ts, wake_hf)
        {
            if crate::log::every_n(&MISS, 100) {
                let miss = MISS.load(std::sync::atomic::Ordering::Relaxed);
                crate::warn!(
                    "wss decode miss: n={miss} pool={pool} topic0={topic0} data_len={} — check topic/abi coverage",
                    data.len()
                );
            }
            return;
        }
        if !wake_hf && topic0 == V3_SWAP_TOPIC {
            // Uni V3 venue not in arena yet — LF hot-refreshes same tick.
            // Skip V2 Sync: QS/Uni V2 are disabled in env and dominate topic spam.
            self.partial.note_observed_live(pool);
        }
    }

    fn cool_down_subscription_endpoint(&self, url: &str) {
        self.cool_down_subscription_endpoint_for(url, WSS_SUBSCRIPTION_FAILURE_COOLDOWN_MS);
    }

    fn cool_down_subscription_endpoint_for(&self, url: &str, cooldown_ms: u64) {
        let until_ms = now_ms().saturating_add(cooldown_ms);
        self.subscription_cooldowns
            .lock()
            .insert(url.to_string(), until_ms);
        warn!(
            "wss cooldown: host={} cooldown_ms={cooldown_ms}",
            rpc_host_label(url)
        );
    }

    fn cool_down_subscription_from_error(&self, url: &str, err: &impl std::fmt::Display) {
        let cooldown = if crate::services::execution::rpc_errors::is_rpc_rate_limited(err) {
            WSS_RATE_LIMIT_COOLDOWN_MS
        } else {
            WSS_SUBSCRIPTION_FAILURE_COOLDOWN_MS
        };
        self.cool_down_subscription_endpoint_for(url, cooldown);
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
    let fanout = config.pipeline.stream_rpc_fanout;
    Some(tokio::spawn(async move {
        let ranked = probe_wss_urls_ranked(&wss_urls).await;
        let preferred = if ranked.is_empty() {
            wss_urls.clone()
        } else {
            distinct_wss_providers(ranked.into_iter().map(|(url, _)| url).collect())
        };
        let fanout = fanout.min(preferred.len());
        let seen_logs = Arc::new(Mutex::new(rustc_hash::FxHashMap::default()));
        let mut feeds = JoinSet::new();
        for url in preferred.into_iter().take(fanout) {
            let feed = PoolLogFeed::with_preferred_url(
                wss_urls.clone(),
                Some(url),
                Arc::clone(&partial),
                addresses.clone(),
                shutdown.clone(),
                Arc::clone(&seen_logs),
            );
            feeds.spawn(async move { feed.run().await });
        }
        while feeds.join_next().await.is_some() {}
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

fn ordered_wss_urls(
    urls: &[String],
    sticky: Option<&str>,
    cooldowns: &rustc_hash::FxHashMap<String, u64>,
    now: u64,
) -> Vec<String> {
    let has_eligible = urls
        .iter()
        .any(|url| cooldowns.get(url).is_none_or(|until| *until <= now));
    let is_candidate =
        |url: &String| !has_eligible || cooldowns.get(url).is_none_or(|until| *until <= now);
    let mut ordered = Vec::with_capacity(urls.len());
    let mut seen = rustc_hash::FxHashSet::default();
    if let Some(url) = sticky.filter(|s| urls.iter().any(|u| u == *s && is_candidate(u))) {
        ordered.push(url.to_string());
        seen.insert(url);
    }
    for url in urls.iter().filter(|url| is_candidate(url)) {
        if seen.insert(url.as_str()) {
            ordered.push(url.clone());
        }
    }
    ordered
}

fn distinct_wss_providers(urls: Vec<String>) -> Vec<String> {
    let mut providers = rustc_hash::FxHashSet::default();
    let mut distinct = Vec::with_capacity(urls.len());
    let mut duplicates = Vec::new();
    for url in urls {
        if providers.insert(wss_provider_label(&url)) {
            distinct.push(url);
        } else {
            duplicates.push(url);
        }
    }
    distinct.extend(duplicates);
    distinct
}

fn wss_provider_label(url: &str) -> String {
    let host = rpc_host_label(url).to_ascii_lowercase();
    if host.contains("drpc") {
        "drpc".to_string()
    } else if host.contains("publicnode") {
        "publicnode".to_string()
    } else if host.contains("tenderly") {
        "tenderly".to_string()
    } else if host.contains("chainstack") {
        "chainstack".to_string()
    } else if host.contains("quicknode") || host.contains("quiknode") {
        "quicknode".to_string()
    } else {
        host
    }
}

async fn probe_wss_latency(url: &str) -> Option<Duration> {
    let started = Instant::now();
    let ws = WsConnect::new(url.to_string());
    let provider = tokio::time::timeout(
        WSS_PROBE_TIMEOUT,
        ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_ws(ws),
    )
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
    probe_wss_urls_ranked(urls).await.into_iter().next()
}

async fn probe_wss_urls_ranked(urls: &[String]) -> Vec<(String, Duration)> {
    let mut probes = tokio::task::JoinSet::new();
    for url in urls {
        let url = url.clone();
        probes.spawn(async move {
            let latency = probe_wss_latency(&url).await?;
            Some((url, latency))
        });
    }
    let mut ranked = Vec::new();
    while let Some(result) = probes.join_next().await {
        let Ok(Some((url, latency))) = result else {
            continue;
        };
        ranked.push((url, latency));
    }
    ranked.sort_by_key(|(_, latency)| *latency);
    ranked
}

enum WssUrlPick {
    Sticky(String),
    Probe(Vec<String>),
}

impl WssUrlPick {
    async fn resolve(self) -> Option<String> {
        match self {
            Self::Sticky(url) => {
                crate::debug!("wss sticky: host={} probe=skipped", rpc_host_label(&url));
                Some(url)
            }
            Self::Probe(candidates) => {
                if candidates.is_empty() {
                    return None;
                }
                let (url, latency) = probe_wss_urls(&candidates).await?;
                info!(
                    "wss selected: host={} probe_ms={}",
                    rpc_host_label(&url),
                    latency.as_millis()
                );
                Some(url)
            }
        }
    }
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
        let cooldowns = rustc_hash::FxHashMap::default();
        assert_eq!(
            ordered_wss_urls(&urls, Some("wss://c"), &cooldowns, 1_000),
            vec![
                "wss://c".to_string(),
                "wss://a".to_string(),
                "wss://b".to_string()
            ]
        );
    }

    #[test]
    fn distinct_wss_providers_keeps_fastest_per_provider_first() {
        assert_eq!(
            distinct_wss_providers(vec![
                "wss://polygon.drpc.org".to_string(),
                "wss://lb.drpc.live/polygon/key".to_string(),
                "wss://polygon-bor-rpc.publicnode.com".to_string(),
            ]),
            vec![
                "wss://polygon.drpc.org".to_string(),
                "wss://polygon-bor-rpc.publicnode.com".to_string(),
                "wss://lb.drpc.live/polygon/key".to_string(),
            ]
        );
    }

    #[test]
    fn ordered_wss_urls_skips_subscription_cooled_endpoint() {
        let urls = vec!["wss://a".to_string(), "wss://b".to_string()];
        let mut cooldowns = rustc_hash::FxHashMap::default();
        cooldowns.insert("wss://a".to_string(), 2_000);

        assert_eq!(
            ordered_wss_urls(&urls, None, &cooldowns, 1_000),
            vec!["wss://b".to_string()]
        );
    }

    #[test]
    fn ordered_wss_urls_retries_all_endpoints_when_all_are_cooled() {
        let urls = vec!["wss://a".to_string(), "wss://b".to_string()];
        let mut cooldowns = rustc_hash::FxHashMap::default();
        cooldowns.insert("wss://a".to_string(), 2_000);
        cooldowns.insert("wss://b".to_string(), 2_000);

        assert_eq!(ordered_wss_urls(&urls, None, &cooldowns, 1_000), urls);
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
