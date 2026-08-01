use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::config::AppConfig;
use crate::info;
use crate::infra::http::{HttpClientOpts, build_static};
use crate::services::execution::private_submit::PrivateSubmitProbe;
use alloy::network::EthereumWallet;
use alloy::primitives::Address;
use alloy::providers::{DynProvider, Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use arc_swap::ArcSwap;
use parking_lot::{Mutex, RwLock};
use reqwest::Client;
use rustc_hash::{FxHashMap, FxHashSet};

/// Fail-fast ranking — 5s × N URLs blocked startup/periodic rank under hung public RPCs.
const HTTP_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
/// Transient multicall/failover cool-off (was 60s — primary stayed deprioritized after recovery).
const STATE_URL_PENALTY: Duration = Duration::from_secs(20);
/// Longer cool-off when callers mark rate-limit / 429.
const STATE_URL_RATE_LIMIT_PENALTY: Duration = Duration::from_secs(45);

/// Shared RPC endpoints and HTTP client (connection-pooled via reqwest).
#[derive(Clone)]
pub struct RpcPool {
    http: Client,
    state_urls: Arc<ArcSwap<Vec<Arc<str>>>>,
    execution_url: Option<String>,
    private_url: Option<String>,
    require_private_submit: bool,
    http_providers: Arc<Mutex<FxHashMap<String, DynProvider>>>,
    submit_providers: Arc<Mutex<FxHashMap<(String, Address), DynProvider>>>,
    polygon_validated_urls: Arc<Mutex<FxHashSet<String>>>,
    validated_executors: Arc<Mutex<FxHashSet<(String, Address)>>>,
    failed_executors: Arc<Mutex<FxHashSet<(String, Address)>>>,
    state_url_penalties: Arc<RwLock<FxHashMap<String, Instant>>>,
    state_probe_inflight: Arc<AtomicBool>,
    private_submit_probe: Arc<RwLock<Option<PrivateSubmitProbe>>>,
    bloxroute_auth_verified: Arc<RwLock<Option<bool>>>,
}

impl std::fmt::Debug for RpcPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcPool")
            .field("state_url_count", &self.state_urls.load().len())
            .field("execution_configured", &self.execution_url.is_some())
            .field("private_configured", &self.private_url.is_some())
            .field("require_private_submit", &self.require_private_submit)
            .field(
                "cached_http_providers",
                &self.http_providers.try_lock().map_or(0, |g| g.len()),
            )
            .field(
                "cached_submit_providers",
                &self.submit_providers.try_lock().map_or(0, |g| g.len()),
            )
            .finish_non_exhaustive()
    }
}

impl RpcPool {
    #[must_use]
    pub fn from_config(config: &AppConfig) -> Self {
        let timeout = Duration::from_millis(config.rpc.request_timeout_ms.max(1));
        let http = build_static(
            HttpClientOpts {
                timeout,
                // Concurrent multicall + flash + hydrate share hosts; 16 idled under
                // LF+HF burst and forced reconnects (live multi-URL state pool).
                pool_max_idle_per_host: 32,
                max_redirects: 0,
            },
            "rpc pool",
        );

        let state_urls: Vec<Arc<str>> = config
            .state_read_urls()
            .into_iter()
            .map(Arc::<str>::from)
            .collect();

        Self {
            http,
            state_urls: Arc::new(ArcSwap::from_pointee(state_urls)),
            execution_url: if config.rpc.execution_rpc_url.is_empty() {
                None
            } else {
                Some(config.rpc.execution_rpc_url.clone())
            },
            private_url: config.rpc.private_rpc_url.clone(),
            require_private_submit: config.execution.require_private_submit,
            http_providers: Arc::new(Mutex::new(FxHashMap::default())),
            submit_providers: Arc::new(Mutex::new(FxHashMap::default())),
            polygon_validated_urls: Arc::new(Mutex::new(FxHashSet::default())),
            validated_executors: Arc::new(Mutex::new(FxHashSet::default())),
            failed_executors: Arc::new(Mutex::new(FxHashSet::default())),
            state_url_penalties: Arc::new(RwLock::new(FxHashMap::default())),
            state_probe_inflight: Arc::new(AtomicBool::new(false)),
            private_submit_probe: Arc::new(RwLock::new(None)),
            bloxroute_auth_verified: Arc::new(RwLock::new(None)),
        }
    }

    fn state_urls_ordered_slice(&self) -> Vec<Arc<str>> {
        let urls = self.state_urls.load();
        let now = Instant::now();
        // Opportunistic prune: only take the write lock when something expired
        // (hot path is read-only; long runs still cannot accumulate dead cool-offs).
        {
            let needs_prune = {
                let penalties = self.state_url_penalties.read();
                !penalties.is_empty() && penalties.values().any(|until| *until <= now)
            };
            if needs_prune {
                self.state_url_penalties
                    .write()
                    .retain(|_, until| *until > now);
            }
        }
        let penalties = self.state_url_penalties.read();
        let mut healthy = Vec::with_capacity(urls.len());
        let mut budget_tight = Vec::new();
        let mut penalized = Vec::new();
        for url in urls.iter() {
            if penalties
                .get(url.as_ref())
                .is_none_or(|until| now >= *until)
            {
                // Demote hosts nearly out of tokens so fallback can absorb load.
                if crate::infra::rpc_budget::approx_tokens(url) >= 1.0 {
                    healthy.push(url.clone());
                } else {
                    budget_tight.push(url.clone());
                }
            } else {
                penalized.push(url.clone());
            }
        }
        healthy.sort_by(|a, b| {
            crate::infra::rpc_budget::status(b)
                .0
                .total_cmp(&crate::infra::rpc_budget::status(a).0)
        });
        healthy.extend(budget_tight);
        healthy.extend(penalized);
        healthy
    }

    pub fn state_url(&self) -> Option<String> {
        // Fast path for common single-URL case — avoids allocating the full partition vecs.
        {
            let urls = self.state_urls.load();
            if urls.len() <= 1 {
                return urls.first().map(ToString::to_string);
            }
        }
        // Multi-URL: same budget/penalty order as [`Self::state_url_candidates`].
        // Prior path only skipped penalties, so a free-tier primary at 0 tokens kept
        // winning `connect_state()` while candidates had already demoted it (live 429s).
        self.state_urls_ordered_slice()
            .first()
            .map(ToString::to_string)
    }

    #[must_use]
    pub fn state_url_candidates(&self) -> Vec<Arc<str>> {
        self.state_urls_ordered_slice()
    }

    /// Move a failing endpoint to the back of the rotation (transient cool-off).
    pub fn deprioritize_state_url(&self, url: &str) {
        self.deprioritize_state_url_for(url, STATE_URL_PENALTY, "error");
    }

    /// Longer cool-off for rate-limited / 429 endpoints (avoid hammering).
    pub fn deprioritize_state_url_rate_limited(&self, url: &str) {
        crate::infra::rpc_budget::note_rate_limited(url);
        self.deprioritize_state_url_for(url, STATE_URL_RATE_LIMIT_PENALTY, "rate_limit");
    }

    fn deprioritize_state_url_for(&self, url: &str, penalty: Duration, reason: &str) {
        let until = Instant::now() + penalty;
        {
            let mut map = self.state_url_penalties.write();
            // Never shorten an existing longer cool-off.
            if map.get(url).is_some_and(|exp| *exp > until) {
                return;
            }
            map.insert(url.to_string(), until);
        }
        crate::debug!(
            "state RPC deprioritized ({}) for {}s reason={reason}",
            rpc_host_label(url),
            penalty.as_secs()
        );
    }

    /// Clear cool-off after a successful probe / healthy multicall.
    pub fn clear_state_url_penalty(&self, url: &str) {
        self.state_url_penalties.write().remove(url);
    }

    /// Re-probe state URLs periodically so degraded endpoints are de-prioritized
    /// and recovered ones regain their position, without operator intervention.
    pub fn spawn_periodic_probe(
        self: &Arc<Self>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
        interval: std::time::Duration,
    ) {
        let pool = Arc::clone(self);
        tokio::spawn(async move {
            let mut timer = tokio::time::interval(interval);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // Startup already ranks before first LF — don't double-probe immediately.
            timer.tick().await;
            loop {
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { break; }
                    }
                    _ = timer.tick() => {
                        pool.probe_and_rank_state_urls().await;
                    }
                }
            }
        });
    }

    /// Probe state HTTP endpoints in parallel and reorder by latency (fastest first).
    pub async fn probe_and_rank_state_urls(&self) {
        struct ProbeInflightGuard<'a>(&'a AtomicBool);
        impl Drop for ProbeInflightGuard<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }

        if self
            .state_probe_inflight
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let _guard = ProbeInflightGuard(&self.state_probe_inflight);
        self.probe_and_rank_state_urls_inner().await;
    }

    async fn probe_and_rank_state_urls_inner(&self) {
        let urls = self.state_urls.load();
        if urls.len() <= 1 {
            return;
        }
        let pool = self.clone();
        let mut probes = tokio::task::JoinSet::new();
        for url in urls.iter().map(Arc::clone) {
            let pool = pool.clone();
            probes.spawn(async move {
                let latency = pool.probe_http_latency(&url).await;
                (url, latency)
            });
        }
        let mut ranked: Vec<(Arc<str>, Duration)> = Vec::new();
        let mut failed: Vec<Arc<str>> = Vec::new();
        while let Some(result) = probes.join_next().await {
            let (url, latency) = match result {
                Ok(result) => result,
                Err(e) => {
                    crate::warn!(
                        "state RPC probe task failed ({e}) — preserving configured endpoint"
                    );
                    continue;
                }
            };
            if let Some(latency) = latency {
                ranked.push((url, latency));
            } else {
                crate::warn!("state RPC probe failed ({})", rpc_host_label(&url));
                failed.push(url);
            }
        }
        if ranked.is_empty() {
            crate::warn!("state RPC probe found no healthy endpoints — keeping configured order");
            return;
        }
        ranked.sort_by(|(a, la), (b, lb)| {
            crate::infra::rpc_budget::status(b)
                .0
                .total_cmp(&crate::infra::rpc_budget::status(a).0)
                .then_with(|| la.cmp(lb))
        });
        // Drop cool-offs for URLs that just answered — otherwise deprioritize(60s)
        // overrode the new rank and `state_url()` kept serving a slower peer
        // (live: fast primary stayed skipped after transient multicall fail).
        {
            let mut penalties = self.state_url_penalties.write();
            for (url, _) in &ranked {
                penalties.remove(url.as_ref());
            }
        }
        let mut ordered: Vec<Arc<str>> = ranked.iter().map(|(url, _)| Arc::clone(url)).collect();
        ordered.extend(failed);
        for url in urls.iter() {
            if !ordered.iter().any(|candidate| candidate == url) {
                ordered.push(url.clone());
            }
        }
        let summary: Vec<String> = ranked
            .iter()
            .map(|(url, latency)| {
                let (tokens, rps) = crate::infra::rpc_budget::status(url);
                format!(
                    "{}={}ms tokens={tokens:.1} rps={rps:.1}",
                    rpc_host_label(url),
                    latency.as_millis()
                )
            })
            .collect();
        info!("state RPC order: {}", summary.join(", "));
        self.state_urls.store(Arc::new(ordered));
    }

    async fn probe_http_latency(&self, url: &str) -> Option<Duration> {
        let started = Instant::now();
        let provider = self.cached_http_provider(url).ok()?;
        let need_chain = !self.polygon_validated_urls.lock().contains(url);
        if need_chain {
            // Parallel block + chain-id — sequential 2×2s was worst-case 4s per URL.
            let (block_res, chain_res) = tokio::join!(
                tokio::time::timeout(HTTP_PROBE_TIMEOUT, provider.get_block_number()),
                tokio::time::timeout(HTTP_PROBE_TIMEOUT, provider.get_chain_id()),
            );
            block_res.ok().and_then(|r| r.ok())?;
            let chain_id = chain_res.ok().and_then(|r| r.ok())?;
            if chain_id != crate::core::constants::POLYGON_CHAIN_ID {
                return None;
            }
            self.polygon_validated_urls.lock().insert(url.to_string());
        } else {
            let block_res =
                tokio::time::timeout(HTTP_PROBE_TIMEOUT, provider.get_block_number()).await;
            block_res.ok().and_then(|r| r.ok())?;
        }
        Some(started.elapsed())
    }

    #[must_use]
    pub fn execution_url(&self) -> Option<&str> {
        self.execution_url.as_deref()
    }

    #[must_use]
    pub fn private_url(&self) -> Option<&str> {
        self.private_url.as_deref()
    }

    #[must_use]
    pub fn require_private_submit(&self) -> bool {
        self.require_private_submit
    }

    pub fn record_private_submit_probe(&self, probe: PrivateSubmitProbe) {
        *self.private_submit_probe.write() = Some(probe);
    }

    #[must_use]
    pub fn private_submit_probe(&self) -> Option<PrivateSubmitProbe> {
        self.private_submit_probe.read().clone()
    }

    pub fn record_bloxroute_auth_probe(&self, verified: bool) {
        *self.bloxroute_auth_verified.write() = Some(verified);
    }

    #[must_use]
    pub fn bloxroute_auth_verified(&self) -> Option<bool> {
        *self.bloxroute_auth_verified.read()
    }

    /// Shared connection pool for JSON-RPC and other HTTP (e.g. Pyth Hermes).
    #[must_use]
    pub fn http(&self) -> &Client {
        &self.http
    }

    /// URL used for read-only execution RPC (dry-run simulation).
    pub fn simulation_url(&self) -> anyhow::Result<String> {
        self.execution_url
            .clone()
            .or_else(|| self.state_url())
            .ok_or_else(|| anyhow::anyhow!("no execution or state RPC configured"))
    }

    /// URL used for live transaction submission (prefers private MEV-protected endpoint).
    pub fn submit_url(&self) -> anyhow::Result<(String, bool)> {
        if let Some(url) = &self.private_url {
            return Ok((url.clone(), true));
        }
        if self.require_private_submit {
            anyhow::bail!("REQUIRE_PRIVATE_SUBMIT is set but PRIVATE_RPC_URL is not configured");
        }
        self.simulation_url().map(|url| (url, false))
    }

    /// JSON-RPC endpoint for nonce/chain-id during live submit.
    /// bloXroute BDN only accepts `polygon_private_tx`; it is not a full node.
    fn wallet_provider_url_for_submit(&self) -> anyhow::Result<String> {
        if std::env::var("BLOXROUTE_AUTH_HEADER")
            .ok()
            .is_some_and(|s| !s.is_empty())
        {
            return self.simulation_url();
        }
        self.submit_url().map(|(url, _)| url)
    }

    fn cached_http_provider(&self, url: &str) -> anyhow::Result<DynProvider> {
        // Double-checked: build outside the map lock so concurrent LF/HF
        // `connect_state` calls do not serialize on ProviderBuilder.
        if let Some(provider) = self.http_providers.lock().get(url) {
            return Ok(provider.clone());
        }
        let endpoint = rpc_host_label(url);
        // Read-only: skip RecommendedFillers (gas/nonce/chain-id) — eth_call only.
        // https://alloy.rs/guides/fillers/
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_reqwest(
                self.http.clone(),
                url.parse()
                    .with_context(|| format!("invalid RPC URL: {endpoint}"))?,
            )
            .erased();
        let mut guard = self.http_providers.lock();
        // Another task may have inserted while we built — reuse theirs.
        if let Some(existing) = guard.get(url) {
            return Ok(existing.clone());
        }
        guard.insert(url.to_string(), provider.clone());
        Ok(provider)
    }

    pub fn connect_state(&self) -> anyhow::Result<DynProvider> {
        let url = self
            .state_url()
            .ok_or_else(|| anyhow::anyhow!("no state RPC configured"))?;
        self.cached_http_provider(&url)
    }

    pub fn connect_state_at(&self, url: &str) -> anyhow::Result<DynProvider> {
        self.cached_http_provider(url)
    }

    pub fn connect_simulation(&self) -> anyhow::Result<DynProvider> {
        let url = self.simulation_url()?;
        self.cached_http_provider(&url)
    }

    async fn validate_polygon_endpoint(
        &self,
        url: &str,
        provider: &DynProvider,
    ) -> anyhow::Result<()> {
        if self.polygon_validated_urls.lock().contains(url) {
            return Ok(());
        }
        let endpoint = rpc_host_label(url);
        let chain_id = provider
            .get_chain_id()
            .await
            .with_context(|| format!("chain-id check failed for RPC endpoint {endpoint}"))?;
        anyhow::ensure!(
            chain_id == crate::core::constants::POLYGON_CHAIN_ID,
            "RPC endpoint {endpoint} is chain {chain_id}, expected Polygon {}",
            crate::core::constants::POLYGON_CHAIN_ID
        );
        self.polygon_validated_urls.lock().insert(url.to_string());
        Ok(())
    }

    pub async fn connect_simulation_checked(
        &self,
        executor: Address,
    ) -> anyhow::Result<DynProvider> {
        let url = self.simulation_url()?;
        let provider = self.cached_http_provider(&url)?;
        self.validate_polygon_endpoint(&url, &provider).await?;

        let key = (url.clone(), executor);
        if self.failed_executors.lock().contains(&key) {
            anyhow::bail!("no executor bytecode at {executor} on Polygon");
        }
        if !self.validated_executors.lock().contains(&key) {
            let code = provider
                .get_code_at(executor)
                .await
                .with_context(|| format!("executor code check failed for {executor}"))?;
            if code.is_empty() {
                if self.failed_executors.lock().insert(key) {
                    crate::warn!(
                        "executor bytecode missing at {executor} on Polygon; dispatch disabled until redeploy + restart"
                    );
                }
                anyhow::bail!("no executor bytecode at {executor} on Polygon");
            }
            self.validated_executors.lock().insert(key);
        }
        Ok(provider)
    }

    pub fn connect_submit(&self, signer: &PrivateKeySigner) -> anyhow::Result<DynProvider> {
        let url = self.wallet_provider_url_for_submit()?;
        let key = (url.clone(), signer.address());
        let mut guard = self.submit_providers.lock();
        if let Some(provider) = guard.get(&key) {
            return Ok(provider.clone());
        }
        let wallet = EthereumWallet::from(signer.clone());
        let endpoint = rpc_host_label(&url);
        // Submit path: RecommendedFillers fill missing chain_id / from; caller sets nonce+fees.
        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect_reqwest(
                self.http.clone(),
                url.parse()
                    .with_context(|| format!("invalid RPC URL: {endpoint}"))?,
            )
            .erased();
        guard.insert(key, provider.clone());
        Ok(provider)
    }

    pub async fn connect_submit_checked(
        &self,
        signer: &PrivateKeySigner,
    ) -> anyhow::Result<DynProvider> {
        let url = self.wallet_provider_url_for_submit()?;
        let provider = self.connect_submit(signer)?;
        self.validate_polygon_endpoint(&url, &provider).await?;
        Ok(provider)
    }
}

/// Host label for logs — omits URL paths that often contain API keys.
#[must_use]
pub fn rpc_host_label(url: &str) -> String {
    url.parse::<reqwest::Url>()
        .ok()
        .and_then(|parsed| parsed.host_str().map(str::to_string))
        .unwrap_or_else(|| "invalid-endpoint".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;

    #[test]
    fn state_urls_exclude_execution_rpc() {
        let mut config = AppConfig::default();
        config.rpc.state_rpc_url = Some("https://state.example".into());
        config.rpc.polygon_rpc_urls = vec!["https://poly.example".into()];
        config.rpc.execution_rpc_url = "https://exec.example".into();
        let pool = RpcPool::from_config(&config);
        let urls = pool.state_url_candidates();
        assert_eq!(urls.len(), 2);
        assert!(
            urls.iter()
                .all(|url| url.as_ref() != "https://exec.example")
        );
    }

    #[test]
    fn state_url_candidates_reuse_endpoint_allocations() {
        let mut config = AppConfig::default();
        config.rpc.state_rpc_url = Some("https://state.example".into());
        let pool = RpcPool::from_config(&config);

        let first = pool.state_url_candidates();
        let second = pool.state_url_candidates();
        assert!(Arc::ptr_eq(&first[0], &second[0]));
    }

    #[test]
    fn rpc_host_label_strips_path() {
        assert_eq!(
            rpc_host_label("https://polygon-mainnet.g.alchemy.com/v2/secret"),
            "polygon-mainnet.g.alchemy.com"
        );
    }

    #[test]
    fn deprioritized_urls_move_to_back() {
        let mut config = AppConfig::default();
        config.rpc.state_rpc_url = Some("https://fast.example".into());
        config.rpc.polygon_rpc_urls = vec!["https://slow.example".into()];
        let pool = RpcPool::from_config(&config);
        pool.deprioritize_state_url("https://fast.example");
        let ordered = pool.state_url_candidates();
        assert_eq!(ordered[0].as_ref(), "https://slow.example");
        assert_eq!(ordered[1].as_ref(), "https://fast.example");
    }

    #[test]
    fn clear_penalty_restores_primary() {
        let mut config = AppConfig::default();
        config.rpc.state_rpc_url = Some("https://fast.example".into());
        config.rpc.polygon_rpc_urls = vec!["https://slow.example".into()];
        let pool = RpcPool::from_config(&config);
        pool.deprioritize_state_url("https://fast.example");
        assert_eq!(pool.state_url().as_deref(), Some("https://slow.example"));
        pool.clear_state_url_penalty("https://fast.example");
        assert_eq!(pool.state_url().as_deref(), Some("https://fast.example"));
    }

    #[test]
    fn state_url_matches_candidates_primary() {
        let mut config = AppConfig::default();
        config.rpc.state_rpc_url = Some("https://fast.example".into());
        config.rpc.polygon_rpc_urls = vec!["https://slow.example".into()];
        let pool = RpcPool::from_config(&config);
        pool.deprioritize_state_url("https://fast.example");
        let primary = pool.state_url().expect("primary");
        let candidates = pool.state_url_candidates();
        assert_eq!(primary.as_str(), candidates[0].as_ref());
        assert_eq!(primary.as_str(), "https://slow.example");
    }

    #[test]
    fn expired_penalty_is_pruned_on_order() {
        let mut config = AppConfig::default();
        config.rpc.state_rpc_url = Some("https://a.example".into());
        config.rpc.polygon_rpc_urls = vec!["https://b.example".into()];
        let pool = RpcPool::from_config(&config);
        pool.state_url_penalties.write().insert(
            "https://a.example".into(),
            Instant::now() - Duration::from_secs(1),
        );
        let _ = pool.state_url_candidates();
        assert!(
            !pool
                .state_url_penalties
                .read()
                .contains_key("https://a.example"),
            "expired cool-off must be dropped"
        );
        assert_eq!(pool.state_url().as_deref(), Some("https://a.example"));
    }

    #[test]
    fn rate_limit_penalty_not_shortened_by_generic() {
        let mut config = AppConfig::default();
        config.rpc.state_rpc_url = Some("https://a.example".into());
        config.rpc.polygon_rpc_urls = vec!["https://b.example".into()];
        let pool = RpcPool::from_config(&config);
        pool.deprioritize_state_url_rate_limited("https://a.example");
        let until_rl = *pool
            .state_url_penalties
            .read()
            .get("https://a.example")
            .expect("rl penalty");
        pool.deprioritize_state_url("https://a.example");
        let until_after = *pool
            .state_url_penalties
            .read()
            .get("https://a.example")
            .expect("still penalized");
        assert!(
            until_after >= until_rl,
            "generic deprioritize must not shorten rate-limit cool-off"
        );
    }

    #[tokio::test]
    async fn concurrent_state_probe_is_coalesced() {
        let mut config = AppConfig::default();
        config.rpc.state_rpc_url = Some("https://fast.example".into());
        config.rpc.polygon_rpc_urls = vec!["https://slow.example".into()];
        let pool = RpcPool::from_config(&config);
        pool.state_probe_inflight.store(true, Ordering::Release);

        pool.probe_and_rank_state_urls().await;

        assert!(pool.state_probe_inflight.load(Ordering::Acquire));
    }

    #[test]
    fn wallet_provider_url_prefers_execution_rpc_when_bloxroute_auth_set() {
        let mut config = AppConfig::default();
        config.rpc.execution_rpc_url = "https://exec.example".into();
        config.rpc.private_rpc_url = Some("https://api.blxrbdn.com".into());
        let pool = RpcPool::from_config(&config);
        // SAFETY: test-only env mutation; no concurrent tests touch this var.
        unsafe {
            std::env::set_var("BLOXROUTE_AUTH_HEADER", "test-auth");
        }
        assert_eq!(
            pool.wallet_provider_url_for_submit()
                .expect("submit URL should be valid"),
            "https://exec.example"
        );
        unsafe {
            std::env::remove_var("BLOXROUTE_AUTH_HEADER");
        }
    }
}
