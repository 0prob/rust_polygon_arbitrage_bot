use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::config::AppConfig;
use crate::info;
use crate::infra::http::{HttpClientOpts, build_static};
use alloy::network::EthereumWallet;
use alloy::primitives::Address;
use alloy::providers::{DynProvider, Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use parking_lot::{Mutex, RwLock};
use reqwest::Client;
use rustc_hash::{FxHashMap, FxHashSet};

const HTTP_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const STATE_URL_PENALTY: Duration = Duration::from_secs(60);

/// Shared RPC endpoints and HTTP client (connection-pooled via reqwest).
#[derive(Clone)]
pub struct RpcPool {
    http: Client,
    state_urls: Arc<RwLock<Vec<String>>>,
    execution_url: Option<String>,
    private_url: Option<String>,
    require_private_submit: bool,
    http_providers: Arc<Mutex<FxHashMap<String, DynProvider>>>,
    submit_providers: Arc<Mutex<FxHashMap<(String, Address), DynProvider>>>,
    polygon_validated_urls: Arc<Mutex<FxHashSet<String>>>,
    validated_executors: Arc<Mutex<FxHashSet<(String, Address)>>>,
    state_url_penalties: Arc<RwLock<FxHashMap<String, Instant>>>,
}

impl std::fmt::Debug for RpcPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcPool")
            .field("state_urls", &*self.state_urls.read())
            .field("execution_url", &self.execution_url)
            .field("private_url", &self.private_url)
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
                pool_max_idle_per_host: 16,
                max_redirects: 0,
            },
            "rpc pool",
        );

        let mut state_urls = Vec::new();
        if let Some(url) = config
            .rpc
            .state_rpc_url
            .as_deref()
            .filter(|u| !u.is_empty())
        {
            state_urls.push(url.to_string());
        }
        for url in &config.rpc.polygon_rpc_urls {
            if !url.is_empty() && !state_urls.contains(url) {
                state_urls.push(url.clone());
            }
        }

        Self {
            http,
            state_urls: Arc::new(RwLock::new(state_urls)),
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
            state_url_penalties: Arc::new(RwLock::new(FxHashMap::default())),
        }
    }

    fn state_urls_ordered_slice(&self) -> Vec<String> {
        let urls = self.state_urls.read().clone();
        let penalties = self.state_url_penalties.read();
        let now = Instant::now();
        let (healthy, penalized): (Vec<_>, Vec<_>) = urls
            .into_iter()
            .partition(|url| penalties.get(url).is_none_or(|until| now >= *until));
        healthy.into_iter().chain(penalized).collect()
    }

    pub fn state_url(&self) -> Option<String> {
        // Fast path for common single-URL case — avoids allocating the full partition vecs.
        let urls = self.state_urls.read();
        if urls.len() <= 1 {
            return urls.first().cloned();
        }
        let penalties = self.state_url_penalties.read();
        let now = Instant::now();
        urls.iter().find(|url| {
            penalties.get(url.as_str()).is_none_or(|until| now >= *until)
        }).or_else(|| urls.first()).cloned()
    }

    #[must_use]
    pub fn state_url_candidates(&self) -> Vec<String> {
        self.state_urls_ordered_slice()
    }

    /// Move a rate-limited or failing endpoint to the back of the rotation.
    pub fn deprioritize_state_url(&self, url: &str) {
        self.state_url_penalties
            .write()
            .insert(url.to_string(), Instant::now() + STATE_URL_PENALTY);
        crate::debug!(
            "state RPC deprioritized ({}) for {}s",
            rpc_host_label(url),
            STATE_URL_PENALTY.as_secs()
        );
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
            loop {
                tokio::select! {
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() { break; }
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
        let urls = self.state_urls.read().clone();
        if urls.len() <= 1 {
            return;
        }
        let pool = self.clone();
        let mut probes = tokio::task::JoinSet::new();
        for url in urls.iter().cloned() {
            let pool = pool.clone();
            probes.spawn(async move {
                let latency = pool.probe_http_latency(&url).await;
                (url, latency)
            });
        }
        let mut ranked: Vec<(String, Duration)> = Vec::new();
        let mut failed: Vec<String> = Vec::new();
        while let Some(result) = probes.join_next().await {
            let Ok((url, latency)) = result else {
                continue;
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
        ranked.sort_by_key(|(_, latency)| *latency);
        let mut ordered: Vec<String> = ranked.iter().map(|(url, _)| url.clone()).collect();
        ordered.extend(failed);
        let summary: Vec<String> = ranked
            .iter()
            .map(|(url, latency)| format!("{}={}ms", rpc_host_label(url), latency.as_millis()))
            .collect();
        info!("state RPC order: {}", summary.join(", "));
        *self.state_urls.write() = ordered;
    }

    async fn probe_http_latency(&self, url: &str) -> Option<Duration> {
        let started = Instant::now();
        let provider = self.cached_http_provider(url).ok()?;
        let (block_res, chain_res) = tokio::join!(
            tokio::time::timeout(HTTP_PROBE_TIMEOUT, provider.get_block_number()),
            tokio::time::timeout(HTTP_PROBE_TIMEOUT, provider.get_chain_id()),
        );
        let _block = block_res.ok().and_then(|r| r.ok());
        let chain_id = chain_res.ok().and_then(|r| r.ok())?;
        if chain_id != crate::core::constants::POLYGON_CHAIN_ID {
            return None;
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
        let mut guard = self.http_providers.lock();
        if let Some(provider) = guard.get(url) {
            return Ok(provider.clone());
        }
        let provider = ProviderBuilder::new()
            .connect_reqwest(
                self.http.clone(),
                url.parse()
                    .with_context(|| format!("invalid RPC URL: {url}"))?,
            )
            .erased();
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
        let chain_id = provider
            .get_chain_id()
            .await
            .with_context(|| format!("chain-id check failed for RPC endpoint {url}"))?;
        anyhow::ensure!(
            chain_id == crate::core::constants::POLYGON_CHAIN_ID,
            "RPC endpoint {url} is chain {chain_id}, expected Polygon {}",
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
        let needs_chain = !self.polygon_validated_urls.lock().contains(&url);
        let needs_exec = !self.validated_executors.lock().contains(&key);
        if needs_chain || needs_exec {
            let (chain_res, code_res) = tokio::join!(
                async {
                    if needs_chain {
                        Some(provider.get_chain_id().await)
                    } else {
                        None
                    }
                },
                async {
                    if needs_exec {
                        Some(provider.get_code_at(executor).await)
                    } else {
                        None
                    }
                },
            );
            if let Some(chain) = chain_res {
                let chain_id = chain
                    .with_context(|| format!("chain-id check failed for RPC endpoint {url}"))?;
                anyhow::ensure!(
                    chain_id == crate::core::constants::POLYGON_CHAIN_ID,
                    "RPC endpoint {url} is chain {chain_id}, expected Polygon {}",
                    crate::core::constants::POLYGON_CHAIN_ID
                );
                self.polygon_validated_urls.lock().insert(url.to_string());
            }
            if let Some(code) = code_res {
                let code =
                    code.with_context(|| format!("executor code check failed for {executor}"))?;
                anyhow::ensure!(
                    !code.is_empty(),
                    "no executor bytecode at {executor} on Polygon"
                );
                self.validated_executors.lock().insert(key);
            }
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
        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect_reqwest(
                self.http.clone(),
                url.parse()
                    .with_context(|| format!("invalid RPC URL: {url}"))?,
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
        .unwrap_or_else(|| url.to_string())
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
        assert!(!urls.contains(&"https://exec.example".to_string()));
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
        assert_eq!(ordered[0], "https://slow.example");
        assert_eq!(ordered[1], "https://fast.example");
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
