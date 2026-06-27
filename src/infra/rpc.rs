use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use alloy::network::EthereumWallet;
use alloy::primitives::Address;
use alloy::providers::{DynProvider, Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use parking_lot::Mutex;
use reqwest::Client;
use tracing::warn;

use crate::config::AppConfig;

#[derive(Default)]
struct HttpProviderCache {
    inner: Mutex<HashMap<String, DynProvider>>,
}

#[derive(Default)]
struct SubmitProviderCache {
    inner: Mutex<HashMap<(String, Address), DynProvider>>,
}

/// Shared RPC endpoints and HTTP client (connection-pooled via reqwest).
#[derive(Clone)]
pub struct RpcPool {
    http: Client,
    state_urls: Vec<String>,
    execution_url: Option<String>,
    private_url: Option<String>,
    require_private_submit: bool,
    http_providers: Arc<HttpProviderCache>,
    submit_providers: Arc<SubmitProviderCache>,
}

impl std::fmt::Debug for RpcPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcPool")
            .field("state_urls", &self.state_urls)
            .field("execution_url", &self.execution_url)
            .field("private_url", &self.private_url)
            .field("require_private_submit", &self.require_private_submit)
            .field(
                "cached_http_providers",
                &self.http_providers.inner.lock().len(),
            )
            .field(
                "cached_submit_providers",
                &self.submit_providers.inner.lock().len(),
            )
            .finish_non_exhaustive()
    }
}

impl RpcPool {
    pub fn from_config(config: &AppConfig) -> Self {
        let timeout = Duration::from_millis(config.rpc.request_timeout_ms.max(1));
        let http = Client::builder()
            .timeout(timeout)
            .connect_timeout(Duration::from_secs(5))
            .pool_max_idle_per_host(8)
            .build()
            .unwrap_or_else(|_| Client::new());

        let mut state_urls = Vec::new();
        let mut seen = std::collections::HashSet::new();
        if let Some(url) = config.rpc.state_rpc_url.as_deref().filter(|u| !u.is_empty()) {
            seen.insert(url.to_string());
            state_urls.push(url.to_string());
        }
        for url in &config.rpc.polygon_rpc_urls {
            if url.is_empty() || !seen.insert(url.clone()) {
                continue;
            }
            state_urls.push(url.clone());
        }
        if !config.rpc.execution_rpc_url.is_empty()
            && seen.insert(config.rpc.execution_rpc_url.clone())
        {
            state_urls.push(config.rpc.execution_rpc_url.clone());
        }

        Self {
            http,
            state_urls,
            execution_url: if config.rpc.execution_rpc_url.is_empty() {
                None
            } else {
                Some(config.rpc.execution_rpc_url.clone())
            },
            private_url: config.rpc.private_rpc_url.clone(),
            require_private_submit: config.execution.require_private_submit,
            http_providers: Arc::new(HttpProviderCache::default()),
            submit_providers: Arc::new(SubmitProviderCache::default()),
        }
    }

    pub fn state_url(&self) -> Option<&str> {
        self.state_urls.first().map(String::as_str)
    }

    pub fn state_url_candidates(&self) -> &[String] {
        &self.state_urls
    }

    pub fn execution_url(&self) -> Option<&str> {
        self.execution_url.as_deref()
    }

    pub fn private_url(&self) -> Option<&str> {
        self.private_url.as_deref()
    }

    pub fn require_private_submit(&self) -> bool {
        self.require_private_submit
    }

    /// URL used for read-only execution RPC (dry-run simulation).
    pub fn simulation_url(&self) -> anyhow::Result<String> {
        self.execution_url
            .clone()
            .or_else(|| self.state_url().map(str::to_string))
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

    fn cached_http_provider(&self, url: &str) -> anyhow::Result<DynProvider> {
        if let Some(provider) = self.http_providers.inner.lock().get(url).cloned() {
            return Ok(provider);
        }
        let provider = ProviderBuilder::new()
            .connect_reqwest(self.http.clone(), url.parse().map_err(anyhow::Error::msg)?)
            .erased();
        self.http_providers
            .inner
            .lock()
            .insert(url.to_string(), provider.clone());
        Ok(provider)
    }

    pub fn connect_state(&self) -> anyhow::Result<DynProvider> {
        let url = self
            .state_url()
            .ok_or_else(|| anyhow::anyhow!("no state RPC configured"))?;
        self.cached_http_provider(url)
    }

    pub fn connect_state_at(&self, url: &str) -> anyhow::Result<DynProvider> {
        self.cached_http_provider(url)
    }

    pub fn connect_simulation(&self) -> anyhow::Result<DynProvider> {
        let url = self.simulation_url()?;
        self.cached_http_provider(&url)
    }

    pub fn connect_submit(&self, signer: &PrivateKeySigner) -> anyhow::Result<DynProvider> {
        let (url, is_private) = self.submit_url()?;
        if !is_private {
            warn!(
                "submitting via public execution RPC — consider setting PRIVATE_RPC_URL \
                 (Flashbots Protect, bloXroute, etc.)"
            );
        }
        let key = (url.clone(), signer.address());
        if let Some(provider) = self.submit_providers.inner.lock().get(&key).cloned() {
            return Ok(provider);
        }
        let wallet = EthereumWallet::from(signer.clone());
        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect_reqwest(self.http.clone(), url.parse().map_err(anyhow::Error::msg)?)
            .erased();
        self.submit_providers
            .inner
            .lock()
            .insert(key, provider.clone());
        Ok(provider)
    }
}
