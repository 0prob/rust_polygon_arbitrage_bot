use std::time::Duration;

use alloy::network::{Ethereum, EthereumWallet};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use reqwest::Client;
use tracing::warn;

use crate::config::AppConfig;

/// Shared RPC endpoints and HTTP client (connection-pooled via reqwest).
#[derive(Debug, Clone)]
pub struct RpcPool {
    http: Client,
    state_url: Option<String>,
    execution_url: Option<String>,
    private_url: Option<String>,
    require_private_submit: bool,
}

impl RpcPool {
    pub fn from_config(config: &AppConfig) -> Self {
        let timeout = Duration::from_millis(config.rpc.request_timeout_ms.max(1));
        let http = Client::builder()
            .timeout(timeout)
            .pool_max_idle_per_host(8)
            .build()
            .unwrap_or_else(|_| Client::new());

        Self {
            http,
            state_url: config.state_rpc_url().map(str::to_string),
            execution_url: if config.rpc.execution_rpc_url.is_empty() {
                None
            } else {
                Some(config.rpc.execution_rpc_url.clone())
            },
            private_url: config.rpc.private_rpc_url.clone(),
            require_private_submit: config.execution.require_private_submit,
        }
    }

    pub fn state_url(&self) -> Option<&str> {
        self.state_url.as_deref()
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
            .or_else(|| self.state_url.clone())
            .ok_or_else(|| anyhow::anyhow!("no execution or state RPC configured"))
    }

    /// URL used for live transaction submission (prefers private MEV-protected endpoint).
    pub fn submit_url(&self) -> anyhow::Result<(String, bool)> {
        if let Some(url) = &self.private_url {
            return Ok((url.clone(), true));
        }
        if self.require_private_submit {
            anyhow::bail!(
                "REQUIRE_PRIVATE_SUBMIT is set but PRIVATE_RPC_URL is not configured"
            );
        }
        self.simulation_url().map(|url| (url, false))
    }

    fn connect_http(&self, url: &str) -> anyhow::Result<impl Provider<Ethereum> + Clone> {
        Ok(ProviderBuilder::new().connect_reqwest(
            self.http.clone(),
            url.parse().map_err(anyhow::Error::msg)?,
        ))
    }

    pub fn connect_state(&self) -> anyhow::Result<impl Provider<Ethereum> + Clone> {
        let url = self
            .state_url()
            .ok_or_else(|| anyhow::anyhow!("no state RPC configured"))?;
        self.connect_http(url)
    }

    pub fn connect_simulation(&self) -> anyhow::Result<impl Provider<Ethereum> + Clone> {
        let url = self.simulation_url()?;
        Ok(ProviderBuilder::new().connect_reqwest(
            self.http.clone(),
            url.parse().map_err(anyhow::Error::msg)?,
        ))
    }

    pub fn connect_submit(
        &self,
        signer: &PrivateKeySigner,
    ) -> anyhow::Result<impl Provider<Ethereum> + Clone> {
        let (url, is_private) = self.submit_url()?;
        if !is_private {
            warn!(
                "submitting via public execution RPC — consider setting PRIVATE_RPC_URL \
                 (Flashbots Protect, bloXroute, etc.)"
            );
        }
        let wallet = EthereumWallet::from(signer.clone());
        Ok(ProviderBuilder::new()
            .wallet(wallet)
            .connect_reqwest(
                self.http.clone(),
                url.parse().map_err(anyhow::Error::msg)?,
            ))
    }
}
