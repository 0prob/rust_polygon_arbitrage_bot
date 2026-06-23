use std::fs;
use std::path::Path;

use alloy::primitives::Address;
use alloy::signers::local::PrivateKeySigner;
use zeroize::Zeroizing;

use super::AppConfig;

/// Parsed signing material — raw key bytes are not retained after construction.
#[derive(Debug)]
pub struct WalletSecrets {
    signer: Option<PrivateKeySigner>,
}

impl WalletSecrets {
    /// Load and validate wallet material. Fails fast in live mode when no valid key is present.
    pub fn load(config: &mut AppConfig) -> anyhow::Result<Self> {
        let material = load_key_material(config)?;
        let signer = match material {
            Some(raw) => {
                let parsed = raw
                    .parse::<PrivateKeySigner>()
                    .map_err(|e| anyhow::anyhow!("invalid private key: {e}"))?;
                Some(parsed)
            }
            None => None,
        };

        if !config.is_dry_run() && signer.is_none() {
            anyhow::bail!(
                "live mode requires PRIVATE_KEY or PRIVATE_KEY_FILE (or execution.private_key in config)"
            );
        }

        Ok(Self { signer })
    }

    pub fn dry_run() -> Self {
        Self { signer: None }
    }

    pub fn signer(&self) -> Option<&PrivateKeySigner> {
        self.signer.as_ref()
    }

    pub fn operator_address(&self, fallback: Address) -> Address {
        self.signer
            .as_ref()
            .map(alloy::signers::Signer::address)
            .unwrap_or(fallback)
    }

    pub fn has_signer(&self) -> bool {
        self.signer.is_some()
    }
}

fn load_key_material(config: &mut AppConfig) -> anyhow::Result<Option<Zeroizing<String>>> {
    if let Some(path) = env_var("PRIVATE_KEY_FILE") {
        let contents = fs::read_to_string(Path::new(&path))
            .map_err(|e| anyhow::anyhow!("failed to read PRIVATE_KEY_FILE {path}: {e}"))?;
        let trimmed = contents.trim().to_string();
        if trimmed.is_empty() {
            anyhow::bail!("PRIVATE_KEY_FILE is empty: {path}");
        }
        config.execution.private_key = None;
        return Ok(Some(Zeroizing::new(trimmed)));
    }

    if config.execution.private_key.is_none()
        && let Some(key) = env_var("PRIVATE_KEY") {
            config.execution.private_key = Some(key);
        }

    Ok(config
        .execution
        .private_key
        .take()
        .map(Zeroizing::new))
}

fn env_var(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ExecutionConfig, OracleConfig, RoutingConfig, RpcConfig};

    fn base_config(mode: &str) -> AppConfig {
        AppConfig {
            hasura_url: "http://localhost:8080/v1/graphql".to_string(),
            hasura_secret: None,
            discovery_interval_ms: 60_000,
            lf_interval_ms: 1_000,
            hf_interval_ms: 200,
            max_multicall_calls: 800,
            rpc: RpcConfig::default(),
            routing: RoutingConfig::default(),
            execution: ExecutionConfig {
                mode: mode.to_string(),
                executor_address: None,
                private_key: Some(
                    "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
                        .to_string(),
                ),
                min_profit_matic_wei: "0".to_string(),
                slippage_bps: 50,
                flash_loan_source: "auto".to_string(),
                receipt_timeout_ms: 30_000,
                receipt_poll_ms: 500,
                max_flash_loan_usd: 50_000,
                deadline_secs: 120,
                require_private_submit: false,
                profit_priority_fee_alpha_bps: 1_000,
                profit_safety_multiplier_bps: 30_000,
                min_operator_matic_wei: "500000000000000000".to_string(),
                max_global_consecutive_failures: 8,
                min_profit_roi_bps: 0,
                circuit_breaker_cooldown_secs: 300,
            },
            oracle: OracleConfig::default(),
            pipeline: crate::config::PipelineConfig::default(),
        }
    }

    #[test]
    fn live_mode_rejects_missing_key() {
        let key = "PRIVATE_KEY";
        let prev = std::env::var(key).ok();
        unsafe {
            std::env::remove_var(key);
        }
        let mut config = base_config("live");
        config.execution.private_key = None;
        let err = WalletSecrets::load(&mut config).unwrap_err();
        assert!(err.to_string().contains("live mode requires"));
        unsafe {
            match prev {
                Some(v) => std::env::set_var(key, v),
                None => std::env::remove_var(key),
            }
        }
    }

    #[test]
    fn dry_run_allows_missing_key() {
        let key = "PRIVATE_KEY";
        let prev = std::env::var(key).ok();
        unsafe {
            std::env::remove_var(key);
        }
        let mut config = base_config("dry-run");
        config.execution.private_key = None;
        let wallet = WalletSecrets::load(&mut config).expect("dry-run ok");
        assert!(!wallet.has_signer());
        unsafe {
            match prev {
                Some(v) => std::env::set_var(key, v),
                None => std::env::remove_var(key),
            }
        }
    }

    #[test]
    fn clears_private_key_from_config_after_load() {
        let mut config = base_config("dry-run");
        let _ = WalletSecrets::load(&mut config).expect("load");
        assert!(config.execution.private_key.is_none());
    }
}
