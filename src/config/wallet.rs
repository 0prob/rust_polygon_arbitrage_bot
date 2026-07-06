use std::fs;
use std::path::Path;

use alloy::primitives::Address;
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;

use super::AppConfig;

#[derive(Debug)]
pub struct WalletSecrets {
    signer: Option<PrivateKeySigner>,
}

impl WalletSecrets {
    pub fn load(config: &mut AppConfig) -> anyhow::Result<Self> {
        let material = load_key_material(config)?;
        let signer = match material {
            Some(raw) => {
                let parsed = raw
                    .parse::<PrivateKeySigner>()
                    .context("invalid private key")?;
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

    #[must_use]
    pub fn dry_run() -> Self {
        Self { signer: None }
    }

    #[must_use]
    pub fn signer(&self) -> Option<&PrivateKeySigner> {
        self.signer.as_ref()
    }

    pub fn operator_address(&self, fallback: Address) -> Address {
        self.signer
            .as_ref()
            .map_or(fallback, alloy::signers::Signer::address)
    }

    #[must_use]
    pub fn has_signer(&self) -> bool {
        self.signer.is_some()
    }
}

fn load_key_material(config: &mut AppConfig) -> anyhow::Result<Option<String>> {
    if let Some(path) = super::env_var("PRIVATE_KEY_FILE") {
        let contents = fs::read_to_string(Path::new(&path))
            .with_context(|| format!("failed to read PRIVATE_KEY_FILE {path}"))?;
        let trimmed = contents.trim().to_string();
        if trimmed.is_empty() {
            anyhow::bail!("PRIVATE_KEY_FILE is empty: {path}");
        }
        config.execution.private_key = None;
        return Ok(Some(trimmed));
    }

    if config.execution.private_key.is_none()
        && let Some(key) = super::env_var("PRIVATE_KEY")
    {
        config.execution.private_key = Some(key);
    }

    Ok(config.execution.private_key.take())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dry_run_has_no_signer() {
        let wallet = WalletSecrets::dry_run();
        assert!(!wallet.has_signer());
    }
}
