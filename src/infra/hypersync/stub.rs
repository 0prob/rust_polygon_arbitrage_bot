//! Compile-time stub when the `hypersync` feature is disabled.
//!
//! `try_from_env` always returns `None`; `HyperSyncService` is never constructed
//! in production paths. Methods exist only so call sites type-check without
//! `cfg` sprawl.

use std::time::Duration;

use alloy::primitives::B256;
use anyhow::{bail, Result};
use tokio::sync::mpsc;

use crate::config::RpcConfig;

/// Mirrors `hypersync_client::HeightStreamEvent` so pass-loop code stays shared.
#[derive(Debug, Clone)]
pub enum HeightStreamEvent {
    Height(u64),
    Reconnecting {
        delay: Duration,
        error_msg: String,
    },
    Connected,
}

/// Unconstructable when the feature is off (`try_from_env` → `None`).
#[derive(Debug)]
pub struct HyperSyncService {
    _private: (),
}

impl HyperSyncService {
    pub fn from_config(_rpc: &RpcConfig, _api_token: &str) -> Result<Self> {
        bail!("hypersync feature disabled at compile time")
    }

    pub fn record_height(&self, _height: u64) {}

    pub fn latest_height(&self) -> Option<u64> {
        None
    }

    pub async fn get_height(&self) -> Result<u64> {
        bail!("hypersync feature disabled at compile time")
    }

    pub async fn probe_height(&self) -> Result<u64> {
        bail!("hypersync feature disabled at compile time")
    }

    pub fn stream_height(&self) -> mpsc::Receiver<HeightStreamEvent> {
        let (_tx, rx) = mpsc::channel(1);
        rx
    }

    pub async fn get_transaction_receipt(
        &self,
        _tx_hash: B256,
        _lookback_blocks: Option<u64>,
    ) -> Result<Option<(bool, u64)>> {
        Ok(None)
    }
}

/// Always `None` when built without the `hypersync` feature.
pub fn try_from_env(_rpc: &RpcConfig) -> Option<HyperSyncService> {
    None
}
