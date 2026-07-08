//! Native Rust HyperSync client wrapper.
//!
//! Complements (does not replace) the Envio HyperIndex data:
//! - **PostgreSQL** — pool/token discovery metadata (LF path, direct SQL)
//! - **HyperSync** — fast head feed, receipts, traces, historical log scans
//!
//! See: <https://docs.rs/hypersync-client/latest/hypersync_client/>

use std::sync::atomic::{AtomicU64, Ordering};

use alloy::primitives::B256;
use anyhow::{Context, Result};
use hypersync_client::Client;
use hypersync_client::HeightStreamEvent;
use hypersync_client::format::{Hash, Quantity, TransactionStatus};
use hypersync_client::net_types::{
    JoinMode, Query, TransactionFilter, TransactionSelection, transaction::TransactionField,
};
use tokio::sync::{Mutex, mpsc};
use tokio::time::{Duration, timeout};

use crate::config::RpcConfig;
use crate::core::constants::POLYGON_CHAIN_ID;
use crate::util::now_ms;

const DEFAULT_RECEIPT_LOOKBACK: u64 = 50;
const HEIGHT_CACHE_TTL_MS: u64 = 15_000;
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// Thin wrapper around [`hypersync_client::Client`] for the arb bot.
pub struct HyperSyncService {
    client: Client,
    request_timeout: Duration,
    cached_height: AtomicU64,
    cached_height_at: AtomicU64,
    height_refresh: Mutex<()>,
}

impl HyperSyncService {
    pub fn from_config(rpc: &RpcConfig, api_token: &str) -> Result<Self> {
        let request_timeout = Duration::from_millis(rpc.request_timeout_ms.max(1));
        let mut builder = Client::builder()
            .chain_id(POLYGON_CHAIN_ID)
            .api_token(api_token)
            .http_req_timeout_millis(rpc.request_timeout_ms);

        if let Some(url) = rpc.hyper_sync_url.as_deref() {
            builder = builder.url(url);
        }

        let client = builder
            .build()
            .context("failed to build hypersync client")?;

        Ok(Self {
            client,
            request_timeout,
            cached_height: AtomicU64::new(0),
            cached_height_at: AtomicU64::new(0),
            height_refresh: Mutex::new(()),
        })
    }

    /// Update the height cache from the live SSE stream.
    pub fn record_height(&self, height: u64) {
        self.cached_height.store(height, Ordering::Relaxed);
        self.cached_height_at.store(now_ms(), Ordering::Relaxed);
    }

    /// Cached chain head when the SSE stream (or a recent `get_height`) populated it.
    pub fn latest_height(&self) -> Option<u64> {
        let now_ms = now_ms();
        let cached_at = self.cached_height_at.load(Ordering::Relaxed);
        if now_ms.saturating_sub(cached_at) >= HEIGHT_CACHE_TTL_MS {
            return None;
        }
        match self.cached_height.load(Ordering::Relaxed) {
            0 => None,
            height => Some(height),
        }
    }

    pub async fn get_height(&self) -> Result<u64> {
        if let Some(height) = self.latest_height() {
            return Ok(height);
        }

        let _refresh = self.height_refresh.lock().await;
        if let Some(height) = self.latest_height() {
            return Ok(height);
        }

        let height = timeout(self.request_timeout, self.client.get_height())
            .await
            .context("hypersync get_height timed out")?
            .context("hypersync get_height failed")?;
        self.record_height(height);
        Ok(height)
    }

    /// Fast connectivity probe without client retries (see `Client::health_check`).
    pub async fn probe_height(&self) -> Result<u64> {
        let height = self
            .client
            .health_check(Some(PROBE_TIMEOUT))
            .await
            .context("hypersync health_check failed")?;
        self.record_height(height);
        Ok(height)
    }

    /// Live height stream with built-in SSE reconnect (see `Client::stream_height`).
    pub fn stream_height(&self) -> mpsc::Receiver<HeightStreamEvent> {
        self.client.stream_height()
    }

    /// HyperSync-first receipt lookup; returns `None` when tx is not in the lookback window.
    pub async fn get_transaction_receipt(
        &self,
        tx_hash: B256,
        lookback_blocks: Option<u64>,
    ) -> Result<Option<(bool, u64)>> {
        let lookback = lookback_blocks.unwrap_or(DEFAULT_RECEIPT_LOOKBACK);
        let height = if let Some(height) = self.latest_height() {
            height
        } else {
            self.get_height().await?
        };
        let from_block = height.saturating_sub(lookback);
        let to_block = height.saturating_add(1);
        let hash = Hash::from(tx_hash.0);
        let filter = TransactionFilter::all()
            .and_hash([hash])
            .context("invalid tx hash filter")?;
        let mut query = Query::new()
            .from_block(from_block)
            .to_block_excl(to_block)
            .join_mode(JoinMode::JoinNothing)
            .select_transaction_fields([
                TransactionField::Hash,
                TransactionField::Status,
                TransactionField::GasUsed,
            ])
            .where_transactions(TransactionSelection::from(filter));
        query.max_num_transactions = Some(1);
        let response = timeout(self.request_timeout, self.client.get(&query))
            .await
            .context("hypersync get_transaction_receipt timed out")?
            .context("hypersync get_transaction_receipt failed")?;
        let tx = response.data.transactions.into_iter().flatten().next();
        let Some(tx) = tx else {
            return Ok(None);
        };
        let success = matches!(tx.status, Some(TransactionStatus::Success));
        let gas_used = tx.gas_used.as_ref().map_or(0, quantity_to_u64);
        Ok(Some((success, gas_used)))
    }
}

fn quantity_to_u64(q: &Quantity) -> u64 {
    let bytes = q.as_ref();
    let start = bytes.len().saturating_sub(8);
    bytes[start..]
        .iter()
        .fold(0u64, |acc, &b| acc.wrapping_shl(8) | u64::from(b))
}

/// Returns `Some(service)` when `ENVIO_API_TOKEN` is set; otherwise `None`.
pub fn try_from_env(rpc: &RpcConfig) -> Option<HyperSyncService> {
    let token = std::env::var("ENVIO_API_TOKEN")
        .ok()
        .filter(|t| !t.is_empty())?;
    HyperSyncService::from_config(rpc, &token).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_height_serves_latest_without_network() {
        let service = HyperSyncService::from_config(
            &RpcConfig::default(),
            "a3cbea70-ad7d-4308-a4be-b14e095ce169",
        )
        .expect("default HyperSync test configuration should be valid");
        assert!(service.latest_height().is_none());
        service.record_height(42_000_000);
        assert_eq!(service.latest_height(), Some(42_000_000));
    }

    #[test]
    fn quantity_to_u64_parses_big_endian_bytes() {
        let q = Quantity::from(vec![0x01, 0x00, 0x00]);
        assert_eq!(quantity_to_u64(&q), 65_536);
    }

    #[test]
    fn quantity_to_u64_uses_least_significant_eight_bytes() {
        let q = Quantity::from(vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09]);
        assert_eq!(quantity_to_u64(&q), 0x0203_0405_0607_0809);
    }
}
