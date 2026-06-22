//! Native Rust HyperSync client wrapper.
//!
//! Complements (does not replace) the TS HyperIndex Hasura feed:
//! - **Hasura GraphQL** — pool/token discovery metadata (LF path)
//! - **HyperSync** — fast head feed, receipts, traces, historical log scans
//!
//! See: <https://docs.rs/hypersync-client/latest/hypersync_client/>

use alloy::primitives::B256;
use anyhow::{Context, Result};
use hypersync_client::Client;
use hypersync_client::format::TransactionStatus;
use hypersync_client::net_types::{
    JoinMode, Query, TransactionFilter, TransactionSelection, transaction::TransactionField,
};

use crate::config::RpcConfig;
use crate::core::constants::POLYGON_CHAIN_ID;

const DEFAULT_RECEIPT_LOOKBACK: u64 = 50;

/// Thin wrapper around [`hypersync_client::Client`] for the arb bot.
pub struct HyperSyncService {
    client: Client,
    chain_id: u64,
}

impl HyperSyncService {
    /// Build a client from RPC config + `ENVIO_API_TOKEN`.
    pub fn from_config(rpc: &RpcConfig, api_token: &str) -> Result<Self> {
        let chain_id = POLYGON_CHAIN_ID;
        let mut builder = Client::builder().chain_id(chain_id).api_token(api_token);

        if let Some(url) = rpc.hyper_sync_url.as_deref() {
            builder = builder.url(url);
        }

        let client = builder
            .build()
            .context("failed to build hypersync client")?;

        Ok(Self { client, chain_id })
    }

    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }

    /// One-shot chain tip — useful for indexer lag checks and reorg guards.
    pub async fn get_height(&self) -> Result<u64> {
        self.client
            .get_height()
            .await
            .context("hypersync get_height failed")
    }

    /// Live head feed via SSE — lower latency than polling JSON-RPC `eth_blockNumber`.
    ///
    /// Pattern from [hypersync-client examples](https://github.com/enviodev/hypersync-client-rust/tree/main/examples/height_stream):
    /// subscribe in a background task and forward heights to the HF orchestrator.
    pub fn stream_height(
        &self,
    ) -> tokio::sync::mpsc::Receiver<hypersync_client::HeightStreamEvent> {
        self.client.stream_height()
    }

    pub fn inner(&self) -> &Client {
        &self.client
    }

    /// HyperSync-first receipt lookup; returns `None` when tx is not in the lookback window.
    pub async fn get_transaction_receipt(
        &self,
        tx_hash: B256,
        lookback_blocks: Option<u64>,
    ) -> Result<Option<(bool, u64)>> {
        let lookback = lookback_blocks.unwrap_or(DEFAULT_RECEIPT_LOOKBACK);
        let height = self.get_height().await?;
        let from_block = height.saturating_sub(lookback);
        let hash = hypersync_client::format::Hash::from(tx_hash.0);
        let filter = TransactionFilter::all()
            .and_hash([hash])
            .context("invalid tx hash filter")?;
        let query = Query::new()
            .from_block(from_block)
            .join_mode(JoinMode::JoinAll)
            .select_transaction_fields([
                TransactionField::Hash,
                TransactionField::Status,
                TransactionField::GasUsed,
            ])
            .where_transactions(TransactionSelection::from(filter));
        let response = self.client.get(&query).await?;
        let tx = response.data.transactions.into_iter().flatten().next();
        let Some(tx) = tx else {
            return Ok(None);
        };
        let success = matches!(tx.status, Some(TransactionStatus::Success));
        let gas_used = tx.gas_used.map(quantity_to_u64).unwrap_or(0);
        Ok(Some((success, gas_used)))
    }
}

fn quantity_to_u64(q: hypersync_client::format::Quantity) -> u64 {
    let bytes = q.as_ref();
    let mut buf = [0u8; 8];
    let start = 8usize.saturating_sub(bytes.len());
    buf[start..].copy_from_slice(bytes);
    u64::from_be_bytes(buf)
}

/// Returns `Some(service)` when `ENVIO_API_TOKEN` is set; otherwise `None`.
pub fn try_from_env(rpc: &RpcConfig) -> Result<Option<HyperSyncService>> {
    let token = match std::env::var("ENVIO_API_TOKEN") {
        Ok(t) if !t.is_empty() => t,
        _ => return Ok(None),
    };
    Ok(Some(HyperSyncService::from_config(rpc, &token)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantity_zero() {
        let q = hypersync_client::format::Quantity::from(0u64);
        assert_eq!(quantity_to_u64(q), 0);
    }

    #[test]
    fn quantity_max_u64() {
        let q = hypersync_client::format::Quantity::from(u64::MAX);
        assert_eq!(quantity_to_u64(q), u64::MAX);
    }

    #[test]
    fn quantity_small_value() {
        let q = hypersync_client::format::Quantity::from(42u64);
        assert_eq!(quantity_to_u64(q), 42);
    }

    #[test]
    fn quantity_large_value() {
        let q = hypersync_client::format::Quantity::from(1_000_000u64);
        assert_eq!(quantity_to_u64(q), 1_000_000);
    }
}
