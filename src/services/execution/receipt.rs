use std::time::{Duration, Instant};

use alloy::network::Ethereum;
use alloy::primitives::B256;
use alloy::providers::Provider;
use alloy::rpc::types::Log;
use tokio::sync::watch;
use tracing::{debug, instrument, warn};

use crate::infra::hypersync::HyperSyncService;
use crate::services::execution::rpc_errors::is_transient_receipt_error;

#[derive(Debug, Clone)]
pub struct ReceiptData {
    pub success: bool,
    pub gas_used: u64,
    pub logs: Vec<Log>,
}

#[derive(Debug, Clone)]
pub struct ReceiptPoller {
    timeout: Duration,
    poll_interval: Duration,
}

impl Default for ReceiptPoller {
    fn default() -> Self {
        Self::new(Duration::from_secs(30), Duration::from_millis(500))
    }
}

impl ReceiptPoller {
    pub fn new(timeout: Duration, poll_interval: Duration) -> Self {
        Self {
            timeout,
            poll_interval,
        }
    }

    pub async fn wait<P: Provider<Ethereum>>(
        &self,
        provider: &P,
        tx_hash: B256,
    ) -> Option<ReceiptData> {
        self.wait_with_hypersync(provider, tx_hash, None, None)
            .await
    }

    #[instrument(
        skip(self, provider, hypersync, shutdown),
        fields(
            tx_hash = %tx_hash,
            success = tracing::field::Empty,
            gas_used = tracing::field::Empty,
            source = tracing::field::Empty,
        )
    )]
    pub async fn wait_with_hypersync<P: Provider<Ethereum>>(
        &self,
        provider: &P,
        tx_hash: B256,
        hypersync: Option<&HyperSyncService>,
        shutdown: Option<&watch::Receiver<bool>>,
    ) -> Option<ReceiptData> {
        let deadline = Instant::now() + self.timeout;
        let mut hard_errors = 0u32;

        loop {
            if shutdown.is_some_and(|rx| *rx.borrow()) {
                warn!(%tx_hash, "receipt poll cancelled — shutdown");
                return None;
            }

            if Instant::now() >= deadline {
                warn!(%tx_hash, "receipt poll timed out");
                return None;
            }

            if let Some(hs) = hypersync
                && let Ok(Some((success, gas_used))) =
                    hs.get_transaction_receipt(tx_hash, None).await
            {
                tracing::Span::current().record("source", "hypersync");
                tracing::Span::current().record("success", success);
                tracing::Span::current().record("gas_used", gas_used);
                debug!(%tx_hash, success, gas_used, "hypersync receipt — fetching logs from RPC");
                if let Some(full) = fetch_receipt_from_rpc(provider, tx_hash).await {
                    return Some(full);
                }
                return Some(ReceiptData {
                    success,
                    gas_used,
                    logs: Vec::new(),
                });
            }

            if let Some(data) = fetch_receipt_from_rpc(provider, tx_hash).await {
                tracing::Span::current().record("source", "rpc");
                tracing::Span::current().record("success", data.success);
                tracing::Span::current().record("gas_used", data.gas_used);
                debug!(%tx_hash, success = data.success, gas_used = data.gas_used, logs = data.logs.len(), "receipt received");
                return Some(data);
            }

            match provider.get_transaction_receipt(tx_hash).await {
                Ok(None) => {
                    hard_errors = 0;
                }
                Ok(Some(_)) => {
                    hard_errors = 0;
                }
                Err(err) => {
                    if is_transient_receipt_error(&err) {
                        hard_errors += 1;
                        if hard_errors >= 10 {
                            warn!(%tx_hash, error = %err, "persistent transient receipt errors");
                        }
                    } else {
                        warn!(%tx_hash, error = %err, "non-transient receipt fetch error");
                        return None;
                    }
                }
            }

            if shutdown.is_some_and(|rx| *rx.borrow()) {
                return None;
            }

            tokio::time::sleep(self.poll_interval).await;
        }
    }
}

async fn fetch_receipt_from_rpc<P: Provider<Ethereum>>(
    provider: &P,
    tx_hash: B256,
) -> Option<ReceiptData> {
    let receipt = provider.get_transaction_receipt(tx_hash).await.ok()??;
    Some(ReceiptData {
        success: receipt.status(),
        gas_used: receipt.gas_used,
        logs: receipt.logs().to_vec(),
    })
}
