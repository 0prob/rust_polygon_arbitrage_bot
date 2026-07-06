use std::time::{Duration, Instant};

use alloy::network::Ethereum;
use alloy::primitives::B256;
use alloy::providers::Provider;
use alloy::rpc::types::Log;
use tokio::sync::watch;

use crate::infra::hypersync::HyperSyncService;
use crate::services::execution::rpc_errors::is_transient_receipt_error;

const HYPERSYNC_RECEIPT_CHECK_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Debug, Clone)]
pub struct ReceiptData {
    pub success: bool,
    pub gas_used: u64,
    pub effective_gas_price: Option<u128>,
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
    #[must_use]
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

    pub async fn wait_with_hypersync<P: Provider<Ethereum>>(
        &self,
        provider: &P,
        tx_hash: B256,
        hypersync: Option<&HyperSyncService>,
        shutdown: Option<&watch::Receiver<bool>>,
    ) -> Option<ReceiptData> {
        let deadline = Instant::now() + self.timeout;
        let mut hypersync_seen_mined = false;
        let mut next_hypersync_check = Instant::now();

        loop {
            if shutdown.is_some_and(|rx| *rx.borrow()) {
                return None;
            }

            if Instant::now() >= deadline {
                return None;
            }

            match provider.get_transaction_receipt(tx_hash).await {
                Ok(Some(receipt)) => {
                    return Some(ReceiptData {
                        success: receipt.status(),
                        gas_used: receipt.gas_used,
                        effective_gas_price: Some(receipt.effective_gas_price),
                        logs: receipt.logs().to_vec(),
                    });
                }
                Ok(None) => {}
                Err(err) => {
                    if !is_transient_receipt_error(&err) {
                        return None;
                    }
                }
            }

            if let Some(hs) = hypersync
                && !hypersync_seen_mined
                && Instant::now() >= next_hypersync_check
            {
                next_hypersync_check = Instant::now() + HYPERSYNC_RECEIPT_CHECK_INTERVAL;
                match hs.get_transaction_receipt(tx_hash, None).await {
                    Ok(Some((_success, _gas_used))) => {
                        hypersync_seen_mined = true;
                        if let Some(full) = fetch_receipt_from_rpc(provider, tx_hash).await {
                            return Some(full);
                        }
                        crate::debug!(
                            "hypersync receipt available but full RPC receipt is not yet available: success={_success}, gas_used={_gas_used}"
                        );
                    }
                    Ok(None) => {}
                    Err(e) => {
                        crate::debug!("hypersync receipt lookup failed: {e}");
                    }
                }
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            let poll = if hypersync_seen_mined {
                self.poll_interval / 2
            } else {
                self.poll_interval
            };
            let delay = poll.min(remaining);
            if let Some(rx) = shutdown {
                let mut shutdown_rx = rx.clone();
                tokio::select! {
                    () = tokio::time::sleep(delay) => {}
                    changed = shutdown_rx.changed() => {
                        if changed.is_ok() && *shutdown_rx.borrow() {
                            return None;
                        }
                    }
                }
            } else {
                tokio::time::sleep(delay).await;
            }
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
        effective_gas_price: Some(receipt.effective_gas_price),
        logs: receipt.logs().to_vec(),
    })
}
