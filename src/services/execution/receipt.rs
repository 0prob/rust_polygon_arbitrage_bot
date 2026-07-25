use std::time::{Duration, Instant};

use alloy::network::Ethereum;
use alloy::primitives::B256;
use alloy::providers::Provider;
use alloy::rpc::types::Log;
use tokio::sync::watch;

use crate::services::execution::rpc_errors::is_transient_receipt_error;

#[derive(Debug, Clone)]
pub struct ReceiptData {
    pub success: bool,
    pub gas_used: u64,
    pub effective_gas_price: Option<u128>,
    pub logs: Vec<Log>,
}

#[derive(Debug, Clone)]
pub enum ReceiptPollOutcome {
    Received(ReceiptData),
    TimedOut,
    Shutdown,
    RpcFailure(String),
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
        shutdown: Option<&watch::Receiver<bool>>,
    ) -> ReceiptPollOutcome {
        let deadline = Instant::now() + self.timeout;

        loop {
            if shutdown.is_some_and(|rx| *rx.borrow()) {
                return ReceiptPollOutcome::Shutdown;
            }

            if Instant::now() >= deadline {
                return ReceiptPollOutcome::TimedOut;
            }

            match provider.get_transaction_receipt(tx_hash).await {
                Ok(Some(receipt)) => {
                    return ReceiptPollOutcome::Received(ReceiptData {
                        success: receipt.status(),
                        gas_used: receipt.gas_used,
                        effective_gas_price: Some(receipt.effective_gas_price),
                        logs: receipt.logs().to_vec(),
                    });
                }
                Ok(None) => {}
                Err(err) => {
                    if !is_transient_receipt_error(&err) {
                        return ReceiptPollOutcome::RpcFailure(err.to_string());
                    }
                }
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            let delay = self.poll_interval.min(remaining);
            if let Some(rx) = shutdown {
                let mut shutdown_rx = rx.clone();
                tokio::select! {
                    () = tokio::time::sleep(delay) => {}
                    changed = shutdown_rx.changed() => {
                        if changed.is_ok() && *shutdown_rx.borrow() {
                            return ReceiptPollOutcome::Shutdown;
                        }
                    }
                }
            } else {
                tokio::time::sleep(delay).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rpc_failure_is_distinct_from_timeout() {
        let outcome = ReceiptPollOutcome::RpcFailure("unauthorized".into());
        assert!(matches!(outcome, ReceiptPollOutcome::RpcFailure(_)));
        assert!(!matches!(outcome, ReceiptPollOutcome::TimedOut));
    }
}
