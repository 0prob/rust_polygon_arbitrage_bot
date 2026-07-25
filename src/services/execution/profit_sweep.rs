//! Post-confirm owner sweep: executor `transferAll(profit_token, recipient)`.

use alloy::network::Ethereum;
use alloy::primitives::{Address, Bytes, U256};
use alloy::providers::Provider;
use alloy::sol_types::SolCall;
use anyhow::{Context, Result};
use tokio::sync::watch;

use crate::abis::IArbExecutor;
use crate::config::AppConfig;
use crate::infra::hypersync::HyperSyncService;
use crate::services::execution::candidate::CandidateExecution;
use crate::services::execution::gas_oracle::GasOracle;
use crate::services::execution::nonce::NonceManager;
use crate::services::execution::private_submit::PrivateSubmitConfig;
use crate::services::execution::receipt::{ReceiptPollOutcome, ReceiptPoller};
use crate::services::execution::submit::{
    resolve_submit_fees, submit_with_recovery,
};

/// Gas ceiling for owner `transferAll` + ERC-20 transfer.
const TRANSFER_ALL_GAS_LIMIT: u64 = 150_000;

/// Resolve sweep destination. `None` means skip (noop / unsafe).
#[must_use]
pub fn resolve_profit_recipient(
    configured: Option<Address>,
    operator: Address,
    executor: Address,
) -> Option<Address> {
    let recipient = configured.unwrap_or(operator);
    if recipient.is_zero() || recipient == executor {
        None
    } else {
        Some(recipient)
    }
}

#[must_use]
pub fn encode_transfer_all_calldata(token: Address, to: Address) -> Bytes {
    Bytes::from(IArbExecutor::transferAllCall { token, to }.abi_encode())
}

/// Submit `transferAll(profit_token, recipient)` and wait for a receipt.
///
/// Failures are returned to the caller; they must not change the arb `Confirmed` outcome.
#[allow(clippy::too_many_arguments)]
pub async fn sweep_profit_to_recipient<P: Provider<Ethereum>, S: Provider<Ethereum>>(
    submit_provider: &P,
    receipt_provider: &S,
    nonce_mgr: &NonceManager,
    gas_oracle: &GasOracle,
    config: &AppConfig,
    candidate: &CandidateExecution,
    operator: Address,
    private: Option<&PrivateSubmitConfig>,
    hypersync: Option<&HyperSyncService>,
    shutdown: Option<&watch::Receiver<bool>>,
) -> Result<()> {
    let Some(recipient) = resolve_profit_recipient(
        config.execution.profit_recipient,
        operator,
        candidate.target_address,
    ) else {
        crate::debug!(
            "profit sweep skipped: recipient unresolved (operator={operator} executor={})",
            candidate.target_address
        );
        return Ok(());
    };

    let mut sweep = candidate.clone();
    sweep.calldata = encode_transfer_all_calldata(candidate.profit_token, recipient);
    sweep.value = U256::ZERO;

    let fees = resolve_submit_fees(gas_oracle).context("profit sweep: gas fee snapshot missing")?;
    let mut nonce = nonce_mgr.next_nonce()?;

    let tx_hash = match submit_with_recovery(
        submit_provider,
        nonce_mgr,
        &sweep,
        &mut nonce,
        fees,
        TRANSFER_ALL_GAS_LIMIT,
        private,
    )
    .await
    {
        Ok(hash) => hash,
        Err(e) => {
            nonce_mgr.release(nonce);
            return Err(e).context("profit sweep submit failed");
        }
    };

    crate::info!(
        "profit sweep submitted: fp={} token={} to={} nonce={} tx_hash={tx_hash}",
        candidate.route_fingerprint,
        candidate.profit_token,
        recipient,
        nonce,
    );

    let poller = ReceiptPoller::new(
        std::time::Duration::from_millis(config.execution.receipt_timeout_ms),
        std::time::Duration::from_millis(config.execution.receipt_poll_ms),
    );
    match poller
        .wait_with_hypersync(receipt_provider, tx_hash, hypersync, shutdown)
        .await
    {
        ReceiptPollOutcome::Received(receipt) => {
            nonce_mgr.confirm(nonce);
            if receipt.success {
                crate::info!(
                    "profit sweep confirmed: fp={} tx_hash={tx_hash} gas={}",
                    candidate.route_fingerprint,
                    receipt.gas_used
                );
                Ok(())
            } else {
                anyhow::bail!(
                    "profit sweep reverted: fp={} tx_hash={tx_hash} gas={}",
                    candidate.route_fingerprint,
                    receipt.gas_used
                );
            }
        }
        ReceiptPollOutcome::Shutdown => {
            nonce_mgr.release(nonce);
            anyhow::bail!("profit sweep aborted on shutdown: tx_hash={tx_hash}");
        }
        ReceiptPollOutcome::RpcFailure(reason) => {
            nonce_mgr.mark_stale(nonce);
            anyhow::bail!("profit sweep receipt RPC failed: tx_hash={tx_hash} reason={reason}");
        }
        ReceiptPollOutcome::TimedOut => {
            nonce_mgr.mark_stale(nonce);
            anyhow::bail!("profit sweep receipt timeout: tx_hash={tx_hash}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::address;

    #[test]
    fn recipient_defaults_to_operator() {
        let operator = address!("0x1111111111111111111111111111111111111111");
        let executor = address!("0x2222222222222222222222222222222222222222");
        assert_eq!(
            resolve_profit_recipient(None, operator, executor),
            Some(operator)
        );
    }

    #[test]
    fn recipient_uses_configured_override() {
        let operator = address!("0x1111111111111111111111111111111111111111");
        let executor = address!("0x2222222222222222222222222222222222222222");
        let treasury = address!("0x3333333333333333333333333333333333333333");
        assert_eq!(
            resolve_profit_recipient(Some(treasury), operator, executor),
            Some(treasury)
        );
    }

    #[test]
    fn recipient_skips_executor_and_zero() {
        let operator = address!("0x1111111111111111111111111111111111111111");
        let executor = address!("0x2222222222222222222222222222222222222222");
        assert_eq!(resolve_profit_recipient(Some(executor), operator, executor), None);
        assert_eq!(
            resolve_profit_recipient(Some(Address::ZERO), operator, executor),
            None
        );
        assert_eq!(resolve_profit_recipient(None, executor, executor), None);
    }

    #[test]
    fn transfer_all_calldata_encodes_selector_and_args() {
        let token = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let to = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let data = encode_transfer_all_calldata(token, to);
        let expected = IArbExecutor::transferAllCall { token, to }.abi_encode();
        assert_eq!(data.as_ref(), expected.as_slice());
        assert_eq!(&data.as_ref()[..4], &IArbExecutor::transferAllCall::SELECTOR);
    }
}
