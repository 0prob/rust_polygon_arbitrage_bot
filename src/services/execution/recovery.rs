use alloy::network::Ethereum;
use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use alloy::rpc::types::TransactionRequest;
use tracing::{info, warn};

use super::gas::u256_to_u128;
use super::nonce::NonceManager;
use super::receipt::ReceiptData;
use super::submit::{FEE_BUMP_BPS, SubmitFees, bump_fees};

#[derive(Debug)]
pub enum NonceRecoveryOutcome {
    /// Original or replacement tx mined (carries receipt).
    Mined(ReceiptData),
    /// Cancel tx (0-value self-transfer) accepted by the node.
    Cancelled(B256),
    /// Tx still pending after recovery attempt.
    StillPending,
    /// No longer in mempool — safe to reuse nonce bookkeeping.
    Dropped,
}

/// After a receipt timeout, attempt to determine tx fate and cancel if still pending.
pub async fn recover_after_receipt_timeout<P: Provider<Ethereum>>(
    provider: &P,
    nonce_mgr: &NonceManager,
    operator: Address,
    tx_hash: B256,
    nonce: u64,
    fees: &SubmitFees,
    gas_limit: u64,
) -> NonceRecoveryOutcome {
    if let Some(r) = provider
        .get_transaction_receipt(tx_hash)
        .await
        .ok()
        .flatten()
    {
        return NonceRecoveryOutcome::Mined(ReceiptData {
            success: r.status(),
            gas_used: r.gas_used,
            logs: r.logs().to_vec(),
        });
    }

    if provider
        .get_transaction_by_hash(tx_hash)
        .await
        .ok()
        .flatten()
        .is_none()
    {
        warn!(%tx_hash, nonce, "timed-out tx absent from node — treating as dropped");
        nonce_mgr.release(nonce);
        if nonce_mgr.resync(provider).await.is_err() {
            nonce_mgr.mark_stale(nonce);
        }
        return NonceRecoveryOutcome::Dropped;
    }

    let bumped = bump_fees(*fees, FEE_BUMP_BPS);
    let cancel_tx = TransactionRequest::default()
        .from(operator)
        .to(operator)
        .value(U256::ZERO)
        .nonce(nonce)
        .gas_limit(gas_limit.min(21_000))
        .max_fee_per_gas(u256_to_u128(bumped.max_fee_per_gas).unwrap_or(u128::MAX))
        .max_priority_fee_per_gas(
            u256_to_u128(bumped.max_priority_fee_per_gas).unwrap_or(u128::MAX),
        );

    match provider.send_transaction(cancel_tx).await {
        Ok(pending) => {
            let hash = *pending.tx_hash();
            info!(%tx_hash, cancel_hash = %hash, nonce, "sent cancel replacement tx");
            nonce_mgr.mark_stale(nonce);
            NonceRecoveryOutcome::Cancelled(hash)
        }
        Err(e) => {
            warn!(%tx_hash, nonce, error = %e, "cancel replacement failed — nonce marked stale");
            nonce_mgr.mark_stale(nonce);
            NonceRecoveryOutcome::StillPending
        }
    }
}
