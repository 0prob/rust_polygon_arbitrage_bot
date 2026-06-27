use alloy::network::Ethereum;
use alloy::primitives::{B256, U256};
use alloy::providers::Provider;
use alloy::rpc::types::TransactionRequest;
use anyhow::{Result, anyhow};
use tracing::{info, instrument, warn};

use super::candidate::CandidateExecution;
use super::gas::{default_priority_fee_wei, u256_to_u128};
use super::gas_oracle::GasOracle;
use super::nonce::NonceManager;
use super::private_submit::{PrivateSubmitConfig, sign_tx_to_raw, submit_signed_raw};
use super::rpc_errors::{SubmitAction, classify_submit_error, extract_tx_hash_from_error};

pub const FEE_BUMP_BPS: u64 = 1500;
const MAX_SUBMIT_ATTEMPTS: u32 = 3;
/// Cap profit-derived priority fee boost (100 gwei).
const MAX_PROFIT_PRIORITY_FEE_WEI: u128 = 100_000_000_000;

#[derive(Debug, Clone, Copy)]
pub struct SubmitFees {
    pub max_fee_per_gas: U256,
    pub max_priority_fee_per_gas: U256,
}

pub fn bump_fees(fees: SubmitFees, bump_bps: u64) -> SubmitFees {
    let num = U256::from(10_000u64 + bump_bps);
    SubmitFees {
        max_fee_per_gas: (fees.max_fee_per_gas * num) / U256::from(10_000u64),
        max_priority_fee_per_gas: (fees.max_priority_fee_per_gas * num) / U256::from(10_000u64),
    }
}

pub fn resolve_submit_fees(gas_oracle: &GasOracle) -> SubmitFees {
    let max_fee_per_gas = gas_oracle.conservative_gas_price();
    let priority_fee = gas_oracle
        .snapshot()
        .map(|s| s.priority_fee)
        .unwrap_or_else(default_priority_fee_wei);
    SubmitFees {
        max_fee_per_gas,
        max_priority_fee_per_gas: priority_fee,
    }
}

/// Blend oracle fees with a profit-proportional priority fee boost (wei per gas).
pub fn resolve_submit_fees_with_profit(
    gas_oracle: &GasOracle,
    expected_profit_matic_wei: U256,
    alpha_bps: u64,
    gas_limit: u64,
) -> SubmitFees {
    let mut fees = resolve_submit_fees(gas_oracle);
    if expected_profit_matic_wei.is_zero() || alpha_bps == 0 || gas_limit == 0 {
        return fees;
    }

    let total_boost = (expected_profit_matic_wei * U256::from(alpha_bps)) / U256::from(10_000u64);
    let per_gas = total_boost / U256::from(gas_limit);
    let capped = per_gas.min(U256::from(MAX_PROFIT_PRIORITY_FEE_WEI));
    fees.max_priority_fee_per_gas = fees.max_priority_fee_per_gas.max(capped);

    if let Some(snap) = gas_oracle.snapshot() {
        let min_max_fee = snap.base_fee + fees.max_priority_fee_per_gas;
        fees.max_fee_per_gas = fees.max_fee_per_gas.max(min_max_fee);
    }

    fees
}

pub fn build_transaction_request(
    candidate: &CandidateExecution,
    nonce: u64,
    fees: &SubmitFees,
    gas_limit: u64,
) -> Result<TransactionRequest> {
    Ok(TransactionRequest::default()
        .to(candidate.target_address)
        .input(candidate.calldata.clone().into())
        .value(candidate.value)
        .nonce(nonce)
        .max_fee_per_gas(u256_to_u128(fees.max_fee_per_gas)?)
        .max_priority_fee_per_gas(u256_to_u128(fees.max_priority_fee_per_gas)?)
        .gas_limit(gas_limit))
}

#[instrument(
    skip(provider, candidate, fees, private),
    fields(
        route_fingerprint = candidate.route_fingerprint,
        nonce,
        gas_limit,
        tx_hash = tracing::field::Empty,
    )
)]
pub async fn submit_live_candidate<P: Provider<Ethereum>>(
    provider: &P,
    candidate: &CandidateExecution,
    nonce: u64,
    fees: &SubmitFees,
    gas_limit: u64,
    private: Option<&PrivateSubmitConfig>,
) -> Result<B256> {
    let tx = build_transaction_request(candidate, nonce, fees, gas_limit)?;

    let hash = if let Some(cfg) = private
        && cfg.mode != super::private_submit::PrivateSubmitMode::Standard
    {
        let chain_id = cfg.chain_id;
        let raw = sign_tx_to_raw(tx, &cfg.signer, chain_id).await?;
        submit_signed_raw(&raw, cfg).await?
    } else {
        let pending = provider.send_transaction(tx).await?;
        *pending.tx_hash()
    };

    tracing::Span::current().record("tx_hash", tracing::field::display(&hash));
    info!(
        route = candidate.route_fingerprint,
        tx_hash = %hash,
        nonce,
        gas_limit,
        max_fee_per_gas = %fees.max_fee_per_gas,
        max_priority_fee_per_gas = %fees.max_priority_fee_per_gas,
        "live transaction submitted"
    );
    Ok(hash)
}

/// Submit with classified RPC error recovery (resync, fee bump, already-known).
pub async fn submit_with_recovery<P: Provider<Ethereum>>(
    provider: &P,
    nonce_mgr: &NonceManager,
    candidate: &CandidateExecution,
    mut nonce: u64,
    mut fees: SubmitFees,
    gas_limit: u64,
    private: Option<&PrivateSubmitConfig>,
) -> Result<B256> {
    let mut attempts = 0u32;
    loop {
        attempts += 1;
        match submit_live_candidate(provider, candidate, nonce, &fees, gas_limit, private).await {
            Ok(hash) => return Ok(hash),
            Err(e) => {
                if attempts >= MAX_SUBMIT_ATTEMPTS {
                    return Err(e);
                }
                match classify_submit_error(&e) {
                    SubmitAction::ResyncAndRetry => {
                        warn!(nonce, error = %e, "nonce too low — resyncing");
                        nonce_mgr.release(nonce);
                        nonce_mgr.resync(provider).await?;
                        nonce = nonce_mgr.next_nonce()?;
                    }
                    SubmitAction::BumpFeesAndRetry => {
                        warn!(nonce, error = %e, "fee bump and retry");
                        fees = bump_fees(fees, FEE_BUMP_BPS);
                    }
                    SubmitAction::AlreadyKnown => {
                        if let Some(hash) = extract_tx_hash_from_error(&e.to_string()) {
                            info!(%hash, nonce, "transaction already known");
                            return Ok(hash);
                        }
                        return Err(anyhow!("transaction already known but hash unavailable"));
                    }
                    SubmitAction::InsufficientFunds => return Err(e),
                    SubmitAction::Fail(msg) => return Err(anyhow!(msg)),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::execution::gas_oracle::GasOracle;

    #[test]
    fn profit_boost_increases_priority_fee() {
        let oracle = GasOracle::default();
        let base = resolve_submit_fees(&oracle);
        let boosted = resolve_submit_fees_with_profit(
            &oracle,
            U256::from(10_000_000_000_000_000_000u64),
            5_000,
            500_000,
        );
        assert!(boosted.max_priority_fee_per_gas > base.max_priority_fee_per_gas);
    }
}
