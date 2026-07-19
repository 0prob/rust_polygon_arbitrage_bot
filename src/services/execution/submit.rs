use alloy::network::Ethereum;
use alloy::primitives::{B256, Bytes, U256};
use alloy::providers::Provider;
use alloy::rpc::types::TransactionRequest;
use anyhow::{Result, anyhow};

use super::candidate::CandidateExecution;
use super::gas::{compute_conservative_gas_price, u256_to_u128};
use super::gas_oracle::GasOracle;
use super::nonce::NonceManager;
use super::private_submit::{PrivateSubmitConfig, sign_tx_to_raw, submit_signed_raw};
use super::rpc_errors::{SubmitAction, classify_submit_error, extract_tx_hash_from_error};

/// ponytail: 15% fee bump on resubmit. Standard EIP-1559 bump.
pub const FEE_BUMP_BPS: u64 = 1500;
const MAX_SUBMIT_ATTEMPTS: u32 = 3;
pub use super::gas::MIN_PRIORITY_FEE_PER_GAS;
/// ponytail: cap profit-derived priority fee boost at 200 gwei.
/// 100 gwei was too conservative during Polygon MEV competition.
use crate::services::execution::profit::profit_priority_uplift_wei;

#[derive(Debug, Clone, Copy)]
pub struct SubmitFees {
    pub max_fee_per_gas: U256,
    pub max_priority_fee_per_gas: U256,
}

#[must_use]
pub fn bump_fees(fees: SubmitFees, bump_bps: u64) -> SubmitFees {
    let num = U256::from(10_000u64 + bump_bps);
    SubmitFees {
        max_fee_per_gas: (fees.max_fee_per_gas * num) / U256::from(10_000u64),
        max_priority_fee_per_gas: (fees.max_priority_fee_per_gas * num) / U256::from(10_000u64),
    }
}

pub fn resolve_submit_fees(gas_oracle: &GasOracle) -> Option<SubmitFees> {
    let snap = gas_oracle.loaded_snapshot()?;
    let priority_fee = snap.priority_fee.max(MIN_PRIORITY_FEE_PER_GAS);
    Some(SubmitFees {
        max_fee_per_gas: compute_conservative_gas_price(snap).max(snap.base_fee + priority_fee),
        max_priority_fee_per_gas: priority_fee,
    })
}

/// Blend oracle fees with a profit-proportional priority fee boost (wei per gas).
pub fn resolve_submit_fees_with_profit(
    gas_oracle: &GasOracle,
    expected_profit_matic_wei: U256,
    alpha_bps: u64,
    gas_limit: u64,
) -> Option<SubmitFees> {
    let snap = gas_oracle.loaded_snapshot()?;
    let max_fee_per_gas = compute_conservative_gas_price(snap);
    let priority_fee = snap.priority_fee.max(MIN_PRIORITY_FEE_PER_GAS);
    let mut fees = SubmitFees {
        max_fee_per_gas,
        max_priority_fee_per_gas: priority_fee,
    };

    if expected_profit_matic_wei.is_zero() || alpha_bps == 0 || gas_limit == 0 {
        return Some(fees);
    }

    let total_boost = profit_priority_uplift_wei(
        expected_profit_matic_wei,
        alpha_bps,
        gas_limit.min(u64::from(u32::MAX)) as u32,
    );
    let per_gas = total_boost / U256::from(gas_limit);
    fees.max_priority_fee_per_gas = fees.max_priority_fee_per_gas.max(per_gas);

    let min_max_fee = snap.base_fee + fees.max_priority_fee_per_gas;
    fees.max_fee_per_gas = fees.max_fee_per_gas.max(min_max_fee);

    Some(fees)
}

pub fn build_transaction_request(
    candidate: &CandidateExecution,
    nonce: u64,
    fees: &SubmitFees,
    gas_limit: u64,
) -> Result<TransactionRequest> {
    build_transaction_request_with_calldata(
        candidate,
        candidate.calldata.clone(),
        nonce,
        fees,
        gas_limit,
    )
}

fn build_transaction_request_with_calldata(
    candidate: &CandidateExecution,
    calldata: Bytes,
    nonce: u64,
    fees: &SubmitFees,
    gas_limit: u64,
) -> Result<TransactionRequest> {
    Ok(TransactionRequest::default()
        .to(candidate.target_address)
        .input(calldata.into())
        .value(candidate.value)
        .nonce(nonce)
        .max_fee_per_gas(u256_to_u128(fees.max_fee_per_gas)?)
        .max_priority_fee_per_gas(u256_to_u128(fees.max_priority_fee_per_gas)?)
        .gas_limit(gas_limit))
}

async fn submit_transaction<P: Provider<Ethereum>>(
    provider: &P,
    tx: TransactionRequest,
    private: Option<&PrivateSubmitConfig>,
) -> Result<B256> {
    if let Some(cfg) = private
        && cfg.mode != super::private_submit::PrivateSubmitMode::Standard
    {
        let chain_id = cfg.chain_id;
        let raw = sign_tx_to_raw(tx, &cfg.signer, chain_id).await?;
        submit_signed_raw(&raw, cfg).await
    } else {
        let pending = provider.send_transaction(tx).await?;
        Ok(*pending.tx_hash())
    }
}

pub async fn submit_live_candidate<P: Provider<Ethereum>>(
    provider: &P,
    candidate: &CandidateExecution,
    nonce: u64,
    fees: &SubmitFees,
    gas_limit: u64,
    private: Option<&PrivateSubmitConfig>,
) -> Result<B256> {
    let tx = build_transaction_request(candidate, nonce, fees, gas_limit)?;
    submit_transaction(provider, tx, private).await
}

/// Submit with classified RPC error recovery (resync, fee bump, already-known).
pub async fn submit_with_recovery<P: Provider<Ethereum>>(
    provider: &P,
    nonce_mgr: &NonceManager,
    candidate: &CandidateExecution,
    nonce: &mut u64,
    mut fees: SubmitFees,
    gas_limit: u64,
    private: Option<&PrivateSubmitConfig>,
) -> Result<B256> {
    let calldata = candidate.calldata.clone();
    let mut attempts = 0u32;
    loop {
        attempts += 1;
        let tx = build_transaction_request_with_calldata(
            candidate,
            calldata.clone(),
            *nonce,
            &fees,
            gas_limit,
        )?;
        match submit_transaction(provider, tx, private).await {
            Ok(hash) => return Ok(hash),
            Err(e) => {
                if attempts >= MAX_SUBMIT_ATTEMPTS {
                    return Err(e);
                }
                match classify_submit_error(&e) {
                    SubmitAction::ResyncAndRetry => {
                        nonce_mgr.release(*nonce);
                        nonce_mgr.resync(provider).await?;
                        *nonce = nonce_mgr.next_nonce()?;
                    }
                    SubmitAction::BumpFeesAndRetry => {
                        fees = bump_fees(fees, FEE_BUMP_BPS);
                    }
                    SubmitAction::AlreadyKnown => {
                        if let Some(hash) = extract_tx_hash_from_error(&e.to_string()) {
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

/// Replace a stuck transaction: same nonce, 12% higher fees.
/// ponytail: 12% bump per EIP-1559 best practice, re-evaluate if v3 blocks need finer control.
pub async fn submit_replacement<P: Provider<Ethereum>>(
    provider: &P,
    nonce_mgr: &NonceManager,
    candidate: &CandidateExecution,
    nonce: u64,
    fees: SubmitFees,
    gas_limit: u64,
    private: Option<&PrivateSubmitConfig>,
) -> Result<B256> {
    let bumped = bump_fees(fees, 1200);
    nonce_mgr.mark_stale(nonce);
    let replacement_nonce = nonce_mgr
        .replace_nonce(nonce)
        .ok_or_else(|| anyhow!("nonce {nonce} not available for replacement"))?;
    submit_live_candidate(
        provider,
        candidate,
        replacement_nonce,
        &bumped,
        gas_limit,
        private,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::execution::FeeSnapshot;

    #[test]
    fn bump_fees_scales_both_fee_fields() {
        let fees = SubmitFees {
            max_fee_per_gas: U256::from(100u64),
            max_priority_fee_per_gas: U256::from(20u64),
        };
        let bumped = bump_fees(fees, FEE_BUMP_BPS);
        assert_eq!(bumped.max_fee_per_gas, U256::from(115u64));
        assert_eq!(bumped.max_priority_fee_per_gas, U256::from(23u64));
    }

    #[test]
    fn submit_fee_resolution_respects_base_plus_priority_floor() {
        let oracle = GasOracle::default();
        oracle.set_fee_snapshot_for_test(FeeSnapshot {
            base_fee: U256::from(10u64),
            priority_fee: U256::from(2u64),
        });

        let fees = resolve_submit_fees_with_profit(&oracle, U256::from(1_000u64), 5_000, 100)
            .expect("snapshot should resolve");
        assert!(fees.max_priority_fee_per_gas >= U256::from(2u64));
        assert!(fees.max_fee_per_gas >= U256::from(12u64));
    }
}
