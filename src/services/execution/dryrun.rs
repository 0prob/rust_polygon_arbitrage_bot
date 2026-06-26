use std::time::Duration;

use alloy::network::Ethereum;
use alloy::primitives::Address;
use alloy::providers::Provider;
use alloy::rpc::types::TransactionRequest;
use tokio::time::timeout;
use tracing::{debug, instrument, warn};

use crate::services::execution::candidate::CandidateExecution;
use crate::services::execution::rpc_errors::classify_submit_error;

const RPC_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, Clone)]
pub struct DryRunResult {
    pub success: bool,
    pub gas_used: Option<u64>,
    pub error: Option<String>,
}

fn build_tx(candidate: &CandidateExecution, from: Address) -> TransactionRequest {
    let mut tx = TransactionRequest::default()
        .to(candidate.target_address)
        .input(candidate.calldata.clone().into())
        .value(candidate.value)
        .from(from);

    if let Some(limit) = candidate.gas_limit {
        tx = tx.gas_limit(limit.to::<u64>());
    }
    tx
}

#[instrument(
    skip(provider, candidate),
    fields(
        route_fingerprint = candidate.route_fingerprint,
        target = %candidate.target_address,
        gas_used = tracing::field::Empty,
        success = tracing::field::Empty,
    )
)]
/// `estimate_gas` only — used for top-N dispatch gas refinement without a full dry-run.
pub async fn estimate_candidate_gas<P: Provider<Ethereum>>(
    provider: &P,
    candidate: &CandidateExecution,
    from: Address,
) -> Option<u64> {
    let tx = build_tx(candidate, from);
    match timeout(RPC_TIMEOUT, provider.estimate_gas(tx)).await {
        Ok(Ok(gas)) => Some(gas),
        Ok(Err(err)) => {
            debug!(
                route = candidate.route_fingerprint,
                error = %err,
                "estimate_gas failed"
            );
            None
        }
        Err(_) => {
            debug!(
                route = candidate.route_fingerprint,
                "estimate_gas timed out"
            );
            None
        }
    }
}

pub async fn dry_run_candidate<P: Provider<Ethereum>>(
    provider: &P,
    candidate: &CandidateExecution,
    from: Address,
) -> DryRunResult {
    let tx = build_tx(candidate, from);

    match timeout(RPC_TIMEOUT, provider.call(tx.clone())).await {
        Ok(Ok(_)) => {}
        Ok(Err(err)) => {
            warn!(
                route = candidate.route_fingerprint,
                error = %err,
                "dry-run eth_call failed"
            );
            tracing::Span::current().record("success", false);
            return DryRunResult {
                success: false,
                gas_used: None,
                error: Some(err.to_string()),
            };
        }
        Err(_) => {
            tracing::Span::current().record("success", false);
            return DryRunResult {
                success: false,
                gas_used: None,
                error: Some("eth_call timed out".into()),
            };
        }
    }

    match timeout(RPC_TIMEOUT, provider.estimate_gas(tx)).await {
        Ok(Ok(gas)) => {
            tracing::Span::current().record("gas_used", gas);
            tracing::Span::current().record("success", true);
            DryRunResult {
                success: true,
                gas_used: Some(gas),
                error: None,
            }
        }
        Ok(Err(err)) => {
            let action = classify_submit_error(&err);
            warn!(
                route = candidate.route_fingerprint,
                error = %err,
                ?action,
                "dry-run estimate_gas failed"
            );
            tracing::Span::current().record("success", false);
            DryRunResult {
                success: false,
                gas_used: None,
                error: Some(err.to_string()),
            }
        }
        Err(_) => {
            tracing::Span::current().record("success", false);
            DryRunResult {
                success: false,
                gas_used: None,
                error: Some("estimate_gas timed out".into()),
            }
        }
    }
}
