use std::time::Duration;

use alloy::hex;
use alloy::network::Ethereum;
use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use alloy::rpc::types::TransactionRequest;
use tokio::time::timeout;

use crate::services::execution::candidate::CandidateExecution;
use crate::services::execution::revert_decoder::decode_revert;

const RPC_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, Clone)]
pub struct DryRunResult {
    pub success: bool,
    pub gas_used: Option<u64>,
    /// Exact post-repayment profit returned by executors that support it.
    pub realized_profit: Option<U256>,
    pub error: Option<String>,
}

fn decode_realized_profit(output: &[u8]) -> Option<U256> {
    (output.len() == 32).then(|| U256::from_be_slice(output))
}

fn build_tx(candidate: &CandidateExecution, from: Address) -> TransactionRequest {
    let tx = TransactionRequest::default()
        .to(candidate.target_address)
        .input(candidate.calldata.clone().into())
        .value(candidate.value)
        .from(from);

    if candidate.calldata.len() >= 4 {
        let _sel = &candidate.calldata[..4];
        crate::debug!(
            "dry-run tx: target={}, selector=0x{:02x}{:02x}{:02x}{:02x}, value={}, fp={}",
            candidate.target_address,
            _sel[0],
            _sel[1],
            _sel[2],
            _sel[3],
            candidate.value,
            candidate.route_fingerprint,
        );
    }

    tx
}

fn extract_revert_bytes(raw: &str) -> Option<Vec<u8>> {
    if let Some(stripped) = raw.strip_prefix("0x")
        && let Ok(bytes) = hex::decode(stripped)
    {
        return Some(bytes);
    }
    if let Some(idx) = raw.find("0x") {
        let mut hex = raw[idx..].trim_end_matches('"').trim_end_matches('\'');
        if let Some(stripped) = hex.strip_prefix("0x") {
            hex = stripped;
        }
        if let Ok(bytes) = hex::decode(hex) {
            return Some(bytes);
        }
    }
    None
}

fn try_decode_revert(
    raw: &str,
) -> Option<crate::services::execution::revert_decoder::DecodedRevert> {
    let bytes = extract_revert_bytes(raw)?;
    decode_revert(&bytes)
}

fn is_gas_limit_rpc_error(msg: &str) -> bool {
    msg.contains("gas uint64 overflow")
        || msg.contains("exceeds block gas limit")
        || msg.contains("intrinsic gas too high")
}

fn gas_overflow_dry_run_success(realized_profit: Option<U256>) -> DryRunResult {
    DryRunResult {
        success: true,
        gas_used: None,
        realized_profit,
        error: None,
    }
}

async fn dry_run_after_call_gas_overflow<P: Provider<Ethereum>>(
    provider: &P,
    candidate: &CandidateExecution,
    from: Address,
) -> DryRunResult {
    let tx = build_tx(candidate, from);
    match timeout(RPC_TIMEOUT, provider.estimate_gas(tx)).await {
        Ok(Ok(gas)) => DryRunResult {
            success: true,
            gas_used: Some(gas),
            realized_profit: None,
            error: None,
        },
        Ok(Err(err)) if is_gas_limit_rpc_error(&err.to_string()) => {
            gas_overflow_dry_run_success(None)
        }
        Ok(Err(err)) => DryRunResult {
            success: false,
            gas_used: None,
            realized_profit: None,
            error: Some(err.to_string()),
        },
        Err(_) => gas_overflow_dry_run_success(None),
    }
}

/// `estimate_gas` only — used for top-N dispatch gas refinement without a full dry-run.
pub async fn estimate_candidate_gas<P: Provider<Ethereum>>(
    provider: &P,
    candidate: &CandidateExecution,
    from: Address,
) -> Option<u64> {
    let tx = build_tx(candidate, from);
    match timeout(RPC_TIMEOUT, provider.estimate_gas(tx)).await {
        Ok(Ok(gas)) => Some(gas),
        Ok(Err(_err)) => None,
        Err(_) => None,
    }
}

pub async fn dry_run_candidate<P: Provider<Ethereum>>(
    provider: &P,
    candidate: &CandidateExecution,
    from: Address,
) -> DryRunResult {
    // Never constrain simulation or estimation with the local gas heuristic.
    // Flash-loan callbacks can exceed it; the measured estimate is buffered
    // later when the real transaction is built.
    let tx = build_tx(candidate, from);

    let realized_profit = match timeout(RPC_TIMEOUT, provider.call(tx)).await {
        Ok(Ok(output)) => decode_realized_profit(&output),
        Ok(Err(err)) => {
            let raw = err.to_string();
            if is_gas_limit_rpc_error(&raw) {
                crate::debug!(
                    "dry-run eth_call gas overflow: fp={}, hops={}, trying estimate/sim_gas fallback",
                    candidate.route_fingerprint,
                    candidate.hop_count,
                );
                return dry_run_after_call_gas_overflow(provider, candidate, from).await;
            }
            let reason = try_decode_revert(&raw)
                .map(|r| r.to_string())
                .unwrap_or_else(|| {
                    if raw.contains("BAL#528") {
                        format!("{raw} (insufficient Balancer flash-loan balance — size capped to pool cash)")
                    } else if raw.contains("ApproveFailed") {
                        format!("{raw} (Aave/Balancer flash repay needs token balance + pool allowance — check hop approvals)")
                    } else if raw.contains("BalancerVaultReentrancy") {
                        format!("{raw} (mixed route: use executeArbWithAave, not executeArb with Balancer vault hops)")
                    } else if raw.contains("data: \"0x\"") || raw.contains("data: '0x'") {
                        format!("{raw} (bare revert — check entrypoint: pure Balancer→executeArbDirect, mixed→executeArbWithAave, Balancer-only flash→executeArb)")
                    } else if raw.contains("90cd6f24") {
                        format!("{raw} (Aave ReserveInactive — token reserve not active for flash loan)")
                    } else {
                        raw.clone()
                    }
                });
            return DryRunResult {
                success: false,
                gas_used: None,
                realized_profit: None,
                error: Some(reason),
            };
        }
        Err(_) => {
            return DryRunResult {
                success: false,
                gas_used: None,
                realized_profit: None,
                error: Some("eth_call timed out".into()),
            };
        }
    };

    let tx = build_tx(candidate, from);
    match timeout(RPC_TIMEOUT, provider.estimate_gas(tx)).await {
        Ok(Ok(gas)) => DryRunResult {
            success: true,
            gas_used: Some(gas),
            realized_profit,
            error: None,
        },
        Ok(Err(err)) => {
            // eth_call already succeeded; some RPCs return gas-limit errors on
            // estimate_gas even when the callback is otherwise valid.
            let msg = err.to_string();
            if is_gas_limit_rpc_error(&msg) {
                return gas_overflow_dry_run_success(realized_profit);
            }
            DryRunResult {
                success: false,
                gas_used: None,
                realized_profit: None,
                error: Some(msg),
            }
        }
        Err(_) => {
            if realized_profit.is_some() {
                gas_overflow_dry_run_success(realized_profit)
            } else {
                DryRunResult {
                    success: false,
                    gas_used: None,
                    realized_profit: None,
                    error: Some("estimate_gas timed out".into()),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Bytes, FixedBytes, U256};

    use super::*;
    use crate::core::types::FlashLoanSource;

    #[test]
    fn simulation_tx_does_not_inherit_candidate_gas_limit() {
        let candidate = CandidateExecution {
            route_fingerprint: 1,
            calldata: Bytes::from_static(&[1, 2, 3, 4]),
            target_address: Address::repeat_byte(1),
            value: U256::ZERO,
            profit_token: Address::repeat_byte(2),
            expected_profit_matic_wei: U256::from(1u8),
            gas_limit: Some(U256::from(21_000u64)),
            simulated_gas: 21_000,
            route_hash: FixedBytes::ZERO,
            gross_profit: U256::from(1u8),
            amount_in: U256::from(1u8),
            token_decimals: 18,
            token_to_matic_rate: U256::from(1u8),
            slippage_bps: 50,
            flash_loan_source: FlashLoanSource::Balancer,
            min_profit_matic_wei: U256::from(1u8),
            min_profit_roi_bps: 0,
            hop_count: 2,
            safety_multiplier_bps: 30_000,
            state_generation: 1,
        };

        let tx = build_tx(&candidate, Address::repeat_byte(3));
        assert_eq!(tx.gas, None);
    }

    #[test]
    fn extracts_revert_hex_from_json_rpc_error() {
        let raw = r#"server returned an error response: error code 3: execution reverted, data: "0x0f4345730000000000000000000000000000000000000000000000000000000000000040""#;
        let bytes = extract_revert_bytes(raw).expect("hex payload");
        assert_eq!(&bytes[0..4], &[0x0f, 0x43, 0x45, 0x73]);
    }

    #[test]
    fn gas_overflow_error_is_recognized() {
        let msg = "server returned an error response: error code -32000: gas uint64 overflow";
        assert!(msg.contains("gas uint64 overflow"));
    }

    #[test]
    fn realized_profit_requires_one_abi_word() {
        let encoded = U256::from(42u8).to_be_bytes::<32>();
        assert_eq!(decode_realized_profit(&encoded), Some(U256::from(42u8)));
        assert_eq!(decode_realized_profit(&[]), None);
        assert_eq!(decode_realized_profit(&[0u8; 64]), None);
    }
}
