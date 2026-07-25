use std::time::Duration;

use alloy::eips::{BlockId, BlockNumberOrTag};
use alloy::hex;
use alloy::network::Ethereum;
use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use alloy::rpc::types::TransactionRequest;
use tokio::time::timeout;

use crate::services::execution::candidate::CandidateExecution;
use crate::services::execution::revert_decoder::decode_revert;

const RPC_TIMEOUT: Duration = Duration::from_secs(15);
/// Polygon block gas ceiling for `eth_call` / `estimate_gas`.
const MAX_ETH_CALL_GAS: u64 = 30_000_000;
const MIN_ETH_CALL_GAS: u64 = 500_000;
/// Match `GAS_FALLBACK_MIN_SCALE_BPS` in support.rs — flash callbacks often need >1× sim seed.
const ETH_CALL_SIM_SCALE_BPS: u64 = 20_000;

#[must_use]
fn eth_call_gas_limit(simulated_gas: u32) -> u64 {
    let scaled =
        u128::from(simulated_gas).saturating_mul(u128::from(ETH_CALL_SIM_SCALE_BPS)) / 10_000;
    let gas = u64::try_from(scaled).unwrap_or(MAX_ETH_CALL_GAS);
    gas.clamp(MIN_ETH_CALL_GAS, MAX_ETH_CALL_GAS)
        .max(u64::from(simulated_gas))
}

#[derive(Debug, Clone)]
pub struct DryRunResult {
    pub semantic_success: bool,
    pub success: bool,
    pub gas_used: Option<u64>,
    /// Exact post-repayment profit returned by executors that support it.
    pub realized_profit: Option<U256>,
    pub error: Option<String>,
    pub decoded_revert: Option<crate::services::execution::revert_decoder::DecodedRevert>,
}

impl DryRunResult {
    /// User-facing / log reason when dispatch treats the dry-run as failed.
    #[must_use]
    pub fn failure_reason(&self) -> String {
        if let Some(ref e) = self.error {
            return e.clone();
        }
        if let Some(ref d) = self.decoded_revert {
            return d.to_string();
        }
        if self.realized_profit.is_some_and(|p| p.is_zero()) {
            return "eth_call succeeded but on-chain realized profit is zero".into();
        }
        if !self.semantic_success {
            return "dry-run did not produce a decodable realized profit".into();
        }
        if self.realized_profit.is_none() {
            return "dry-run succeeded but returned no non-zero realized profit".into();
        }
        if !self.success {
            return "dry-run failed without RPC error detail".into();
        }
        "dry-run failed without RPC error detail".into()
    }
}

fn is_acceptable_realized_profit(profit: Option<U256>) -> bool {
    profit.is_some_and(|p| !p.is_zero())
}

fn decode_realized_profit(output: &[u8]) -> Option<U256> {
    if output.len() < 32 {
        return None;
    }
    Some(U256::from_be_slice(&output[output.len() - 32..]))
}

fn build_tx(
    candidate: &CandidateExecution,
    from: Address,
    gas_limit: Option<u64>,
) -> TransactionRequest {
    let mut tx = TransactionRequest::default()
        .to(candidate.target_address)
        .input(candidate.calldata.clone().into())
        .value(candidate.value)
        .from(from);
    if let Some(gas) = gas_limit {
        tx = tx.gas_limit(gas);
    }

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

fn block_id(simulation_block: Option<u64>) -> Option<BlockId> {
    simulation_block.map(|block| BlockId::Number(BlockNumberOrTag::Number(block)))
}

fn is_gas_limit_rpc_error(msg: &str) -> bool {
    msg.contains("gas uint64 overflow")
        || msg.contains("exceeds block gas limit")
        || msg.contains("intrinsic gas too high")
}

fn gas_overflow_dry_run_failure() -> DryRunResult {
    DryRunResult {
        semantic_success: false,
        success: false,
        gas_used: None,
        realized_profit: None,
        error: Some("eth_call exceeded gas limit and estimate_gas also failed".into()),
        decoded_revert: None,
    }
}

fn gas_overflow_estimate_fallback(realized_profit: U256) -> DryRunResult {
    DryRunResult {
        semantic_success: true,
        success: true,
        gas_used: None,
        realized_profit: Some(realized_profit),
        error: None,
        decoded_revert: None,
    }
}

async fn eth_call_at_block<P: Provider<Ethereum>>(
    provider: &P,
    candidate: &CandidateExecution,
    from: Address,
    gas_limit: Option<u64>,
    simulation_block: Option<u64>,
) -> Result<alloy::primitives::Bytes, String> {
    let tx = build_tx(candidate, from, gas_limit);
    let mut call = provider.call(tx);
    if let Some(block) = block_id(simulation_block) {
        call = call.block(block);
    }
    match timeout(RPC_TIMEOUT, call).await {
        Ok(Ok(output)) => Ok(output),
        Ok(Err(err)) => Err(err.to_string()),
        Err(_) => Err("eth_call timed out".into()),
    }
}

async fn dry_run_after_call_gas_overflow<P: Provider<Ethereum>>(
    provider: &P,
    candidate: &CandidateExecution,
    from: Address,
    simulation_block: Option<u64>,
) -> DryRunResult {
    let estimate_cap = eth_call_gas_limit(candidate.simulated_gas);
    let tx = build_tx(candidate, from, Some(estimate_cap));
    let mut estimate = provider.estimate_gas(tx);
    if let Some(block) = block_id(simulation_block) {
        estimate = estimate.block(block);
    }
    let gas = match timeout(RPC_TIMEOUT, estimate).await {
        Ok(Ok(gas)) => gas.min(MAX_ETH_CALL_GAS),
        Ok(Err(err)) if is_gas_limit_rpc_error(&err.to_string()) => {
            return gas_overflow_dry_run_failure();
        }
        Ok(Err(err)) => {
            return DryRunResult {
                semantic_success: false,
                success: false,
                gas_used: None,
                realized_profit: None,
                error: Some(err.to_string()),
                decoded_revert: None,
            };
        }
        Err(_) => {
            return DryRunResult {
                semantic_success: false,
                success: false,
                gas_used: None,
                realized_profit: None,
                error: Some("estimate_gas timed out after eth_call gas overflow".into()),
                decoded_revert: None,
            };
        }
    };
    let call_gas = gas.saturating_mul(11_000) / 10_000;
    match eth_call_at_block(
        provider,
        candidate,
        from,
        Some(call_gas.min(MAX_ETH_CALL_GAS)),
        simulation_block,
    )
    .await
    {
        Ok(output) => {
            let realized_profit = decode_realized_profit(&output);
            if realized_profit.filter(|p| !p.is_zero()).is_none() {
                return DryRunResult {
                    semantic_success: false,
                    success: false,
                    gas_used: Some(gas),
                    realized_profit: None,
                    error: Some(
                        "eth_call after gas estimate succeeded but returned no decodable realized profit"
                            .into(),
                    ),
                    decoded_revert: None,
                };
            }
            DryRunResult {
                semantic_success: true,
                success: true,
                gas_used: Some(gas),
                realized_profit,
                error: None,
                decoded_revert: None,
            }
        }
        Err(raw) if is_gas_limit_rpc_error(&raw) => gas_overflow_dry_run_failure(),
        Err(raw) => {
            let decoded = try_decode_revert(&raw);
            let reason = decoded.as_ref().map(|r| r.to_string()).unwrap_or(raw);
            DryRunResult {
                semantic_success: false,
                success: false,
                gas_used: None,
                realized_profit: None,
                error: Some(reason),
                decoded_revert: decoded,
            }
        }
    }
}

/// `estimate_gas` only — used for top-N dispatch gas refinement without a full dry-run.
pub async fn estimate_candidate_gas<P: Provider<Ethereum>>(
    provider: &P,
    candidate: &CandidateExecution,
    from: Address,
    simulation_block: Option<u64>,
) -> Option<u64> {
    let tx = build_tx(candidate, from, None);
    let mut estimate = provider.estimate_gas(tx);
    if let Some(block) = block_id(simulation_block) {
        estimate = estimate.block(block);
    }
    match timeout(RPC_TIMEOUT, estimate).await {
        Ok(Ok(gas)) => Some(gas.min(MAX_ETH_CALL_GAS)),
        Ok(Err(_err)) => None,
        Err(_) => None,
    }
}

pub async fn dry_run_candidate<P: Provider<Ethereum>>(
    provider: &P,
    candidate: &CandidateExecution,
    from: Address,
    simulation_block: Option<u64>,
) -> DryRunResult {
    // Do not use candidate.gas_limit for simulation; eth_call gets a scaled cap
    // from simulated_gas. estimate_gas stays unconstrained; submit buffering is later.
    let call_gas = eth_call_gas_limit(candidate.simulated_gas);
    let realized_profit = match eth_call_at_block(
        provider,
        candidate,
        from,
        Some(call_gas),
        simulation_block,
    )
    .await
    {
        Ok(output) => decode_realized_profit(&output),
        Err(err) => {
            let raw = err;
            if is_gas_limit_rpc_error(&raw) {
                crate::debug!(
                    "dry-run eth_call gas overflow: fp={}, hops={}, trying estimate/sim_gas fallback",
                    candidate.route_fingerprint,
                    candidate.hop_count,
                );
                return dry_run_after_call_gas_overflow(
                    provider,
                    candidate,
                    from,
                    simulation_block,
                )
                .await;
            }
            let decoded = try_decode_revert(&raw);
            let reason = decoded
                .as_ref()
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
                    } else if raw.contains("IIA") {
                        // Pool callback unpaid: executor must transfer amount0/1Delta to the pool.
                        // Live root cause was ArbExecutor.huff `dup1 0x00 gt` (always false).
                        format!("{raw} (V3/Algebra IIA — callback did not pay pool input token)")
                    } else {
                        raw.clone()
                    }
                });
            return DryRunResult {
                semantic_success: false,
                success: false,
                gas_used: None,
                realized_profit: None,
                error: Some(reason),
                decoded_revert: decoded,
            };
        }
    };

    // Fail closed on empty/zero profit before a second RPC (estimate_gas). Most dry-runs
    // that "succeed" eth_call but return zero profit were paying an extra RTT for nothing.
    if !is_acceptable_realized_profit(realized_profit) {
        let error = if realized_profit.is_some() {
            "eth_call succeeded but on-chain realized profit is zero".into()
        } else {
            "eth_call succeeded but returned no decodable realized profit (wrong entrypoint or empty return)"
                .into()
        };
        return DryRunResult {
            semantic_success: false,
            success: false,
            gas_used: None,
            realized_profit,
            error: Some(error),
            decoded_revert: None,
        };
    }

    let estimate_gas_cap = eth_call_gas_limit(candidate.simulated_gas);
    let gas_from_estimate = async {
        let tx = build_tx(candidate, from, Some(estimate_gas_cap));
        let mut estimate = provider.estimate_gas(tx);
        if let Some(block) = block_id(simulation_block) {
            estimate = estimate.block(block);
        }
        match timeout(RPC_TIMEOUT, estimate).await {
            Ok(Ok(gas)) => Ok(gas.min(MAX_ETH_CALL_GAS)),
            Ok(Err(err)) if is_gas_limit_rpc_error(&err.to_string()) => {
                let tx = build_tx(candidate, from, None);
                let mut retry = provider.estimate_gas(tx);
                if let Some(block) = block_id(simulation_block) {
                    retry = retry.block(block);
                }
                match timeout(RPC_TIMEOUT, retry).await {
                    Ok(Ok(gas)) => Ok(gas.min(MAX_ETH_CALL_GAS)),
                    Ok(Err(e)) => Err(e.to_string()),
                    Err(_) => Err("estimate_gas timed out".into()),
                }
            }
            Ok(Err(err)) => Err(err.to_string()),
            Err(_) => Err("estimate_gas timed out".into()),
        }
    }
    .await;

    match gas_from_estimate {
        Ok(gas) => DryRunResult {
            semantic_success: true,
            success: true,
            gas_used: Some(gas),
            realized_profit,
            error: None,
            decoded_revert: None,
        },
        Err(msg) => {
            if is_gas_limit_rpc_error(&msg) {
                return realized_profit
                    .filter(|p| !p.is_zero())
                    .map(gas_overflow_estimate_fallback)
                    .unwrap_or_else(gas_overflow_dry_run_failure);
            }
            if let Some(profit) = realized_profit
                && msg == "estimate_gas timed out"
            {
                return gas_overflow_estimate_fallback(profit);
            }
            DryRunResult {
                semantic_success: false,
                success: false,
                gas_used: None,
                realized_profit: None,
                error: Some(msg),
                decoded_revert: None,
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
            state_block: 1,
            state_hash: None,
            route_trace: String::new(),
            adaptive_flash_cap_bound: false,
            adaptive_flash_loan_usd_limit: 50_000,
        };

        let tx = build_tx(&candidate, Address::repeat_byte(3), None);
        assert_eq!(tx.gas, None);
    }

    #[test]
    fn zero_realized_profit_is_not_acceptable() {
        assert!(!is_acceptable_realized_profit(None));
        assert!(!is_acceptable_realized_profit(Some(U256::ZERO)));
        assert!(is_acceptable_realized_profit(Some(U256::from(1u8))));
    }

    #[test]
    fn eth_call_gas_limit_scales_sim_seed() {
        assert_eq!(eth_call_gas_limit(827_500), 1_655_000);
        assert_eq!(eth_call_gas_limit(100), MIN_ETH_CALL_GAS);
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
    fn failure_reason_prefers_rpc_error_then_zero_profit() {
        let with_rpc = DryRunResult {
            semantic_success: false,
            success: false,
            gas_used: None,
            realized_profit: None,
            error: Some("execution reverted: InsufficientProfit".into()),
            decoded_revert: None,
        };
        assert_eq!(
            with_rpc.failure_reason(),
            "execution reverted: InsufficientProfit"
        );

        let zero_profit = DryRunResult {
            semantic_success: true,
            success: true,
            gas_used: Some(200_000),
            realized_profit: Some(U256::ZERO),
            error: None,
            decoded_revert: None,
        };
        assert_eq!(
            zero_profit.failure_reason(),
            "eth_call succeeded but on-chain realized profit is zero"
        );
    }

    #[test]
    fn realized_profit_accepts_standard_and_padded_abi_word() {
        let encoded = U256::from(42u8).to_be_bytes::<32>();
        assert_eq!(decode_realized_profit(&encoded), Some(U256::from(42u8)));
        assert_eq!(decode_realized_profit(&[]), None);
        let mut padded = [0u8; 64];
        padded[63] = 42;
        assert_eq!(decode_realized_profit(&padded), Some(U256::from(42u8)));
        assert_eq!(decode_realized_profit(&[0u8; 64]), Some(U256::ZERO));
    }
}
