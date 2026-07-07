use alloy::primitives::{Address, B256, U256};
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;

use crate::abis::IERC20;
use crate::core::constants::BPS_SCALE;
use crate::core::types::Edge;
use crate::core::types::FlashLoanSource;
use crate::pipeline::arena::StateArena;
use crate::pipeline::local_sim::simulate_route_minimal;
use crate::pipeline::types::MinimalSimResult;

// --- flash_policy ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlashLoanPolicy {
    Auto,
    BalancerOnly,
    AaveOnly,
}

#[must_use]
pub fn parse_flash_policy(raw: &str) -> FlashLoanPolicy {
    let s = raw.trim();
    if s.eq_ignore_ascii_case("auto") {
        FlashLoanPolicy::Auto
    } else if s.eq_ignore_ascii_case("aave") || s.eq_ignore_ascii_case("aave_v3") {
        FlashLoanPolicy::AaveOnly
    } else {
        FlashLoanPolicy::BalancerOnly
    }
}

#[must_use]
pub fn hf_eval_flash_source(policy: FlashLoanPolicy) -> FlashLoanSource {
    match policy {
        FlashLoanPolicy::AaveOnly | FlashLoanPolicy::Auto => FlashLoanSource::AaveV3,
        FlashLoanPolicy::BalancerOnly => FlashLoanSource::Balancer,
    }
}

// --- profit_logs ---

pub fn parse_transfer_profit(
    logs: &[Log],
    executor: Address,
    profit_token: Option<Address>,
) -> Option<U256> {
    let mut net = U256::ZERO;
    let mut matched = false;
    for log in logs {
        if let Some(token) = profit_token
            && log.address() != token
        {
            continue;
        }
        let Ok(decoded) = IERC20::Transfer::decode_log(&log.inner) else {
            continue;
        };
        let is_to = decoded.to == executor;
        let is_from = decoded.from == executor;
        if is_to || is_from {
            matched = true;
        }
        if is_to && is_from {
            continue;
        }
        if is_to {
            net = net.saturating_add(decoded.value);
        } else if is_from {
            net = net.saturating_sub(decoded.value);
        }
    }
    matched.then_some(net)
}

// --- rpc_errors ---

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmitAction {
    ResyncAndRetry,
    BumpFeesAndRetry,
    AlreadyKnown,
    InsufficientFunds,
    Fail(String),
}

pub(crate) fn ic(haystack: &str, needle: &str) -> bool {
    let h = haystack.as_bytes();
    let n = needle.as_bytes();
    h.len() >= n.len() && h.windows(n.len()).any(|w| w.eq_ignore_ascii_case(n))
}

pub fn classify_submit_error(err: &impl std::fmt::Display) -> SubmitAction {
    let msg = err.to_string();

    if ic(&msg, "nonce too low") || ic(&msg, "nonce has already been used") {
        return SubmitAction::ResyncAndRetry;
    }
    if ic(&msg, "already known") || ic(&msg, "already imported") {
        return SubmitAction::AlreadyKnown;
    }
    if ic(&msg, "replacement transaction underpriced")
        || ic(&msg, "fee too low")
        || ic(&msg, "underpriced")
    {
        return SubmitAction::BumpFeesAndRetry;
    }
    if ic(&msg, "insufficient funds") || ic(&msg, "insufficient balance") {
        return SubmitAction::InsufficientFunds;
    }
    if ic(&msg, "429") || ic(&msg, "rate limit") || ic(&msg, "timeout") {
        return SubmitAction::BumpFeesAndRetry;
    }

    SubmitAction::Fail(msg)
}

pub fn is_transient_receipt_error(err: &impl std::fmt::Display) -> bool {
    let msg = err.to_string();
    ic(&msg, "429")
        || ic(&msg, "rate limit")
        || ic(&msg, "timeout")
        || ic(&msg, "connection")
        || ic(&msg, "temporarily unavailable")
        || ic(&msg, "server error")
}

#[must_use]
pub fn extract_tx_hash_from_error(err: &str) -> Option<B256> {
    err.split("0x").skip(1).find_map(|segment| {
        let hex: String = segment.chars().take(64).collect();
        if hex.len() == 64 {
            format!("0x{hex}").parse().ok()
        } else {
            None
        }
    })
}

// --- gas ---

#[derive(Debug, Clone, Copy)]
pub struct FeeSnapshot {
    pub base_fee: U256,
    pub priority_fee: U256,
}

pub const ROUTE_EXECUTION_GAS_OVERHEAD: u32 = 150_000;
pub const PER_HOP_EXECUTOR_GAS_OVERHEAD: u32 = 30_000;
/// Raised post-revert (actual 1.87M vs sim 720k on BAL-ish ~2.6x); 25% buffer + fallback 50% for headroom w/o inflating sim gas used in net calc.
pub const GAS_LIMIT_BUFFER_BPS: u64 = 2500;
/// Extra headroom when RPC cannot return `estimate_gas` (Balancer batchSwap overflow).
pub const GAS_FALLBACK_BUFFER_BPS: u64 = 5000;

pub fn u256_to_u128(v: U256) -> anyhow::Result<u128> {
    use anyhow::Context;

    v.try_into()
        .with_context(|| format!("value {v} exceeds u128"))
}

#[must_use]
pub fn buffer_gas_limit(simulated_gas: u32) -> Option<U256> {
    if simulated_gas == 0 {
        return None;
    }
    let units = U256::from(simulated_gas);
    let buffer = (units * U256::from(GAS_LIMIT_BUFFER_BPS)) / U256::from(10_000u64);
    Some(units + buffer + U256::from(1u8))
}

#[must_use]
pub fn pick_buffered_gas_limit(simulated_gas: u32, dry_run_gas: Option<u64>) -> Option<U256> {
    let observed = dry_run_gas
        .map(|gas| u32::try_from(gas).unwrap_or(u32::MAX))
        .unwrap_or(0);
    let base = simulated_gas.max(observed);
    buffer_gas_limit(base)
}

pub fn pick_live_gas_limit(simulated_gas: u32, dry_run_gas: u64) -> anyhow::Result<u64> {
    pick_live_gas_limit_with_buffer(simulated_gas, dry_run_gas, GAS_LIMIT_BUFFER_BPS)
}

pub fn pick_live_gas_limit_with_buffer(
    simulated_gas: u32,
    dry_run_gas: u64,
    buffer_bps: u64,
) -> anyhow::Result<u64> {
    if simulated_gas == 0 || dry_run_gas == 0 {
        anyhow::bail!("dry-run passed but gas estimate is zero");
    }
    let base = simulated_gas.max(u32::try_from(dry_run_gas).unwrap_or(simulated_gas));
    let units = U256::from(base);
    let buffer = (units * U256::from(buffer_bps)) / U256::from(10_000u64);
    let limit = units + buffer + U256::from(1u8);
    u256_to_u128(limit).map(|g| g as u64)
}

/// Minimum sim→live uplift when RPC `estimate_gas` is unavailable (eth_call-only pass).
/// Calibrated from on-chain OOG: 640k sim → 1.16M+ limit ceiling.
const GAS_FALLBACK_MIN_SCALE_BPS: u64 = 30_000;

#[must_use]
pub fn scaled_simulated_gas(simulated_gas: u32, scale_bps: u64) -> u32 {
    let scaled =
        u128::from(simulated_gas).saturating_mul(u128::from(scale_bps.max(10_000))) / 10_000;
    u32::try_from(scaled).unwrap_or(u32::MAX).max(simulated_gas)
}

/// Gas units for post-dry-run profit reassessment (not tx gas limit).
/// Fallback dry-runs have no RPC gas observation — use route history or sim heuristic
/// without the submit-limit scale, since HF already priced at sim gas.
#[must_use]
pub fn profit_reassess_gas(
    observed_route_gas: Option<u32>,
    simulated_gas: u32,
    dry_run_gas: Option<u64>,
    _gas_fallback: bool,
) -> u64 {
    if let Some(g) = dry_run_gas {
        return g;
    }
    u64::from(observed_route_gas.unwrap_or(simulated_gas))
}

/// Basis for submit gas limit: prefer dry-run observation, else route history, else scaled heuristic.
#[must_use]
pub fn submit_gas_basis(
    observed_route_gas: Option<u32>,
    sim_scale_bps: u64,
    simulated_gas: u32,
    dry_run_gas: Option<u64>,
) -> u64 {
    if let Some(g) = dry_run_gas {
        return g;
    }
    u64::from(observed_route_gas.unwrap_or_else(|| {
        scaled_simulated_gas(simulated_gas, sim_scale_bps.max(GAS_FALLBACK_MIN_SCALE_BPS))
    }))
}

#[must_use]
pub fn estimate_route_gas_from_hops(hop_gas: u32, hop_count: usize) -> u32 {
    hop_gas + ROUTE_EXECUTION_GAS_OVERHEAD + hop_count as u32 * PER_HOP_EXECUTOR_GAS_OVERHEAD
}

#[must_use]
pub fn compute_conservative_gas_price(snapshot: FeeSnapshot) -> U256 {
    snapshot.base_fee * U256::from(11_250u64) / U256::from(10_000u64) + snapshot.priority_fee
}

// --- impact_slippage ---

fn marginal_shortfall_bps(
    base_out: U256,
    base_amount: U256,
    probe_out: U256,
    probe_amount: U256,
) -> u64 {
    if base_out.is_zero() || base_amount.is_zero() || probe_out.is_zero() || probe_amount.is_zero()
    {
        return 10_000;
    }
    let Some(base_rate) = base_out
        .checked_mul(U256::from(1_000_000u64))
        .map(|scaled| scaled / base_amount)
    else {
        return 10_000;
    };
    let Some(probe_rate) = probe_out
        .checked_mul(U256::from(1_000_000u64))
        .map(|scaled| scaled / probe_amount)
    else {
        return 10_000;
    };
    if probe_rate >= base_rate {
        return 0;
    }
    let shortfall = base_rate - probe_rate;
    let bps =
        (shortfall * BPS_SCALE / base_rate.max(U256::from(1u8))).min(BPS_SCALE - U256::from(1u8));
    u64::try_from(bps).unwrap_or(10_000)
}

#[must_use]
pub fn depth_impact_slippage_bps(arena: &StateArena, edges: &[Edge], amount_in: U256) -> u64 {
    depth_impact_slippage_bps_with_base(arena, edges, amount_in, None)
}

#[must_use]
pub fn depth_impact_slippage_bps_with_base(
    arena: &StateArena,
    edges: &[Edge],
    amount_in: U256,
    base_sim: Option<&MinimalSimResult>,
) -> u64 {
    if amount_in.is_zero() || edges.is_empty() {
        return 0;
    }

    let base_out = if let Some(sim) = base_sim {
        if sim.profit.is_zero() {
            return 10_000;
        }
        sim.amount_out
    } else {
        let Some(base) = simulate_route_minimal(arena, edges, amount_in) else {
            return 10_000;
        };
        if base.profit.is_zero() {
            return 10_000;
        }
        base.amount_out
    };

    let probe_in = amount_in.saturating_mul(U256::from(10_100u64)) / BPS_SCALE;
    if probe_in == amount_in {
        return 0;
    }
    let Some(probe) = simulate_route_minimal(arena, edges, probe_in) else {
        return 0;
    };

    marginal_shortfall_bps(base_out, amount_in, probe.amount_out, probe_in)
}

#[must_use]
pub fn effective_slippage_bps(configured_bps: u64, depth_bps: u64) -> u64 {
    configured_bps.max(depth_bps).min(9_999)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_gas_limit_nonzero() {
        assert!(buffer_gas_limit(100_000).is_some());
    }

    #[test]
    fn submit_gas_basis_scales_when_estimate_missing() {
        // GAS_FALLBACK_MIN_SCALE_BPS = 30_000 (3×), so 885_000 × 3 = 2_655_000.
        let basis = submit_gas_basis(None, 10_000, 885_000, None);
        assert_eq!(basis, 2_655_000);
        let limit = pick_live_gas_limit_with_buffer(885_000, basis, GAS_FALLBACK_BUFFER_BPS)
            .expect("limit");
        assert!(limit > 1_017_751);
    }

    #[test]
    fn profit_reassess_gas_skips_fallback_scale() {
        assert_eq!(profit_reassess_gas(None, 640_000, None, true), 640_000);
        assert_eq!(
            profit_reassess_gas(Some(1_276_000), 640_000, None, true),
            1_276_000
        );
        assert_eq!(
            profit_reassess_gas(None, 640_000, Some(700_000), false),
            700_000
        );
    }

    #[test]
    fn profit_reassess_gas_ignores_fallback_flag_when_no_dry_run() {
        assert_eq!(profit_reassess_gas(None, 640_000, None, false), 640_000);
        assert_eq!(
            profit_reassess_gas(Some(700_000), 640_000, None, false),
            700_000
        );
    }

    #[test]
    fn depth_slippage_tracks_output_rate_not_profit_margin() {
        assert_eq!(
            marginal_shortfall_bps(
                U256::from(1_100u64),
                U256::from(1_000u64),
                U256::from(1_109u64),
                U256::from(1_010u64),
            ),
            18
        );
    }

    #[test]
    fn improving_output_rate_has_no_depth_slippage() {
        assert_eq!(
            marginal_shortfall_bps(
                U256::from(1_100u64),
                U256::from(1_000u64),
                U256::from(1_112u64),
                U256::from(1_010u64),
            ),
            0
        );
    }
}
