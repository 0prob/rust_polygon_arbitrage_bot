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
use crate::services::execution::profit::compound_slippage_bps;

// --- flash_policy ---

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FlashLoanPolicy {
    #[default]
    Auto,
    BalancerOnly,
    AaveOnly,
}

/// Parse flash policy; unknown values return `None` (callers must fail closed).
#[must_use]
pub fn try_parse_flash_policy(raw: &str) -> Option<FlashLoanPolicy> {
    let s = raw.trim();
    if s.eq_ignore_ascii_case("auto") {
        Some(FlashLoanPolicy::Auto)
    } else if s.eq_ignore_ascii_case("aave") || s.eq_ignore_ascii_case("aave_v3") {
        Some(FlashLoanPolicy::AaveOnly)
    } else if s.eq_ignore_ascii_case("balancer") || s.eq_ignore_ascii_case("balancer_only") {
        Some(FlashLoanPolicy::BalancerOnly)
    } else {
        None
    }
}

#[cfg(test)]
mod flash_policy_tests {
    use super::*;

    #[test]
    fn try_parse_flash_policy_accepts_known_values() {
        assert_eq!(try_parse_flash_policy("auto"), Some(FlashLoanPolicy::Auto));
        assert_eq!(
            try_parse_flash_policy("balancer"),
            Some(FlashLoanPolicy::BalancerOnly)
        );
        assert_eq!(
            try_parse_flash_policy("aave_v3"),
            Some(FlashLoanPolicy::AaveOnly)
        );
        assert_eq!(try_parse_flash_policy("typo"), None);
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
    Fail,
}

pub(crate) fn ic(haystack: &str, needle: &str) -> bool {
    let h = haystack.as_bytes();
    let n = needle.as_bytes();
    h.len() >= n.len() && h.windows(n.len()).any(|w| w.eq_ignore_ascii_case(n))
}

/// Classify submit failures from the full anyhow/Display chain (`{err:#}`).
/// Plain `{}` only shows the outermost `.context(...)` layer.
pub fn classify_submit_error(err: &impl std::fmt::Display) -> SubmitAction {
    let msg = format!("{err:#}");

    if ic(&msg, "nonce too low")
        || ic(&msg, "nonce has already been used")
        || ic(&msg, "nonce too high")
    {
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

    SubmitAction::Fail
}

pub fn is_transient_receipt_error(err: &impl std::fmt::Display) -> bool {
    let msg = format!("{err:#}");
    ic(&msg, "429")
        || ic(&msg, "rate limit")
        || ic(&msg, "timeout")
        || ic(&msg, "connection")
        || ic(&msg, "temporarily unavailable")
        || ic(&msg, "server error")
}

/// True when the RPC rejected the request due to provider rate limiting.
///
/// Uses alternate `Display` (`{err:#}`) so anyhow `.context(...)` wrappers still
/// expose the underlying RPC body. Plain `{}` only shows the outermost layer
/// (e.g. `"chunk multicall failed"`), which previously made rate limits look
/// like generic errors and triggered bisect storms that worsened 429s.
#[must_use]
pub fn is_rpc_rate_limited(err: &impl std::fmt::Display) -> bool {
    let msg = format!("{err:#}");
    msg.contains("429")
        // Ankr / some Polygon providers encode rate limits as code 15.
        || msg.contains("error code 15")
        || ic(&msg, "rate limit")
        || ic(&msg, "usage limit")
        || ic(&msg, "too many request")
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
/// Floor tip for assess + submit (Polygon MEV); shared so ranking matches live fees.
pub const MIN_PRIORITY_FEE_PER_GAS: U256 = U256::from_limbs([30_000_000_000, 0, 0, 0]);
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
/// ponytail: 2x instead of 3x — keeps gas budget lower for operators with less MATIC.
/// At 451 gwei with 2M sim_gas: 2x → ~4M gas → ~1.8 MATIC; 3x → ~6M gas → ~2.7 MATIC.
/// On-chain OOG data shows 1.8x ceiling covers <99% of cases — 2x is safe.
const GAS_FALLBACK_MIN_SCALE_BPS: u64 = 20_000;

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
    gas_fallback: bool,
    sim_scale_bps: u64,
) -> u64 {
    if let Some(g) = dry_run_gas {
        return g;
    }
    if let Some(observed) = observed_route_gas {
        return u64::from(observed);
    }
    if gas_fallback {
        u64::from(scaled_simulated_gas(
            simulated_gas,
            sim_scale_bps.max(GAS_FALLBACK_MIN_SCALE_BPS),
        ))
    } else {
        u64::from(simulated_gas)
    }
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

/// EVM storage read costs (EIP-2929 warm / legacy cold).
pub const GAS_COLD_SLOAD: u32 = 2100;
pub const GAS_WARM_SLOAD: u32 = 100;

/// Huff executor: ~4 warm `SLOAD`s per hop (pool, token0, token1, reserve/slot0).
#[must_use]
pub fn estimate_route_storage_gas(hop_count: usize, cold_pool_slots: u32) -> u32 {
    let warm_reads = hop_count as u32 * 4;
    cold_pool_slots * GAS_COLD_SLOAD + warm_reads * GAS_WARM_SLOAD
}

#[must_use]
pub fn estimate_route_gas_from_hops(hop_gas: u32, hop_count: usize) -> u32 {
    hop_gas + ROUTE_EXECUTION_GAS_OVERHEAD + hop_count as u32 * PER_HOP_EXECUTOR_GAS_OVERHEAD
}

/// Opcode-aware route gas: hop simulation + executor overhead + cold/warm storage reads.
#[must_use]
pub fn estimate_route_gas_from_hops_evm(
    hop_gas: u32,
    hop_count: usize,
    cold_pool_slots: u32,
) -> u32 {
    estimate_route_gas_from_hops(hop_gas, hop_count)
        .saturating_add(estimate_route_storage_gas(hop_count, cold_pool_slots))
}

#[must_use]
pub fn compute_conservative_gas_price(snapshot: FeeSnapshot) -> U256 {
    let priority = snapshot.priority_fee.max(MIN_PRIORITY_FEE_PER_GAS);
    snapshot.base_fee * U256::from(11_250u64) / U256::from(10_000u64) + priority
}

/// Spot effective gas price for HF probe/Brent/assess (no 12.5% base-fee buffer).
/// Submit path keeps [`compute_conservative_gas_price`]; using the buffer in ranking
/// was rejecting near-misses that could clear gas at the live tip.
#[must_use]
pub fn compute_assessment_gas_price(snapshot: FeeSnapshot) -> U256 {
    snapshot
        .base_fee
        .saturating_add(snapshot.priority_fee.max(MIN_PRIORITY_FEE_PER_GAS))
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

    // +5% may collapse to the same integer on dust sizes; step by 1 so depth is
    // still measured instead of optimistically reporting 0 impact.
    let mut probe_in = amount_in.saturating_mul(U256::from(10_500u64)) / BPS_SCALE;
    if probe_in <= amount_in {
        probe_in = amount_in.saturating_add(U256::ONE);
    }
    let Some(probe) = simulate_route_minimal(arena, edges, probe_in) else {
        // Probe failure means unknown depth impact — do not report 0 slippage.
        return 10_000;
    };

    marginal_shortfall_bps(base_out, amount_in, probe.amount_out, probe_in)
}

/// Route-level slippage for profit assessment / Brent.
///
/// - `configured_per_hop_bps` is the per-hop floor used in calldata `minOut` → compound
///   across `hop_count` so the profit haircut matches multi-hop execution.
/// - Floored at [`EXECUTION_MIN_SLIPPAGE_BPS`] so default config `0` still matches
///   V2/Curve/Balancer encode haircuts (else `minProfit` exceeds on-chain final).
/// - `depth_route_bps` is already a **full-route** shortfall from the +5% size probe
///   ([`depth_impact_slippage_bps_with_base`]) → applied once, never re-compounded.
///
/// Prior bug: `max(config, depth)` then `compound(…, hops)` treated depth as per-hop and
/// roughly ×hops over-penalized multi-hop routes (e.g. 229 bps depth → ~672 bps on 3 hops).
#[must_use]
pub fn effective_slippage_bps(
    configured_per_hop_bps: u64,
    hop_count: u32,
    depth_route_bps: u64,
) -> u64 {
    use crate::core::constants::EXECUTION_MIN_SLIPPAGE_BPS;
    let per_hop = configured_per_hop_bps.max(EXECUTION_MIN_SLIPPAGE_BPS);
    let config_route = compound_slippage_bps(per_hop, hop_count);
    config_route.max(depth_route_bps).min(9_999)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_submit_error_sees_through_anyhow_context() {
        let err = anyhow::anyhow!("nonce too low")
            .context("submit_transaction failed");
        assert_eq!(classify_submit_error(&err), SubmitAction::ResyncAndRetry);
        let rate = anyhow::anyhow!("HTTP 429 rate limit")
            .context("private submit failed");
        assert_eq!(classify_submit_error(&rate), SubmitAction::BumpFeesAndRetry);
        assert!(is_transient_receipt_error(&rate));
    }

    #[test]
    fn test_buffer_gas_limit_nonzero() {
        assert!(buffer_gas_limit(100_000).is_some());
    }

    #[test]
    fn assessment_gas_price_omits_base_fee_buffer() {
        let snap = FeeSnapshot {
            base_fee: U256::from(200_000_000_000u64),
            priority_fee: U256::from(30_000_000_000u64),
        };
        let spot = compute_assessment_gas_price(snap);
        let conservative = compute_conservative_gas_price(snap);
        assert_eq!(spot, U256::from(230_000_000_000u64));
        assert!(conservative > spot);
        assert_eq!(
            conservative,
            U256::from(200_000_000_000u64) * U256::from(11_250u64) / U256::from(10_000u64)
                + U256::from(30_000_000_000u64)
        );
    }

    #[test]
    fn gas_price_helpers_floor_priority_to_min_tip() {
        let snap = FeeSnapshot {
            base_fee: U256::from(200_000_000_000u64),
            priority_fee: U256::from(1u64),
        };
        assert_eq!(
            compute_assessment_gas_price(snap),
            U256::from(200_000_000_000u64) + MIN_PRIORITY_FEE_PER_GAS
        );
        assert_eq!(
            compute_conservative_gas_price(snap),
            U256::from(200_000_000_000u64) * U256::from(11_250u64) / U256::from(10_000u64)
                + MIN_PRIORITY_FEE_PER_GAS
        );
    }

    #[test]
    fn submit_gas_basis_scales_when_estimate_missing() {
        // GAS_FALLBACK_MIN_SCALE_BPS = 20_000 (2×), so 885_000 × 2 = 1_770_000.
        let basis = submit_gas_basis(None, 10_000, 885_000, None);
        assert_eq!(basis, 1_770_000);
        let limit = pick_live_gas_limit_with_buffer(885_000, basis, GAS_FALLBACK_BUFFER_BPS)
            .expect("limit");
        assert!(limit > 1_017_751);
    }

    #[test]
    fn profit_reassess_gas_scales_on_fallback_without_dry_run() {
        assert_eq!(
            profit_reassess_gas(None, 640_000, None, true, 10_000),
            1_280_000
        );
        assert_eq!(
            profit_reassess_gas(Some(1_276_000), 640_000, None, true, 10_000),
            1_276_000
        );
        assert_eq!(
            profit_reassess_gas(None, 640_000, Some(700_000), false, 10_000),
            700_000
        );
    }

    #[test]
    fn profit_reassess_gas_ignores_fallback_flag_when_no_dry_run() {
        assert_eq!(
            profit_reassess_gas(None, 640_000, None, false, 10_000),
            640_000
        );
        assert_eq!(
            profit_reassess_gas(Some(700_000), 640_000, None, false, 10_000),
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

    #[test]
    fn effective_slippage_does_not_compound_route_depth() {
        // Depth is already full-route (e.g. 229 bps). Compounding as per-hop would
        // inflate ~3× on a 3-hop path; max(compound(config), depth) keeps 229.
        // Depth is full-route; floor compound(100,3)=298 beats depth 229, but
        // depth still wins when larger (not re-compounded × hops).
        assert_eq!(effective_slippage_bps(0, 3, 229), 298);
        assert_eq!(effective_slippage_bps(0, 3, 500), 500);
        // Config below EXECUTION_MIN_SLIPPAGE_BPS floors to 100.
        assert_eq!(effective_slippage_bps(50, 1, 10), 100);
        // 100 bps × 4 hops: retained 9604 → 396 route bps; depth 10 stays under that.
        assert_eq!(effective_slippage_bps(50, 4, 10), 396);
        assert_eq!(effective_slippage_bps(50, 4, 500), 500);
        // Config 0 floors to encode min (100) so 2-hop → 199 route bps.
        assert_eq!(effective_slippage_bps(0, 2, 0), 199);
        assert_eq!(effective_slippage_bps(0, 1, 0), 100);
    }

    #[test]
    fn depth_dust_probe_steps_instead_of_zero() {
        use crate::core::types::{PoolState, ProtocolType, V2PoolState};
        use std::sync::Arc;

        let mut arena = StateArena::default();
        let t0 = arena.register_token(Address::repeat_byte(0x01));
        let t1 = arena.register_token(Address::repeat_byte(0x02));
        let pool = arena.register_pool(
            Address::repeat_byte(0xaa),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: U256::from(100u64),
                reserve1: U256::from(100u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 0,
            })),
        );
        let edge = Edge {
            pool_index: pool,
            token_in: t0,
            token_out: t1,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: true,
        };
        let base = MinimalSimResult {
            profit: U256::ONE,
            amount_out: U256::from(20u64),
            total_gas: 1,
        };
        // amount=10 → +5% collapses to 10; new path probes 11 instead of returning 0.
        let bps =
            depth_impact_slippage_bps_with_base(&arena, &[edge], U256::from(10u64), Some(&base));
        assert!(
            bps > 0,
            "dust probe must measure depth, not short-circuit to 0"
        );
    }
}
