use std::sync::atomic::AtomicU64;

use crate::core::constants::BPS_SCALE;
use crate::core::types::{FlashLoanSource, ProfitAssessment, TokenIndex};
use crate::pipeline::arena::StateArena;
use crate::pipeline::types::MinimalSimResult;
use crate::services::execution::gas_oracle::{GasOracle, RouteGasLookup};
use crate::services::oracle::resolve_token_to_matic_rate;
use alloy::primitives::Address;
use alloy::primitives::U256;
use rustc_hash::FxHashMap;

pub const ON_CHAIN_MIN_PROFIT_RATIO_BPS: u64 = 9500;
pub use crate::core::constants::{
    MAX_SANE_PROFIT_MATIC_WEI, MIN_TOKEN_TO_MATIC_RATE, RATE_PRECISION,
};

// ponytail: 5 bps is the Aave V3 default FLASHLOAN_PREMIUM_TOTAL.
// Refetch on-chain periodically via fetch_and_cache_aave_flash_loan_fee_bps.
static AAVE_FLASH_LOAN_FEE_BPS: AtomicU64 = AtomicU64::new(5);

pub fn set_aave_flash_loan_fee_bps(fee_bps: u64) {
    AAVE_FLASH_LOAN_FEE_BPS.store(fee_bps, std::sync::atomic::Ordering::Relaxed);
}

#[must_use]
pub fn aave_flash_loan_fee_bps_cached() -> u64 {
    AAVE_FLASH_LOAN_FEE_BPS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Balancer V2 ProtocolFeesCollector flash fee as 1e18 FixedPoint (0 on Polygon today).
/// Refetch via `fetch_and_cache_balancer_flash_loan_fee_pct`.
static BALANCER_FLASH_LOAN_FEE_PCT_1E18: AtomicU64 = AtomicU64::new(0);

pub fn set_balancer_flash_loan_fee_pct(pct_1e18: u64) {
    BALANCER_FLASH_LOAN_FEE_PCT_1E18.store(pct_1e18, std::sync::atomic::Ordering::Relaxed);
}

#[must_use]
pub fn balancer_flash_loan_fee_pct_cached() -> u64 {
    BALANCER_FLASH_LOAN_FEE_PCT_1E18.load(std::sync::atomic::Ordering::Relaxed)
}

/// FixedPoint.ONE — Balancer fee percentage denominator.
const BALANCER_FP_ONE: u128 = 1_000_000_000_000_000_000;

// ponytail: DODO flash loan fee = pool swap fee. Most DODO pools use 0.1% = 10 bps.
const DODO_FLASH_LOAN_FEE_BPS: u64 = 10;
/// External (non-route) DODO flash lenders are not wired — keep false until hop-excluded pools exist.
pub const DODO_EXTERNAL_FLASH_ENABLED: bool = false;
/// Cap profit-derived priority fee boost at 200 gwei (matches submit.rs).
pub const MAX_PROFIT_PRIORITY_FEE_WEI: u128 = 200_000_000_000;

pub fn flash_loan_fee_bps(source: FlashLoanSource) -> u64 {
    match source {
        // batchSwap flash-swap pays no Vault flashLoan fee.
        FlashLoanSource::Direct => 0,
        FlashLoanSource::Balancer => {
            balancer_flash_fee_pct_to_bps(balancer_flash_loan_fee_pct_cached())
        }
        FlashLoanSource::AaveV3 => {
            AAVE_FLASH_LOAN_FEE_BPS.load(std::sync::atomic::Ordering::Relaxed)
        }
        FlashLoanSource::Dodo => {
            if DODO_EXTERNAL_FLASH_ENABLED {
                DODO_FLASH_LOAN_FEE_BPS
            } else {
                0
            }
        }
    }
}

/// Convert Balancer 1e18 FixedPoint fee % → bps (ceil) for coarse callers.
#[must_use]
pub fn balancer_flash_fee_pct_to_bps(pct_1e18: u64) -> u64 {
    if pct_1e18 == 0 {
        return 0;
    }
    let num = (pct_1e18 as u128).saturating_mul(10_000);
    num.div_ceil(BALANCER_FP_ONE) as u64
}

/// Flash premium in token units.
/// Aave: PercentageMath.percentMul (half-up). Balancer vault flash: FixedPoint.mulUp.
#[must_use]
pub fn flash_loan_fee_amount(source: FlashLoanSource, amount: U256) -> Option<U256> {
    match source {
        FlashLoanSource::Direct => Some(U256::ZERO),
        FlashLoanSource::Balancer => {
            balancer_mul_up(amount, U256::from(balancer_flash_loan_fee_pct_cached()))
        }
        FlashLoanSource::AaveV3 => {
            let bps = flash_loan_fee_bps(source);
            if bps == 0 {
                return Some(U256::ZERO);
            }
            aave_percent_mul(amount, bps)
        }
        FlashLoanSource::Dodo => {
            if !DODO_EXTERNAL_FLASH_ENABLED {
                return Some(U256::ZERO);
            }
            let bps = DODO_FLASH_LOAN_FEE_BPS;
            amount.checked_mul(U256::from(bps)).map(|v| v / BPS_SCALE)
        }
    }
}

/// balancer-v2 FixedPoint.mulUp(amount, feePercentage).
#[inline]
#[must_use]
pub fn balancer_mul_up(amount: U256, pct_1e18: U256) -> Option<U256> {
    if amount.is_zero() || pct_1e18.is_zero() {
        return Some(U256::ZERO);
    }
    let product = amount.checked_mul(pct_1e18)?;
    let one = U256::from(BALANCER_FP_ONE);
    Some(((product - U256::from(1u8)) / one) + U256::from(1u8))
}

/// aave-v3-core PercentageMath.percentMul: (value * bps + 5000) / 10000.
#[inline]
#[must_use]
pub fn aave_percent_mul(value: U256, bps: u64) -> Option<U256> {
    if bps == 0 {
        return Some(U256::ZERO);
    }
    value
        .checked_mul(U256::from(bps))?
        .checked_add(U256::from(5_000u64))
        .map(|v| v / BPS_SCALE)
}

#[must_use]
pub fn on_chain_min_profit(token_profit: U256) -> Option<U256> {
    if token_profit.is_zero() {
        return Some(U256::ZERO);
    }
    // ponytail: floor at 1 to prevent integer division from silently disabling
    // the on-chain minProfit check for sub-2-wei profits.
    let base = token_profit.checked_mul(U256::from(ON_CHAIN_MIN_PROFIT_RATIO_BPS))? / BPS_SCALE;
    if base.is_zero() {
        Some(U256::from(1u8))
    } else {
        Some(base)
    }
}

/// Token-denominated net profit after slippage and flash fee (matches `assess_profit` basis).
#[must_use]
pub fn modeled_net_profit_tokens(
    gross_profit: U256,
    amount_in: U256,
    slippage_bps: u64,
    hop_count: u32,
    flash_source: FlashLoanSource,
) -> Option<U256> {
    // Callers pass **per-hop** config bps (calldata minOut); compound for the route haircut.
    let route_slippage = compound_slippage_bps(slippage_bps, hop_count);
    let amount_out = gross_profit.checked_add(amount_in)?;
    let adjusted_out = slippage_adjusted(amount_out, route_slippage)?;
    let adjusted_gross = adjusted_out.saturating_sub(amount_in);
    let flash_fee = flash_loan_fee_amount(flash_source, amount_in)?;
    Some(adjusted_gross.saturating_sub(flash_fee))
}

/// On-chain `minProfit` aligned with off-chain modeled net (95% floor).
#[must_use]
pub fn on_chain_min_profit_for_route(
    gross_profit: U256,
    amount_in: U256,
    slippage_bps: u64,
    hop_count: u32,
    flash_source: FlashLoanSource,
) -> Option<U256> {
    let net = modeled_net_profit_tokens(
        gross_profit,
        amount_in,
        slippage_bps,
        hop_count,
        flash_source,
    )?;
    on_chain_min_profit(net)
}

#[must_use]
pub fn on_chain_min_profit_from_assessment(assessment: &ProfitAssessment) -> Option<U256> {
    on_chain_min_profit(assessment.net_profit)
}

/// Absolute priority tip (wei/gas) from profit × alpha, capped at [`MAX_PROFIT_PRIORITY_FEE_WEI`].
/// Used by submit to set `max_priority_fee_per_gas = max(oracle_tip, this)`.
#[must_use]
pub fn profit_priority_tip_per_gas(
    expected_profit_matic_wei: U256,
    alpha_bps: u64,
    gas_units: u32,
) -> U256 {
    if alpha_bps == 0 || gas_units == 0 || expected_profit_matic_wei.is_zero() {
        return U256::ZERO;
    }
    let total_boost = expected_profit_matic_wei.saturating_mul(U256::from(alpha_bps)) / BPS_SCALE;
    let per_gas = total_boost / U256::from(gas_units);
    per_gas.min(U256::from(MAX_PROFIT_PRIORITY_FEE_WEI))
}

/// MATIC-wei **incremental** tip cost above the floor already priced into assess `gas_price_wei`.
///
/// HF gas_price = base + [`crate::services::execution::MIN_PRIORITY_FEE_PER_GAS`] (30 gwei).
/// Submit bids `max(floor, profit_tip)`. Charging the full profit tip again double-counted
/// the floor (live: +30 gwei × ~1M gas ≈ 0.03 MATIC phantom shortfall on every near-miss).
#[must_use]
pub fn profit_priority_uplift_wei(
    expected_profit_matic_wei: U256,
    alpha_bps: u64,
    gas_units: u32,
) -> U256 {
    use super::support::MIN_PRIORITY_FEE_PER_GAS;
    let tip = profit_priority_tip_per_gas(expected_profit_matic_wei, alpha_bps, gas_units);
    let incremental = tip.saturating_sub(MIN_PRIORITY_FEE_PER_GAS);
    incremental.saturating_mul(U256::from(gas_units))
}

/// Safety floor in native MATIC wei (`revert_penalty × safety_bps / 10_000`).
/// `safety_bps == 0` disables the ratio gate and gas gate (dry-run eth_call); default 25_000.
#[must_use]
pub fn safety_floor_matic_wei(revert_penalty_wei: U256, safety_bps: u64) -> U256 {
    if safety_bps == 0 {
        return U256::ZERO;
    }
    revert_penalty_wei.saturating_mul(U256::from(safety_bps)) / BPS_SCALE
}

#[must_use]
pub fn slippage_adjusted(amount_out: U256, slippage_bps: u64) -> Option<U256> {
    if amount_out.is_zero() || slippage_bps >= 10_000 {
        return None;
    }
    let min_out = amount_out.checked_mul(BPS_SCALE - U256::from(slippage_bps))? / BPS_SCALE;
    if min_out.is_zero() {
        None
    } else {
        Some(min_out)
    }
}

/// Per-hop slippage compounded to match calldata `min_out` applied on every hop.
#[must_use]
pub fn compound_slippage_bps(per_hop_bps: u64, hop_count: u32) -> u64 {
    if per_hop_bps >= 10_000 {
        return 10_000;
    }
    let per_hop = per_hop_bps;
    if hop_count <= 1 || per_hop == 0 {
        return per_hop;
    }
    let complement = 10_000u64 - per_hop;
    let mut retained = 10_000u128;
    for _ in 0..hop_count {
        retained = retained * u128::from(complement) / 10_000u128;
        if retained == 0 {
            return 9_999;
        }
    }
    (10_000 - u64::try_from(retained).unwrap_or(0)).min(9_999)
}

#[inline]
fn ceil_div(numer: U256, denom: U256) -> Option<U256> {
    if denom.is_zero() {
        return None;
    }
    let q = numer / denom;
    let r = numer % denom;
    Some(if r.is_zero() {
        q
    } else {
        q.saturating_add(U256::from(1u8))
    })
}

/// Default 2.5× worst-case gas loss buffer before submitting (25_000 bps = 2.5×).
pub const DEFAULT_PROFIT_SAFETY_MULTIPLIER_BPS: u64 = 25_000;

#[derive(Clone)]
pub struct AssessProfitInput {
    pub gross_profit: U256,
    pub amount_in: U256,
    pub gas_units: u32,
    pub gas_price_wei: U256,
    pub token_to_matic_rate: U256,
    pub token_decimals: u8,
    pub hop_count: u32,
    pub min_profit_matic_wei: U256,
    /// Minimum net ROI in basis points of `amount_in` (0 = disabled).
    pub min_profit_roi_bps: u64,
    /// Route-level slippage haircut in bps (already compounded / depth-merged).
    pub slippage_bps: u64,
    pub flash_loan_source: FlashLoanSource,
    /// Net profit must exceed `gas_cost_matic * safety_multiplier_bps / 10_000`.
    pub safety_multiplier_bps: u64,
    /// Profit-proportional priority fee alpha in bps (0 = disabled).
    pub profit_priority_alpha_bps: u64,
}

/// Context for Brent sizing — maximizes net profit after gas/fees/slippage.
#[derive(Debug, Clone, Copy)]
pub struct ProfitEvalContext {
    pub gas_price: U256,
    pub flash_source: FlashLoanSource,
    /// Route-level slippage bps for Brent / probe assess (same convention as [`AssessProfitInput`]).
    pub slippage_bps: u64,
    pub token_to_matic_rate: U256,
    pub token_decimals: u8,
    pub safety_multiplier_bps: u64,
    /// Gas-oracle scale in bps (10_000 = 1.0×). Applied to simulated gas before
    /// cost deduction so the Brent objective matches the final assessment.
    pub gas_scale_bps: u64,
    pub profit_priority_alpha_bps: u64,
    pub hop_count: u32,
}

impl ProfitEvalContext {
    #[must_use]
    pub fn for_cycle(
        cycle_start: TokenIndex,
        arena: &StateArena,
        token_to_matic_rates: &FxHashMap<TokenIndex, U256>,
        token_decimals: &FxHashMap<Address, u8>,
        gas_price: U256,
        slippage_bps: u64,
        flash_source: FlashLoanSource,
    ) -> Self {
        Self::with_safety_multiplier(
            cycle_start,
            arena,
            token_to_matic_rates,
            token_decimals,
            gas_price,
            slippage_bps,
            flash_source,
            0,
        )
    }

    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn with_safety_multiplier(
        cycle_start: TokenIndex,
        arena: &StateArena,
        token_to_matic_rates: &FxHashMap<TokenIndex, U256>,
        token_decimals: &FxHashMap<Address, u8>,
        gas_price: U256,
        slippage_bps: u64,
        flash_source: FlashLoanSource,
        safety_multiplier_bps: u64,
    ) -> Self {
        let token_to_matic_rate = resolve_token_to_matic_rate(cycle_start, token_to_matic_rates);
        let token_decimals = crate::services::oracle::resolve_token_decimals_for_index(
            cycle_start,
            arena,
            token_decimals,
        );
        Self {
            gas_price,
            flash_source,
            slippage_bps,
            token_to_matic_rate,
            token_decimals,
            safety_multiplier_bps,
            gas_scale_bps: 10_000,
            profit_priority_alpha_bps: 0,
            hop_count: 1,
        }
    }
}

pub struct RouteProfitParams {
    pub gross_profit: U256,
    pub amount_in: U256,
    pub gas_units: u32,
    pub hop_count: u32,
    pub slippage_bps: u64,
    pub flash_loan_source: FlashLoanSource,
}

#[derive(Clone, Copy)]
pub struct ProfitThresholds {
    pub min_profit_matic_wei: U256,
    pub min_profit_roi_bps: u64,
    pub safety_multiplier_bps: u64,
    pub profit_priority_alpha_bps: u64,
}

#[must_use]
pub fn route_profit_thresholds(
    min_profit_matic: U256,
    min_profit_roi_bps: u64,
    safety_multiplier_bps: u64,
    profit_priority_alpha_bps: u64,
    risk_bps: u64,
) -> ProfitThresholds {
    ProfitThresholds {
        min_profit_matic_wei: min_profit_matic.saturating_mul(U256::from(risk_bps))
            / U256::from(10_000u64),
        min_profit_roi_bps,
        safety_multiplier_bps,
        profit_priority_alpha_bps,
    }
}

/// How to resolve simulated gas into assessment gas units (single source of truth).
pub enum AssessmentGas<'a> {
    /// HF eval: per-tick prefetch table, then oracle fallback.
    TickRoute {
        lookup: &'a RouteGasLookup,
        oracle: &'a GasOracle,
        route_fp: u64,
    },
    /// Dispatch / capped re-opt: direct oracle lookup by route fingerprint.
    Route {
        oracle: &'a GasOracle,
        route_fp: u64,
    },
}

/// Resolve gas units for profitability assessment without double-scaling.
#[must_use]
pub fn assessment_gas_units(simulated_gas: u32, gas: &AssessmentGas<'_>) -> u32 {
    match gas {
        AssessmentGas::TickRoute {
            lookup,
            oracle,
            route_fp,
        } => lookup.route_gas_or_heuristic(oracle, *route_fp, simulated_gas),
        AssessmentGas::Route { oracle, route_fp } => {
            oracle.route_gas_or_heuristic(*route_fp, simulated_gas)
        }
    }
}

pub struct RouteAssessRequest<'a> {
    pub cycle_start: TokenIndex,
    pub arena: &'a StateArena,
    pub gross_profit: U256,
    pub amount_in: U256,
    pub simulated_gas: u32,
    pub hop_count: u32,
    pub slippage_bps: u64,
    pub flash_source: FlashLoanSource,
    pub gas: AssessmentGas<'a>,
    pub thresholds: ProfitThresholds,
    pub token_to_matic_rates: &'a FxHashMap<TokenIndex, U256>,
    pub token_decimals: &'a FxHashMap<Address, u8>,
    pub gas_price: U256,
}

/// Assess a simulated route using centralized gas resolution and thresholds.
#[must_use]
pub fn assess_route_from_sim(request: &RouteAssessRequest<'_>) -> ProfitAssessment {
    let route = RouteProfitParams {
        gross_profit: request.gross_profit,
        amount_in: request.amount_in,
        gas_units: assessment_gas_units(request.simulated_gas, &request.gas),
        hop_count: request.hop_count,
        slippage_bps: request.slippage_bps,
        flash_loan_source: request.flash_source,
    };
    assess_route_profit(
        request.cycle_start,
        request.arena,
        &route,
        request.token_to_matic_rates,
        request.token_decimals,
        request.gas_price,
        &request.thresholds,
    )
}

/// Single entry point for route profitability after simulation.
#[must_use]
pub fn assess_route_profit(
    cycle_start: TokenIndex,
    arena: &StateArena,
    route: &RouteProfitParams,
    token_to_matic_rates: &FxHashMap<TokenIndex, U256>,
    token_decimals: &FxHashMap<Address, u8>,
    gas_price: U256,
    thresholds: &ProfitThresholds,
) -> ProfitAssessment {
    assess_profit(&AssessProfitInput {
        gross_profit: route.gross_profit,
        amount_in: route.amount_in,
        gas_units: route.gas_units,
        gas_price_wei: gas_price,
        token_to_matic_rate: resolve_token_to_matic_rate(cycle_start, token_to_matic_rates),
        token_decimals: crate::services::oracle::resolve_token_decimals_for_index(
            cycle_start,
            arena,
            token_decimals,
        ),
        hop_count: route.hop_count,
        min_profit_matic_wei: thresholds.min_profit_matic_wei,
        min_profit_roi_bps: thresholds.min_profit_roi_bps,
        slippage_bps: route.slippage_bps,
        flash_loan_source: route.flash_loan_source,
        safety_multiplier_bps: thresholds.safety_multiplier_bps,
        profit_priority_alpha_bps: thresholds.profit_priority_alpha_bps,
    })
}

fn probe_assess_input(
    sim: &MinimalSimResult,
    amount_in: U256,
    ctx: &ProfitEvalContext,
) -> AssessProfitInput {
    let gas_units = if ctx.gas_scale_bps == 10_000 {
        sim.total_gas
    } else {
        crate::services::execution::support::scaled_simulated_gas(sim.total_gas, ctx.gas_scale_bps)
    };
    AssessProfitInput {
        gross_profit: sim.profit,
        amount_in,
        gas_units,
        gas_price_wei: ctx.gas_price,
        token_to_matic_rate: ctx.token_to_matic_rate,
        token_decimals: ctx.token_decimals,
        hop_count: ctx.hop_count,
        min_profit_matic_wei: U256::ZERO,
        min_profit_roi_bps: 0,
        slippage_bps: ctx.slippage_bps,
        flash_loan_source: ctx.flash_source,
        safety_multiplier_bps: ctx.safety_multiplier_bps,
        profit_priority_alpha_bps: ctx.profit_priority_alpha_bps,
    }
}

#[must_use]
pub fn net_profit_after_gas_from_sim(
    sim: &MinimalSimResult,
    amount_in: U256,
    ctx: &ProfitEvalContext,
) -> U256 {
    assess_profit(&probe_assess_input(sim, amount_in, ctx)).net_profit_after_gas
}

/// MATIC-denominated net after gas + priority uplift (probe ranking).
#[must_use]
pub fn net_profit_matic_from_sim(
    sim: &MinimalSimResult,
    amount_in: U256,
    ctx: &ProfitEvalContext,
) -> U256 {
    assess_profit(&probe_assess_input(sim, amount_in, ctx)).net_profit_after_gas_matic_wei
}

/// Bias so Brent can rank sizes that are still below gas breakeven.
/// Without this, every unprofitable size scores `U256::ZERO` and size search is blind.
const BRENT_MATIC_SCORE_BIAS: U256 = U256::from_limbs([0, 0, 1, 0]); // 2^128 wei ≈ 3.4e20 MATIC

struct ProbeMaticParts {
    gross_matic: U256,
    flash_matic: U256,
    gas_cost_wei: U256,
    priority_uplift: U256,
}

fn probe_matic_parts(
    sim: &MinimalSimResult,
    amount_in: U256,
    ctx: &ProfitEvalContext,
) -> Option<ProbeMaticParts> {
    let input = probe_assess_input(sim, amount_in, ctx);
    let flash_loan_fee = flash_loan_fee_amount(input.flash_loan_source, input.amount_in)?;
    if input.slippage_bps >= 10_000 {
        return None;
    }
    let amount_out = input.gross_profit.saturating_add(input.amount_in);
    let adjusted_out = slippage_adjusted(amount_out, input.slippage_bps)?;
    let adjusted_gross = adjusted_out.saturating_sub(input.amount_in);
    let gas_cost_wei = U256::from(input.gas_units).checked_mul(input.gas_price_wei)?;
    let scale = crate::util::ten_pow_u256(input.token_decimals);
    if input.token_to_matic_rate < MIN_TOKEN_TO_MATIC_RATE || scale.is_zero() {
        return None;
    }
    let gross_matic = adjusted_gross
        .checked_mul(input.token_to_matic_rate)
        .map(|v| v / scale)?;
    let flash_matic = flash_loan_fee
        .checked_mul(input.token_to_matic_rate)
        .map(|v| v / scale)?;
    let priority_uplift = profit_priority_uplift_wei(
        gross_matic,
        input.profit_priority_alpha_bps,
        input.gas_units,
    );
    Some(ProbeMaticParts {
        gross_matic,
        flash_matic,
        gas_cost_wei,
        priority_uplift,
    })
}

/// Slip-adjusted gross − flash in MATIC wei (gas not deducted).
/// Rank underwater probe routes by absolute edge so low-gas dust does not crowd out
/// larger gross candidates that Brent can still size into profitability.
#[must_use]
pub fn cover_matic_from_sim(
    sim: &MinimalSimResult,
    amount_in: U256,
    ctx: &ProfitEvalContext,
) -> U256 {
    probe_matic_parts(sim, amount_in, ctx)
        .map(|p| p.gross_matic.saturating_sub(p.flash_matic))
        .unwrap_or(U256::ZERO)
}

/// Brent objective: maximize MATIC net (gross − flash − gas − priority) with a bias so
/// below-breakeven sizes keep relative order (closer-to-profitable wins).
#[must_use]
pub fn brent_score_matic_from_sim(
    sim: &MinimalSimResult,
    amount_in: U256,
    ctx: &ProfitEvalContext,
) -> U256 {
    let Some(p) = probe_matic_parts(sim, amount_in, ctx) else {
        return U256::ZERO;
    };
    // bias + gross - flash - gas - priority (each sub saturates independently)
    BRENT_MATIC_SCORE_BIAS
        .saturating_add(p.gross_matic)
        .saturating_sub(p.flash_matic)
        .saturating_sub(p.gas_cost_wei)
        .saturating_sub(p.priority_uplift)
}

#[must_use]
pub fn assess_profit(input: &AssessProfitInput) -> ProfitAssessment {
    let Some(flash_loan_fee) = flash_loan_fee_amount(input.flash_loan_source, input.amount_in)
    else {
        return rejected_arithmetic(input, "flash-loan fee overflow");
    };

    // Slippage reduces output, never input.
    // `slippage_bps` is **route-level** (see `effective_slippage_bps`): already compounded
    // for per-hop config and/or carrying full-route depth impact — do not compound again.
    if input.slippage_bps >= 10_000 {
        return rejected_arithmetic(input, "invalid slippage or slippage arithmetic overflow");
    }
    let route_slippage_bps = input.slippage_bps;
    let amount_out = input.gross_profit.saturating_add(input.amount_in);
    let Some(adjusted_out) = slippage_adjusted(amount_out, route_slippage_bps) else {
        return rejected_arithmetic(input, "invalid slippage or slippage arithmetic overflow");
    };
    let adjusted_gross = adjusted_out.saturating_sub(input.amount_in);
    let slippage_deduction = input.gross_profit.saturating_sub(adjusted_gross);

    let net_before_gas = adjusted_gross.saturating_sub(flash_loan_fee);

    let Some(gas_cost_wei) = U256::from(input.gas_units).checked_mul(input.gas_price_wei) else {
        return rejected_arithmetic(input, "gas cost overflow");
    };
    // Worst-case revert loss for flash arb is gas spent (borrow repays on revert).
    let revert_penalty = gas_cost_wei;
    let scale = crate::util::ten_pow_u256(input.token_decimals);
    let rate_ok = input.token_to_matic_rate >= MIN_TOKEN_TO_MATIC_RATE;
    let gas_cost_in_tokens = if rate_ok {
        let Some(scaled_gas) = gas_cost_wei.checked_mul(scale) else {
            return rejected_arithmetic(input, "token-denominated gas cost overflow");
        };
        let Some(cost) = ceil_div(scaled_gas, input.token_to_matic_rate) else {
            return rejected_arithmetic(input, "token-denominated gas cost overflow");
        };
        cost
    } else {
        U256::MAX
    };

    let net_profit_after_gas = net_before_gas.saturating_sub(gas_cost_in_tokens);
    let gross_profit_matic_wei = if rate_ok {
        let Some(native_profit) = adjusted_gross.checked_mul(input.token_to_matic_rate) else {
            return rejected_arithmetic(input, "native profit conversion overflow");
        };
        native_profit / scale
    } else {
        U256::ZERO
    };
    let flash_fee_matic_wei = if rate_ok {
        let Some(native_fee) = flash_loan_fee.checked_mul(input.token_to_matic_rate) else {
            return rejected_arithmetic(input, "flash fee MATIC conversion overflow");
        };
        native_fee / scale
    } else {
        U256::ZERO
    };
    let net_profit_after_gas_matic_wei = gross_profit_matic_wei
        .saturating_sub(flash_fee_matic_wei)
        .saturating_sub(gas_cost_wei);

    let required_net_matic = safety_floor_matic_wei(revert_penalty, input.safety_multiplier_bps);
    let estimated_matic = gross_profit_matic_wei;
    let priority_uplift = profit_priority_uplift_wei(
        estimated_matic,
        input.profit_priority_alpha_bps,
        input.gas_units,
    );
    let net_after_priority = net_profit_after_gas_matic_wei.saturating_sub(priority_uplift);

    // ponytail: safety_bps==0 (dry-run) gates on pre-gas net so eth_call can run at high gas.
    let ignore_gas = input.safety_multiplier_bps == 0;
    let gate_net_tokens = if ignore_gas {
        net_before_gas
    } else {
        net_profit_after_gas
    };
    let gate_net_matic = if ignore_gas {
        gross_profit_matic_wei
            .saturating_sub(flash_fee_matic_wei)
            .saturating_sub(priority_uplift)
    } else {
        net_after_priority
    };

    let meets_absolute_min = gate_net_matic >= input.min_profit_matic_wei;
    // Safety floor hedges revert gas loss — compare pre-tip net. Priority uplift is a
    // success-path inclusion cost (already in gate_net for min_profit). Live: tip ate
    // ~0.03 MATIC and 1.5× floor rejected a route with pre-tip net≈gas.
    let meets_safety_ratio = if ignore_gas {
        gate_net_matic >= required_net_matic
    } else {
        net_profit_after_gas_matic_wei >= required_net_matic
    };
    let meets_sane_matic_cap = gate_net_matic <= U256::from(MAX_SANE_PROFIT_MATIC_WEI);
    let roi_bps = if input.amount_in.is_zero() || gate_net_tokens.is_zero() {
        0u64
    } else {
        gate_net_tokens
            .checked_mul(BPS_SCALE)
            .and_then(|v| u64::try_from(v / input.amount_in).ok())
            .unwrap_or(0)
    };
    let meets_roi = input.min_profit_roi_bps == 0 || roi_bps >= input.min_profit_roi_bps;

    let should_execute = rate_ok
        && gate_net_tokens > U256::ZERO
        && gate_net_matic > U256::ZERO
        && meets_absolute_min
        && meets_safety_ratio
        && meets_sane_matic_cap
        && meets_roi;

    let roi = if input.amount_in.is_zero() {
        0.0
    } else {
        roi_bps as f64 / 10_000.0
    };

    let reject_reason = if should_execute {
        None
    } else if !rate_ok {
        Some("token/MATIC rate too low or unavailable".into())
    } else if gate_net_matic.is_zero() || gate_net_tokens.is_zero() {
        Some("non-positive net profit after gas".into())
    } else if !meets_safety_ratio {
        Some(format!(
            "net profit {gate_net_matic} MATIC wei below safety floor {required_net_matic} (incl priority uplift {priority_uplift})"
        ))
    } else if !meets_absolute_min {
        Some("below min profit threshold".into())
    } else if !meets_sane_matic_cap {
        Some(format!(
            "net profit {gate_net_matic} MATIC wei exceeds sane cap {}",
            U256::from(MAX_SANE_PROFIT_MATIC_WEI)
        ))
    } else if !meets_roi {
        Some(format!(
            "net ROI {roi_bps} bps below minimum {} bps",
            input.min_profit_roi_bps
        ))
    } else {
        Some("non-positive net profit after gas".into())
    };

    ProfitAssessment {
        should_execute,
        gross_profit: input.gross_profit,
        gas_cost_wei,
        gas_cost_in_tokens,
        flash_loan_fee,
        slippage_deduction,
        revert_penalty,
        net_profit: net_before_gas,
        net_profit_after_gas,
        net_profit_after_gas_matic_wei: net_after_priority,
        roi,
        reject_reason,
    }
}

fn rejected_arithmetic(input: &AssessProfitInput, reason: &str) -> ProfitAssessment {
    // Prefer a real gas_cost when units×price fit — U256::MAX polluted best-eval logs
    // (cover_bps=0, gas_cost_wei≈2^256-1) on slippage rejects that still had a valid fee snapshot.
    let gas_cost_wei = U256::from(input.gas_units)
        .checked_mul(input.gas_price_wei)
        .unwrap_or(U256::ZERO);
    ProfitAssessment {
        should_execute: false,
        gross_profit: input.gross_profit,
        gas_cost_wei,
        gas_cost_in_tokens: U256::ZERO,
        flash_loan_fee: U256::ZERO,
        slippage_deduction: U256::ZERO,
        revert_penalty: gas_cost_wei,
        net_profit: U256::ZERO,
        net_profit_after_gas: U256::ZERO,
        net_profit_after_gas_matic_wei: U256::ZERO,
        roi: 0.0,
        reject_reason: Some(reason.to_string()),
    }
}

#[cfg(test)]
mod safety_tests {
    use super::*;

    fn input() -> AssessProfitInput {
        AssessProfitInput {
            gross_profit: U256::from(1_000_000u64),
            amount_in: U256::from(1_000_000u64),
            gas_units: 1,
            gas_price_wei: U256::from(1u8),
            token_to_matic_rate: RATE_PRECISION,
            token_decimals: 18,
            hop_count: 1,
            min_profit_matic_wei: U256::ZERO,
            min_profit_roi_bps: 0,
            slippage_bps: 100,
            flash_loan_source: FlashLoanSource::Balancer,
            safety_multiplier_bps: 10_000,
            profit_priority_alpha_bps: 0,
        }
    }

    #[test]
    fn invalid_slippage_fails_closed() {
        let mut i = input();
        i.slippage_bps = 10_000;
        let result = assess_profit(&i);
        assert!(!result.should_execute);
        assert!(
            result
                .reject_reason
                .expect("invalid slippage should provide a rejection reason")
                .contains("invalid slippage")
        );
    }

    #[test]
    fn multiplication_overflow_fails_closed() {
        let mut i = input();
        i.gross_profit = U256::MAX;
        let result = assess_profit(&i);
        assert!(!result.should_execute);
        assert!(result.reject_reason.is_some());
    }

    #[test]
    fn on_chain_min_profit_rejects_invalid_slippage() {
        assert!(
            on_chain_min_profit_for_route(
                U256::from(10u8),
                U256::from(100u8),
                10_000,
                1,
                FlashLoanSource::Balancer,
            )
            .is_none()
        );
    }

    #[test]
    fn on_chain_min_profit_matches_modeled_net() {
        let gross = U256::from(50_000u64);
        let amount_in = U256::from(1_000_000u64);
        let slippage_bps = 100;
        let input = AssessProfitInput {
            gross_profit: gross,
            amount_in,
            gas_units: 0,
            gas_price_wei: U256::ZERO,
            token_to_matic_rate: RATE_PRECISION,
            token_decimals: 18,
            hop_count: 1,
            min_profit_matic_wei: U256::ZERO,
            min_profit_roi_bps: 0,
            slippage_bps,
            flash_loan_source: FlashLoanSource::Balancer,
            safety_multiplier_bps: 0,
            profit_priority_alpha_bps: 0,
        };
        let assessment = assess_profit(&input);
        assert_eq!(
            on_chain_min_profit_for_route(
                gross,
                amount_in,
                slippage_bps,
                1,
                FlashLoanSource::Balancer
            ),
            on_chain_min_profit_from_assessment(&assessment)
        );
    }

    /// Depth-inflated route slip is already route-level on assessment; re-compounding
    /// per-hop config would set a higher minProfit than assess_profit modeled.
    #[test]
    fn on_chain_min_profit_from_assessment_respects_route_level_depth_slip() {
        let gross = U256::from(1_000_000u64);
        let amount_in = U256::from(10_000_000u64);
        // Depth haircut 500 bps on a 3-hop path exceeds compound(50, 3).
        let route_level_slip =
            crate::services::execution::support::effective_slippage_bps(50, 3, 500);
        assert!(route_level_slip >= 500);
        let assessment = assess_profit(&AssessProfitInput {
            gross_profit: gross,
            amount_in,
            gas_units: 0,
            gas_price_wei: U256::ZERO,
            token_to_matic_rate: RATE_PRECISION,
            token_decimals: 18,
            hop_count: 3,
            min_profit_matic_wei: U256::ZERO,
            min_profit_roi_bps: 0,
            slippage_bps: route_level_slip,
            flash_loan_source: FlashLoanSource::Balancer,
            safety_multiplier_bps: 0,
            profit_priority_alpha_bps: 0,
        });
        assert!(assessment.net_profit > U256::ZERO);
        let from_assessment =
            on_chain_min_profit_from_assessment(&assessment).expect("assessment min profit");
        // Wrong path: treat base 50 as per-hop and compound (ignores depth 500).
        let from_per_hop =
            on_chain_min_profit_for_route(gross, amount_in, 50, 3, FlashLoanSource::Balancer)
                .expect("per-hop min profit");
        // Assessment uses higher slip → lower net → lower minProfit (matches on-chain reality).
        assert!(
            from_assessment < from_per_hop,
            "assessment minProfit {from_assessment} should be < per-hop rebuild {from_per_hop}"
        );
    }

    #[test]
    fn priority_uplift_above_floor_reduces_reported_net() {
        let mut i = input();
        // Large gross so profit tip per gas exceeds 30 gwei floor → incremental uplift > 0.
        i.gross_profit = U256::from(5u128 * 10u128.pow(17)); // 0.5 MATIC gross
        i.amount_in = U256::from(5u128 * 10u128.pow(17));
        i.gas_units = 100_000;
        i.gas_price_wei = U256::from(280_000_000_000u64); // 280 gwei (base+floor)
        i.profit_priority_alpha_bps = 5_000; // 50% → tip well above floor
        i.safety_multiplier_bps = 0;
        i.min_profit_matic_wei = U256::ZERO;
        i.slippage_bps = 0;
        let without = assess_profit(&{
            let mut c = i.clone();
            c.profit_priority_alpha_bps = 0;
            c
        });
        let with = assess_profit(&i);
        // Field is post-tip (net_after_priority); above-floor tip must reduce it.
        assert!(
            with.net_profit_after_gas_matic_wei < without.net_profit_after_gas_matic_wei,
            "with={} without={}",
            with.net_profit_after_gas_matic_wei,
            without.net_profit_after_gas_matic_wei
        );
        // Full tip charged would be tip*gas; we only charge tip-floor (no double-count).
        let tip = profit_priority_tip_per_gas(U256::from(5u128 * 10u128.pow(17)), 5_000, 100_000);
        let floor = super::super::support::MIN_PRIORITY_FEE_PER_GAS;
        assert!(tip > floor);
        let charged = without
            .net_profit_after_gas_matic_wei
            .saturating_sub(with.net_profit_after_gas_matic_wei);
        let expected_incr = tip.saturating_sub(floor) * U256::from(100_000u64);
        assert_eq!(charged, expected_incr);
    }

    #[test]
    fn priority_uplift_zero_when_tip_at_or_below_floor() {
        // 0.01 MATIC × 10% / 1e6 gas = 1 gwei tip << 30 gwei floor → incremental 0.
        let uplift = profit_priority_uplift_wei(U256::from(10u128.pow(16)), 1_000, 1_000_000);
        assert!(uplift.is_zero());
        let tip = profit_priority_tip_per_gas(U256::from(10u128.pow(16)), 1_000, 1_000_000);
        assert!(tip < super::super::support::MIN_PRIORITY_FEE_PER_GAS);
    }

    #[test]
    fn insane_net_matic_profit_fails_closed() {
        let mut i = input();
        // MAX_SANE_PROFIT_MATIC_WEI = 1 POL; use 2 POL to exceed the cap.
        i.gross_profit = U256::from(2u128 * 10u128.pow(18));
        i.amount_in = U256::from(2u128 * 10u128.pow(18));
        i.slippage_bps = 0;
        i.safety_multiplier_bps = 0;
        let result = assess_profit(&i);
        assert!(!result.should_execute);
        assert!(
            result
                .reject_reason
                .as_deref()
                .is_some_and(|r| r.contains("sane cap"))
        );
    }

    #[test]
    fn brent_score_ranks_below_breakeven_by_shortfall() {
        // Gas ≈ 0.2 MATIC; both sizes underwater, larger gross must score higher.
        let ctx = ProfitEvalContext {
            gas_price: U256::from(300_000_000_000u64), // 300 gwei
            flash_source: FlashLoanSource::Balancer,
            slippage_bps: 0,
            token_to_matic_rate: RATE_PRECISION,
            token_decimals: 18,
            safety_multiplier_bps: 0,
            gas_scale_bps: 10_000,
            profit_priority_alpha_bps: 0,
            hop_count: 2,
        };
        let small = MinimalSimResult {
            amount_out: U256::from(10u128.pow(17)) + U256::from(10u128.pow(15)),
            profit: U256::from(10u128.pow(15)), // 0.001 MATIC gross
            total_gas: 700_000,
        };
        let large = MinimalSimResult {
            amount_out: U256::from(10u128.pow(17)) + U256::from(5 * 10u128.pow(15)),
            profit: U256::from(5 * 10u128.pow(15)), // 0.005 MATIC gross
            total_gas: 700_000,
        };
        let amount = U256::from(10u128.pow(17));
        assert!(
            net_profit_matic_from_sim(&small, amount, &ctx).is_zero()
                && net_profit_matic_from_sim(&large, amount, &ctx).is_zero(),
            "fixture must be below gas breakeven so net saturates to 0"
        );
        let small_score = brent_score_matic_from_sim(&small, amount, &ctx);
        let large_score = brent_score_matic_from_sim(&large, amount, &ctx);
        assert!(
            large_score > small_score,
            "brent must prefer closer-to-breakeven size: large={large_score} small={small_score}"
        );
        assert!(!small_score.is_zero() && !large_score.is_zero());
    }

    #[test]
    fn cover_matic_prefers_larger_gross_over_low_gas_dust() {
        // Dust: tiny gross, cheap gas → smaller shortfall but worse absolute cover.
        // Fat: 10× gross, higher gas → larger shortfall, better cover for Brent.
        let ctx = ProfitEvalContext {
            gas_price: U256::from(280_000_000_000u64), // 280 gwei
            flash_source: FlashLoanSource::Balancer,
            slippage_bps: 0,
            token_to_matic_rate: RATE_PRECISION,
            token_decimals: 18,
            safety_multiplier_bps: 0,
            gas_scale_bps: 10_000,
            profit_priority_alpha_bps: 0,
            hop_count: 2,
        };
        let dust = MinimalSimResult {
            amount_out: U256::from(10u128.pow(15)) + U256::from(56 * 10u128.pow(13)),
            profit: U256::from(56 * 10u128.pow(13)), // ~0.00056 MATIC
            total_gas: 525_000,
        };
        let fat = MinimalSimResult {
            amount_out: U256::from(10u128.pow(17)) + U256::from(5 * 10u128.pow(16)),
            profit: U256::from(5 * 10u128.pow(16)), // 0.05 MATIC
            total_gas: 900_000,
        };
        let dust_in = U256::from(10u128.pow(15));
        let fat_in = U256::from(10u128.pow(17));
        assert!(
            net_profit_matic_from_sim(&dust, dust_in, &ctx).is_zero()
                && net_profit_matic_from_sim(&fat, fat_in, &ctx).is_zero()
        );
        let dust_cover = cover_matic_from_sim(&dust, dust_in, &ctx);
        let fat_cover = cover_matic_from_sim(&fat, fat_in, &ctx);
        assert!(
            fat_cover > dust_cover,
            "cover must prefer absolute edge: fat={fat_cover} dust={dust_cover}"
        );
        // Brent shortfall score can still prefer dust (lower gas); that is OK for sizing.
        let dust_brent = brent_score_matic_from_sim(&dust, dust_in, &ctx);
        let fat_brent = brent_score_matic_from_sim(&fat, fat_in, &ctx);
        assert!(
            dust_brent > fat_brent,
            "fixture expects dust to win brent shortfall: dust={dust_brent} fat={fat_brent}"
        );
    }

    #[test]
    fn compound_slippage_matches_per_hop_product() {
        assert_eq!(compound_slippage_bps(50, 1), 50);
        // 10000*(9950/10000)^4 with integer division → retained 9800 → 200 bps.
        assert_eq!(compound_slippage_bps(50, 4), 200);
    }

    #[test]
    fn assess_profit_treats_slippage_as_route_level_not_recompounded() {
        let mut i = input();
        i.gross_profit = U256::from(100_000u64);
        i.amount_in = U256::from(1_000_000u64);
        i.hop_count = 4;
        // Route-level 100 bps once (not compound(100,4) ≈ 400).
        i.slippage_bps = 100;
        i.safety_multiplier_bps = 0;
        let result = assess_profit(&i);
        assert_eq!(result.slippage_deduction, U256::from(11_000u64));
        assert!(result.should_execute);
    }

    #[test]
    fn slippage_applied_to_amount_out_not_gross_profit() {
        let mut i = input();
        i.gross_profit = U256::from(100_000u64);
        i.amount_in = U256::from(1_000_000u64);
        i.slippage_bps = 100;
        i.safety_multiplier_bps = 0;
        let result = assess_profit(&i);
        assert!(!result.slippage_deduction.is_zero());
        assert_eq!(result.slippage_deduction, U256::from(11_000u64));
        assert!(result.should_execute);
    }

    #[test]
    fn safety_zero_ignores_gas_for_execute_gate() {
        let mut i = input();
        i.gross_profit = U256::from(10u128.pow(16)); // 0.01 token @ 1:1 rate
        i.amount_in = U256::from(10u128.pow(17));
        i.slippage_bps = 0;
        i.gas_units = 700_000;
        i.gas_price_wei = U256::from(300_000_000_000u64); // 300 gwei → gas >> gross
        i.safety_multiplier_bps = 10_000;
        i.min_profit_matic_wei = U256::ZERO;
        assert!(!assess_profit(&i).should_execute);
        i.safety_multiplier_bps = 0;
        assert!(assess_profit(&i).should_execute);
    }

    #[test]
    fn live_gas_floor_accepts_breakeven_cover_and_rejects_less() {
        let mut i = input();
        i.flash_loan_source = FlashLoanSource::Direct;
        i.slippage_bps = 0;
        i.gas_units = 1;
        i.gas_price_wei = U256::from(100u64);
        i.gross_profit = U256::from(200u64);
        i.amount_in = U256::from(200u64);
        i.min_profit_matic_wei = U256::from(1u64);
        assert!(assess_profit(&i).should_execute);
        i.gross_profit = U256::from(199u64);
        assert!(!assess_profit(&i).should_execute);
    }

    #[test]
    fn zero_slippage_preserves_full_profit() {
        let mut i = input();
        i.slippage_bps = 0;
        let result = assess_profit(&i);
        assert_eq!(result.slippage_deduction, U256::ZERO);
        assert_eq!(
            result.net_profit,
            result.gross_profit.saturating_sub(result.flash_loan_fee)
        );
    }

    #[test]
    fn aave_flash_fee_matches_percent_mul_half_up() {
        // aave-v3-core: (value * bps + 5000) / 10000 — differs from floor at small sizes.
        set_aave_flash_loan_fee_bps(5);
        let amount = U256::from(1_000u64);
        let fee = flash_loan_fee_amount(FlashLoanSource::AaveV3, amount)
            .expect("Aave flash fee must be representable");
        assert_eq!(fee, U256::from(1u64)); // floor would be 0
        assert_eq!(
            aave_percent_mul(U256::from(10_000u64), 5),
            Some(U256::from(5u64))
        );
    }

    #[test]
    fn balancer_flash_fee_uses_fixed_point_mul_up() {
        set_balancer_flash_loan_fee_pct(0);
        assert_eq!(
            flash_loan_fee_amount(FlashLoanSource::Balancer, U256::from(1_000_000u64)),
            Some(U256::ZERO)
        );
        // 1 bps = 1e14 in 1e18 FixedPoint
        set_balancer_flash_loan_fee_pct(100_000_000_000_000);
        let fee = flash_loan_fee_amount(FlashLoanSource::Balancer, U256::from(1_000_000u64))
            .expect("Balancer flash fee must be representable");
        assert_eq!(fee, U256::from(100u64));
        assert_eq!(balancer_flash_fee_pct_to_bps(100_000_000_000_000), 1);
        const { assert!(!DODO_EXTERNAL_FLASH_ENABLED) };
        assert_eq!(
            flash_loan_fee_amount(FlashLoanSource::Dodo, U256::from(1_000_000u64)),
            Some(U256::ZERO)
        );
    }

    #[test]
    fn route_and_tick_route_gas_match_for_single_fingerprint() {
        let oracle = crate::services::execution::gas_oracle::GasOracle::default();
        oracle.record_route_gas(0xABCD, 250_000);
        let lookup = RouteGasLookup::for_fingerprints(&oracle, [0xABCD]);
        let tick = AssessmentGas::TickRoute {
            lookup: &lookup,
            oracle: &oracle,
            route_fp: 0xABCD,
        };
        let route = AssessmentGas::Route {
            oracle: &oracle,
            route_fp: 0xABCD,
        };
        let tick_gas = assessment_gas_units(100_000, &tick);
        let route_gas = assessment_gas_units(100_000, &route);
        assert_eq!(tick_gas, route_gas);
        assert_eq!(tick_gas, 250_000);
    }

    #[test]
    fn scaled_heuristic_gas_matches_oracle_route_lookup() {
        let oracle = crate::services::execution::gas_oracle::GasOracle::default();
        for _ in 0..8 {
            oracle.record_sim_observed(100_000, 200_000);
        }
        let lookup = RouteGasLookup::for_fingerprints(&oracle, [0xBEEF]);
        let tick = AssessmentGas::TickRoute {
            lookup: &lookup,
            oracle: &oracle,
            route_fp: 0xBEEF,
        };
        let route = AssessmentGas::Route {
            oracle: &oracle,
            route_fp: 0xBEEF,
        };
        let tick_gas = assessment_gas_units(100_000, &tick);
        let route_gas = assessment_gas_units(100_000, &route);
        assert_eq!(tick_gas, route_gas);
        assert!(tick_gas > 100_000);
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn modeled_net_matches_assessment_zero_gas(
            gross in 1u64..1_000_000u64,
            amount_in in 1_000_000u64..10_000_000u64,
            slippage in 0u64..500u64,
        ) {
            let gross = U256::from(gross);
            let amount_in = U256::from(amount_in);
            let modeled = modeled_net_profit_tokens(gross, amount_in, slippage, 1, FlashLoanSource::Balancer);
            let assessment = assess_profit(&AssessProfitInput {
                gross_profit: gross,
                amount_in,
                gas_units: 0,
                gas_price_wei: U256::ZERO,
                token_to_matic_rate: RATE_PRECISION,
                token_decimals: 18,
                hop_count: 1,
                min_profit_matic_wei: U256::ZERO,
                min_profit_roi_bps: 0,
                slippage_bps: slippage,
                flash_loan_source: FlashLoanSource::Balancer,
                safety_multiplier_bps: 0,
                profit_priority_alpha_bps: 0,
            });
            prop_assert_eq!(modeled, Some(assessment.net_profit));
        }

        #[test]
        fn slippage_at_or_above_10000_rejects(
            gross in 1u64..1000u64,
            amount_in in 1u64..1000u64,
            slippage in 10_000u64..=50_000u64,
        ) {
            let input = AssessProfitInput {
                gross_profit: U256::from(gross),
                amount_in: U256::from(amount_in),
                gas_units: 1,
                gas_price_wei: U256::from(1u8),
                token_to_matic_rate: RATE_PRECISION,
                token_decimals: 18,
                hop_count: 1,
                min_profit_matic_wei: U256::ZERO,
                min_profit_roi_bps: 0,
                slippage_bps: slippage,
                flash_loan_source: FlashLoanSource::Balancer,
                safety_multiplier_bps: 0,
                profit_priority_alpha_bps: 0,
            };
            prop_assert!(!assess_profit(&input).should_execute);
        }
    }
}
