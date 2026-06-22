use std::collections::HashMap;

use alloy::primitives::Address;
use ruint::aliases::U256;
use rustc_hash::FxHashMap;
use tracing::instrument;

use crate::core::constants::BPS_SCALE;
use crate::core::types::{FlashLoanSource, ProfitAssessment, TokenIndex};
use crate::pipeline::arena::StateArena;
use crate::pipeline::types::MinimalSimResult;
use crate::services::oracle::resolve_token_to_matic_rate;

pub const ON_CHAIN_MIN_PROFIT_RATIO_BPS: u64 = 9500;
pub use crate::core::constants::{MIN_TOKEN_TO_MATIC_RATE, RATE_PRECISION};

pub fn flash_loan_fee_bps(source: FlashLoanSource) -> u64 {
    match source {
        FlashLoanSource::Balancer => 0,
        FlashLoanSource::AaveV3 => 5,
    }
}

pub fn on_chain_min_profit(token_profit: U256) -> U256 {
    if token_profit.is_zero() {
        return U256::ZERO;
    }
    (token_profit * U256::from(ON_CHAIN_MIN_PROFIT_RATIO_BPS)) / BPS_SCALE
}

/// On-chain `minProfit` aligned with off-chain slippage-adjusted gross.
pub fn on_chain_min_profit_for_route(gross_profit: U256, slippage_bps: u64) -> U256 {
    let basis = slippage_adjusted(gross_profit, slippage_bps).unwrap_or(gross_profit);
    on_chain_min_profit(basis)
}

pub fn slippage_adjusted(amount_out: U256, slippage_bps: u64) -> Option<U256> {
    if amount_out.is_zero() || slippage_bps >= 10_000 {
        return None;
    }
    let min_out = (amount_out * (BPS_SCALE - U256::from(slippage_bps))) / BPS_SCALE;
    if min_out.is_zero() {
        None
    } else {
        Some(min_out)
    }
}

/// Default 3× worst-case gas loss buffer before submitting (30_000 bps = 3.0×).
pub const DEFAULT_PROFIT_SAFETY_MULTIPLIER_BPS: u64 = 30_000;

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
    pub slippage_bps: u64,
    pub flash_loan_source: FlashLoanSource,
    /// Net profit must exceed `gas_cost_matic * safety_multiplier_bps / 10_000`.
    pub safety_multiplier_bps: u64,
}

/// Context for Brent sizing — maximizes net profit after gas/fees/slippage.
#[derive(Debug, Clone, Copy)]
pub struct ProfitEvalContext {
    pub gas_price: U256,
    pub flash_source: FlashLoanSource,
    pub slippage_bps: u64,
    pub token_to_matic_rate: U256,
    pub token_decimals: u8,
    pub safety_multiplier_bps: u64,
}

impl ProfitEvalContext {
    pub fn for_cycle(
        cycle_start: TokenIndex,
        arena: &StateArena,
        token_to_matic_rates: &FxHashMap<TokenIndex, U256>,
        token_decimals: &HashMap<Address, u8>,
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

    pub fn with_safety_multiplier(
        cycle_start: TokenIndex,
        arena: &StateArena,
        token_to_matic_rates: &FxHashMap<TokenIndex, U256>,
        token_decimals: &HashMap<Address, u8>,
        gas_price: U256,
        slippage_bps: u64,
        flash_source: FlashLoanSource,
        safety_multiplier_bps: u64,
    ) -> Self {
        let token_to_matic_rate =
            resolve_token_to_matic_rate(cycle_start, arena, token_to_matic_rates);
        let token_decimals = arena
            .token_address(cycle_start)
            .and_then(|a| token_decimals.get(&a).copied())
            .unwrap_or(18);
        Self {
            gas_price,
            flash_source,
            slippage_bps,
            token_to_matic_rate,
            token_decimals,
            safety_multiplier_bps,
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

pub struct ProfitThresholds {
    pub min_profit_matic_wei: U256,
    pub min_profit_roi_bps: u64,
    pub safety_multiplier_bps: u64,
}

pub fn build_assess_input(
    cycle_start: TokenIndex,
    arena: &StateArena,
    route: RouteProfitParams,
    token_to_matic_rates: &FxHashMap<TokenIndex, U256>,
    token_decimals: &HashMap<Address, u8>,
    gas_price: U256,
    thresholds: ProfitThresholds,
) -> AssessProfitInput {
    AssessProfitInput {
        gross_profit: route.gross_profit,
        amount_in: route.amount_in,
        gas_units: route.gas_units,
        gas_price_wei: gas_price,
        token_to_matic_rate: resolve_token_to_matic_rate(cycle_start, arena, token_to_matic_rates),
        token_decimals: arena
            .token_address(cycle_start)
            .and_then(|a| token_decimals.get(&a).copied())
            .unwrap_or(18),
        hop_count: route.hop_count,
        min_profit_matic_wei: thresholds.min_profit_matic_wei,
        min_profit_roi_bps: thresholds.min_profit_roi_bps,
        slippage_bps: route.slippage_bps,
        flash_loan_source: route.flash_loan_source,
        safety_multiplier_bps: thresholds.safety_multiplier_bps,
    }
}

pub fn net_profit_after_gas_from_sim(
    sim: &MinimalSimResult,
    amount_in: U256,
    ctx: &ProfitEvalContext,
) -> U256 {
    assess_profit(AssessProfitInput {
        gross_profit: sim.profit,
        amount_in,
        gas_units: sim.total_gas,
        gas_price_wei: ctx.gas_price,
        token_to_matic_rate: ctx.token_to_matic_rate,
        token_decimals: ctx.token_decimals,
        hop_count: 0,
        min_profit_matic_wei: U256::ZERO,
        min_profit_roi_bps: 0,
        slippage_bps: ctx.slippage_bps,
        flash_loan_source: ctx.flash_source,
        safety_multiplier_bps: ctx.safety_multiplier_bps,
    })
    .net_profit_after_gas
}

#[instrument(
    skip(input),
    fields(
        hop_count = input.hop_count,
        gross_profit = %input.gross_profit,
        gas_units = input.gas_units,
        should_execute = tracing::field::Empty,
        net_profit_matic_wei = tracing::field::Empty,
    )
)]
pub fn assess_profit(input: AssessProfitInput) -> ProfitAssessment {
    let flash_fee_bps = flash_loan_fee_bps(input.flash_loan_source);
    let flash_loan_fee = (input.amount_in * U256::from(flash_fee_bps)) / BPS_SCALE;

    // Slippage conservatively reduces expected gross profit, not borrow size.
    let slippage_deduction = (input.gross_profit * U256::from(input.slippage_bps)) / BPS_SCALE;

    let net_before_gas = input
        .gross_profit
        .saturating_sub(flash_loan_fee)
        .saturating_sub(slippage_deduction);

    let gas_cost_wei = U256::from(input.gas_units) * input.gas_price_wei;
    // Worst-case revert loss for flash arb is gas spent (borrow repays on revert).
    let revert_penalty = gas_cost_wei;
    let scale = U256::from(10u128).pow(U256::from(input.token_decimals as u32));
    let rate_ok = input.token_to_matic_rate >= MIN_TOKEN_TO_MATIC_RATE;
    let gas_cost_in_tokens = if !rate_ok {
        U256::MAX
    } else {
        (gas_cost_wei * scale) / input.token_to_matic_rate
    };

    let net_profit_after_gas = net_before_gas.saturating_sub(gas_cost_in_tokens);
    let net_profit_after_gas_matic_wei = if !rate_ok {
        U256::ZERO
    } else {
        (net_profit_after_gas * input.token_to_matic_rate) / scale
    };

    let safety_bps = if input.safety_multiplier_bps == 0 {
        DEFAULT_PROFIT_SAFETY_MULTIPLIER_BPS
    } else {
        input.safety_multiplier_bps
    };
    let required_net_matic = (revert_penalty * U256::from(safety_bps)) / BPS_SCALE;

    let meets_absolute_min = net_profit_after_gas_matic_wei >= input.min_profit_matic_wei;
    let meets_safety_ratio = net_profit_after_gas_matic_wei >= required_net_matic;
    let roi_bps = if input.amount_in.is_zero() || net_profit_after_gas.is_zero() {
        0u64
    } else {
        u64::try_from((net_profit_after_gas * BPS_SCALE) / input.amount_in).unwrap_or(0)
    };
    let meets_roi = input.min_profit_roi_bps == 0 || roi_bps >= input.min_profit_roi_bps;

    let should_execute = rate_ok
        && net_profit_after_gas > U256::ZERO
        && meets_absolute_min
        && meets_safety_ratio
        && meets_roi;

    tracing::Span::current().record("should_execute", should_execute);
    tracing::Span::current().record(
        "net_profit_matic_wei",
        tracing::field::display(&net_profit_after_gas_matic_wei),
    );

    let roi = if input.amount_in.is_zero() {
        0.0
    } else {
        roi_bps as f64 / 10_000.0
    };

    let reject_reason = if should_execute {
        None
    } else if !rate_ok {
        Some("token/MATIC rate too low or unavailable".into())
    } else if !meets_safety_ratio {
        Some(format!(
            "net profit {net_profit_after_gas_matic_wei} MATIC wei below safety floor {required_net_matic} ({safety_bps} bps × worst-case gas)"
        ))
    } else if !meets_absolute_min {
        Some("below min profit threshold".into())
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
        net_profit_after_gas_matic_wei,
        roi,
        reject_reason,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_input() -> AssessProfitInput {
        AssessProfitInput {
            gross_profit: U256::from(1_000_000_000_000_000_000u128), // 1 token
            amount_in: U256::from(100_000_000_000_000_000_000u128),  // 100 tokens
            gas_units: 500_000,
            gas_price_wei: U256::from(30_000_000_000u64), // 30 gwei
            token_to_matic_rate: U256::from(1_000_000_000_000_000_000u64), // 1:1
            token_decimals: 18,
            hop_count: 3,
            min_profit_matic_wei: U256::from(1_000_000_000_000_000u64), // 0.001 MATIC
            min_profit_roi_bps: 0,
            slippage_bps: 50,
            flash_loan_source: FlashLoanSource::Balancer,
            safety_multiplier_bps: DEFAULT_PROFIT_SAFETY_MULTIPLIER_BPS,
        }
    }

    #[test]
    fn on_chain_min_profit_95_percent() {
        let p = U256::from(1000u64);
        assert_eq!(on_chain_min_profit(p), U256::from(950u64));
    }

    #[test]
    fn non_18_decimal_token_gas_cost_is_accurate() {
        let mut input = default_input();
        input.token_decimals = 6; // USDC-like
        input.amount_in = U256::from(100_000_000u64); // 100 USDC
        input.gross_profit = U256::from(1_000_000u64); // 1 USDC profit
        input.token_to_matic_rate = U256::from(1_428_571_428_571_428_571u64); // 1 USDC ≈ 1.428 MATIC
        let a = assess_profit(input);
        // Gas cost should not exceed profit for 6-decimals (old bug would have)
        assert!(a.gas_cost_in_tokens < a.gross_profit, "6-dec gas cost {} should be < profit {}", a.gas_cost_in_tokens, a.gross_profit);
        // MATIC conversion should be sensible
        assert!(a.net_profit_after_gas_matic_wei > U256::ZERO);
    }

    #[test]
    fn rate_failure_rejects_trade() {
        let mut input = default_input();
        input.token_to_matic_rate = U256::from(100u64); // Below MIN_TOKEN_TO_MATIC_RATE
        let a = assess_profit(input);
        assert!(!a.should_execute);
        assert!(a.reject_reason.is_some());
        assert!(a.reject_reason.unwrap().contains("rate"));
    }

    #[test]
    fn safety_multiplier_override_works() {
        let mut input = default_input();
        // Set gas cost to make profit marginal
        input.gas_units = 5_000_000;
        input.gas_price_wei = U256::from(100_000_000_000u64); // 100 gwei
        // Low safety multiplier should pass, high should fail
        input.safety_multiplier_bps = 1; // 0.01% — essentially no safety
        let a_low = assess_profit(input.clone());
        input.safety_multiplier_bps = 100_000; // 10x — absurdly high
        let a_high = assess_profit(input);
        assert!(a_low.should_execute || !a_high.should_execute,
            "low safety should be more permissive than high safety");
    }

    #[test]
    fn zero_gas_means_no_revert_penalty() {
        let mut input = default_input();
        input.gas_units = 0;
        input.gas_price_wei = U256::ZERO;
        let a = assess_profit(input);
        assert_eq!(a.revert_penalty, U256::ZERO);
        assert_eq!(a.gas_cost_wei, U256::ZERO);
    }

    #[test]
    fn slippage_applied_to_gross_profit_not_amount_in() {
        let input = AssessProfitInput {
            gross_profit: U256::from(1000u64),
            amount_in: U256::from(1_000_000u64),
            gas_units: 0,
            gas_price_wei: U256::ZERO,
            token_to_matic_rate: U256::from(1_000_000_000_000_000_000u64),
            token_decimals: 18,
            hop_count: 2,
            min_profit_matic_wei: U256::ZERO,
            min_profit_roi_bps: 0,
            slippage_bps: 100,
            flash_loan_source: FlashLoanSource::Balancer,
            safety_multiplier_bps: DEFAULT_PROFIT_SAFETY_MULTIPLIER_BPS,
        };
        let a = assess_profit(input);
        assert_eq!(a.slippage_deduction, U256::from(10u64));
    }

    #[test]
    fn roi_threshold_rejects_low_margin() {
        let input = AssessProfitInput {
            gross_profit: U256::from(100u64),
            amount_in: U256::from(1_000_000u64),
            gas_units: 0,
            gas_price_wei: U256::ZERO,
            token_to_matic_rate: U256::from(1_000_000_000_000_000_000u64),
            token_decimals: 18,
            hop_count: 2,
            min_profit_matic_wei: U256::ZERO,
            min_profit_roi_bps: 50,
            slippage_bps: 0,
            flash_loan_source: FlashLoanSource::Balancer,
            safety_multiplier_bps: DEFAULT_PROFIT_SAFETY_MULTIPLIER_BPS,
        };
        let a = assess_profit(input);
        assert!(!a.should_execute);
    }
}
