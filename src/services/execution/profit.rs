use std::sync::atomic::AtomicU64;

use alloy::primitives::Address;
use alloy::primitives::U256;
use rustc_hash::FxHashMap;

use crate::core::constants::BPS_SCALE;
use crate::core::types::{FlashLoanSource, ProfitAssessment, TokenIndex};
use crate::pipeline::arena::StateArena;
use crate::pipeline::types::MinimalSimResult;
use crate::services::oracle::resolve_token_to_matic_rate;

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

pub fn flash_loan_fee_bps(source: FlashLoanSource) -> u64 {
    match source {
        FlashLoanSource::Balancer | FlashLoanSource::Direct => 0,
        FlashLoanSource::AaveV3 => {
            AAVE_FLASH_LOAN_FEE_BPS.load(std::sync::atomic::Ordering::Relaxed)
        }
    }
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

/// On-chain `minProfit` aligned with off-chain slippage-adjusted gross.
#[must_use]
pub fn on_chain_min_profit_for_route(gross_profit: U256, slippage_bps: u64) -> Option<U256> {
    let basis = slippage_adjusted(gross_profit, slippage_bps)?;
    on_chain_min_profit(basis)
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
    /// Gas-oracle scale in bps (10_000 = 1.0×). Applied to simulated gas before
    /// cost deduction so the Brent objective matches the final assessment.
    pub gas_scale_bps: u64,
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
        let token_to_matic_rate =
            resolve_token_to_matic_rate(cycle_start, arena, token_to_matic_rates);
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
        token_to_matic_rate: resolve_token_to_matic_rate(cycle_start, arena, token_to_matic_rates),
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
    })
}

#[must_use]
pub fn net_profit_after_gas_from_sim(
    sim: &MinimalSimResult,
    amount_in: U256,
    ctx: &ProfitEvalContext,
) -> U256 {
    let gas_units = if ctx.gas_scale_bps == 10_000 {
        sim.total_gas
    } else {
        crate::services::execution::support::scaled_simulated_gas(sim.total_gas, ctx.gas_scale_bps)
    };
    assess_profit(&AssessProfitInput {
        gross_profit: sim.profit,
        amount_in,
        gas_units,
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

#[must_use]
pub fn assess_profit(input: &AssessProfitInput) -> ProfitAssessment {
    let flash_fee_bps = flash_loan_fee_bps(input.flash_loan_source);
    let Some(flash_loan_fee) = input
        .amount_in
        .checked_mul(U256::from(flash_fee_bps))
        .map(|v| v / BPS_SCALE)
    else {
        return rejected_arithmetic(input, "flash-loan fee overflow");
    };

    // Slippage reduces output, never input. Apply to amount_out, not gross_profit.
    let amount_out = input.gross_profit.saturating_add(input.amount_in);
    let Some(adjusted_out) = slippage_adjusted(amount_out, input.slippage_bps) else {
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
        scaled_gas / input.token_to_matic_rate
    } else {
        U256::MAX
    };

    let net_profit_after_gas = net_before_gas.saturating_sub(gas_cost_in_tokens);
    let net_profit_after_gas_matic_wei = if rate_ok {
        let Some(native_profit) = net_profit_after_gas.checked_mul(input.token_to_matic_rate)
        else {
            return rejected_arithmetic(input, "native profit conversion overflow");
        };
        native_profit / scale
    } else {
        U256::ZERO
    };

    let safety_bps = if input.safety_multiplier_bps == 0 {
        DEFAULT_PROFIT_SAFETY_MULTIPLIER_BPS
    } else {
        input.safety_multiplier_bps
    };
    let Some(required_net_matic) = revert_penalty
        .checked_mul(U256::from(safety_bps))
        .map(|v| v / BPS_SCALE)
    else {
        return rejected_arithmetic(input, "safety-floor overflow");
    };

    let meets_absolute_min = net_profit_after_gas_matic_wei >= input.min_profit_matic_wei;
    let meets_safety_ratio = net_profit_after_gas_matic_wei >= required_net_matic;
    let meets_sane_matic_cap =
        net_profit_after_gas_matic_wei <= U256::from(MAX_SANE_PROFIT_MATIC_WEI);
    let roi_bps = if input.amount_in.is_zero() || net_profit_after_gas.is_zero() {
        0u64
    } else {
        net_profit_after_gas
            .checked_mul(BPS_SCALE)
            .and_then(|v| u64::try_from(v / input.amount_in).ok())
            .unwrap_or(0)
    };
    let meets_roi = input.min_profit_roi_bps == 0 || roi_bps >= input.min_profit_roi_bps;

    let should_execute = rate_ok
        && net_profit_after_gas > U256::ZERO
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
    } else if !meets_safety_ratio {
        Some(format!(
            "net profit {net_profit_after_gas_matic_wei} MATIC wei below safety floor {required_net_matic} ({safety_bps} bps × worst-case gas)"
        ))
    } else if !meets_absolute_min {
        Some("below min profit threshold".into())
    } else if !meets_sane_matic_cap {
        Some(format!(
            "net profit {net_profit_after_gas_matic_wei} MATIC wei exceeds sane cap {}",
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
        net_profit_after_gas_matic_wei,
        roi,
        reject_reason,
    }
}

fn rejected_arithmetic(input: &AssessProfitInput, reason: &str) -> ProfitAssessment {
    ProfitAssessment {
        should_execute: false,
        gross_profit: input.gross_profit,
        gas_cost_wei: U256::MAX,
        gas_cost_in_tokens: U256::MAX,
        flash_loan_fee: U256::ZERO,
        slippage_deduction: U256::ZERO,
        revert_penalty: U256::MAX,
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
        assert!(on_chain_min_profit_for_route(U256::from(10u8), 10_000).is_none());
    }

    #[test]
    fn insane_net_matic_profit_fails_closed() {
        let mut i = input();
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
}
