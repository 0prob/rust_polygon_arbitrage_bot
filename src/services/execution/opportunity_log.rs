use ruint::aliases::U256;
use tracing::info;

use crate::core::types::ProfitAssessment;

/// Structured opportunity record for offline tuning (enable with `TRACING_JSON=1`).
#[derive(Debug, Clone)]
pub struct OpportunityRecord {
    pub route_fingerprint: u64,
    pub hop_count: u32,
    pub amount_in: U256,
    pub gross_profit: U256,
    pub gas_units: u32,
    pub gas_price_wei: U256,
    pub net_profit_matic_wei: U256,
    pub slippage_bps: u64,
    pub flash_fee: U256,
    pub gas_cost_wei: U256,
    pub should_execute: bool,
    pub reject_reason: Option<String>,
    pub elapsed_ms: u64,
    pub outcome: Option<String>,
}

impl OpportunityRecord {
    pub fn from_assessment(
        route_fingerprint: u64,
        hop_count: u32,
        amount_in: U256,
        gas_units: u32,
        gas_price_wei: U256,
        slippage_bps: u64,
        assessment: &ProfitAssessment,
        should_execute: bool,
        elapsed_ms: u64,
    ) -> Self {
        Self {
            route_fingerprint,
            hop_count,
            amount_in,
            gross_profit: assessment.gross_profit,
            gas_units,
            gas_price_wei,
            net_profit_matic_wei: assessment.net_profit_after_gas_matic_wei,
            slippage_bps,
            flash_fee: assessment.flash_loan_fee,
            gas_cost_wei: assessment.gas_cost_wei,
            should_execute,
            reject_reason: assessment.reject_reason.clone(),
            elapsed_ms,
            outcome: None,
        }
    }
}

pub fn log_opportunity_evaluated(record: &OpportunityRecord) {
    info!(
        target: "arb.opportunity",
        route_fingerprint = record.route_fingerprint,
        hop_count = record.hop_count,
        amount_in = %record.amount_in,
        gross_profit = %record.gross_profit,
        gas_units = record.gas_units,
        gas_price_wei = %record.gas_price_wei,
        gas_cost_wei = %record.gas_cost_wei,
        flash_fee = %record.flash_fee,
        slippage_bps = record.slippage_bps,
        net_profit_matic_wei = %record.net_profit_matic_wei,
        should_execute = record.should_execute,
        reject_reason = record.reject_reason.as_deref().unwrap_or(""),
        outcome = record.outcome.as_deref().unwrap_or(""),
        elapsed_ms = record.elapsed_ms,
        "opportunity evaluated"
    );
    crate::services::execution::opportunity_journal::journal_from_record(record);
}

pub fn log_opportunity_outcome(
    route_fingerprint: u64,
    assessment: &ProfitAssessment,
    slippage_bps: u64,
    dry_run_gas: Option<u64>,
    outcome: &str,
) {
    info!(
        target: "arb.opportunity",
        route_fingerprint,
        quoted_net_matic = %assessment.net_profit_after_gas_matic_wei,
        gross_profit = %assessment.gross_profit,
        gas_cost_wei = %assessment.gas_cost_wei,
        flash_fee = %assessment.flash_loan_fee,
        slippage_bps,
        dry_run_gas,
        outcome,
        "opportunity outcome"
    );
    crate::services::execution::opportunity_journal::journal_outcome(
        route_fingerprint,
        assessment,
        slippage_bps,
        dry_run_gas,
        outcome,
    );
}
