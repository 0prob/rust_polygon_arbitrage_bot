use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::LazyLock;

use parking_lot::Mutex;
use serde::Serialize;

use crate::core::types::ProfitAssessment;
use crate::services::execution::opportunity_log::OpportunityRecord;

static JOURNAL: LazyLock<Mutex<Option<std::fs::File>>> = LazyLock::new(|| Mutex::new(None));

pub fn init_from_env() {
    if let Ok(path) = std::env::var("OPPORTUNITY_JOURNAL_PATH")
        && !path.trim().is_empty()
    {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(PathBuf::from(path))
            .ok();
        *JOURNAL.lock() = file;
    }
}

#[derive(Serialize)]
struct JournalEntry<'a> {
    ts_ms: u64,
    route_fingerprint: u64,
    hop_count: u32,
    amount_in: String,
    gross_profit: String,
    net_profit_matic_wei: String,
    gas_units: u32,
    gas_price_wei: String,
    gas_cost_wei: String,
    flash_fee: String,
    slippage_bps: u64,
    should_execute: bool,
    reject_reason: Option<&'a str>,
    flash_source: &'a str,
    outcome: Option<&'a str>,
    dry_run_gas: Option<u64>,
    tx_hash: Option<&'a str>,
}

fn append(entry: &JournalEntry<'_>) {
    let mut guard = JOURNAL.lock();
    let Some(file) = guard.as_mut() else {
        return;
    };
    if let Ok(line) = serde_json::to_string(entry) {
        let _ = writeln!(file, "{line}");
    }
}

pub fn journal_from_record(record: &OpportunityRecord) {
    append(&JournalEntry {
        ts_ms: crate::util::now_ms(),
        route_fingerprint: record.route_fingerprint,
        hop_count: record.hop_count,
        amount_in: record.amount_in.to_string(),
        gross_profit: record.gross_profit.to_string(),
        net_profit_matic_wei: record.net_profit_matic_wei.to_string(),
        gas_units: record.gas_units,
        gas_price_wei: record.gas_price_wei.to_string(),
        gas_cost_wei: record.gas_cost_wei.to_string(),
        flash_fee: record.flash_fee.to_string(),
        slippage_bps: record.slippage_bps,
        should_execute: record.should_execute,
        reject_reason: record.reject_reason.as_deref(),
        flash_source: "eval",
        outcome: record.outcome.as_deref(),
        dry_run_gas: None,
        tx_hash: None,
    });
}

pub fn journal_outcome(
    route_fingerprint: u64,
    assessment: &ProfitAssessment,
    slippage_bps: u64,
    dry_run_gas: Option<u64>,
    outcome: &str,
) {
    append(&JournalEntry {
        ts_ms: crate::util::now_ms(),
        route_fingerprint,
        hop_count: 0,
        amount_in: "0".into(),
        gross_profit: assessment.gross_profit.to_string(),
        net_profit_matic_wei: assessment.net_profit_after_gas_matic_wei.to_string(),
        gas_units: 0,
        gas_price_wei: "0".into(),
        gas_cost_wei: assessment.gas_cost_wei.to_string(),
        flash_fee: assessment.flash_loan_fee.to_string(),
        slippage_bps,
        should_execute: assessment.should_execute,
        reject_reason: assessment.reject_reason.as_deref(),
        flash_source: "dispatch",
        outcome: Some(outcome),
        dry_run_gas,
        tx_hash: None,
    });
}
