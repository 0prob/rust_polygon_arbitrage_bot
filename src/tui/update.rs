use std::collections::HashMap;

use crate::core::types::{FoundCycle, ProtocolType};
use crate::tui::app::{Alert, AlertLevel, BotStatus, TradeRecord, TradeStatus};

/// Messages sent from the pipeline / orchestrator to the TUI task.
#[derive(Debug, Clone)]
pub enum UiUpdate {
    StatusChange(BotStatus),
    NewCycles(Vec<UiOpportunity>),
    MetricsUpdate(ScannerMetrics),
    GraphStats(GraphStatsSnapshot),
    TradeExecuted(TradeRecord),
    Alert(Alert),
    BlockUpdate {
        block: u64,
        lag_ms: u64,
    },
    GasUpdate {
        gwei: f64,
    },
    SimulationResult {
        fingerprint: u64,
        result: UiSimResult,
    },
    PnlTick(f64),
    ConfigSnapshot(ConfigSnapshot),
}

#[derive(Debug, Clone)]
pub struct UiOpportunity {
    pub cycle: FoundCycle,
    pub fingerprint: u64,
    pub route_summary: String,
    pub route_detail: String,
    pub protocols: Vec<String>,
    pub bf_score: f64,
    pub live_score: Option<f64>,
    pub est_profit_native: Option<String>,
    pub est_profit_usd: Option<f64>,
    pub source_hub: String,
    pub call_count: u32,
    pub freshness_ms: u64,
    pub is_long_tail: bool,
    pub liquidity_risk: f64,
}

#[derive(Debug, Clone, Default)]
pub struct ScannerMetrics {
    pub negative_cycles: usize,
    pub routes_executed: u64,
    pub win_rate_pct: f64,
    pub avg_hops: f64,
    pub avg_profit_usd: f64,
    pub last_search_ms: u64,
    pub cycles_pass_limited: usize,
    pub cycles_pass_full: usize,
    pub bf_sources_used: usize,
    pub call_count_total: u64,
    pub global_pnl_usd: f64,
}

#[derive(Debug, Clone, Default)]
pub struct GraphStatsSnapshot {
    pub pool_count: usize,
    pub edge_count: usize,
    pub token_count: u32,
    pub top_hubs: Vec<(String, usize)>,
    pub protocol_counts: HashMap<String, usize>,
    pub recent_discoveries: usize,
}

#[derive(Debug, Clone)]
pub struct UiSimResult {
    pub slippage_bps: u64,
    pub expected_out: String,
    pub bottleneck_hop: Option<u32>,
    pub profitable: bool,
}

#[derive(Debug, Clone)]
pub struct BalanceRow {
    pub symbol: String,
    pub address: String,
    pub balance: String,
    pub usd_value: f64,
    pub pct_total: f64,
    pub is_long_tail: bool,
}

/// Tunable scanner parameters mirrored from config (live view).
#[derive(Debug, Clone)]
pub struct ConfigSnapshot {
    pub max_hops: u32,
    pub max_cycles: u32,
    pub time_budget_ms: u64,
    pub dry_run: bool,
    pub cycle_finder: String,
    pub slippage_bps: u64,
    pub min_profit_matic_wei: String,
    pub disabled_protocols: Vec<String>,
}

impl Default for ConfigSnapshot {
    fn default() -> Self {
        Self {
            max_hops: 8,
            max_cycles: 10_000,
            time_budget_ms: 1_000,
            dry_run: true,
            cycle_finder: "bellman-ford".into(),
            slippage_bps: 50,
            min_profit_matic_wei: "0".into(),
            disabled_protocols: Vec::new(),
        }
    }
}

pub fn protocol_short_label(protocol: ProtocolType, label: Option<&str>) -> String {
    if let Some(raw) = label {
        let u = raw.to_ascii_uppercase();
        if u.contains("QUICK") {
            return "Quick".into();
        }
        if u.contains("SUSHI") {
            return "Sushi".into();
        }
        if u.contains("KYBER") || u.contains("ELASTIC") {
            return "Kyber".into();
        }
        if u.contains("RAMSES") {
            return "Ramses".into();
        }
        if u.contains("DFYN") {
            return "DFYN".into();
        }
        if u.contains("APESWAP") {
            return "ApeSwap".into();
        }
        if u.contains("MESH") {
            return "Mesh".into();
        }
        if u.contains("JET") {
            return "JetSwap".into();
        }
        if u.contains("COMETH") {
            return "Cometh".into();
        }
        if u.contains("WOOFI") {
            return "Woofi".into();
        }
        if u.contains("DODO") {
            return "Dodo".into();
        }
        if u.contains("BALANCER") {
            return "Balancer".into();
        }
        if u.contains("CURVE") {
            return "Curve".into();
        }
        if u.contains("V4") {
            return "Uni V4".into();
        }
        if u.contains("V3") {
            return "Uni V3".into();
        }
        if u.contains("V2") {
            return "Uni V2".into();
        }
        return raw.to_string();
    }
    match protocol {
        ProtocolType::UniswapV2 => "V2".into(),
        ProtocolType::UniswapV3 => "V3".into(),
        ProtocolType::UniswapV4 => "V4".into(),
        ProtocolType::BalancerV2 => "Balancer".into(),
        ProtocolType::CurveStable | ProtocolType::CurveCrypto => "Curve".into(),
        ProtocolType::Dodo => "Dodo".into(),
        ProtocolType::Woofi => "Woofi".into(),
    }
}

pub fn trade_from_outcome(
    fingerprint: u64,
    route_summary: String,
    hops: u32,
    protocols: Vec<String>,
    profit_native: String,
    profit_usd: f64,
    gas_used: u64,
    tx_hash: Option<String>,
    status: TradeStatus,
) -> TradeRecord {
    TradeRecord {
        fingerprint,
        route_summary,
        hops,
        protocols,
        profit_native,
        profit_usd,
        gas_used,
        tx_hash,
        status,
        timestamp_ms: crate::util::now_ms(),
    }
}

pub fn alert_info(msg: impl Into<String>) -> Alert {
    Alert {
        level: AlertLevel::Info,
        message: msg.into(),
        timestamp_ms: crate::util::now_ms(),
    }
}

pub fn alert_warn(msg: impl Into<String>) -> Alert {
    Alert {
        level: AlertLevel::Warn,
        message: msg.into(),
        timestamp_ms: crate::util::now_ms(),
    }
}

pub fn alert_error(msg: impl Into<String>) -> Alert {
    Alert {
        level: AlertLevel::Error,
        message: msg.into(),
        timestamp_ms: crate::util::now_ms(),
    }
}
