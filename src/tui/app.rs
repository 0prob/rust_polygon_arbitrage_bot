use std::time::Instant;

use crate::tui::update::{
    BalanceRow, ConfigSnapshot, GraphStatsSnapshot, ScannerMetrics, UiOpportunity, UiSimResult,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tab {
    Overview,
    Opportunities,
    Graph,
    Simulations,
    Trades,
    Portfolio,
    Diagnostics,
    Config,
}

impl Tab {
    pub const ALL: [Tab; 8] = [
        Tab::Overview,
        Tab::Opportunities,
        Tab::Graph,
        Tab::Simulations,
        Tab::Trades,
        Tab::Portfolio,
        Tab::Diagnostics,
        Tab::Config,
    ];

    pub fn title(self) -> &'static str {
        match self {
            Tab::Overview => "Overview",
            Tab::Opportunities => "Opportunities",
            Tab::Graph => "Graph",
            Tab::Simulations => "Simulations",
            Tab::Trades => "Trades",
            Tab::Portfolio => "Portfolio",
            Tab::Diagnostics => "Diagnostics",
            Tab::Config => "Config",
        }
    }

    pub fn index(self) -> usize {
        Self::ALL.iter().position(|t| *t == self).unwrap_or(0)
    }

    pub fn from_index(i: usize) -> Self {
        Self::ALL.get(i).copied().unwrap_or(Tab::Overview)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BotStatus {
    Starting,
    Scanning,
    Executing,
    Idle,
    Error,
    Mock,
}

impl BotStatus {
    pub fn label(self) -> &'static str {
        match self {
            BotStatus::Starting => "STARTING",
            BotStatus::Scanning => "SCANNING",
            BotStatus::Executing => "EXECUTING",
            BotStatus::Idle => "IDLE",
            BotStatus::Error => "ERROR",
            BotStatus::Mock => "MOCK",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertLevel {
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone)]
pub struct Alert {
    pub level: AlertLevel,
    pub message: String,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeStatus {
    Confirmed,
    Reverted,
    DryRun,
    Skipped,
    Failed,
}

impl TradeStatus {
    pub fn label(self) -> &'static str {
        match self {
            TradeStatus::Confirmed => "OK",
            TradeStatus::Reverted => "REVERT",
            TradeStatus::DryRun => "DRY",
            TradeStatus::Skipped => "SKIP",
            TradeStatus::Failed => "FAIL",
        }
    }
}

#[derive(Debug, Clone)]
pub struct TradeRecord {
    pub fingerprint: u64,
    pub route_summary: String,
    pub hops: u32,
    pub protocols: Vec<String>,
    pub profit_native: String,
    pub profit_usd: f64,
    pub gas_used: u64,
    pub tx_hash: Option<String>,
    pub status: TradeStatus,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, Default)]
pub struct FilterState {
    pub text: String,
    pub editing: bool,
    pub min_hops: u32,
    pub max_hops: u32,
    pub min_score: f64,
    pub long_tail_only: bool,
    pub hop_stratified: bool,
    pub protocol_filter: Vec<String>,
}

impl FilterState {
    pub fn new_default() -> Self {
        Self {
            min_hops: 2,
            max_hops: 5,
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone)]
pub struct UiSimulation {
    pub fingerprint: u64,
    pub route_summary: String,
    pub bf_score: f64,
    pub live_score: f64,
    pub result: Option<UiSimResult>,
}

pub struct App {
    pub tab: Tab,
    pub show_help: bool,
    pub should_quit: bool,
    pub started_at: Instant,
    pub status: BotStatus,
    pub block: u64,
    pub block_lag_ms: u64,
    pub gas_gwei: f64,
    pub opportunities: Vec<UiOpportunity>,
    pub opp_selected: usize,
    pub opp_scroll: usize,
    pub filter: FilterState,
    pub trades: Vec<TradeRecord>,
    pub metrics: ScannerMetrics,
    pub graph_stats: GraphStatsSnapshot,
    pub portfolio: Vec<BalanceRow>,
    pub alerts: Vec<Alert>,
    pub pnl_history: Vec<(f64,)>,
    pub config_view: ConfigSnapshot,
    pub simulations: Vec<UiSimulation>,
    pub show_detail: bool,
}

impl App {
    pub fn new(mock: bool) -> Self {
        Self {
            tab: Tab::Overview,
            show_help: false,
            should_quit: false,
            started_at: Instant::now(),
            status: if mock {
                BotStatus::Mock
            } else {
                BotStatus::Starting
            },
            block: 0,
            block_lag_ms: 0,
            gas_gwei: 30.0,
            opportunities: Vec::new(),
            opp_selected: 0,
            opp_scroll: 0,
            filter: FilterState::new_default(),
            trades: Vec::new(),
            metrics: ScannerMetrics::default(),
            graph_stats: GraphStatsSnapshot::default(),
            portfolio: Vec::new(),
            alerts: Vec::new(),
            pnl_history: Vec::new(),
            config_view: ConfigSnapshot::default(),
            simulations: Vec::new(),
            show_detail: false,
        }
    }

    pub fn uptime_secs(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    pub fn filtered_opportunities(&self) -> Vec<&UiOpportunity> {
        self.opportunities
            .iter()
            .filter(|o| self.passes_filter(o))
            .collect()
    }

    fn passes_filter(&self, o: &UiOpportunity) -> bool {
        let hops = o.cycle.hop_count;
        if hops < self.filter.min_hops || hops > self.filter.max_hops {
            return false;
        }
        if self.filter.min_score != 0.0 && o.bf_score > self.filter.min_score {
            return false;
        }
        if self.filter.long_tail_only && !o.is_long_tail {
            return false;
        }
        if !self.filter.text.is_empty() {
            let q = self.filter.text.to_ascii_lowercase();
            if !o.route_summary.to_ascii_lowercase().contains(&q)
                && !format!("{:016x}", o.fingerprint).contains(&q)
            {
                return false;
            }
        }
        if !self.filter.protocol_filter.is_empty() {
            let hit = o
                .protocols
                .iter()
                .any(|p| self.filter.protocol_filter.iter().any(|f| p.contains(f)));
            if !hit {
                return false;
            }
        }
        true
    }

    pub fn clamp_selection(&mut self) {
        let n = self.filtered_opportunities().len();
        if n == 0 {
            self.opp_selected = 0;
            self.opp_scroll = 0;
            return;
        }
        if self.opp_selected >= n {
            self.opp_selected = n - 1;
        }
        if self.opp_selected < self.opp_scroll {
            self.opp_scroll = self.opp_selected;
        }
        if self.opp_selected >= self.opp_scroll.saturating_add(12) {
            self.opp_scroll = self.opp_selected.saturating_sub(11);
        }
    }

    pub fn selected_opportunity(&self) -> Option<&UiOpportunity> {
        self.filtered_opportunities()
            .get(self.opp_selected)
            .copied()
    }

    pub fn push_alert(&mut self, alert: Alert) {
        self.alerts.push(alert);
        if self.alerts.len() > 50 {
            let drain = self.alerts.len() - 50;
            self.alerts.drain(0..drain);
        }
    }
}
