use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy::primitives::{Address, U256};
use rustc_hash::FxHasher;

use crate::services::execution::service::ExecutionOutcome;

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
    Help,
}

impl Tab {
    pub const ALL: [Tab; 9] = [
        Tab::Overview,
        Tab::Opportunities,
        Tab::Graph,
        Tab::Simulations,
        Tab::Trades,
        Tab::Portfolio,
        Tab::Diagnostics,
        Tab::Config,
        Tab::Help,
    ];

    #[must_use]
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
            Tab::Help => "Help",
        }
    }

    #[must_use]
    pub const fn index(self) -> usize {
        match self {
            Tab::Overview => 0,
            Tab::Opportunities => 1,
            Tab::Graph => 2,
            Tab::Simulations => 3,
            Tab::Trades => 4,
            Tab::Portfolio => 5,
            Tab::Diagnostics => 6,
            Tab::Config => 7,
            Tab::Help => 8,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputMode {
    Normal,
    Search,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Info,
    Warn,
    Error,
    Good,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteStatus {
    New,
    Hot,
    Executed,
    Quarantined,
    Ignored,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortMode {
    Score,
    Profit,
    Risk,
    Hops,
    Freshness,
}

#[derive(Debug, Clone)]
pub struct KeyValueRow {
    pub key: String,
    pub value: String,
    pub severity: Severity,
}

#[derive(Debug, Clone)]
pub struct ActivityItem {
    pub at: Instant,
    pub severity: Severity,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct OverviewSnapshot {
    pub uptime: Duration,
    pub total_trades: u64,
    pub total_losses: u64,
    pub daily_pnl_wei: i128,
    pub profitable_routes: usize,
    pub discovered_pools: usize,
    pub routable_pools: usize,
    pub cycle_count: usize,
    pub search_ms: u64,
    pub hf_ms: u64,
    pub gas_gwei: Option<f64>,
    pub win_rate: f64,
    pub snapshot_age_ms: u64,
    pub rates_age_ms: u64,
}

#[derive(Debug, Clone)]
pub struct GraphHealth {
    pub graph_generation: u64,
    pub token_count: usize,
    pub pool_count: usize,
    pub top_out_degree: usize,
    pub protocol_count: usize,
    pub indexer_lag_blocks: u64,
    pub hypersync_height: Option<u64>,
    pub stale_indexer: bool,
}

#[derive(Debug, Clone)]
pub struct GraphHubRow {
    pub token: String,
    pub out_degree: usize,
}

#[derive(Debug, Clone)]
pub struct GraphSnapshot {
    pub health: GraphHealth,
    pub protocol_counts: Vec<KeyValueRow>,
    pub hubs: Vec<GraphHubRow>,
    pub recent_discoveries: Vec<KeyValueRow>,
}

#[derive(Debug, Clone)]
pub struct RouteSummary {
    pub fingerprint: u64,
    pub route: String,
    pub route_detail: String,
    /// Lowercased route/protocol/detail/fingerprint for fast search filtering.
    pub search_blob: String,
    pub protocols: String,
    pub tokens: Vec<Address>,
    pub hops: usize,
    pub raw_score: f64,
    pub rescored: f64,
    pub amount_in_token: String,
    pub amount_in_matic: f64,
    pub amount_out_token: String,
    pub profit_matic: f64,
    pub net_profit_matic: f64,
    pub profit_usd: f64,
    pub gas_estimate: u32,
    pub risk_score: u8,
    pub liquidity_score: u8,
    pub long_tail: bool,
    pub status: RouteStatus,
}

#[derive(Debug, Clone)]
pub struct SimulationRow {
    pub fingerprint: u64,
    pub route: String,
    pub amount_in: String,
    pub amount_out: String,
    pub gross_profit: String,
    pub net_profit: String,
    pub gas: u32,
    pub note: String,
}

#[derive(Debug, Clone)]
pub struct TradeRow {
    pub at: Instant,
    pub fingerprint: u64,
    pub outcome: String,
    pub tx_hash: Option<String>,
    pub gas_used: Option<u64>,
    pub profit_wei: Option<U256>,
    pub severity: Severity,
}

#[derive(Debug, Clone)]
pub struct PortfolioRow {
    pub label: String,
    pub address: String,
    pub balance: String,
    pub usd: String,
    pub source: String,
    pub severity: Severity,
}

#[derive(Debug, Clone)]
pub struct DashboardSnapshot {
    pub generation: u64,
    pub captured_at: Instant,
    pub overview: OverviewSnapshot,
    pub graph: GraphSnapshot,
    pub opportunities: Arc<Vec<RouteSummary>>,
    pub simulations: Arc<Vec<SimulationRow>>,
    pub portfolio: Vec<PortfolioRow>,
    pub diagnostics: Vec<KeyValueRow>,
    pub config: Vec<KeyValueRow>,
}

#[derive(Debug)]
pub struct App {
    pub tab: Tab,
    pub input_mode: InputMode,
    pub search: String,
    pub sort_mode: SortMode,
    pub selected_index: usize,
    pub scroll: usize,
    pub snapshot: Option<Arc<DashboardSnapshot>>,
    pub activity: VecDeque<ActivityItem>,
    pub trade_history: VecDeque<TradeRow>,
    pub chart_cycles: VecDeque<u64>,
    pub chart_profitable: VecDeque<u64>,
    pub chart_search_ms: VecDeque<u64>,
    pub chart_gas_gwei: VecDeque<u64>,
    /// Total routes produced by the latest LF search.
    pub last_cycle_count: usize,
    pub last_search_ms: u64,
    pub last_hf_ms: u64,
    pub last_cycles_considered: usize,
    pub last_profitable_count: usize,
    pub last_best_profit_wei: Option<String>,
    pub should_quit: bool,
    pub snapshot_refresh_pending: bool,
    pub show_help: bool,
    pub last_input_at: Option<Instant>,
    search_lower: String,
    route_view_key: u64,
    route_view_indices: Vec<usize>,
    route_sort_key: Option<(u64, SortMode)>,
    route_sort_indices: Vec<usize>,
    route_view_dirty: bool,
}

impl Default for App {
    fn default() -> Self {
        Self::new()
    }
}

impl App {
    #[must_use]
    pub fn new() -> Self {
        Self {
            tab: Tab::Overview,
            input_mode: InputMode::Normal,
            search: String::new(),
            sort_mode: SortMode::Score,
            selected_index: 0,
            scroll: 0,
            snapshot: None,
            activity: VecDeque::with_capacity(128),
            trade_history: VecDeque::with_capacity(128),
            chart_cycles: VecDeque::with_capacity(120),
            chart_profitable: VecDeque::with_capacity(120),
            chart_search_ms: VecDeque::with_capacity(120),
            chart_gas_gwei: VecDeque::with_capacity(120),

            last_cycle_count: 0,
            last_search_ms: 0,
            last_hf_ms: 0,
            last_cycles_considered: 0,
            last_profitable_count: 0,
            last_best_profit_wei: None,
            should_quit: false,
            snapshot_refresh_pending: false,
            show_help: false,
            last_input_at: None,
            search_lower: String::new(),
            route_view_key: 0,
            route_view_indices: Vec::new(),
            route_sort_key: None,
            route_sort_indices: Vec::new(),
            route_view_dirty: true,
        }
    }

    #[must_use]
    pub fn active_rows(&self) -> usize {
        self.current_rows_len()
    }

    #[must_use]
    pub fn selected_route(&self) -> Option<&RouteSummary> {
        let snapshot = self.snapshot.as_ref()?;
        if self.route_view_dirty {
            return None;
        }
        self.route_view_indices
            .get(self.selected_index)
            .and_then(|&idx| snapshot.opportunities.get(idx))
    }

    #[must_use]
    pub fn selected_trade(&self) -> Option<&TradeRow> {
        let len = self.trade_history.len();
        if len == 0 {
            return None;
        }
        let idx = len - 1 - self.selected_index.min(len - 1);
        self.trade_history.get(idx)
    }

    #[must_use]
    pub fn route_view(&self) -> Option<(&DashboardSnapshot, &[usize])> {
        let snapshot = self.snapshot.as_ref()?;
        if self.route_view_dirty {
            return None;
        }
        Some((snapshot.as_ref(), self.route_view_indices.as_slice()))
    }

    pub fn push_activity(&mut self, severity: Severity, message: impl Into<String>) {
        if self.activity.len() >= 240 {
            self.activity.pop_front();
        }
        self.activity.push_back(ActivityItem {
            at: Instant::now(),
            severity,
            message: message.into(),
        });
    }

    pub fn push_trade(&mut self, row: TradeRow) {
        if self.trade_history.len() >= 240 {
            self.trade_history.pop_front();
        }
        self.trade_history.push_back(row);
    }

    pub fn set_snapshot(&mut self, mut snapshot: Arc<DashboardSnapshot>) {
        // LF/HF timings arrive on the event stream, while the slower runtime
        // poller builds snapshots independently.  Do not let placeholder
        // values from that poller erase measurements already observed by the
        // UI.
        {
            let snapshot = Arc::make_mut(&mut snapshot);
            snapshot.overview.search_ms = self.last_search_ms;
            snapshot.overview.hf_ms = self.last_hf_ms;
            snapshot.overview.profitable_routes = self.last_profitable_count;
            snapshot.overview.snapshot_age_ms = snapshot.captured_at.elapsed().as_millis() as u64;
            self.last_cycle_count = snapshot.overview.cycle_count;
        }
        self.snapshot = Some(snapshot);
        self.route_view_dirty = true;
        self.rebuild_route_view();
        let row_count = self.current_rows_len();
        if row_count == 0 {
            self.selected_index = 0;
            self.scroll = 0;
        } else {
            self.selected_index = self.selected_index.min(row_count.saturating_sub(1));
            self.scroll = self.scroll.min(self.selected_index);
        }
    }

    pub fn rebuild_route_view(&mut self) {
        let key = self.route_view_cache_key();
        if self.route_view_dirty {
            self.route_view_key = 0;
        }
        if key == self.route_view_key {
            self.route_view_dirty = false;
            self.normalize_route_selection();
            return;
        }
        self.route_view_key = key;
        self.route_view_indices.clear();

        let Some(snapshot) = self.snapshot.as_ref() else {
            return;
        };
        let rows = snapshot.opportunities.as_ref();
        let sort_key = (snapshot.generation, self.sort_mode);
        if self.route_sort_key != Some(sort_key) {
            self.route_sort_indices.clear();
            self.route_sort_indices.extend(0..rows.len());
            match self.sort_mode {
                SortMode::Score => self
                    .route_sort_indices
                    .sort_by(|a, b| rows[*b].rescored.total_cmp(&rows[*a].rescored)),
                SortMode::Profit => self
                    .route_sort_indices
                    .sort_by(|a, b| rows[*b].profit_matic.total_cmp(&rows[*a].profit_matic)),
                SortMode::Risk => self
                    .route_sort_indices
                    .sort_by_key(|idx| rows[*idx].risk_score),
                SortMode::Hops => self.route_sort_indices.sort_by_key(|idx| rows[*idx].hops),
                SortMode::Freshness => self
                    .route_sort_indices
                    .sort_by_key(|idx| std::cmp::Reverse(rows[*idx].fingerprint)),
            }
            self.route_sort_key = Some(sort_key);
        }
        let needle = self.search_lower.as_str();
        self.route_view_indices.extend(
            self.route_sort_indices
                .iter()
                .copied()
                .filter(|&idx| needle.is_empty() || rows[idx].search_blob.contains(needle)),
        );
        self.route_view_dirty = false;
        self.normalize_route_selection();
    }

    fn route_view_cache_key(&self) -> u64 {
        let mut hasher = FxHasher::default();
        self.search_lower.hash(&mut hasher);
        (self.sort_mode as u8).hash(&mut hasher);
        if let Some(snapshot) = &self.snapshot {
            snapshot.generation.hash(&mut hasher);
        }
        hasher.finish()
    }

    pub fn mark_route_view_dirty(&mut self) {
        self.route_view_dirty = true;
    }

    #[must_use]
    pub fn route_view_is_dirty(&self) -> bool {
        self.route_view_dirty
    }

    pub fn select_next(&mut self) {
        let len = self.current_rows_len().max(1);
        self.selected_index = (self.selected_index + 1).min(len.saturating_sub(1));
        self.scroll = self.scroll.min(self.selected_index);
    }

    pub fn select_prev(&mut self) {
        self.selected_index = self.selected_index.saturating_sub(1);
        self.scroll = self.scroll.min(self.selected_index);
    }

    pub fn select_top(&mut self) {
        self.selected_index = 0;
        self.scroll = 0;
    }

    pub fn select_bottom(&mut self) {
        let len = self.current_rows_len();
        self.selected_index = len.saturating_sub(1);
        self.scroll = self.selected_index.saturating_sub(10);
    }

    fn normalize_route_selection(&mut self) {
        if self.tab != Tab::Opportunities {
            return;
        }
        let len = self.route_view_indices.len();
        if len == 0 {
            self.selected_index = 0;
            self.scroll = 0;
            return;
        }
        self.selected_index = self.selected_index.min(len - 1);
        self.scroll = self.scroll.min(self.selected_index);
    }

    pub fn cycle_tab(&mut self, step: isize) {
        let len = Tab::ALL.len() as isize;
        let mut idx = self.tab.index() as isize;
        idx = (idx + step).rem_euclid(len);
        self.tab = Tab::ALL[idx as usize];
        self.select_top();
    }

    pub fn toggle_help(&mut self) {
        self.show_help = !self.show_help;
    }

    pub fn clear_search(&mut self) {
        self.search.clear();
        self.search_lower.clear();
        self.input_mode = InputMode::Normal;
        self.route_view_dirty = true;
        self.rebuild_route_view();
        self.select_top();
    }

    pub(crate) fn sync_search_lower(&mut self) {
        self.search_lower = self.search.trim().to_ascii_lowercase();
    }

    #[must_use]
    pub fn current_rows_len(&self) -> usize {
        if self.route_view_dirty && matches!(self.tab, Tab::Opportunities) {
            return 0;
        }
        match self.tab {
            Tab::Trades => self.trade_history.len(),
            Tab::Opportunities => self.route_view_indices.len(),
            Tab::Simulations => self
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.simulations.len())
                .unwrap_or(0),
            Tab::Portfolio => self
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.portfolio.len())
                .unwrap_or(0),
            Tab::Overview | Tab::Graph | Tab::Diagnostics | Tab::Config | Tab::Help => 0,
        }
    }

    #[must_use]
    pub fn selected_row_index(&self) -> Option<usize> {
        let len = self.current_rows_len();
        if len == 0 {
            None
        } else {
            Some(self.selected_index.min(len - 1))
        }
    }

    pub fn register_trade_outcome(&mut self, outcome: ExecutionOutcome, route_fingerprint: u64) {
        let (severity, outcome_label, gas_used, tx_hash, profit_wei) = match outcome {
            ExecutionOutcome::DryRunPassed { gas_used } => (
                Severity::Info,
                "dry-run passed".to_string(),
                Some(gas_used),
                None,
                None,
            ),
            ExecutionOutcome::DryRunFailed { reason } => (
                Severity::Error,
                format!("dry-run failed: {reason}"),
                None,
                None,
                None,
            ),
            ExecutionOutcome::SkippedCircuitBreaker => (
                Severity::Warn,
                "skipped circuit breaker".to_string(),
                None,
                None,
                None,
            ),
            ExecutionOutcome::SkippedQuarantined => (
                Severity::Warn,
                "skipped quarantined".to_string(),
                None,
                None,
                None,
            ),
            ExecutionOutcome::SkippedCooldown => (
                Severity::Warn,
                "skipped cooldown".to_string(),
                None,
                None,
                None,
            ),
            ExecutionOutcome::SkippedNoWallet => (
                Severity::Warn,
                "skipped no wallet".to_string(),
                None,
                None,
                None,
            ),
            ExecutionOutcome::SkippedNoPrivateRpc => (
                Severity::Warn,
                "skipped no private rpc".to_string(),
                None,
                None,
                None,
            ),
            ExecutionOutcome::SkippedUnprofitablePreDryRun => (
                Severity::Info,
                "skipped below learned floor".to_string(),
                None,
                None,
                None,
            ),
            ExecutionOutcome::SkippedUnprofitableAfterDryRun => (
                Severity::Info,
                "skipped after dry-run".to_string(),
                None,
                None,
                None,
            ),
            ExecutionOutcome::SkippedShutdown => (
                Severity::Info,
                "skipped shutdown".to_string(),
                None,
                None,
                None,
            ),
            ExecutionOutcome::Confirmed {
                tx_hash,
                gas_used,
                profit_wei,
            } => (
                Severity::Good,
                "confirmed".to_string(),
                Some(gas_used),
                Some(tx_hash),
                Some(profit_wei),
            ),
            ExecutionOutcome::Reverted { tx_hash, gas_used } => (
                Severity::Error,
                "reverted".to_string(),
                Some(gas_used),
                Some(tx_hash),
                None,
            ),
            ExecutionOutcome::ReceiptTimeout { tx_hash } => (
                Severity::Warn,
                "receipt timeout".to_string(),
                None,
                Some(tx_hash),
                None,
            ),
            ExecutionOutcome::SubmitFailed { reason } => (
                Severity::Error,
                format!("submit failed: {reason}"),
                None,
                None,
                None,
            ),
        };

        self.push_trade(TradeRow {
            at: Instant::now(),
            fingerprint: route_fingerprint,
            outcome: outcome_label.clone(),
            tx_hash,
            gas_used,
            profit_wei,
            severity,
        });
        // ponytail: outcome_label only; fingerprint visible in Trade History panel
        self.push_activity(severity, outcome_label);
    }

    pub fn apply_lf_sample(&mut self, cycles: usize, search_ms: u64, _discoveries: usize) {
        self.last_cycle_count = cycles;
        self.last_search_ms = search_ms;
        if let Some(snapshot) = self.snapshot.as_mut() {
            let snapshot = Arc::make_mut(snapshot);
            snapshot.overview.cycle_count = cycles;
            snapshot.overview.search_ms = search_ms;
        }
        push_series(&mut self.chart_cycles, cycles as u64, 120);
        push_series(&mut self.chart_search_ms, search_ms, 120);
        // LF metrics on Freshness card + sparklines; activity would be redundant
    }

    pub fn apply_hf_sample(
        &mut self,
        cycles_considered: usize,
        profitable_count: usize,
        best_profit_wei: &str,
        elapsed_ms: u64,
    ) {
        self.last_cycles_considered = cycles_considered;
        self.last_profitable_count = profitable_count;
        self.last_hf_ms = elapsed_ms;
        self.last_best_profit_wei = Some(best_profit_wei.to_string());
        if let Some(snapshot) = self.snapshot.as_mut() {
            let snapshot = Arc::make_mut(snapshot);
            snapshot.overview.profitable_routes = profitable_count;
            snapshot.overview.hf_ms = elapsed_ms;
        }
        push_series(&mut self.chart_profitable, profitable_count as u64, 120);
        // ponytail: HF metrics on Yielding card + sparkline; activity would be redundant
    }

    pub fn apply_gas_sample(&mut self, gwei: f64) {
        if let Some(snapshot) = self.snapshot.as_mut() {
            Arc::make_mut(snapshot).overview.gas_gwei = Some(gwei);
        }
        push_series(&mut self.chart_gas_gwei, gwei.max(0.0).round() as u64, 120);
    }
}

fn push_series(series: &mut VecDeque<u64>, value: u64, cap: usize) {
    if series.len() >= cap {
        series.pop_front();
    }
    series.push_back(value);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_view_respects_search_filter() {
        let mut app = App::new();
        app.set_snapshot(Arc::new(DashboardSnapshot {
            generation: 1,
            captured_at: Instant::now(),
            overview: OverviewSnapshot {
                uptime: Duration::ZERO,
                total_trades: 0,
                total_losses: 0,
                daily_pnl_wei: 0,
                profitable_routes: 0,
                discovered_pools: 0,
                routable_pools: 0,
                cycle_count: 0,
                search_ms: 0,
                hf_ms: 0,
                gas_gwei: None,
                win_rate: 0.0,
                snapshot_age_ms: 0,
                rates_age_ms: 0,
            },
            graph: GraphSnapshot {
                health: GraphHealth {
                    graph_generation: 1,
                    token_count: 0,
                    pool_count: 0,
                    top_out_degree: 0,
                    protocol_count: 0,
                    indexer_lag_blocks: 0,
                    hypersync_height: None,
                    stale_indexer: false,
                },
                protocol_counts: Vec::new(),
                hubs: Vec::new(),
                recent_discoveries: Vec::new(),
            },
            opportunities: Arc::new(vec![
                RouteSummary {
                    fingerprint: 1,
                    route: "WMATIC route".to_string(),
                    route_detail: String::new(),
                    search_blob: "wmatic route".to_string(),
                    protocols: String::new(),
                    tokens: Vec::new(),
                    hops: 2,
                    raw_score: 1.0,
                    rescored: 1.0,
                    amount_in_token: String::new(),
                    amount_in_matic: 0.0,
                    amount_out_token: String::new(),
                    profit_matic: 0.0,
                    net_profit_matic: 0.0,
                    profit_usd: 0.0,
                    gas_estimate: 0,
                    risk_score: 0,
                    liquidity_score: 0,
                    long_tail: false,
                    status: RouteStatus::New,
                },
                RouteSummary {
                    fingerprint: 2,
                    route: "USDC route".to_string(),
                    route_detail: String::new(),
                    search_blob: "usdc route".to_string(),
                    protocols: String::new(),
                    tokens: Vec::new(),
                    hops: 2,
                    raw_score: 2.0,
                    rescored: 2.0,
                    amount_in_token: String::new(),
                    amount_in_matic: 0.0,
                    amount_out_token: String::new(),
                    profit_matic: 0.0,
                    net_profit_matic: 0.0,
                    profit_usd: 0.0,
                    gas_estimate: 0,
                    risk_score: 0,
                    liquidity_score: 0,
                    long_tail: false,
                    status: RouteStatus::New,
                },
            ]),
            simulations: Arc::new(Vec::new()),
            portfolio: Vec::new(),
            diagnostics: Vec::new(),
            config: Vec::new(),
        }));

        assert_eq!(app.route_view().expect("view").1.len(), 2);
        app.search = "wmatic".to_string();
        app.sync_search_lower();
        app.rebuild_route_view();
        let (_, indices) = app.route_view().expect("view");
        assert_eq!(indices.len(), 1);
        assert_eq!(indices[0], 0);
        assert_eq!(app.route_sort_indices, vec![1, 0]);
    }

    #[test]
    fn non_navigable_tabs_report_zero_rows() {
        let app = App::new();
        assert_eq!(app.current_rows_len(), 0);
    }

    #[test]
    fn snapshot_does_not_erase_live_pipeline_metrics() {
        let mut app = App::new();
        app.last_search_ms = 37;
        app.last_hf_ms = 11;
        app.last_profitable_count = 4;

        let snapshot = DashboardSnapshot {
            generation: 1,
            captured_at: Instant::now(),
            overview: OverviewSnapshot {
                uptime: Duration::ZERO,
                total_trades: 0,
                total_losses: 0,
                daily_pnl_wei: 0,
                profitable_routes: 0,
                discovered_pools: 0,
                routable_pools: 0,
                cycle_count: 0,
                search_ms: 0,
                hf_ms: 0,
                gas_gwei: None,
                win_rate: 0.0,
                snapshot_age_ms: 0,
                rates_age_ms: 0,
            },
            graph: GraphSnapshot {
                health: GraphHealth {
                    graph_generation: 0,
                    token_count: 0,
                    pool_count: 0,
                    top_out_degree: 0,
                    protocol_count: 0,
                    indexer_lag_blocks: 0,
                    hypersync_height: None,
                    stale_indexer: false,
                },
                protocol_counts: Vec::new(),
                hubs: Vec::new(),
                recent_discoveries: Vec::new(),
            },
            opportunities: Arc::new(Vec::new()),
            simulations: Arc::new(Vec::new()),
            portfolio: Vec::new(),
            diagnostics: Vec::new(),
            config: Vec::new(),
        };
        app.set_snapshot(Arc::new(snapshot));

        let overview = &app
            .snapshot
            .as_ref()
            .expect("snapshot should be installed")
            .overview;
        assert_eq!(overview.search_ms, 37);
        assert_eq!(overview.hf_ms, 11);
        assert_eq!(overview.profitable_routes, 4);
    }

    #[test]
    fn pipeline_events_update_only_their_own_series() {
        let mut app = App::new();

        app.apply_lf_sample(17, 23, 42);
        assert_eq!(app.last_cycle_count, 17);
        assert_eq!(app.last_search_ms, 23);
        assert_eq!(app.chart_cycles.iter().copied().collect::<Vec<_>>(), [17]);
        assert_eq!(
            app.chart_search_ms.iter().copied().collect::<Vec<_>>(),
            [23]
        );
        assert!(app.chart_profitable.is_empty());

        app.apply_hf_sample(9, 3, "100", 7);
        assert_eq!(app.last_cycle_count, 17);
        assert_eq!(app.last_cycles_considered, 9);
        assert_eq!(app.last_search_ms, 23);
        assert_eq!(app.last_hf_ms, 7);
        assert_eq!(app.chart_cycles.iter().copied().collect::<Vec<_>>(), [17]);
        assert_eq!(
            app.chart_search_ms.iter().copied().collect::<Vec<_>>(),
            [23]
        );
        assert_eq!(
            app.chart_profitable.iter().copied().collect::<Vec<_>>(),
            [3]
        );

        app.apply_gas_sample(31.6);
        assert_eq!(app.chart_gas_gwei.iter().copied().collect::<Vec<_>>(), [32]);
        assert_eq!(app.chart_cycles.iter().copied().collect::<Vec<_>>(), [17]);
        assert_eq!(
            app.chart_search_ms.iter().copied().collect::<Vec<_>>(),
            [23]
        );
    }
}
