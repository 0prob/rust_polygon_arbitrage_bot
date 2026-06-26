use std::collections::HashMap;

use tokio::sync::mpsc;

use crate::config::AppConfig;
use crate::core::types::FoundCycle;
use crate::orchestrator::hf::HfTickResult;
use crate::pipeline::arena::StateArena;
use crate::pipeline::cycle_finder::prioritize_cycle_start_tokens_from_out_degrees;
use crate::pipeline::types::PoolMeta;
use crate::tui::app::{BotStatus, TradeStatus};
use crate::tui::route_viz::{cycle_to_ui_opportunity, token_label};
use crate::tui::update::{
    GraphStatsSnapshot, ScannerMetrics, UiOpportunity, UiUpdate, alert_info, trade_from_outcome,
};

/// Non-blocking channel from pipeline/orchestrator to the TUI.
#[derive(Clone)]
pub struct UiBridge {
    tx: mpsc::Sender<UiUpdate>,
}

impl UiBridge {
    pub fn new(tx: mpsc::Sender<UiUpdate>) -> Self {
        Self { tx }
    }

    pub fn sender(&self) -> mpsc::Sender<UiUpdate> {
        self.tx.clone()
    }

    pub fn try_send(&self, update: UiUpdate) {
        let _ = self.tx.try_send(update);
    }

    pub fn notify_status(&self, status: BotStatus) {
        self.try_send(UiUpdate::StatusChange(status));
    }

    pub fn notify_lf_complete(
        &self,
        arena: &StateArena,
        cycles: &[FoundCycle],
        pool_metas: &[PoolMeta],
        search_ms: u64,
        discoveries: usize,
    ) {
        let graph = build_graph_stats(arena, pool_metas, discoveries);
        let ui_cycles: Vec<UiOpportunity> = cycles
            .iter()
            .take(500)
            .map(|c| cycle_to_ui_opportunity(arena, c.clone(), pool_metas, None, 0))
            .collect();
        self.try_send(UiUpdate::GraphStats(graph));
        self.try_send(UiUpdate::NewCycles(ui_cycles));
        self.try_send(UiUpdate::MetricsUpdate(ScannerMetrics {
            negative_cycles: cycles.len(),
            last_search_ms: search_ms,
            ..Default::default()
        }));
        self.try_send(UiUpdate::StatusChange(BotStatus::Idle));
        self.try_send(UiUpdate::Alert(alert_info(format!(
            "LF tick: {} cycles, {} pools",
            cycles.len(),
            pool_metas.len()
        ))));
    }

    pub fn notify_hf_tick(&self, result: &HfTickResult, cycles_considered: usize) {
        self.try_send(UiUpdate::MetricsUpdate(ScannerMetrics {
            negative_cycles: cycles_considered,
            last_search_ms: result.elapsed_ms,
            ..Default::default()
        }));
        if result.profitable_count > 0 {
            self.try_send(UiUpdate::StatusChange(BotStatus::Executing));
        } else {
            self.try_send(UiUpdate::StatusChange(BotStatus::Scanning));
        }
    }

    pub fn notify_trade(
        &self,
        fingerprint: u64,
        route_summary: String,
        hops: u32,
        protocols: Vec<String>,
        profit_native: String,
        profit_usd: f64,
        gas: u64,
        tx_hash: Option<String>,
        status: TradeStatus,
    ) {
        self.try_send(UiUpdate::TradeExecuted(trade_from_outcome(
            fingerprint,
            route_summary,
            hops,
            protocols,
            profit_native,
            profit_usd,
            gas,
            tx_hash,
            status,
        )));
        self.try_send(UiUpdate::PnlTick(profit_usd));
    }

    pub fn notify_config(&self, config: &AppConfig) {
        use crate::tui::update::ConfigSnapshot;
        self.try_send(UiUpdate::ConfigSnapshot(ConfigSnapshot {
            max_hops: config.routing.max_hops,
            max_cycles: config.routing.enumeration_max_paths,
            time_budget_ms: 1_000,
            dry_run: config.is_dry_run(),
            cycle_finder: config.routing.cycle_finder.to_string(),
            slippage_bps: config.execution.slippage_bps,
            min_profit_matic_wei: config.execution.min_profit_matic_wei.clone(),
            disabled_protocols: Vec::new(),
        }));
    }
}

pub fn build_graph_stats(
    arena: &StateArena,
    pool_metas: &[PoolMeta],
    recent_discoveries: usize,
) -> GraphStatsSnapshot {
    let token_count = arena.token_count();
    let pool_count = arena.pool_count();
    let edge_count = pool_metas.len() * 2;

    let out_degrees: Vec<usize> = (0..token_count as usize)
        .map(|i| {
            pool_metas
                .iter()
                .filter(|p| p.token0.0 as usize == i || p.token1.0 as usize == i)
                .count()
        })
        .collect();

    let hubs: Vec<(String, usize)> =
        prioritize_cycle_start_tokens_from_out_degrees(out_degrees.iter().copied())
            .into_iter()
            .take(12)
            .filter_map(|t| {
                let deg = out_degrees.get(t.0 as usize).copied().unwrap_or(0);
                if deg == 0 {
                    return None;
                }
                Some((token_label(arena, t), deg))
            })
            .collect();

    let mut protocol_counts: HashMap<String, usize> = HashMap::new();
    for meta in pool_metas {
        let key = meta
            .protocol_label
            .clone()
            .unwrap_or_else(|| format!("{:?}", meta.protocol));
        *protocol_counts.entry(key).or_default() += 1;
    }

    GraphStatsSnapshot {
        pool_count,
        edge_count,
        token_count,
        top_hubs: hubs,
        protocol_counts,
        recent_discoveries,
    }
}
