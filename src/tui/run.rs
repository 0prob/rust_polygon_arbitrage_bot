use std::io;
use std::time::Duration;

use anyhow::Context;
use crossterm::event::{self, Event, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use tokio::sync::mpsc;
use tokio::time;

use crate::tui::app::{App, BotStatus};
use crate::tui::bridge::UiBridge;
use crate::tui::events::handle_key;
use crate::tui::update::UiUpdate;
use crate::tui::widgets;

pub async fn run_tui(
    mut app: App,
    mut ui_rx: mpsc::Receiver<UiUpdate>,
) -> anyhow::Result<()> {
    enable_raw_mode().context("enable raw mode")?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen).context("enter alternate screen")?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend).context("create terminal")?;
    terminal.clear().context("clear terminal")?;

    let (key_tx, mut key_rx) = mpsc::channel::<crossterm::event::KeyEvent>(64);
    std::thread::spawn(move || {
        loop {
            if event::poll(Duration::from_millis(100)).unwrap_or(false) {
                if let Ok(Event::Key(key)) = event::read() {
                    if key.kind == KeyEventKind::Press {
                        let _ = key_tx.blocking_send(key);
                    }
                }
            }
        }
    });

    let tick_rate = Duration::from_millis(250);
    let mut tick = time::interval(tick_rate);

    let result = loop {
        tokio::select! {
            _ = tick.tick() => {
                terminal
                    .draw(|f| widgets::render_main(f, &app))
                    .context("draw")?;
            }
            Some(key) = key_rx.recv() => {
                handle_key(&mut app, key);
                if app.should_quit {
                    break Ok(());
                }
                terminal
                    .draw(|f| widgets::render_main(f, &app))
                    .context("draw")?;
            }
            msg = ui_rx.recv() => {
                match msg {
                    Some(update) => {
                        apply_update(&mut app, update);
                        while let Ok(next) = ui_rx.try_recv() {
                            apply_update(&mut app, next);
                        }
                    }
                    None => break Ok(()),
                }
            }
        }
    };

    disable_raw_mode().ok();
    execute!(terminal.backend_mut(), LeaveAlternateScreen).ok();
    terminal.show_cursor().ok();

    result
}

fn apply_update(app: &mut App, update: UiUpdate) {
    use crate::tui::update::UiUpdate;

    match update {
        UiUpdate::StatusChange(s) => app.status = s,
        UiUpdate::NewCycles(cycles) => {
            app.opportunities = cycles;
            app.clamp_selection();
            app.simulations = app
                .opportunities
                .iter()
                .take(40)
                .map(|o| crate::tui::app::UiSimulation {
                    fingerprint: o.fingerprint,
                    route_summary: o.route_summary.clone(),
                    bf_score: o.bf_score,
                    live_score: o.live_score.unwrap_or(o.bf_score),
                    result: None,
                })
                .collect();
        }
        UiUpdate::MetricsUpdate(m) => {
            if app.metrics.routes_executed == 0 && m.routes_executed > 0 {
                app.metrics.routes_executed = m.routes_executed;
            }
            if m.global_pnl_usd != 0.0 || app.metrics.global_pnl_usd == 0.0 {
                app.metrics.global_pnl_usd = m.global_pnl_usd;
            }
            app.metrics.negative_cycles = m.negative_cycles;
            app.metrics.last_search_ms = m.last_search_ms;
            app.metrics.win_rate_pct = if m.win_rate_pct > 0.0 {
                m.win_rate_pct
            } else {
                app.metrics.win_rate_pct
            };
            app.metrics.avg_hops = if m.avg_hops > 0.0 {
                m.avg_hops
            } else {
                app.metrics.avg_hops
            };
            app.metrics.avg_profit_usd = if m.avg_profit_usd > 0.0 {
                m.avg_profit_usd
            } else {
                app.metrics.avg_profit_usd
            };
            app.metrics.cycles_pass_limited = m.cycles_pass_limited;
            app.metrics.cycles_pass_full = m.cycles_pass_full;
            app.metrics.bf_sources_used = m.bf_sources_used;
            app.metrics.call_count_total = m.call_count_total;
            if m.routes_executed > 0 {
                app.metrics.routes_executed = m.routes_executed;
            }
        }
        UiUpdate::GraphStats(g) => app.graph_stats = g,
        UiUpdate::TradeExecuted(t) => {
            app.metrics.routes_executed += 1;
            app.trades.push(t);
            if app.trades.len() > 500 {
                app.trades.drain(0..100);
            }
        }
        UiUpdate::Alert(a) => app.push_alert(a),
        UiUpdate::BlockUpdate { block, lag_ms } => {
            app.block = block;
            app.block_lag_ms = lag_ms;
        }
        UiUpdate::GasUpdate { gwei } => app.gas_gwei = gwei,
        UiUpdate::SimulationResult { fingerprint, result } => {
            if let Some(sim) = app
                .simulations
                .iter_mut()
                .find(|s| s.fingerprint == fingerprint)
            {
                sim.result = Some(result);
            }
        }
        UiUpdate::PnlTick(delta) => {
            app.metrics.global_pnl_usd += delta;
            app.pnl_history.push((delta,));
            if app.pnl_history.len() > 120 {
                app.pnl_history.remove(0);
            }
        }
        UiUpdate::ConfigSnapshot(c) => app.config_view = c,
    }
}

pub fn spawn_snapshot_poller(
    bridge: UiBridge,
    snapshots: std::sync::Arc<crate::services::hf_snapshot::SnapshotStore>,
    interval_ms: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_gen = 0u64;
        let mut interval = time::interval(Duration::from_millis(interval_ms));
        loop {
            interval.tick().await;
            let snap = snapshots.read();
            if snap.generation != last_gen && !snap.cycles.is_empty() {
                last_gen = snap.generation;
                let ui_cycles: Vec<_> = snap
                    .cycles
                    .iter()
                    .take(500)
                    .map(|c| {
                        crate::tui::route_viz::cycle_to_ui_opportunity(
                            &snap.arena,
                            c.clone(),
                            &snap.pool_metas,
                            None,
                            0,
                        )
                    })
                    .collect();
                bridge.try_send(UiUpdate::NewCycles(ui_cycles));
                bridge.try_send(UiUpdate::GraphStats(
                    crate::tui::bridge::build_graph_stats(
                        &snap.arena,
                        &snap.pool_metas,
                        snap.discovered_pools.len(),
                    ),
                ));
                bridge.try_send(UiUpdate::StatusChange(BotStatus::Idle));
            }
        }
    })
}
