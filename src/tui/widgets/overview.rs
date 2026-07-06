use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Sparkline};
use smallvec::SmallVec;

use crate::tui::app::App;
use crate::tui::theme;
pub fn render(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let Some(snapshot) = app.snapshot.as_ref() else {
        frame.render_widget(
            Paragraph::new("waiting for snapshot...")
                .block(Block::default().borders(Borders::ALL).title("Overview")),
            area,
        );
        return;
    };

    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(9),
            Constraint::Length(10),
            Constraint::Min(10),
        ])
        .split(area);

    let top = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
        ])
        .split(sections[0]);

    let pnl_matic = snapshot.overview.daily_pnl_wei as f64 / 1e18;
    metric_card(
        frame,
        top[0],
        "P&L",
        format!("{pnl_matic:.4} MATIC"),
        format!("{:.2}% win rate", snapshot.overview.win_rate * 100.0),
        if pnl_matic >= 0.0 {
            theme::good()
        } else {
            theme::bad()
        },
    );
    metric_card(
        frame,
        top[1],
        "Cycles",
        snapshot.overview.cycle_count.to_string(),
        format!(
            "{} profitable | {} HF considered",
            app.last_profitable_count, app.last_cycles_considered
        ),
        theme::accent(),
    );
    metric_card(
        frame,
        top[2],
        "Latency",
        format!("{} ms", snapshot.overview.search_ms),
        format!("HF {} ms", snapshot.overview.hf_ms),
        theme::warn(),
    );
    metric_card(
        frame,
        top[3],
        "Graph",
        format!(
            "{} pools / gen {}",
            snapshot.graph.health.pool_count, snapshot.graph.health.graph_generation
        ),
        if snapshot.graph.health.stale_indexer {
            "indexer stale".to_string()
        } else {
            "healthy".to_string()
        },
        if snapshot.graph.health.stale_indexer {
            theme::warn()
        } else {
            theme::good()
        },
    );

    let lower = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(64), Constraint::Percentage(36)])
        .split(sections[1]);
    recent_activity(frame, lower[0], app);
    spark_panel(frame, lower[1], app, snapshot);

    let bottom = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(sections[2]);
    risk_panel(frame, bottom[0], snapshot);
    history_panel(frame, bottom[1], app);
}

fn metric_card(
    frame: &mut Frame<'_>,
    area: Rect,
    title: &str,
    primary: String,
    secondary: String,
    accent: ratatui::style::Style,
) {
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(theme::muted());
    let text: SmallVec<[Line; 2]> = SmallVec::from_buf([
        Line::from(Span::styled(primary, accent)),
        Line::from(Span::styled(secondary, theme::muted())),
    ]);
    frame.render_widget(Paragraph::new(text.as_slice()).block(block), area);
}

fn recent_activity(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let items: SmallVec<[Line; 8]> = app
        .activity
        .iter()
        .rev()
        .take(8)
        .map(|item| {
            Line::from(vec![
                Span::styled(format!("{:>5?}", item.at.elapsed()), theme::muted()),
                Span::raw(" "),
                Span::styled(item.message.clone(), theme::severity_style(item.severity)),
            ])
        })
        .collect();
    frame.render_widget(
        Paragraph::new(items.as_slice())
            .block(Block::default().borders(Borders::ALL).title("Activity")),
        area,
    );
}

fn spark_panel(
    frame: &mut Frame<'_>,
    area: Rect,
    app: &App,
    snapshot: &crate::tui::app::DashboardSnapshot,
) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(34),
            Constraint::Percentage(33),
            Constraint::Percentage(33),
        ])
        .split(area);
    sparkline(frame, chunks[0], "Search ms", &app.chart_search_ms);
    sparkline(frame, chunks[1], "Cycles", &app.chart_cycles);
    sparkline(frame, chunks[2], "Profitable", &app.chart_profitable);
    let _ = snapshot;
}

fn sparkline(
    frame: &mut Frame<'_>,
    area: Rect,
    title: &str,
    data: &std::collections::VecDeque<u64>,
) {
    let max = data.iter().copied().max().unwrap_or(1);
    let values: SmallVec<[u64; 120]> = data.iter().copied().collect();
    frame.render_widget(
        Sparkline::default()
            .block(Block::default().borders(Borders::ALL).title(title))
            .data(values.as_slice())
            .style(Style::default().fg(theme::ACCENT))
            .max(max),
        area,
    );
}

fn risk_panel(frame: &mut Frame<'_>, area: Rect, snapshot: &crate::tui::app::DashboardSnapshot) {
    let lines: SmallVec<[Line; 4]> = SmallVec::from_buf([
        Line::from(format!(
            "indexer lag: {} blocks",
            snapshot.graph.health.indexer_lag_blocks
        )),
        Line::from(format!(
            "protocols: {}",
            snapshot.graph.health.protocol_count
        )),
        Line::from(format!("hubs: {}", snapshot.graph.health.top_out_degree)),
        Line::from(format!(
            "snapshot age: {} ms",
            snapshot.captured_at.elapsed().as_millis()
        )),
    ]);
    frame.render_widget(
        Paragraph::new(lines.as_slice())
            .block(Block::default().borders(Borders::ALL).title("Health")),
        area,
    );
}

fn history_panel(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let lines: SmallVec<[Line; 6]> = app
        .trade_history
        .iter()
        .rev()
        .take(6)
        .map(|row| {
            Line::from(vec![
                Span::styled(format!("{:x}", row.fingerprint), theme::muted()),
                Span::raw(" "),
                Span::styled(row.outcome.clone(), theme::severity_style(row.severity)),
            ])
        })
        .collect();
    frame.render_widget(
        Paragraph::new(lines.as_slice()).block(
            Block::default()
                .borders(Borders::ALL)
                .title("Trade history"),
        ),
        area,
    );
}
