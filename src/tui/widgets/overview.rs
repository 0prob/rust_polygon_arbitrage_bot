use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Sparkline};
use smallvec::SmallVec;

use crate::tui::app::App;
use crate::tui::layout;
use crate::tui::theme;
pub fn render(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let Some(snapshot) = app.snapshot.as_ref() else {
        frame.render_widget(
            Paragraph::new("waiting for snapshot...").block(theme::panel_block("Overview")),
            area,
        );
        return;
    };

    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(9),
            Constraint::Min(11),
            Constraint::Length(10),
        ])
        .split(area);

    let top = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(22),
            Constraint::Percentage(22),
            Constraint::Percentage(28),
            Constraint::Percentage(28),
        ])
        .split(sections[0]);

    let pnl_matic = snapshot.overview.daily_pnl_wei as f64 / 1e18;
    let profitable_share = if snapshot.overview.cycle_count > 0 {
        (app.last_profitable_count as f64 / snapshot.overview.cycle_count as f64) * 100.0
    } else {
        0.0
    };
    metric_card(
        frame,
        top[0],
        "Net P&L",
        format!("{pnl_matic:+.4} MATIC"),
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
        "Yielding",
        format!(
            "{}/{}",
            app.last_profitable_count, app.last_cycles_considered
        ),
        format!("{profitable_share:.1}% of candidates"),
        theme::accent(),
    );
    metric_card(
        frame,
        top[2],
        "Freshness",
        format!("search {} ms", snapshot.overview.search_ms),
        format!(
            "HF {} ms | age {} ms",
            snapshot.overview.hf_ms, snapshot.overview.snapshot_age_ms
        ),
        if snapshot.overview.snapshot_age_ms > 2_500 {
            theme::warn()
        } else {
            theme::good()
        },
    );
    metric_card(
        frame,
        top[3],
        "Graph Health",
        format!(
            "{} pools | {} tokens",
            snapshot.graph.health.pool_count, snapshot.graph.health.token_count
        ),
        if snapshot.graph.health.stale_indexer {
            format!("lag {} blocks", snapshot.graph.health.indexer_lag_blocks)
        } else {
            format!("gen {} | live", snapshot.graph.health.graph_generation)
        },
        if snapshot.graph.health.stale_indexer {
            theme::warn()
        } else {
            theme::good()
        },
    );

    let lower = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
        .split(sections[1]);
    recent_activity(frame, lower[0], app);
    spark_panel(frame, lower[1], app, snapshot);

    let bottom = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(42), Constraint::Percentage(58)])
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
        .title(Line::from(vec![
            Span::styled(" ", theme::muted()),
            Span::styled(title, theme::title()),
        ]))
        .borders(Borders::ALL)
        .border_style(theme::muted())
        .style(ratatui::style::Style::default().bg(theme::PANEL));
    let text: SmallVec<[Line; 2]> = SmallVec::from_buf([
        Line::from(Span::styled(primary, accent)),
        Line::from(Span::styled(secondary, theme::muted())),
    ]);
    frame.render_widget(Paragraph::new(text.as_slice()).block(block), area);
}

fn recent_activity(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(Line::from(vec![
            Span::styled(" ", theme::muted()),
            Span::styled("Activity", theme::title()),
        ]));
    let max_lines = layout::inner_lines(&block, area);
    let items: Vec<Line> = app
        .activity
        .iter()
        .rev()
        .take(max_lines)
        .map(|item| {
            Line::from(vec![
                Span::styled(format!("{:>5?}", item.at.elapsed()), theme::muted()),
                Span::raw(" "),
                Span::styled(item.message.clone(), theme::severity_style(item.severity)),
            ])
        })
        .collect();
    frame.render_widget(Paragraph::new(items).block(block), area);
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
            Constraint::Percentage(33),
            Constraint::Percentage(34),
            Constraint::Percentage(33),
        ])
        .split(area);
    sparkline(
        frame,
        chunks[0],
        "Search latency",
        &app.chart_search_ms,
        theme::warn(),
    );
    sparkline(
        frame,
        chunks[1],
        "Cycles found",
        &app.chart_cycles,
        theme::accent(),
    );
    sparkline(
        frame,
        chunks[2],
        "Profitable routes",
        &app.chart_profitable,
        theme::good(),
    );
    let _ = snapshot;
}

fn sparkline(
    frame: &mut Frame<'_>,
    area: Rect,
    title: &str,
    data: &std::collections::VecDeque<u64>,
    style: ratatui::style::Style,
) {
    let max = data.iter().copied().max().unwrap_or(1);
    let values: SmallVec<[u64; 120]> = data.iter().copied().collect();
    frame.render_widget(
        Sparkline::default()
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .style(ratatui::style::Style::default().bg(theme::PANEL))
                    .title(Line::from(vec![
                        Span::styled(" ", theme::muted()),
                        Span::styled(title, theme::title()),
                    ])),
            )
            .data(values.as_slice())
            .style(style)
            .max(max),
        area,
    );
}

fn risk_panel(frame: &mut Frame<'_>, area: Rect, snapshot: &crate::tui::app::DashboardSnapshot) {
    let lines: SmallVec<[Line; 5]> = SmallVec::from_buf([
        Line::from(format!(
            "indexer lag {} blocks",
            snapshot.graph.health.indexer_lag_blocks
        )),
        theme::label_value(
            "protocols",
            snapshot.graph.health.protocol_count.to_string(),
            crate::tui::app::Severity::Info,
        ),
        theme::label_value(
            "hubs",
            snapshot.graph.health.top_out_degree.to_string(),
            crate::tui::app::Severity::Info,
        ),
        Line::from(format!(
            "snapshot age {} ms",
            snapshot.captured_at.elapsed().as_millis()
        )),
        Line::from(format!(
            "stale indexer {}",
            snapshot.graph.health.stale_indexer
        )),
    ]);
    frame.render_widget(
        Paragraph::new(lines.as_slice()).block(theme::panel_block("Health")),
        area,
    );
}

fn history_panel(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let block = theme::panel_block("Trade history");
    let max_lines = layout::inner_lines(&block, area);
    let lines: Vec<Line> = app
        .trade_history
        .iter()
        .rev()
        .take(max_lines)
        .map(|row| {
            Line::from(vec![
                Span::styled(format!("{:x}", row.fingerprint), theme::muted()),
                Span::raw(" "),
                Span::styled(row.outcome.clone(), theme::severity_style(row.severity)),
            ])
        })
        .collect();
    frame.render_widget(Paragraph::new(lines).block(block), area);
}
