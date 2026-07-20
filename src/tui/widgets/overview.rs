use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Paragraph, Sparkline};

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

    let [top_section, mid_section, bottom_section] = Layout::vertical([
        Constraint::Length(9),
        Constraint::Fill(1),
        Constraint::Length(10),
    ])
    .areas(area);

    let [m0, m1, m2, m3] = Layout::horizontal([
        Constraint::Percentage(22),
        Constraint::Percentage(22),
        Constraint::Percentage(28),
        Constraint::Percentage(28),
    ])
    .areas(top_section);

    let pnl_matic = snapshot.overview.daily_pnl_wei as f64 / 1e18;
    let profitable_share = if app.last_cycles_considered > 0 {
        (app.last_profitable_count as f64 / app.last_cycles_considered as f64) * 100.0
    } else {
        0.0
    };
    metric_card(
        frame,
        m0,
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
        m1,
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
        m2,
        "Freshness",
        format!("search {} ms", snapshot.overview.search_ms),
        format!(
            "HF {} ms | age {} ms",
            snapshot.overview.hf_ms,
            snapshot.captured_at.elapsed().as_millis()
        ),
        if snapshot.captured_at.elapsed().as_millis() > 2_500 {
            theme::warn()
        } else {
            theme::good()
        },
    );
    metric_card(
        frame,
        m3,
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

    let [activity_area, spark_area] =
        Layout::horizontal([Constraint::Percentage(58), Constraint::Percentage(42)])
            .areas(mid_section);
    recent_activity(frame, activity_area, app);
    spark_panel(frame, spark_area, app, snapshot);

    let [risk_area, history_area] =
        Layout::horizontal([Constraint::Percentage(42), Constraint::Percentage(58)])
            .areas(bottom_section);
    risk_panel(frame, risk_area, snapshot);
    history_panel(frame, history_area, app);
}

fn metric_card(
    frame: &mut Frame<'_>,
    area: Rect,
    title: &str,
    primary: String,
    secondary: String,
    accent: ratatui::style::Style,
) {
    let block = Block::bordered()
        .title(Line::from(vec![
            Span::styled(" ", theme::muted()),
            Span::styled(title, theme::title()),
        ]))
        .border_style(theme::muted())
        .style(ratatui::style::Style::default().bg(theme::PANEL));
    // Fixed 2-line KPI — plain vec; SmallVec buys nothing here.
    let text = vec![
        Line::from(Span::styled(primary, accent)),
        Line::from(Span::styled(secondary, theme::muted())),
    ];
    frame.render_widget(Paragraph::new(text).block(block), area);
}

fn recent_activity(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let block = Block::bordered().title(Line::from(vec![
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
    let [s0, s1, s2] = Layout::vertical([
        Constraint::Percentage(33),
        Constraint::Percentage(34),
        Constraint::Percentage(33),
    ])
    .areas(area);
    sparkline(
        frame,
        s0,
        "Search latency",
        &app.chart_search_ms,
        theme::warn(),
    );
    sparkline(
        frame,
        s1,
        "Cycles found",
        &app.chart_cycles,
        theme::accent(),
    );
    sparkline(
        frame,
        s2,
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
    // Chart series is already a VecDeque (cap 120); copying to Vec beats a 960-byte
    // inline SmallVec that mostly just mirrors heap storage.
    let values: Vec<u64> = data.iter().copied().collect();
    frame.render_widget(
        Sparkline::default()
            .block(
                Block::bordered()
                    .style(ratatui::style::Style::default().bg(theme::PANEL))
                    .title(Line::from(vec![
                        Span::styled(" ", theme::muted()),
                        Span::styled(title, theme::title()),
                    ])),
            )
            .data(&values)
            .style(style)
            .max(max),
        area,
    );
}

fn risk_panel(frame: &mut Frame<'_>, area: Rect, snapshot: &crate::tui::app::DashboardSnapshot) {
    let lines = vec![
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
        Line::from(format!("rates age {} ms", snapshot.overview.rates_age_ms)),
        Line::from(format!(
            "stale indexer {}",
            snapshot.graph.health.stale_indexer
        )),
    ];
    frame.render_widget(
        Paragraph::new(lines).block(theme::panel_block("Health")),
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
