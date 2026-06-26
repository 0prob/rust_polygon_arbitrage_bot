use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Sparkline};

use crate::tui::app::App;
use crate::tui::layout::{kpi_row, overview_layout, split_horizontal};
use crate::tui::theme::Theme;

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    let chunks = overview_layout(area);
    render_kpis(f, chunks[0], app);
    let bottom = split_horizontal(chunks[1], 50);
    render_sparkline(f, bottom[0], app);
    render_alerts(f, bottom[1], app);
}

fn render_kpis(f: &mut Frame, area: Rect, app: &App) {
    let cards = kpi_row(area, 6);
    let m = &app.metrics;
    let _g = &app.graph_stats;

    let items: [(&str, String, ratatui::style::Style); 6] = [
        (
            "Neg Cycles",
            format!("{}", m.negative_cycles),
            Theme::profit(),
        ),
        (
            "Executed",
            format!("{}", m.routes_executed),
            Theme::accent(),
        ),
        (
            "Win Rate",
            format!("{:.1}%", m.win_rate_pct),
            Theme::profit(),
        ),
        ("Avg Hops", format!("{:.1}", m.avg_hops), Theme::muted()),
        (
            "Avg Profit",
            format!("${:.2}", m.avg_profit_usd),
            Theme::profit(),
        ),
        (
            "Search ms",
            format!("{}", m.last_search_ms),
            if m.last_search_ms < 1000 {
                Theme::profit()
            } else {
                Theme::warn()
            },
        ),
    ];

    for (i, card) in cards.iter().enumerate().take(6) {
        if let Some((title, value, style)) = items.get(i) {
            let block = Block::default()
                .borders(Borders::ALL)
                .border_style(Theme::block_border())
                .title(*title);
            let inner = block.inner(*card);
            f.render_widget(block, *card);
            f.render_widget(
                Paragraph::new(Line::from(Span::styled(value.clone(), *style))),
                inner,
            );
        }
    }
}

fn render_sparkline(f: &mut Frame, area: Rect, app: &App) {
    let data: Vec<u64> = if app.pnl_history.is_empty() {
        vec![0, 1, 2, 1, 3, 2, 4, 3, 5, 4]
    } else {
        app.pnl_history
            .iter()
            .map(|(v,)| ((v.abs() * 100.0) as u64).max(1))
            .collect()
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .title("P&L / cycle volume")
        .border_style(Theme::block_border());
    let spark = Sparkline::default()
        .block(block)
        .data(&data)
        .style(Theme::profit());
    f.render_widget(spark, area);
}

fn render_alerts(f: &mut Frame, area: Rect, app: &App) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title("Alerts")
        .border_style(Theme::block_border());
    let inner = block.inner(area);
    f.render_widget(block, area);

    let lines: Vec<Line> = app
        .alerts
        .iter()
        .rev()
        .take(8)
        .map(|a| {
            let style = match a.level {
                crate::tui::app::AlertLevel::Info => Theme::muted(),
                crate::tui::app::AlertLevel::Warn => Theme::warn(),
                crate::tui::app::AlertLevel::Error => Theme::loss(),
            };
            Line::from(Span::styled(format!("• {}", a.message), style))
        })
        .collect();

    let text = if lines.is_empty() {
        vec![Line::from(Span::styled("No alerts", Theme::muted()))]
    } else {
        lines
    };
    f.render_widget(Paragraph::new(text), inner);
}
