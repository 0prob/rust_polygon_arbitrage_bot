use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};

use crate::tui::app::App;
use crate::tui::layout::split_horizontal;
use crate::tui::theme::Theme;

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    let chunks = split_horizontal(area, 50);
    render_metrics(f, chunks[0], app);
    render_logs(f, chunks[1], app);
}

fn render_metrics(f: &mut Frame, area: Rect, app: &App) {
    let m = &app.metrics;
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Scanner Diagnostics ")
        .border_style(Theme::block_border());
    let inner = block.inner(area);
    f.render_widget(block, area);

    let lines = vec![
        Line::from(format!(
            "Last search: {} ms (target <1000)",
            m.last_search_ms
        )),
        Line::from(format!("BF sources used: {}", m.bf_sources_used)),
        Line::from(format!(
            "Pass limited: {}  Pass full: {}",
            m.cycles_pass_limited, m.cycles_pass_full
        )),
        Line::from(format!("Total route calls: {}", m.call_count_total)),
        Line::from(format!("Negative cycles: {}", m.negative_cycles)),
        Line::from(format!("Routes executed: {}", m.routes_executed)),
        Line::from(format!("Win rate: {:.1}%", m.win_rate_pct)),
        Line::from(""),
        Line::from(Span::styled(
            "Graph build / Envio stats appear after LF tick.",
            Theme::muted(),
        )),
    ];
    f.render_widget(Paragraph::new(lines), inner);
}

fn render_logs(f: &mut Frame, area: Rect, app: &App) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Recent Events ")
        .border_style(Theme::block_border());
    let inner = block.inner(area);
    f.render_widget(block, area);

    let lines: Vec<Line> = app
        .alerts
        .iter()
        .rev()
        .take(20)
        .map(|a| Line::from(Span::raw(format!("[{}] {}", a.timestamp_ms, a.message))))
        .collect();

    let text = if lines.is_empty() {
        vec![Line::from(Span::styled(
            "Waiting for pipeline events…",
            Theme::muted(),
        ))]
    } else {
        lines
    };
    f.render_widget(Paragraph::new(text), inner);
}
