use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};

use crate::tui::app::App;
use crate::tui::theme;

pub fn render(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let Some(snapshot) = app.snapshot.as_ref() else {
        frame.render_widget(
            Paragraph::new("waiting for diagnostics...")
                .block(Block::default().borders(Borders::ALL).title("Diagnostics")),
            area,
        );
        return;
    };

    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(area);

    let left = snapshot
        .diagnostics
        .iter()
        .map(|row| theme::label_value(row.key.clone(), row.value.clone(), row.severity))
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(left).block(Block::default().borders(Borders::ALL).title(Line::from(vec![
            Span::styled(" ", theme::muted()),
            Span::styled("Health", theme::title()),
        ]))),
        chunks[0],
    );

    let right = vec![
        theme::label_value(
            "search ms",
            snapshot.overview.search_ms.to_string(),
            crate::tui::app::Severity::Info,
        ),
        theme::label_value(
            "hf ms",
            snapshot.overview.hf_ms.to_string(),
            crate::tui::app::Severity::Info,
        ),
        theme::label_value(
            "cycles",
            snapshot.overview.cycle_count.to_string(),
            crate::tui::app::Severity::Info,
        ),
        theme::label_value(
            "trades/losses",
            format!("{}/{}", snapshot.overview.total_trades, snapshot.overview.total_losses),
            crate::tui::app::Severity::Info,
        ),
        theme::label_value(
            "pnl",
            format!("{:+.4} MATIC", snapshot.overview.daily_pnl_wei as f64 / 1e18),
            if snapshot.overview.daily_pnl_wei >= 0 {
                crate::tui::app::Severity::Good
            } else {
                crate::tui::app::Severity::Warn
            },
        ),
    ];
    frame.render_widget(
        Paragraph::new(right).block(Block::default().borders(Borders::ALL).title(Line::from(vec![
            Span::styled(" ", theme::muted()),
            Span::styled("Runtime", theme::title()),
        ]))),
        chunks[1],
    );
}
