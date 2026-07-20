use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Paragraph};

use crate::tui::app::App;
use crate::tui::layout;
use crate::tui::theme;

pub fn render(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let Some(snapshot) = app.snapshot.as_ref() else {
        frame.render_widget(
            Paragraph::new("waiting for diagnostics...").block(theme::panel_block("Diagnostics")),
            area,
        );
        return;
    };

    let [left_area, right_area] =
        Layout::horizontal([Constraint::Percentage(55), Constraint::Percentage(45)]).areas(area);

    let left_block = Block::bordered().title(Line::from(vec![
        Span::styled(" ", theme::muted()),
        Span::styled("Health", theme::title()),
    ]));
    let max_lines = layout::inner_lines(&left_block, left_area);
    let left: Vec<Line> = snapshot
        .diagnostics
        .iter()
        .take(max_lines)
        .map(|row| theme::label_value(row.key.clone(), row.value.clone(), row.severity))
        .collect();
    frame.render_widget(Paragraph::new(left).block(left_block), left_area);

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
            format!(
                "{}/{}",
                snapshot.overview.total_trades, snapshot.overview.total_losses
            ),
            crate::tui::app::Severity::Info,
        ),
        theme::label_value(
            "pnl",
            format!(
                "{:+.4} MATIC",
                snapshot.overview.daily_pnl_wei as f64 / 1e18
            ),
            if snapshot.overview.daily_pnl_wei >= 0 {
                crate::tui::app::Severity::Good
            } else {
                crate::tui::app::Severity::Warn
            },
        ),
    ];
    frame.render_widget(
        Paragraph::new(right).block(Block::bordered().title(Line::from(vec![
            Span::styled(" ", theme::muted()),
            Span::styled("Runtime", theme::title()),
        ]))),
        right_area,
    );
}
