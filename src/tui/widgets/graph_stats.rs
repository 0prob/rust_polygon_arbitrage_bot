use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};

use crate::tui::app::App;
use crate::tui::layout;
use crate::tui::theme;

pub fn render(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let Some(snapshot) = app.snapshot.as_ref() else {
        frame.render_widget(
            Paragraph::new("waiting for graph snapshot...").block(theme::panel_block("Graph")),
            area,
        );
        return;
    };

    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(36),
            Constraint::Percentage(32),
            Constraint::Percentage(32),
        ])
        .split(area);

    render_panel(
        frame,
        chunks[0],
        "Health",
        vec![
            theme::label_value(
                "generation",
                snapshot.graph.health.graph_generation.to_string(),
                crate::tui::app::Severity::Info,
            ),
            theme::label_value(
                "tokens",
                snapshot.graph.health.token_count.to_string(),
                crate::tui::app::Severity::Info,
            ),
            theme::label_value(
                "pools",
                snapshot.graph.health.pool_count.to_string(),
                crate::tui::app::Severity::Info,
            ),
            theme::label_value(
                "top degree",
                snapshot.graph.health.top_out_degree.to_string(),
                crate::tui::app::Severity::Info,
            ),
            theme::label_value(
                "stale indexer",
                snapshot.graph.health.stale_indexer.to_string(),
                if snapshot.graph.health.stale_indexer {
                    crate::tui::app::Severity::Warn
                } else {
                    crate::tui::app::Severity::Good
                },
            ),
        ],
    );

    render_panel(
        frame,
        chunks[1],
        "Protocols",
        snapshot
            .graph
            .protocol_counts
            .iter()
            .map(|row| theme::label_value(row.key.clone(), row.value.clone(), row.severity))
            .collect(),
    );

    render_panel(
        frame,
        chunks[2],
        "Recent",
        snapshot
            .graph
            .recent_discoveries
            .iter()
            .map(|row| theme::label_value(row.key.clone(), row.value.clone(), row.severity))
            .collect(),
    );
}

fn render_panel(frame: &mut Frame<'_>, area: Rect, title: &str, lines: Vec<Line>) {
    let block = Block::default()
        .borders(Borders::ALL)
        .style(ratatui::style::Style::default().bg(theme::PANEL))
        .title(Line::from(vec![
            Span::styled(" ", theme::muted()),
            Span::styled(title, theme::title()),
        ]));
    let max_lines = layout::inner_lines(&block, area);
    let visible: Vec<Line> = lines.into_iter().take(max_lines).collect();
    frame.render_widget(Paragraph::new(visible).block(block), area);
}
