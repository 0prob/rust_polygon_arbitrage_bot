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
            Paragraph::new("waiting for graph snapshot...").block(theme::panel_block("Graph")),
            area,
        );
        return;
    };

    let [health, protocols, hubs, recent] = Layout::horizontal([
        Constraint::Percentage(25),
        Constraint::Percentage(25),
        Constraint::Percentage(25),
        Constraint::Percentage(25),
    ])
    .areas(area);

    render_panel(
        frame,
        health,
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
        protocols,
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
        hubs,
        "Hubs",
        snapshot
            .graph
            .hubs
            .iter()
            .map(|row| {
                theme::label_value(
                    row.token.clone(),
                    format!("{} hits", row.out_degree),
                    crate::tui::app::Severity::Info,
                )
            })
            .collect(),
    );

    render_panel(
        frame,
        recent,
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
    let block = Block::bordered()
        .style(ratatui::style::Style::default().bg(theme::PANEL))
        .title(Line::from(vec![
            Span::styled(" ", theme::muted()),
            Span::styled(title, theme::title()),
        ]));
    let max_lines = layout::inner_lines(&block, area);
    let visible: Vec<Line> = lines.into_iter().take(max_lines).collect();
    frame.render_widget(Paragraph::new(visible).block(block), area);
}
