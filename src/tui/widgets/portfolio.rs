use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};

use crate::tui::app::App;
use crate::tui::theme;

pub fn render(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let Some(snapshot) = app.snapshot.as_ref() else {
        frame.render_widget(
            Paragraph::new("waiting for portfolio data...")
                .block(Block::default().borders(Borders::ALL).title("Portfolio")),
            area,
        );
        return;
    };

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(12), Constraint::Length(8)])
        .split(area);

    let rows = snapshot
        .portfolio
        .iter()
        .map(|row| {
            Row::new(vec![
                Cell::from(row.label.clone()),
                Cell::from(row.address.clone()),
                Cell::from(row.balance.clone()),
                Cell::from(row.usd.clone()),
                Cell::from(row.source.clone()),
            ])
        })
        .collect::<Vec<_>>();

    frame.render_widget(
        Table::new(
            rows,
            [
                Constraint::Length(12),
                Constraint::Percentage(38),
                Constraint::Length(16),
                Constraint::Length(16),
                Constraint::Percentage(24),
            ],
        )
        .header(Row::new(vec![
            Cell::from("asset"),
            Cell::from("address"),
            Cell::from("balance"),
            Cell::from("USD"),
            Cell::from("source"),
        ]))
        .block(Block::default().borders(Borders::ALL).title(Line::from(vec![
            Span::styled(" ", theme::muted()),
            Span::styled("Exposure", theme::title()),
        ]))),
        chunks[0],
    );

    let summary = vec![
        theme::label_value(
            "total routes",
            snapshot.overview.cycle_count.to_string(),
            crate::tui::app::Severity::Info,
        ),
        theme::label_value(
            "pools",
            snapshot.overview.discovered_pools.to_string(),
            crate::tui::app::Severity::Info,
        ),
        theme::label_value(
            "win rate",
            format!("{:.2}%", snapshot.overview.win_rate * 100.0),
            crate::tui::app::Severity::Info,
        ),
    ];
    frame.render_widget(
        Paragraph::new(summary).block(Block::default().borders(Borders::ALL).title(Line::from(vec![
            Span::styled(" ", theme::muted()),
            Span::styled("Summary", theme::title()),
        ]))),
        chunks[1],
    );
}
