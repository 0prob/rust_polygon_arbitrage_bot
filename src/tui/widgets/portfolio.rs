use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};

use crate::tui::app::App;

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
        .block(Block::default().borders(Borders::ALL).title("Exposure")),
        chunks[0],
    );

    let summary = vec![
        Line::from(format!("total routes {}", snapshot.overview.cycle_count)),
        Line::from(format!("pools {}", snapshot.overview.discovered_pools)),
        Line::from(format!(
            "win rate {:.2}%",
            snapshot.overview.win_rate * 100.0
        )),
    ];
    frame.render_widget(
        Paragraph::new(summary).block(Block::default().borders(Borders::ALL).title("Summary")),
        chunks[1],
    );
}
